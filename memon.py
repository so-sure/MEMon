#!/usr/bin/python
import argparse
import boto
import json
import time
from boto.sqs.message import RawMessage
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.fields import RangeKey
import pprint

class Notification:
    Unknown, Down, Up, Late, ConfigError = range(0, 5)

class MEMon(object):

    # Default constructor of the class.
    def __init__(self):
        self.queue = "memon"
        self.table = "memon"
        self.sns = "memon"
        self.sqs = boto.connect_sqs()
        self.sns_conn = boto.connect_sns()
        self.args = None
        self.pp = pprint.PrettyPrinter()
        self.now = int(time.time())
    
    def aws_init(self):
        print 'Creating sqs queue %s' % (self.args.queue)
        self.sqs.create_queue(self.args.queue)

        print 'Creating dynamodb table %s' % (self.args.table)
        try:
            Table.create(self.args.table, schema=[HashKey('Name')], throughput={ 'read': 1, 'write': 1})
        except boto.exception.JSONResponseError as e:
            print e
        
        print 'Creating sns topic %s' % (self.args.sns)
        self.sns_conn.create_topic(self.args.sns)
        
        if self.args.sns_email:
            print 'Subscribing %s to the sns topic %s (click confirmation link in email)' % (self.args.sns_email, self.args.sns)
            self.sns_conn.subscribe(self.get_topic_arn(), 'email', self.args.sns_email)
        else:
            print 'Remember to subscribe to the sns topic %s' % (self.args.sns)

    def send(self, name):
        q = self.sqs.get_queue(self.args.queue)
        m = RawMessage()
        msg = {
            'name': name,
            'time': self.now,
        }
        if self.args.debug:
            self.pp.pprint(msg)
        m.set_body(json.dumps(msg))
        q.write(m)

    def poll(self):
        q = self.sqs.get_queue(self.args.queue)
        results = q.get_messages(10)
        for result in results:
            msg = json.loads(result.get_body())
            if self.args.debug:
                print msg
            self.record(msg['name'], msg['time'])
            q.delete_message(result)

        self.notify_down_events()

    def show(self, name = None):
        table = Table(self.args.table)
        results = table.scan()
        for event in results:
            if name is None or name == event['Name']:
                print "\n%s\n---" % (event['Name'])
                self.pp.pprint(dict(event))
        print "\n"

    def notify_down_events(self):
        table = Table(self.args.table)
        results = table.scan()
        for event in results:
            # we only want to notify based on the period, so we're not notifying every minute
            if 'expected' in event and event['expected'] + event['error_count'] * event['period'] <= self.now and event['enabled']:
                if self.args.debug:
                    print "%s\n---" % (event['Name'])
                    self.pp.pprint(dict(event))
                if event['error_count'] < int(self.args.max_notify_count):
                    self.notify(event['Name'], Notification.Down)
                elif self.args.debug:
                    print "Exceeded notify count for %s" % (event['Name'])
                event['error_count'] = event['error_count'] + 1
                event.partial_save()            

    def get_topic_arn(self):
        # todo: handle pagination of topics
        topics = self.sns_conn.get_all_topics()['ListTopicsResponse']['ListTopicsResult']['Topics']
        topicArn = None
        for topic in topics:
            if topic['TopicArn'].endswith(':' + self.args.sns):
                # todo: cache arn
                return topic['TopicArn']

        raise Exception('Unable to locate topic arn for %s' % (self.args.sns))

    def notify(self, name, notification):
        topicArn = self.get_topic_arn()

        message = None
        if notification == Notification.Down:
            message = 'Down: %s' % (name)
        elif notification == Notification.Up:
            message = 'Up: %s' % (name)
        elif notification == Notification.Late:
            message = 'Late: %s' % (name)
        elif notification == Notification.ConfigError:
            message = 'Config: %s has a configuration error' % (name)
        else:
            raise Exception('Invalid notification type')

        if self.args.debug:
            print message

        subject = "[MEMon] %s" % (message)
        message = "MEMon Alert\n-----------\n\n%s\n\n--\nMEMon" % (message)
        self.sns_conn.publish(topicArn, message, subject)

    def record(self, name, event_time):
        table = Table(self.args.table)
        if self.args.prefer_server_time:
            event_time = self.now
        else:
            event_time = int(event_time)

        try:
            event = table.get_item(True, Name=name)

            if not 'period' in event:
                return self.notify(name, Notification.ConfigError)

            # The possiblity exists that we're processing an older sqs message - if so, we can just ignore it
            if time < event['time']:
                return

            if event['expected'] < event_time and event['enabled']:
                if event['error_count'] == 0:
                    self.notify(name, Notification.Late)
                else:
                    self.notify(name, Notification.Up)

            event['time'] = event_time
            event['expected'] = event_time + int(event['period'])
            event['error_count'] = int(0)
            event.partial_save()
        except boto.dynamodb2.exceptions.ItemNotFound as e:
            self.notify(name, Notification.ConfigError)
            
    def config(self, name, period):
        table = Table(self.args.table)
        try:
            event = table.get_item(True, Name=name)
            event['period'] = period
            event['enabled'] = self.args.enabled
            if self.args.description:
                event['description'] = self.args.description
            event.partial_save()
        except boto.dynamodb2.exceptions.ItemNotFound as e:
            # error_count should only be set when adding new items
            data = {
                'Name': name,
                'period': period,
                'enabled': self.args.enabled,
                'error_count': 0
            }
            if self.args.description:
                data['description'] = self.args.description

            table.put_item(data)
            
        self.show(name)

    def main(self):

        parser = argparse.ArgumentParser(description='Missing Event Monitor')
        parser.add_argument('action', choices=['init', 'send', 'poll', 'config', 'show'], help='Action to perform')
        parser.add_argument('name', nargs='?', help='Event name (required for send and config)')
        parser.add_argument('--queue', default=self.queue, nargs='?', help='MEMon SQS Name (default: %(default)s)')
        parser.add_argument('--table', default=self.table, nargs='?', help='MEMon DynamoDb Table Name (default: %(default)s)')
        parser.add_argument('--sns', default=self.sns, nargs='?', help='MEMon SNS Topic Name (default: %(default)s)')
        parser.add_argument('--sns-email', help='Init only - Subscribe email to sns notifications')
        parser.add_argument('--prefer-server-time', default=False, help='If you do not trust the client times, you can use the server process time')
        parser.add_argument('--debug', default=False, help='Print debug statements')
        parser.add_argument('--max-notify-count', type=int, default=3, help='Max # of sns notify events per name')
        parser.add_argument('--poll-count', type=int, default=3, help='Number of times to poll in period')

        parser.add_argument('--period', type=int, default=None, help='Config only - how long should the notification period be? (seconds)')
        parser.add_argument('--description', default=None, help='Config only - optional description for event')
        parser.add_argument('--enabled', dest='enabled', action='store_true', help='Config only - enable/disable event')
        parser.add_argument('--disabled', dest='enabled', action='store_false', help='Config only - enable/disable event')
        parser.set_defaults(enabled=True)

        self.args = parser.parse_args()

        if self.args.action == 'init':
            self.aws_init()
        elif self.args.action == 'send':                
            if not self.args.name:
                raise Exception('Missing event name')

            self.send(self.args.name)
        elif self.args.action == 'poll':
            for i in xrange(self.args.poll_count):
                self.poll()
        elif self.args.action == 'config':
            if not self.args.name:
                raise Exception('Missing event name')

            self.config(self.args.name, self.args.period)
        elif self.args.action == 'show':
            self.show()
        else:
            raise Exception('Unknown action')

if __name__ == '__main__':

    memon = MEMon()
    memon.main()

