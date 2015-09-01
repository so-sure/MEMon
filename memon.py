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
    Unknown, Standard, LatePeriod, MissingPeriod = range(0, 4)

class MEMon(object):

    # Default constructor of the class.
    def __init__(self):
        self.queue = "memon"
        self.table = "memon"
        self.sns = "memon"
        self.sqs = boto.connect_sqs()
        self.sns_conn = boto.connect_sns()
        self.args = None
    
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
        
        print 'Remember to subscribe to the sns topic %s' % (self.args.sns)

    def send(self):
        q = self.sqs.get_queue(self.args.queue)
        m = RawMessage()
        body = json.dumps({
            'name': self.args.name,
            'time': int(time.time()),
            'period': int(self.args.add_period)
        })
        if self.args.debug:
            print body
        m.set_body(body)
        q.write(m)

    def poll(self):
        q = self.sqs.get_queue(self.args.queue)
        results = q.get_messages()
        for result in results:
            msg = json.loads(result.get_body())
            print msg
            period = None
            if 'period' in msg:
                period = msg['period']
            self.record(msg['name'], msg['time'], period)

            q.delete_message(result)

    def notify(self, name, notification):
        # todo: handle pagination of topics
        topics = self.sns_conn.get_all_topics()['ListTopicsResponse']['ListTopicsResult']['Topics']
        topicArn = None
        for topic in topics:
            if topic['TopicArn'].endswith(':' + self.args.sns):
                # todo: cache arn
                topicArn = topic['TopicArn']

        message = None
        if notification == Notification.Standard:
            message = 'Event %s failed to send' % (name)
        elif notification == Notification.LatePeriod:
            message = 'Event %s ran late' % (name)
        elif notification == Notification.MissingPeriod:
            message = 'Event %s is missing its notification period' % (name)
        else:
            raise Exception('Invalid notification type')

        if self.args.debug:
            print message

        if topicArn:
            self.sns_conn.publish(topicArn, message)
        else:
            raise Exception('Unable to locate topic arn for %s' % (self.args.sns))

    def record(self, name, event_time, period):
        table = Table(self.args.table)
        if self.args.prefer_server_time:
            event_time = time.time()
        try:
            event = table.get_item(True, Name=name)

            # The possiblity exists that we're processing an older sqs message - if so, we can just ignore it
            if time < event['time']:
                return

            # Verify db record is setup properly
            if period and self.args.add_unknown_events:
                if self.args.debug:
                    print 'Using cli period'
                period = period
            elif 'period' in event:
                if self.args.debug:
                    print 'Using db period'
                period = event['period']
            else:
                if self.args.debug:
                    print 'Missing period'
                self.notify(name, Notification.MissingPeriod)
                return
 
            if event['expected'] < event_time and event['error_count'] < self.args.max_notify_count:
                self.notify(name, Notification.LatePeriod)
            
            event['time'] = int(event_time)
            event['expected'] = int(event_time) + int(period)
            event['error_count'] = 0
            event['period'] = int(period)
            event.partial_save()
        except boto.dynamodb2.exceptions.ItemNotFound as e:
            if self.args.add_unknown_events and event['period']:
                table.put_item(data = {
                    'Name': name,
                    'time': event_time,
                    'error_count': 0,
                    'period': event['period'],
                    'expected': event_time + period
                })
            else:
                print 'Unknown event %s.  Skipping...' % (name)

    def main(self):

        parser = argparse.ArgumentParser(description='Missing Event Monitor')
        parser.add_argument('action', choices=['init', 'send', 'poll'], help='Action to perform')
        parser.add_argument('name', nargs='?', help='Event name (required for send)')
        parser.add_argument('--queue', default=self.queue, nargs='?', help='MEMon SQS Name (default: %(default)s)')
        parser.add_argument('--table', default=self.table, nargs='?', help='MEMon DynamoDb Table Name (default: %(default)s)')
        parser.add_argument('--sns', default=self.sns, nargs='?', help='MEMon SNS Topic Name (default: %(default)s)')
        parser.add_argument('--prefer-server-time', default=False, help='If you do not trust the client times, you can use the server process time')
        parser.add_argument('--add-unknown-events', default=False, help='Should new events, automatically be added?')
        parser.add_argument('--debug', default=False, help='Print debug statements')
        parser.add_argument('--add-period', type=int, default=300, help='If adding new events, how long should the notification period be? (seconds)')
        parser.add_argument('--max-notify-count', type=int, default=3, help='Max # of sns notify events per name')
        self.args = parser.parse_args()

        if self.args.action == 'init':
            self.aws_init()
        elif self.args.action == 'send':                
            if not self.args.name:
                raise Exception('Missing event name')
        
            self.send()
        elif self.args.action == 'poll':
            self.poll()
        else:
            raise Exception('Unknown action')

if __name__ == '__main__':

    memon = MEMon()
    memon.main()

