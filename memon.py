#!/usr/bin/python
import argparse
import boto
import json
import datetime
import time
from boto.sqs.message import RawMessage
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.fields import RangeKey
import pprint
import sys

MEMON_VERSION = '0.0.1'


class Notification:
    Unknown, Down, Up, Late, ConfigError = range(0, 5)


class PeriodType:
    Fixed = 'fixed'
    Rolling = 'rolling'


class Schema:
    Name = 'Name'
    Enabled = 'Enabled'
    ErrorCount = 'ErrorCount'
    Period = 'Period'
    Description = 'Description'
    Type = 'Type'
    LastBlockTime = 'LastBlockTime'
    NextBlockTime = 'NextBlockTime'
    LastSuccessTime = 'LastSuccessTime'


class MEMon(object):

    # Default constructor of the class.
    def __init__(self):
        self.queue = "memon"
        self.table_name = "memon"
        self.table = None
        self.sns = "memon"
        self.sns_email = None
        self.debug = False
        self.max_notify_count = 3
        self.server_time = True

        self.sqs = boto.connect_sqs()
        self.sns_conn = boto.connect_sns()
        self.db = boto.connect_dynamodb()
        self.pp = pprint.PrettyPrinter()
        self.now = int(time.time())

    def aws_init(self):
        print 'Creating sqs queue %s' % (self.queue)
        self.sqs.create_queue(self.queue)

        print 'Creating dynamodb table %s' % (self.table_name)
        try:
            Table.create(self.table_name,
                         schema=[HashKey(Schema.Name)],
                         throughput={'read': 1, 'write': 1})
        except boto.exception.JSONResponseError as e:
            print e

        print 'Creating sns topic %s' % (self.sns)
        self.sns_conn.create_topic(self.sns)

        if self.sns_email:
            print ('Subscribing %s to the sns topic %s'
                   '(click confirmation link in email)' %
                   (self.sns_email, self.sns))
            self.sns_conn.subscribe(self.get_topic_arn(),
                                    'email',
                                    self.sns_email)
        else:
            print 'Remember to subscribe to the sns topic %s' % (self.sns)

    def send(self, name):
        q = self.sqs.get_queue(self.queue)
        m = RawMessage()
        msg = {
            'name': name,
            'time': self.now,
        }
        if self.debug:
            self.pp.pprint(msg)
        m.set_body(json.dumps(msg))
        q.write(m)

    def poll(self):
        q = self.sqs.get_queue(self.queue)
        results = q.get_messages(10)
        for result in results:
            msg = json.loads(result.get_body())
            if self.debug:
                print msg
            self.record(msg['name'], msg['time'])
            q.delete_message(result)

    def show(self, name=None):
        results = self.table.scan()
        for event in results:
            if name is None or name == event[Schema.Name]:
                print "\n%s\n---" % (event[Schema.Name])
                try:
                    if not event[Schema.Enabled]:
                        print "***DISABLED***"
                    if (Schema.ErrorCount in event and
                            event[Schema.ErrorCount] > 0):
                        print ("***ERRORS (%d)***" %
                               (event[Schema.ErrorCount]))
                    if Schema.Description in event:
                        print "Desc: %s" % event[Schema.Description]
                    print "Type: %s" % event[Schema.Type]
                    period_sec = datetime.timedelta(
                        seconds=event[Schema.Period])
                    print ("Period: %s (%ds)" %
                           (str(period_sec), event[Schema.Period]))
                    if Schema.NextBlockTime in event:
                        print ("Due: %s" %
                               time.ctime(event[Schema.NextBlockTime]))
                    if Schema.LastSuccessTime in event:
                        print ("Last Ran: %s" %
                               time.ctime(event[Schema.LastSuccessTime]))

                    if self.debug:
                        self.pp.pprint(dict(event))
                except Exception:
                    self.pp.pprint(dict(event))
        print "\n"

    def notify_down_events(self):
        results = self.table.scan()
        for event in results:
            # we only want to notify based on the period,
            # so we're not notifying every minute
            error_count = 0
            if Schema.ErrorCount in event and event[Schema.ErrorCount]:
                error_count = event[Schema.ErrorCount]
            next_notify = None
            if Schema.NextBlockTime in event:
                next_notify = (event[Schema.NextBlockTime] +
                               error_count * event[Schema.Period])
            is_enabled = Schema.Enabled in event and event[Schema.Enabled]
            if next_notify and next_notify <= self.now and is_enabled:
                if self.debug:
                    print "%s\n---" % (event[Schema.Name])
                    self.pp.pprint(dict(event))
                if error_count < int(self.max_notify_count):
                    self.notify(event[Schema.Name], Notification.Down, event)
                elif self.debug:
                    print "Exceeded notify count for %s" % (event[Schema.Name])
                event[Schema.ErrorCount] = error_count + 1
                event.save()

    def get_topic_arn(self):
        # todo: handle pagination of topics
        all_topics = self.sns_conn.get_all_topics()
        topics = all_topics['ListTopicsResponse']['ListTopicsResult']['Topics']
        for topic in topics:
            if topic['TopicArn'].endswith(':' + self.sns):
                # todo: cache arn
                return topic['TopicArn']

        raise Exception('Unable to locate topic arn for %s' % (self.sns))

    def notify(self, name, notification, event=None):
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

        if self.debug:
            print message

        subject = "[MEMon] %s" % (message)
        message = "MEMon Alert\n-----------\n\n%s" % (message)
        if event:
            if Schema.Description in event:
                message = ("%s\n%s: %s" %
                          (message, name, event[Schema.Description]))

        message = "%s\n\n--\nMEMon" % (message)
        self.sns_conn.publish(topicArn, message, subject)

    def record(self, name, event_time):
        if self.server_time:
            event_time = self.now
        else:
            event_time = int(event_time)

        try:
            event = self.table.get_item(name)

            if not Schema.Period in event:
                return self.notify(name, Notification.ConfigError)

            # If we're processing an older sqs message, we can just ignore it
            if (Schema.LastSuccessTime in event and
                    event_time < event[Schema.LastSuccessTime]):
                return

            if (Schema.NextBlockTime in event and
                    event[Schema.NextBlockTime] < event_time and
                    event[Schema.Enabled]):
                if event[Schema.ErrorCount] == 0:
                    self.notify(name, Notification.Late, event)
                else:
                    self.notify(name, Notification.Up, event)

            event[Schema.LastSuccessTime] = event_time
            if event[Schema.Type] == PeriodType.Rolling:
                event[Schema.LastBlockTime] = event_time
                event[Schema.NextBlockTime] = (event_time +
                                               int(event[Schema.Period]))
            elif event[Schema.Type] == PeriodType.Fixed:
                periods = 1
                while (event_time > event[Schema.LastBlockTime]
                       + int(event[Schema.Period]) * periods):
                    periods += 1
                event[Schema.LastBlockTime] = (event[Schema.LastBlockTime] +
                                               int(event[Schema.Period]) *
                                               periods)
                event[Schema.NextBlockTime] = (event[Schema.LastBlockTime] +
                                               int(event[Schema.Period]))
            else:
                return self.notify(name, Notification.ConfigError)

            event[Schema.ErrorCount] = int(0)
            event.save()
        except Exception:
            self.notify(name, Notification.ConfigError)

    def config(self, name, period, enabled, event_type=None,
               description=None, initial_date=None, initial_time=None):
        set_date = False
        date = datetime.datetime.now().date()
        if not initial_date is None:
            date = initial_date
            print date
            set_date = True
        if not initial_time is None:
            # Add 1 day if earlier time to avoid initial alerts for new events
            if datetime.datetime.now().time() > initial_time:
                date = date + datetime.timedelta(days=1)
            date = datetime.datetime.combine(date, initial_time)
            set_date = True
        try:
            event = self.table.get_item(hash_key=name)
            if self.debug:
                print 'Get Event %s' % event
            if not period is None:
                event[Schema.Period] = period
            if not enabled is None:
                event[Schema.Enabled] = enabled
            if not event_type is None:
                event[Schema.Type] = event_type
            if not description is None:
                event[Schema.Description] = description
            if set_date:
                event[Schema.NextBlockTime] = int(date.strftime('%s'))
                event[Schema.LastBlockTime] = (int(date.strftime('%s')) -
                                               event[Schema.Period])

            event.save()
        except boto.dynamodb.exceptions.DynamoDBValidationError as v_err:
            raise v_err
        except Exception as e:
            if self.debug:
                print e
            if not period:
                raise Exception('Period is required for new events')
            if not event_type:
                raise Exception('Event Type is required for new events')

            # ErrorCount should only be set when adding new items
            data = {
                Schema.Period: period,
                Schema.Enabled: enabled,
                Schema.ErrorCount: 0,
                Schema.Type: event_type
            }
            if description:
                data[Schema.Description] = description
            if set_date:
                data[Schema.NextBlockTime] = int(date.strftime('%s'))
                data[Schema.LastBlockTime] = int(date.strftime('%s')) - period

            print data

            #self.table.put_item(data)
            item = self.table.new_item(hash_key=name, attrs=data)
            item.put()

        if self.debug:
            self.show(name)

    def main(self):

        parser = argparse.ArgumentParser(description='Missing Event Monitor')
        parser.add_argument('--queue',
                            default=self.queue,
                            help='MEMon SQS Name (default: %(default)s)')
        parser.add_argument('--table',
                            default=self.table_name,
                            help=('MEMon DynamoDb Table Name '
                                  '(default: %(default)s)'))
        parser.add_argument('--sns',
                            default=self.sns,
                            help='MEMon SNS Topic Name (default: %(default)s)')
        parser.add_argument('--sns-email',
                            help=('Init only: Subscribe email to '
                                  'sns notifications'))
        parser.add_argument('--use-server-time',
                            dest='server_time',
                            action='store_true',
                            help=('Base time calculations on when '
                                  'the server processes the event'))
        parser.add_argument('--use-client-time',
                            dest='server_time',
                            action='store_false',
                            help=('Base time calculations on when '
                                  'the client sent the event'))
        parser.set_defaults(server_time=True)
        parser.add_argument('--debug',
                            default=False,
                            help='Print debug statements')
        parser.add_argument('--max-notify-count',
                            type=int,
                            default=3,
                            help='Max # of sns notify events per name')
        parser.add_argument('--poll-count',
                            type=int,
                            default=3,
                            help='Number of times to poll in period')

        parser.add_argument('--period',
                            type=int,
                            default=None,
                            help=('Config only: Notification '
                                  'period in seconds)'))
        parser.add_argument('--initial-date',
                            type=mkdate,
                            help=('Config only: Initial date (yyy-mm-dd) '
                                  'when the first run should be expected'))
        parser.add_argument('--initial-time',
                            type=mktime,
                            help=('Config only: Initial time (HH:MM) '
                                  'when the first run should be expected'))
        parser.add_argument('--description',
                            default=None,
                            help=('Config only: '
                                  'Optional description for event'))
        parser.add_argument('--type',
                            choices=[PeriodType.Fixed, PeriodType.Rolling],
                            default=None,
                            help='Config only')
        parser.add_argument('--enabled',
                            dest='enabled',
                            action='store_true',
                            help='Config only - enable/disable event')
        parser.add_argument('--disabled',
                            dest='enabled',
                            action='store_false',
                            help='Config only - enable/disable event')
        parser.set_defaults(enabled=True)
        parser.add_argument('action',
                            choices=['init', 'send', 'poll',
                                     'config', 'show', 'version'],
                            help='Action to perform')
        parser.add_argument('name',
                            nargs='?',
                            help='Event name (required for send and config)')

        args = parser.parse_args()
        self.table_name = args.table
        self.queue = args.queue
        self.sns = args.sns
        self.sns_email = args.sns_email
        self.debug = args.debug
        self.max_notify_count = args.max_notify_count

        if args.action == 'init':
            self.aws_init()
            sys.exit(0)

        # get_table needs to be after init or init will fail
        self.table = self.db.get_table(self.table_name)

        if args.action == 'send':
            if not args.name:
                raise Exception('Missing event name')

            self.send(args.name)
        elif args.action == 'poll':
            for i in xrange(args.poll_count):
                if self.debug:
                    print "Poll attempt %d" % (i)
                self.poll()
            self.notify_down_events()
        elif args.action == 'config':
            if not args.name:
                raise Exception('Missing event name')

            print 'Setting config for %s' % (args.name)
            self.config(args.name,
                        args.period,
                        args.enabled,
                        args.type,
                        args.description,
                        args.initial_date,
                        args.initial_time)
        elif args.action == 'show':
            self.show()
        elif args.action == 'version':
            print "MEMon Version %s" % (MEMON_VERSION)
            sys.exit(0)
        else:
            raise Exception('Unknown action')


def mkdate(datestring):
    try:
        dt = datetime.datetime.strptime(datestring, '%Y-%m-%d').date()
        print dt
        return dt
    except Exception as e:
        print e
        raise e


def mktime(timestring):
    try:
        ts = datetime.datetime.strptime(timestring, '%H:%M').time()
        return ts
    except Exception as e:
        print e
        raise e

if __name__ == '__main__':

    memon = MEMon()
    memon.main()
