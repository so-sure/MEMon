#!/usr/bin/python
import unittest
import boto
import datetime
from memon import MEMon
from memon import PeriodType
from moto import mock_sns
from moto import mock_dynamodb
from boto import kms
import httpretty
import sure
from six.moves.urllib.parse import parse_qs
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.table import Item


class TestMEMon(unittest.TestCase):
    TABLE_NAME = 'memon'
    TABLE_RT = 1
    TABLE_WT = 1
    TABLE_HK_NAME = u'Name'
    TABLE_HK_TYPE = u'S'

    FIXED = {
        u'Period': 5,
        u'Enabled': 1,
        u'Type': 'fixed',
        u'ErrorCount': 0,
        u'LastBlockTime': 5,
        u'NextBlockTime': 10,
        u'LastSuccessTime': 5
    }
    ROLLING = {
        u'Period': 5,
        u'Enabled': 1,
        u'Type': 'rolling',
        u'ErrorCount': 0,
        u'LastBlockTime': 5,
        u'NextBlockTime': 10,
        u'LastSuccessTime': 5
    }
    DOWN = {
        u'Period': 5,
        u'Enabled': 1,
        u'Type': 'rolling',
        u'ErrorCount': 0,
        u'LastBlockTime': 1,
        u'NextBlockTime': 6,
        u'LastSuccessTime': 1
    }
    TEST = {
        u'Period': 1,
        u'Enabled': 1,
        u'Type': 'rolling',
        u'ErrorCount': 0,
        u'Description': 'desc',
    }
    BLANK = {
    }

    memon = None
    sns_conn = None
    table = None

    @mock_sns
    def setUp(self):
        from ddbmock import connect_boto_patch
        from ddbmock.database.db import dynamodb
        from ddbmock.database.table import Table
        from ddbmock.database.key import PrimaryKey

        # Do a full database wipe
        dynamodb.hard_reset()

        # Instanciate the keys
        hash_key = PrimaryKey(self.TABLE_HK_NAME, self.TABLE_HK_TYPE)

        # Create a test table
        new_table = Table(self.TABLE_NAME,
                          self.TABLE_RT,
                          self.TABLE_WT,
                          hash_key,
                          None)

        # Very important: register the table in the DB
        dynamodb.data[self.TABLE_NAME] = new_table

        # Create the database connection ie: patch boto
        self.db = connect_boto_patch()

        self.table = self.db.get_table(self.TABLE_NAME)
        self.sns_conn = boto.connect_sns()

        self.memon = MEMon()
        self.memon.debug = False
        self.memon.table = self.table
        self.memon.sns_conn = self.sns_conn
        self.memon.server_time = False

    def tearDown(self):
        from ddbmock.database.db import dynamodb
        from ddbmock import clean_boto_patch

        # Do a full database wipe
        dynamodb.hard_reset()

        # Remove the patch from Boto code (if any)
        clean_boto_patch()

    def initSns(self):
        httpretty.HTTPretty.register_uri(
            method="POST",
            uri="http://example.com/foobar",
        )
        self.sns_conn.create_topic(self.memon.sns)
        self.sns_conn.subscribe(self.memon.get_topic_arn(),
                                "http",
                                "http://example.com/foobar")

    def getPostMessage(self):
        last_request = httpretty.last_request()
        last_request.method.should.equal("POST")
        return str(parse_qs(last_request.body.decode('utf-8'))['Message'])

    def test_config_create(self):
        date = datetime.date(2010, 1, 1)
        time = datetime.time(01, 02)
        dt = datetime.datetime.combine(date, time)
        self.memon.config('newName', 1, True, PeriodType.Fixed,
                          'desc', date, time)

        event = self.table.get_item(hash_key='newName')
        self.assertEquals(1, event['Period'])
        self.assertEquals(True, event['Enabled'])
        self.assertEquals(PeriodType.Fixed, event['Type'])
        self.assertEquals('desc', event['Description'])
        self.assertEquals(0, event['ErrorCount'])
        self.assertEquals(int(dt.strftime('%s')), event['NextBlockTime'])

    def test_config_partial_update(self):
        self.table.new_item(hash_key='Test', attrs=self.TEST).put()

        event = self.table.get_item(hash_key='Test')

        self.assertEquals(self.TEST['Period'], event['Period'])
        self.assertEquals(self.TEST['Enabled'], event['Enabled'])
        self.assertEquals(self.TEST['Type'], event['Type'])
        self.assertEquals(self.TEST['Description'], event['Description'])
        self.assertEquals(self.TEST['ErrorCount'], event['ErrorCount'])

        self.memon.config('Test', 2, True)

        event = self.table.get_item(hash_key='Test')
        self.assertEquals(2, event['Period'])
        self.assertEquals(True, event['Enabled'])
        self.assertEquals(self.TEST['Type'], event['Type'])
        self.assertEquals(self.TEST['Description'], event['Description'])
        self.assertEquals(self.TEST['ErrorCount'], event['ErrorCount'])

    def test_config_full_update(self):
        self.table.new_item(hash_key='Test', attrs=self.TEST).put()

        date = datetime.date(2010, 1, 1)
        time = datetime.time(01, 02)
        dt = datetime.datetime.combine(date, time)
        self.memon.config('Test', 3, False, PeriodType.Fixed,
                          'desc2', date, time)

        event = self.table.get_item(hash_key='Test')
        self.assertEquals(3, event['Period'])
        self.assertEquals(False, event['Enabled'])
        self.assertEquals(PeriodType.Fixed, event['Type'])
        self.assertEquals('desc2', event['Description'])
        self.assertEquals(int(dt.strftime('%s')), event['NextBlockTime'])
        self.assertEquals(event['NextBlockTime'] - event['Period'], event['LastBlockTime'])

    def test_config_date_update(self):
        self.table.new_item(hash_key='Test', attrs=self.TEST).put()

        date = datetime.date(2010, 1, 1)
        self.memon.config('Test', 3, False, PeriodType.Fixed,
                          'desc2', date, None)

        event = self.table.get_item(hash_key='Test')
        self.assertEquals(3, event['Period'])
        self.assertEquals(False, event['Enabled'])
        self.assertEquals(PeriodType.Fixed, event['Type'])
        self.assertEquals('desc2', event['Description'])
        self.assertEquals(int(date.strftime('%s')), event['NextBlockTime'])
        self.assertEquals(event['NextBlockTime'] - event['Period'], event['LastBlockTime'])

    def test_config_time_update(self):
        self.table.new_item(hash_key='Test', attrs=self.TEST).put()

        today = datetime.datetime.now().date()
        time = datetime.time(01, 02)
        dt = datetime.datetime.combine(today, time)
        self.memon.config('Test', 3, False, PeriodType.Fixed,
                          'desc2', None, time)

        event = self.table.get_item(hash_key='Test')
        self.assertEquals(3, event['Period'])
        self.assertEquals(False, event['Enabled'])
        self.assertEquals(PeriodType.Fixed, event['Type'])
        self.assertEquals('desc2', event['Description'])
        self.assertEquals(int(dt.strftime('%s')), event['NextBlockTime'])

    @mock_sns
    def test_record_unknown_event(self):
        self.initSns()
        self.memon.record('UnknownEvent', 1)
        self.getPostMessage().should.contain('Config: UnknownEvent')

    @mock_sns
    def test_record_blank_event(self):
        self.initSns()
        self.table.new_item(hash_key='Blank', attrs=self.BLANK).put()

        self.memon.record('Blank', 1)

        self.getPostMessage().should.contain('Config: Blank')

    @mock_sns
    def test_record_fixed_event(self):
        self.initSns()
        self.table.new_item(hash_key='Fixed', attrs=self.FIXED).put()

        self.memon.record('Fixed', 8)

        event = self.table.get_item(hash_key='Fixed')
        self.assertEquals(10, event['LastBlockTime'])
        self.assertEquals(15, event['NextBlockTime'])
        self.assertEquals(8, event['LastSuccessTime'])

        # empty body request - presumably from the sns subscription
        self.assertEqual(0, len(httpretty.last_request().body))

    @mock_sns
    def test_record_fixed_late_event(self):
        self.initSns()
        self.table.new_item(hash_key='Fixed', attrs=self.FIXED).put()

        self.memon.record('Fixed', 12)

        event = self.table.get_item(hash_key='Fixed')
        self.assertEquals(15, event['LastBlockTime'])
        self.assertEquals(20, event['NextBlockTime'])
        self.assertEquals(12, event['LastSuccessTime'])
        self.getPostMessage().should.contain('Late:')

    @mock_sns
    def test_record_rolling_event(self):
        self.initSns()
        self.table.new_item(hash_key='Rolling', attrs=self.ROLLING).put()

        self.memon.record('Rolling', 8)

        event = self.table.get_item(hash_key='Rolling')
        self.assertEquals(8, event['LastBlockTime'])
        self.assertEquals(13, event['NextBlockTime'])
        self.assertEquals(8, event['LastSuccessTime'])

        # empty body request - presumably from the sns subscription
        self.assertEqual(0, len(httpretty.last_request().body))

    @mock_sns
    def test_record_rolling_late_event(self):
        self.initSns()
        self.table.new_item(hash_key='Rolling', attrs=self.ROLLING).put()

        self.memon.record('Rolling', 12)

        event = self.table.get_item(hash_key='Rolling')
        self.assertEquals(12, event['LastBlockTime'])
        self.assertEquals(17, event['NextBlockTime'])
        self.assertEquals(12, event['LastSuccessTime'])
        self.getPostMessage().should.contain('Late: Rolling')

    @mock_sns
    def test_record_up_event(self):
        self.initSns()
        self.table.new_item(hash_key='Rolling', attrs=self.ROLLING).put()

        event = self.table.get_item(hash_key='Rolling')
        event['ErrorCount'] = 1
        event.save()

        self.memon.record('Rolling', 12)

        event = self.table.get_item(hash_key='Rolling')
        self.assertEquals(0, event['ErrorCount'])

        self.getPostMessage().should.contain('Up: Rolling')

    @mock_sns
    def test_record_disabled_event(self):
        self.initSns()
        self.table.new_item(hash_key='Rolling', attrs=self.ROLLING).put()

        self.memon.config('Rolling', self.ROLLING['Period'], False)

        event = self.table.get_item(hash_key='Rolling')
        self.assertEquals(False, event['Enabled'])

        self.memon.record('Rolling', 12)

        # empty body request - presumably from the sns subscription
        self.assertEqual(0, len(httpretty.last_request().body))

    @mock_sns
    def test_record_prior_event(self):
        self.initSns()
        self.table.new_item(hash_key='Rolling', attrs=self.ROLLING).put()

        self.memon.record('Rolling', 1)

        event = self.table.get_item(hash_key='Rolling')
        self.assertEquals(self.ROLLING['LastBlockTime'],
                          event['LastBlockTime'])
        self.assertEquals(self.ROLLING['NextBlockTime'],
                          event['NextBlockTime'])
        self.assertEquals(self.ROLLING['LastSuccessTime'],
                          event['LastSuccessTime'])

        # empty body request - presumably from the sns subscription
        self.assertEqual(0, len(httpretty.last_request().body))

    @mock_sns
    def test_record_new_event(self):
        self.initSns()
        self.table.new_item(hash_key='Test', attrs=self.TEST).put()

        event = self.table.get_item(hash_key='Test')
        self.assertFalse('LastBlockTime' in event)
        self.assertFalse('NextBlockTime' in event)
        self.assertFalse('LastSuccessTime' in event)

        self.memon.record('Test', 50)

        event = self.table.get_item(hash_key='Test')
        self.assertEquals(50, event['LastBlockTime'])
        self.assertEquals(51, event['NextBlockTime'])
        self.assertEquals(50, event['LastSuccessTime'])

        # empty body request - presumably from the sns subscription
        self.assertEqual(0, len(httpretty.last_request().body))

    @mock_sns
    def test_new_event_no_alert(self):
        self.initSns()
        self.table.new_item(hash_key='Test', attrs=self.TEST).put()

        event = self.table.get_item(hash_key='Test')
        self.assertFalse('LastBlockTime' in event)
        self.assertFalse('NextBlockTime' in event)
        self.assertFalse('LastSuccessTime' in event)

        self.memon.notify_down_events()

        # empty body request - presumably from the sns subscription
        self.assertEqual(0, len(httpretty.last_request().body))

    @mock_sns
    def test_down_notify(self):
        self.initSns()
        self.table.new_item(hash_key='Down', attrs=self.DOWN).put()

        event = self.table.get_item(hash_key='Down')
        event['ErrorCount'] = self.memon.max_notify_count - 1
        event.save()

        self.memon.notify_down_events()

        event = self.table.get_item(hash_key='Down')
        self.assertEquals(self.memon.max_notify_count, event['ErrorCount'])

        self.getPostMessage().should.contain('Down: Down')

    @mock_sns
    def test_down_missing_errorcount_notify(self):
        self.initSns()
        self.table.new_item(hash_key='Blank', attrs=self.BLANK).put()

        event = self.table.get_item(hash_key='Blank')
        event['Enabled'] = 1
        event['Type'] = 'rolling'
        event['Period'] = 1
        event['LastBlockTime'] = self.memon.now - event['Period']
        event['NextBlockTime'] = self.memon.now - 1
        event['LastSuccessTime'] = self.memon.now - 1
        event.save()
        self.assertTrue('ErrorCount' not in event)

        self.memon.notify_down_events()

        event = self.table.get_item(hash_key='Blank')
        print event
        self.assertEquals(1, event['ErrorCount'])

        self.getPostMessage().should.contain('Down: Blank')

    @mock_sns
    def test_down_period_notify(self):
        self.initSns()
        self.table.new_item(hash_key='Down', attrs=self.DOWN).put()

        event = self.table.get_item(hash_key='Down')
        event['ErrorCount'] = 0
        event['LastBlockTime'] = self.memon.now
        event['NextBlockTime'] = self.memon.now + self.DOWN['Period']
        event['LastSuccessTime'] = self.memon.now
        event.save()

        self.memon.notify_down_events()

        event = self.table.get_item(hash_key='Down')
        self.assertEquals(0, event['ErrorCount'])

        # empty body request - presumably from the sns subscription
        self.assertEqual(0, len(httpretty.last_request().body))

    @mock_sns
    def test_down_max_error(self):
        self.initSns()
        self.table.new_item(hash_key='Down', attrs=self.DOWN).put()

        event = self.table.get_item(hash_key='Down')
        event['ErrorCount'] = self.memon.max_notify_count
        event.save()

        self.memon.notify_down_events()

        event = self.table.get_item(hash_key='Down')
        self.assertEquals(self.memon.max_notify_count + 1, event['ErrorCount'])

        # empty body request - presumably from the sns subscription
        self.assertEqual(0, len(httpretty.last_request().body))

    @mock_sns
    def test_down_disabled(self):
        self.initSns()
        self.table.new_item(hash_key='Down', attrs=self.DOWN).put()

        event = self.table.get_item(hash_key='Down')
        event['ErrorCount'] = 0
        event['Enabled'] = False
        event.save()

        self.memon.notify_down_events()

        event = self.table.get_item(hash_key='Down')
        self.assertEquals(0, event['ErrorCount'])

        self.assertEqual(0, len(httpretty.last_request().body))


if __name__ == '__main__':
    unittest.main()
