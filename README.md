Missing Event MONitor (MEMon)
---
Detect errors in events(scripts/processes/etc) by the failure of the event to report that it ran successfully.

At the moment, MEMon is heavily dependent on AWS services (SQS, DynamoDb, & SNS), although the concept could easily
be ported to other architectures.  Running MEMon should cost under $1/month for low-moderate workloads.

`MEMon send EVENT` will enqueue a sqs message to indicate the event ran successfully.

`MEMon poll` should be run every minute (via cron) and will process the sqs message as well
as check for any events that haven't run in the timeperiod expected.  Notifications are handled via SNS.

`MEMon config EVENT` will create/edit events. Use `--period` `--description` `--enabled or --disabled`.
Remember to include an additional ~90 seconds in the period to account for the sqs + polling times and also account for variations
in the process runtime if that's likely.

Requires:
--
AWS SQS
AWS DyanamoDb
AWS SNS
Python
Boto
Boto configured (~/.boto file)

Install:
--
Download MEMon
```
wget -o /usr/local/bin/memon.py https://raw.github.com/PeerJ/MEMon/master/memon.py
chmod 755 /usr/local/bin/memon.py
```

Create AWS SQS Queue, SNS Topic, and DynamoDb Table **Requires IAM permissions to create sqs, dynamodb, & sns**
Note option `--sns-email` to subscribe to SNS Notifications
```
/usr/local/bin/memon.py init
```

In your scripts
--
**Requires IAM permissions to send to SQS Queue**
```
#!/bin/bash
set -e
# script contents
/usr/local/bin/memon.py send **NAME**
```

On 1 server **Requires IAM permissions to send to SQS Queue**
--
crontab
* * * * * /usr/local/bin/memon.py poll

DynamoDb Schema
--
HashKey (string): Name
(int): error_count (# of notifications sent)
(int): expected (time (epoch) of when the next run should be completed by = time + period)
(int): period (how often the process is expected to run in seconds)
(int): time (last successfully time (epoch) ran)

Future work
--
* improve schema names
* Improve the error messages
* Support processes that need to be run on a set scheduled (e.g. 8am every day)
* Add basic reporting over a timeperiod (weekly/monthly) on events. # of failures in period, last failure
* A web component to view and edit the dynamodb data
* An additional optional history dynamodb table to display nice graphs for the web component