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

Status
---
MEMon is in alpha and subject to quite a bit of change.  We hope to be more stable towards end of 2015.

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
wget -O /usr/local/bin/memon.py https://raw.github.com/PeerJ/MEMon/master/memon.py
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

On 1 server
--
**Requires IAM permissions to send to SQS Queue**

crontab
```
* * * * * /usr/local/bin/memon.py poll
```

DynamoDb Schema
--
* HashKey (string)Name : unique name of the process
* (string)Description : optional description
* (int)Enabled : disabled tasks will not be alerted on
* (int)ErrorCount : number of times the event has errored (will reset on success)
* (int)Period : number of seconds inbetween expected event runs
* (string)Type : 'rolling' where a success will add period, or 'fixed' where the event is supposed to run at a set time
* (int)LastBlockTime : epoch of last run (rolling = last success, fixed = last supposed run)
* (int)NextBlockTime : epoch of next expected run
* (int)LastSuccessTime : epoch of last successful run

Future work
--
* Improve the error messages
* Add basic reporting over a timeperiod (weekly/monthly) on events. # of failures in period, last failure
* A web component to view and edit the dynamodb data
* An additional optional history dynamodb table to display nice graphs for the web component
