"""
check for job errors and take necessary steps to reset, flag, email, etc.
"""

# -----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

utils_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_dir)

from config_utility import BranchConfig

config_attempts = 0
while True:
    config = BranchConfig(branch=branch)
    config_attempts += 1
    if config.connection_status == 0 or config_attempts > 5:
        break

# -----------------------------------------------------------------------------

if config.connection_status != 0:
    raise Exception('Could not connect db')


import time
from datetime import datetime

from bson.objectid import ObjectId

from email_utility import GeoEmail

email = GeoEmail(config)


client = config.client


# time in seconds since last update
# used to determine if a task is no longer running due to job error
extract_error_interval = 60 * 2

# maximum number of restart attempts allowed for tasks before
# setting true error flag
max_extract_errors = 5
max_msr_errors = 5


dev_email = "geodev@aiddata.wm.edu"
reply_to = "Geo Dev <{0}>".format(dev_email)


# ----------------------------------------
# check extract tasks

c_extracts = client.asdf.extracts

extract_errors = c_extracts.find({
    'version': config.versions['extracts'],
    '$or': [
        {'status': -1},
        {
            'status': 2,
            'update_time': {'$lt': time.time() - extract_error_interval}
        }
    ]
})


for i in extract_errors:

    attempts = 0 if 'attempts' not in i else i['attempts']

    new_status = 0 if attempts < max_extract_errors else -2

    updates = {
        'status': new_status,
        'update_time': int(time.time()),
        'attempts': attempts + 1
    }

    results = c_extracts.update_one(
        {'_id': ObjectId(i['_id'])},
        {'$set': updates}
    )
    if new_status == 0:
        print "Resetting extract task with {0} attempts ({1})".format(attempts+1, str(i['_id']))

    if new_status == -2:

        print "Flagging extract task with {0} attempts as error ({1})".format(attempts+1, str(i['_id']))

        subject = "Geo ({0}) - Error : extract task [{1}]".format(config.branch, str(i['_id']))

        last_update = datetime.fromtimestamp(float(i['update_time'])).strftime('%Y-%m-%d %H:%M:%S')

        message = (
            """
            An error was encountered in an extract task

            Error status: {0}

            Last updated on: {1}

            Task id:

            \t'_id': ObjectId('{2}')

            Task:

            {3}

            """).format(new_status, last_update, str(i['_id']), i)

        mail_status = email.send_email(dev_email, subject, message, reply_to=reply_to)

        if not mail_status[0]:
            print mail_status[1]
            raise mail_status[2]


# ----------------------------------------
# check msr tasks


c_msr = client.asdf.msr

msr_errors = c_msr.find({
    'version': config.versions['msr'],
    'status': -1
})


for i in msr_errors:

    attempts = 0 if 'attempts' not in i else i['attempts']

    new_status = 0 if attempts < max_msr_errors else -2

    updates = {
        'status': new_status,
        'update_time': int(time.time()),
        'attempts': attempts + 1
    }

    results = c_msr.update_one(
        {'_id': ObjectId(i['_id'])},
        {'$set': updates}
    )

    if new_status == 0:
        print "Resetting msr task with {0} attempts ({1})".format(attempts+1, str(i['_id']))

    if new_status == -2:

        print "Flagging msr task with {0} attempts as error ({1})".format(attempts+1, str(i['_id']))

        subject = "Geo ({0}) - Error : msr task [{1}]".format(config.branch, str(i['_id']))

        last_update = datetime.fromtimestamp(float(i['update_time'])).strftime('%Y-%m-%d %H:%M:%S')

        message = (
            """
            An error was encountered in an msr task\n

            Error status: {0}\n

            Last updated on: {1}\n

            Task id:\n

            \t'_id': ObjectId('{2}')

            Task:

            {3}

            """).format(new_status, last_update, str(i['_id']), i)

        mail_status = email.send_email(dev_email, subject, message, reply_to=reply_to)

        if not mail_status[0]:
            print mail_status[1]
            raise mail_status[2]
