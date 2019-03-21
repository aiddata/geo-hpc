"""process geoquery request for comment emails

called by cronjob on server for branch

"""


# -----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

utils_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_dir)

from config_utility import BranchConfig

config = BranchConfig(branch=branch)
config.test_connection()

# -------------------------------------------------------------------------


if config.connection_status != 0:
    raise Exception('Could not connect to mongodb')

import textwrap
import time
import pandas as pd

# # used for logging
# sys.stdout = sys.stderr = open(
#     os.path.dirname(os.path.abspath(__file__)) + '/processing.log', 'a')

from email_utility import GeoEmail
from geoquery_requests import QueueToolBox

# =============================================================================

print '\n======================================='
print '\nRequest for Comments Script'
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())


# -------------------------------------
# modifiable parameters


# mode = "auto"
mode = "manual"

# dry_run = True
dry_run = False

# maximum number of emails to send per batch (should really be per day for gmail limits, but we only run this once a week+ 1 )
email_limit = 50

# filters for searching requests
f = {
    "n_days": 365, # number of days to search for any requests
    "request_count": 3, # minimum number of requests in n_days required for an email
    "earliest_request": 14, # minimum number of days since earliest request
    "latest_request": 7, # minimum number of days since latest request
}

# -------------------------------------


queue = QueueToolBox()

# load config setting for branch script is running on
branch_info = queue.set_branch_info(config)
print "`{0}` branch on {1}".format(branch_info.name, branch_info.database)


current_timestamp = int(time.time())


def to_seconds(days):
    """convert days to seconds"""
    return days*24*60*60


# get timestamp for ndays before present time
# used to get requests for past n days
search_timestamp = current_timestamp - to_seconds(f["n_days"])


try:
    search = queue.c_queue.find(
        {
            "stage.0.time": {"$gt": search_timestamp}
        },
        {
            "release_data": 0,
            "raster_data": 0,
            "boundary": 0
        }
    )

    request_objects = list(search)

except Exception as e:
    print "Error while searching for requests in queue"
    raise

# verify that we have some requests
if not request_objects:
    print "Request queue is empty"



else:

    # convert to dataframe
    request_df_data = []

    for r in request_objects:
        request_dict = {
            'email': r['email'],
            'request_time': r['stage'][0]['time'],
            'complete_time': r['stage'][3]['time'],
            'status': r['status'],
            'count': 1
        }

        if 'comments_requested' in r:
            request_dict['comments_requested'] = r['comments_requested']
        else:
            request_dict['comments_requested'] = 0

        if 'contact_flag' in r:
            request_dict['contact_flag'] = r['contact_flag']
        else:
            request_dict['contact_flag'] = 0


        request_df_data.append(request_dict)


    request_df = pd.DataFrame(request_df_data)


    # time_field = "request_time"
    time_field = "complete_time"

    request_df["earliest_time"] = request_df[time_field]
    request_df["latest_time"] = request_df[time_field]


    # convert to user aggregated dataframe
    user_df = request_df.groupby('email', as_index=False).agg({
        "count": "sum",
        "comments_requested": "sum",
        "contact_flag": "sum",
        "earliest_time": "min",
        "latest_time": "max"
    })


    # filter
    valid_df = user_df.loc[
        (user_df["comments_requested"] == 0) &
        (user_df["contact_flag"] == 0) &
        (user_df["count"] > f["request_count"]) &
        (current_timestamp - user_df["earliest_time"] > to_seconds(f["earliest_request"])) &
        (current_timestamp - user_df["latest_time"] > to_seconds(f["latest_request"]))
    ]

    valid_user_count = len(valid_df)
    print "\n{} valid users found:\n".format(valid_user_count)


    valid_df.reset_index(drop=True, inplace=True)

    # send list of users to staff emails
    if not dry_run and mode == "manual" and valid_user_count > 0:

        email_list = valid_df["email"].tolist()
        email_list_str = "\n".join(email_list)

        mail_to = "geo@aiddata.org, info@aiddata.org, eteare@aiddata.wm.edu"

        dev = " (dev) " if branch == "develop" else " "

        mail_subject = ("Your weekly list of GeoQuery{0} user emails").format(dev)

        mail_message = (
            """
            Hello there team!

            Below you will find the list of users who satisfy the criteria for contact. For details
            on what these criteria actually are, contact your GeoQuery Admin. At the end of this email
            is some sample language for contacting users.

            --------------------
            {}
            --------------------

            Hello there!

            We would like to hear about your experience using AidData's GeoQuery tool. Would you
            please respond to this email with a couple sentences about how GeoQuery has helped you?

            We are able to make GeoQuery freely available thanks to the generosity of donors and
            open source data providers. These people love to hear about new research enabled by
            GeoQuery, and what kind of difference this research is making in the world.

            Also, we love feedback of all kinds. If something did not go the way you expected, we
            want to hear about that too.

            Thanks!
            \tAidData's GeoQuery Team
            """).format(email_list_str)


        mail_message = textwrap.dedent(mail_message)

        email = GeoEmail(config)

        mail_status = email.send_email(mail_to, mail_subject, mail_message)

        if not mail_status[0]:
            print mail_status[1]
            raise mail_status[2]


    # email any users who pass above filtering with request for comments
    # add "comments_requested" = 1 flag to all of their existing requests
    for ix, user_info in valid_df.iterrows():

        user_email = user_info["email"]

        if mode == "auto" and ix >= email_limit:
            print "\n Warning: maximum emails reached. Exiting."
            break

        print '\t{}: {}'.format(ix, user_email)

        # automated request for comments
        if not dry_run and mode == "auto":

            print "sending emails..."

            # avoid gmail email per second limits
            time.sleep(1)

            queue.notify_comments(user_email)

            queue.c_queue.update_many(
                {"email": user_email},
                {"$set": {"comments_requested": 1}}
            )

        # flag as being included in list for staff to manually email
        elif not dry_run and mode == "manual":

            queue.c_queue.update_many(
                {"email": user_email},
                {"$set": {"contact_flag": 1}}
            )



print '\n---------------------------------------'
print "\nFinished checking requests"
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())
