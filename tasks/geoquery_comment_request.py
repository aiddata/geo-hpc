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



import time
import pandas as pd

# # used for logging
# sys.stdout = sys.stderr = open(
#     os.path.dirname(os.path.abspath(__file__)) + '/processing.log', 'a')

from geoquery_requests import QueueToolBox

# =============================================================================

print '\n======================================='
print '\nRequest for Comments Script'
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())

dry_run = True

queue = QueueToolBox()

# load config setting for branch script is running on
branch_info = queue.set_branch_info(config)
print "`{0}` branch on {1}".format(branch_info.name, branch_info.database)


current_timestamp = int(time.time())


# filters for searching requests
f = {
    "n_days": 90, # number of days to search for any requests
    "request_count": 5, # minimum number of requests in n_days required for an email
    "earliest_request": 14, # minimum number of days since earliest request
    "latest_request": 7, # minimum number of days since latest request
}


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
            "email": 1,
            "status": 1,
            "stage": 1
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
        "earliest_time": "min",
        "latest_time": "max"
    })


    # filter

    valid_df = user_df.loc[
        (user_df["count"] > f["request_count"]) &
        (user_df["comments_requested"] == 0) &
        (current_timestamp - user_df["earliest_time"] > to_seconds(f["earliest_request"])) &
        (current_timestamp - user_df["latest_time"] > to_seconds(f["latest_request"]))
    ]

    print "\n{} valid users found:\n".format(len(valid_df))

    # email any users who pass above filtering with request for comments
    # add "comments_requested" = 1 flag to all of their existing requests
    for ix, user_info in valid_df.iterrows():

        print '\t{}'.format(user_info["email"])

        if not dry_run:

            # send email that request was completed
            queue.notify_comments(user_info['email'])

            queue.c_queue.update_many(
                {"email": user_info["email"]},
                {"$set": {"comments_requested": 1}}
            )




print '\n---------------------------------------'
print "\nFinished checking requests"
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())
