"""
check if any items that are ready for processing exist in msr queue

ready for processing = status set to 0

msr queue = mongodb db/collection: asdf->extracts
"""


# ----------------------------------------------------------------------------

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


# ----------------------------------------------------------------------------


# check mongodb connection
if config.connection_status != 0:
    print "error"
    # sys.exit("connection status error: " + str(config.connection_error))


# ----------------------------------------------------------------------------


import pymongo

client = pymongo.MongoClient(config.database)

msr = client.asdf.msr

request_count = msr.find({'status':0}).count()

# make sure request was found
if request_count > 0:
    print "ready"

else:
    print "empty"


