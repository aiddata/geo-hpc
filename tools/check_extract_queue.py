"""
check if any items that are ready for processing exist in extract queue

ready for processing = status set to 0

extract queue = mongodb db/collection: asdf->extracts
"""


# ----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

utils_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_dir)

from config_utility import BranchConfig

config = BranchConfig(branch=branch)
config.test_connection()

# ----------------------------------------------------------------------------


# check mongodb connection
if config.connection_status != 0:
    print "error"
    # sys.exit("connection status error: " + str(config.connection_error))


# ----------------------------------------------------------------------------

job_type = sys.argv[2]

import pymongo

client = pymongo.MongoClient(config.database)

c_extracts = client.asdf.extracts


if job_type == "det":
    request_count = c_extracts.find({'status': 0, 'priority': {'$gt': -1}}).count()

elif job_type == "default":
    request_count = c_extracts.find({'status': 0}).count()

elif job_type == "raster":
    request_count = c_extracts.find({'status': 0, 'classification': 'raster'}).count()

elif job_type == "msr":
    request_count = c_extracts.find({'status': 0, 'classification': 'msr'}).count()

else:
    request_count = "invalid"



if request_count == "invalid":
    print "invalid"

elif request_count > 0:
    print "ready"

else:
    print "empty"
