"""
check if any items that are ready for processing exist in extract queue

ready for processing = status set to 0

extract queue = mongodb db/collection: asdf->extracts
"""


# ----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
sys.path.insert(0, config_dir)

from config_utility import BranchConfig

config = BranchConfig(branch=branch)


# ----------------------------------------------------------------------------


# check mongodb connection
if config.connection_status != 0:
    print "error"
    # sys.exit("connection status error: " + str(config.connection_error))


# ----------------------------------------------------------------------------


import pymongo

client = pymongo.MongoClient(config.server)

extracts = client.asdf.extracts

request_count = extracts.find({'status':0}).count()

# make sure request was found
if request_count > 0:
    print "ready"

else:
    print "empty"
