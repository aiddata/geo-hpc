"""
generic script for editing data in "trackers" db
"""

# -----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'geo', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'geo-hpc/utils')
sys.path.insert(0, config_dir)

from config_utility import BranchConfig

config_attempts = 0
while True:
    config = BranchConfig(branch=branch)
    config_attempts += 1
    if config.connection_status == 0 or config_attempts > 5:
        break


# -----------------------------------------------------------------------------


if config.connection_status != 0:
    print "mongodb connection error ({0})".format(config.connection_error)
    sys.exit()


# -----------------------------------------------------------------------------


import pymongo


# connect to mongodb
client = config.client
db_trackers = client.trackers


for bnd_group in db_trackers.collection_names():

    c_bnd = db_trackers[bnd_group]

    find = {
        "name": "worldbank_geocodedresearchrelease_level1_v1_4_1"
    }

    update = {
        "$set": {"status": -1}
    }

    c_bnd.update_one(find, update, upsert=False)
