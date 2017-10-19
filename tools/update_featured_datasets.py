"""
loads config.json options from specified branch into mongodb

mongodb db/collection: info->config
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

else:

    import json

    client = config.client
    c_data = client.asdf.data

    featured_datasets = config.det['featured_datasets']


    # remove featured tag from datasets not in list
    c_data.update_many(
        {'name': {'$nin': featured_datasets}},
        {'$pull': {'extras.tags': 'featured'}}
    )

    # add featured tag to datasets in list
    c_data.update_many(
        {'name': {'$in': featured_datasets}},
        {'$addToSet': {'extras.tags': 'featured'}}
    )

    print "success"
