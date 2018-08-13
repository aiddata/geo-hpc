"""
loads config.json options from specified branch into mongodb

mongodb db/collection: info->config

Note:
BranchConfig instance from config_utility loads directly from config.json,
not the existing config collections. May be better to have config_utility
load from config collection and this script load directly from config.json
(potential change in future)

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

    branch_info = config.branch_settings

    c_config = client.asdf.config

    # c_config.remove({'name': branch_info['name']})
    c_config.delete_many({})

    c_config.inset_one(branch_info)

    print "success"
