"""
loads config.json options from specified branch into mongodb

mongodb db/collection: info->config
"""


# ----------------------------------------------------------------------------

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


# ----------------------------------------------------------------------------


# check mongodb connection
if config.connection_status != 0:
    print "error"
    # sys.exit("connection status error: " + str(config.connection_error))


# ----------------------------------------------------------------------------


import json


client = config.client


branch_info = config.branch_settings


c_config = client.asdf.config

# c_config.remove({'name': branch_info['name']})
c_config.remove({})

c_config.insert(branch_info)

