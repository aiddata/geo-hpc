# 

# --------------------------------------------------

import sys
import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
sys.path.insert(0, config_dir)

from config_utility import *

config = BranchConfig(branch=branch)


# --------------------------------------------------

import pymongo


client = pymongo.MongoClient(config.server)

msr = client[config['det-test']].msr

print msr.find().limit(1)[0]
