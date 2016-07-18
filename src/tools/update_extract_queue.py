# update asdf/det extract tracker with all possible extract
# combinations available from the asdf

# -----------------------------------------------------------------------------

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


# check mongodb connection
if config.connection_status != 0:
    sys.exit("connection status error: " + str(config.connection_error))

# -----------------------------------------------------------------------------


import time
import copy


# connect to mongodb
client = config.client
c_asdf = client.asdf.data
c_extracts = client.asdf.extracts

version = config.versions["extract-scripts"]


# lookup all boundary datasets
boundaries = c_asdf.find({
    "type": "boundary",
    "active": {'$gte': 1}
})

active_iso3_list = config.release_gadm.values() + config.other_gadm

# get boundary names
bnds = [
    b['resources'][0]['name'] for b in boundaries
    if not 'gadm_iso3' in b['extras']
    or ('gadm_iso3' in b['extras']
        and b['extras']['gadm_iso3'].upper() in active_iso3_list)
]

# -------------------------------------


# remove items in queue with old version(s)
# used as versioning for both queue and processing
#   - if queue generation changes version will change and all unprocessed
#     datasets will be removed and replaced
#   - if extract script changes version will change so that extracts from
#     old version of extracts scripts are no longer used
delete_call = c_extracts.delete_many({
    '$or': [
        {'boundary': {'$nin': bnds}},
        {'version': {'$ne': version}},
    ],
    'status': 0,
    'priority': -1
})

deleted_count = delete_call.deleted_count
print '\n'
print (str(deleted_count) + ' unprocessed outdated automated extract ' +
       'requests have been removed.')


# -------------------------------------
# add raster extracts

# lookup all raster datasets
rasters = c_asdf.find({
    "type": "raster",
    "active": {'$gte': 1}
})


items = []

# build list of dicts for all combinations of boundary names,
# rasters names/reliabiity and respective raster extract types
for raster in rasters:

    extract_types = raster['options']['extract_types']

    items += [
        {
            'boundary': b,
            'data': r['name'],
            'extract_type': e,
            'version': version
        }
        for r in raster['resources']
        for e in extract_types
        for b in bnds
    ]


# check if unique extract combinations exist in tracker
# and add if they do not
add_count = 0
for i in items:

    # build full doc
    ctime = int(time.time())

    i_full = copy.deepcopy(i)
    i_full["status"] = 0
    i_full["classification"] = "raster"
    i_full["generator"] = "auto"
    i_full["priority"] = -1

    i_full["submit_time"] = ctime
    i_full["update_time"] = ctime


    # update/upsert and check if it exists in extracts queue
    exists = c_extracts.update_one(i, {'$setOnInsert': i_full}, upsert=True)

    if exists.upserted_id != None:
        add_count += 1


print ('Added ' + str(add_count) + ' items to extract queue (' +
       str(len(items)) + ' total possible).')


# -------------------------------------


# example extract tracker document

# {
#     "_id" : ObjectId("566baebf6050d566eca1f25d"),

#     "boundary" : "npl_adm3",

#     "raster" : "selv",
#     "extract_type" : "mean",

#     "status" : 0,
#     "classification" : "automated",
#     "priority" : -1

#     "submit_time" : 1449897663,
#     "update_time" : 1450383510,
# }
