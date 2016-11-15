"""
update asdf/det extract tracker with all possible extract
combinations available from the asdf
"""


# -----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'utils')
sys.path.insert(0, config_dir)

from config_utility import *

config = BranchConfig(branch=branch)


# check mongodb connection
if config.connection_status != 0:
    sys.exit("connection status error: " + str(config.connection_error))

# -----------------------------------------------------------------------------


import time
import copy
from warnings import warn


# connect to mongodb
client = config.client
c_asdf = client.asdf.data
c_extracts = client.asdf.extracts
c_msr = client.asdf.msr
db_trackers = client.trackers

version = config.versions["extract-scripts"]


# lookup all boundary datasets
boundaries = c_asdf.find({
    "type": "boundary",
    "active": {'$gte': 1}
})

active_iso3_list = config.release_iso3.values() + config.other_iso3

# get boundary names
bnds_info = {
    b['resources'][0]['name']:b for b in boundaries
    if not 'gadm_iso3' in b['extras']
    or ('gadm_iso3' in b['extras']
        and b['extras']['gadm_iso3'].upper() in active_iso3_list)
}

bnd_groups = {}
for name, b in bnds_info.iteritems():
    group = b['options']['group']
    if not group in bnd_groups:
        bnd_groups[group] = []
    bnd_groups[group] += [name]


# -------------------------------------


# remove extract items in queue with old version(s)
# used as versioning for both queue and processing
#   - if queue generation changes version will change and all unprocessed
#     datasets will be removed and replaced
#   - if extract script changes version will change so that extracts from
#     old version of extracts scripts are no longer used
delete_call = c_extracts.delete_many({
    '$or': [
        {'boundary': {'$nin': bnds_info.keys()}},
        {'version': {'$ne': version}},
    ],
    'status': 0,
    'priority': -1
})

deleted_count = delete_call.deleted_count
print "\n"
print ("{0} unprocessed outdated automated extract "
       "requests have been removed.").format(deleted_count)


# -------------------------------------

extract_items = []

raster_total_count = 0
release_total_count = 0

for group, group_bnds in bnd_groups.iteritems():

    datasets = db_trackers[group].find({"status":1})

    for data in datasets:

        if data["type"] == "raster":

            raster = c_asdf.find_one({
                "name": data["name"],
                "active": {'$gte': 1}
            })

            if raster is None:
                msg = "No active raster found (name: {0})".format(
                    data["name"])
                warn(msg)
                continue

            extract_types = raster['options']['extract_types']

            base_count = len(extract_items)

            extract_items += [
                {
                    'boundary': b,
                    'data': r['name'],
                    'extract_type': e,
                    'version': version,
                    'classification': 'raster'
                }
                for r in raster['resources']
                for e in extract_types
                for b in group_bnds
            ]

            raster_total_count += len(extract_items) - base_count


        elif data["type"] == "release":


            release_filters = c_msr.find({
                "dataset": data["name"],
                "status": 1
            })

            ###
            tmp_extract_type = 'reliability'
            if data["name"].startswith('worldbank'):
                tmp_extract_type = 'sum'
            ###

            base_count = len(extract_items)

            extract_items += [
                {
                    'boundary': b,
                    'data': '{0}_{1}'.format(r["dataset"], r["hash"]),
                    'extract_type': tmp_extract_type,
                    'version': version,
                    'classification': 'msr'
                }
                for r in release_filters
                for b in group_bnds
            ]

            release_total_count += len(extract_items) - base_count

        else:

            msg = ("Invalid type ({0}) for dataset ({1}) "
                   "in tracker ({2})").format(data["type"], data["name"],
                                              group)
            warn(msg)



print ('Potential raster extracts: {0}').format(raster_total_count)
print ('Potential msr extracts: {0}').format(raster_total_count)



# check if unique extract combinations exist in tracker
# and add if they do not
raster_add_count = 0
release_add_count = 0

for i in extract_items:

    # build full doc
    ctime = int(time.time())

    i_full = copy.deepcopy(i)
    i_full["status"] = 0
    i_full["generator"] = "auto"
    i_full["priority"] = -1

    i_full["submit_time"] = ctime
    i_full["update_time"] = ctime


    # update/upsert and check if it exists in extracts queue
    exists = c_extracts.update_one(i, {'$setOnInsert': i_full}, upsert=True)

    if exists.upserted_id != None:
        if i['classification'] == 'raster':
            raster_add_count += 1
        elif i['classification'] == 'msr':
            release_add_count += 1


print ('Added {0} raster extracts to queue out of {1} possible.').format(
    raster_add_count, raster_total_count)
print ('Added {0} msr extracts to queue out of {1} possible.').format(
    release_add_count, release_total_count)



# -------------------------------------


# example extract queue documents

# raster

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


# msr

# {
#     "_id" : ObjectId("566baebf6050d566eca1f25d"),

#     "boundary" : "npl_adm3",

#     "raster" : "timorlesteaims_geocodedresearchrelease_level1_v1_4_1_47c6a3c265e1e605708560e30fb2e1662238b18b",
#     "extract_type" : "reliability",

#     "status" : 0,
#     "classification" : "automated",
#     "priority" : -1

#     "submit_time" : 1449897663,
#     "update_time" : 1450383510,
# }

