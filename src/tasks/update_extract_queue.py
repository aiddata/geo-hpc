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


import time
from copy import deepcopy
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

raster_extract_items = []
release_extract_items = []

raster_total_count = 0
release_total_count = 0

build_start_time = int(time.time())

for group, group_bnds in bnd_groups.iteritems():

    print "running extract updates for boundary group: {0}".format(group)

    datasets = db_trackers[group].find({"status":1})

    for data in datasets:

        print "\tchecking dataset: {0} ({1})".format(data["name"], data["type"])

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

            base_count = len(raster_extract_items)

            raster_extract_items += [
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

            additional_raster_count = len(raster_extract_items) - base_count
            raster_total_count += additional_raster_count
            print "\t\tpotential dataset raster extracts: {0}".format(
                additional_raster_count)



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

            base_count = len(release_extract_items)

            release_extract_items += [
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

            additional_release_count = len(release_extract_items) - base_count
            release_total_count += additional_release_count
            print "\t\tpotential dataset release extracts: {0}".format(
                additional_release_count)

        else:

            msg = ("Invalid type ({0}) for dataset ({1}) "
                   "in tracker ({2})").format(data["type"], data["name"],
                                              group)
            warn(msg)



print "Potential raster extracts: {0}".format(raster_total_count)
print "Potential msr extracts: {0}".format(release_total_count)

build_end_time = int(time.time())
build_duration = build_end_time - build_start_time
print "time to build potential extract list: {0} seconds".format(build_duration)



# check if unique extract combinations exist in tracker
# and add if they do not

raster_add_count = 0
bulk_raster = c_extracts.initialize_unordered_bulk_op()

for i in raster_extract_items:

    # build full doc
    ctime = int(time.time())

    i_full = deepcopy(i)
    i_full["status"] = 0
    i_full["generator"] = "auto"
    i_full["priority"] = -1

    i_full["submit_time"] = ctime
    i_full["update_time"] = ctime

    bulk_raster.find(i).upsert().update({'$setOnInsert': i_full})


raster_result = bulk_raster.execute()
raster_add_count += raster_result['nUpserted']

print ("Added {0} raster extracts to queue out of {1} possible.").format(
    raster_add_count, raster_total_count)



release_add_count = 0
bulk_release = c_extracts.initialize_unordered_bulk_op()

for i in release_extract_items:

    # build full doc
    ctime = int(time.time())

    i_full = deepcopy(i)
    i_full["status"] = 0
    i_full["generator"] = "auto"
    i_full["priority"] = -1

    i_full["submit_time"] = ctime
    i_full["update_time"] = ctime

    bulk_release.find(i).upsert().update({'$setOnInsert': i_full})


release_result = bulk_release.execute()
release_add_count += release_result['nUpserted']

print ("Added {0} msr extracts to queue out of {1} possible.").format(
    release_add_count, release_total_count)




update_end_time = int(time.time())
update_duration = update_end_time - build_end_time
print "time to check and insert extract items: {0} seconds".format(update_duration)


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

