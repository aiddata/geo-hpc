"""
search and index datasets based on boundary
according to whether the datasets were found to be
relevant for that particular boundary
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


import pymongo
import fiona
from shapely.geometry import Point, shape, box
from shapely.ops import cascaded_union
import rasterstats as rs


# connect to mongodb
client = config.client
c_asdf = client.asdf.data
db_trackers = client.trackers

# lookup all boundary datasets
bnds = c_asdf.find({
    "type": "boundary",
    "options.group_class": "actual"
})


active_iso3_list = config.release_iso3.values() + config.other_iso3
print "Active iso3 list: {0}".format(active_iso3_list)

# for each boundary dataset get boundary tracker
for bnd in bnds:

    print "\nTracker for: {0}".format(bnd['options']['group'])
    print "\tdataset active status: {0}".format(bnd["active"])

    is_active = 0

    # manage active state for gadm boundaries based on config settings
    # do not process inactive boundaries
    if "extras" in bnd and "gadm_iso3" in bnd["extras"]:

        print "\tGADM iso3: {0}".format(bnd["extras"]["gadm_iso3"])
        is_active_gadm = bnd["extras"]["gadm_iso3"].upper() in active_iso3_list

        print "\tGADM boundary is active: {0}".format(is_active_gadm)

        if is_active_gadm:
            print "\t\tsetting group active"
            c_asdf.update_many({"options.group": bnd["options"]["group"], "active": 0}, {"$set":{"active": 1}})
            is_active = 1

        elif not is_active_gadm:
            print "\t\tsetting group inactive"
            c_asdf.update_many({"options.group": bnd["options"]["group"], "active": 1}, {"$set":{"active": 0}})
            continue


    if not is_active and bnd["active"] == 0:
        print "\tdataset inactive"
        continue


    # ---------------------------------

    print '\tInitializing and populating tracker...'

    if not bnd["options"]["group"] in db_trackers.collection_names():
        c_bnd = db_trackers[bnd["options"]["group"]]
        c_bnd.create_index("name", unique=True)
        c_bnd.create_index([("spatial", pymongo.GEOSPHERE)])
    else:
        c_bnd = db_trackers[bnd["options"]["group"]]

    # ---------------------------------


    # add each non-boundary dataset item to boundary tracker
    # collection with "unprocessed" flag if it is not already
    # in collection
    # (no longer done during ingest)
    dsets = c_asdf.find({
        'type': {'$ne': 'boundary'},
        'active': {'$gte': 1}
    })

    for full_dset in dsets:
        dset = {
            'name': full_dset["name"],
            'spatial': full_dset["spatial"],
            'scale': full_dset["scale"],
        }

        if c_bnd.find_one(dset) == None:
            dset['status'] = -1
            c_bnd.insert(dset)

    # ---------------------------------


    print '\tRunning relevance checks...'

    # lookup unprocessed data in boundary tracker that
    # intersect boundary (first stage search)
    matches = c_bnd.find({
        "status": -1,
        "$or": [
            {
                "spatial": {
                    "$geoIntersects": {
                        "$geometry": bnd["spatial"]
                    }
                }
            },
            {
                "scale": "global"
            }
        ]
    })

    # for each unprocessed dataset in boundary tracker matched in
    # first stage search (second stage search)
    # search boundary actual vs dataset actual
    for match in matches:
        print "\tChecking dataset: {0}".format(match['name'])

        # boundary base and type
        bnd_base = bnd['base'] +"/"+ bnd["resources"][0]["path"]
        bnd_type = bnd['type']

        meta = c_asdf.find({'name':match['name']})[0]

        if "active" in meta and meta["active"] == 0:
            c_bnd.update_one({"name": match['name']},
                             {"$set": {"status": -3}}, upsert=False)
            continue

        # dataset base and type
        dset_base = meta['base'] +"/"+ meta["resources"][0]["path"]
        dset_type = meta['type']

        result = False

        if dset_type == "raster":
            # python raster stats extract
            # bnd_geo = cascaded_union(
            #     [shape(shp) for shp in shapefile.Reader(bnd_base).shapes()])
            bnd_geo = cascaded_union([shape(shp['geometry'])
                                      for shp in fiona.open(bnd_base, 'r')])

            extract = rs.zonal_stats(bnd_geo, dset_base, stats="min max")

            if extract[0]['min'] != extract[0]['max']:
                result = True

        elif dset_type == "release":

            # iterate over active (premable, iso3) in
            # release_iso3 field of config
            for k, v in config.release_iso3.items():
                if (match['name'].startswith(k.lower()) and
                        (bnd["extras"]["gadm_iso3"].upper() in v or
                         "Global" in v)):

                    result = True


        # elif dset_type == "polydata":

        #   # shapely intersect
        #   bnd_geo = cascaded_union(
        #       [shape(shp) for shp in shapefile.Reader(bnd_base).shapes()])
        #   dset_geo = cascaded_union(
        #       [shape(shp) for shp in shapefile.Reader(dset_base).shapes()])

        #   intersect = bnd_geo.intersects(dset_geo)

        #   if intersect == True:
        #       result = True


        else:
            print ("\tError - Dataset type not yet supported (skipping)")
            c_bnd.update_one({"name": match['name']},
                             {"$set": {"status": -2}}, upsert=False)
            continue


        print '\t\tactive: {0}'.format(result)

        # check results and update tracker
        if result == True:
            c_bnd.update_one({"name": match['name']},
                             {"$set": {"status": 1}}, upsert=False)
        else:
            c_bnd.update_one({"name": match['name']},
                             {"$set": {"status": 0}}, upsert=False)




        # run third stage search on second stage matches
        # request actual vs dataset actual
        # may only be needed for user point input files
        #

        # update tracker for third stage search
        #



    # update tracker for all unprocessed dataset not matching first
    # stage search
    c_bnd.update_many({"status": -1}, {"$set": {"status": 0}}, upsert=False)

    # reset all inactive from placeholder status (-3) to unprocessed (-1)
    # so that their active state will be rechecked in case it changes
    #
    # Warning: datasets that have already been processed which are now inactive
    #          will be left alone. Applications should do their own checks on
    #          the active field.
    #
    # Note: As it related to this script, we must assume that
    #       a dataset is inactive because there is an error that may prohibit
    #       it being properly indexed, so it is continually left out until
    #       it is removed from data collection or set to active and indexed.
    c_bnd.update_many({"status": -3}, {"$set": {"status": -1}}, upsert=False)

