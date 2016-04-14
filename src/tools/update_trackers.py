# search and index datasets based on boundary


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

# -------------------------------------


# check mongodb connection
if config.connection_status != 0:
    sys.exit("connection status error: " + str(config.connection_error))


# -----------------------------------------------------------------------------


import pymongo
from osgeo import gdal,ogr,osr
# import shapefile
import pygeoj
from shapely.geometry import Point, shape, box
from shapely.ops import cascaded_union
import rasterstats as rs



# connect to mongodb
client = pymongo.MongoClient(config.server)
db = client[config.asdf_db]
c_data = db.data

# lookup all boundary datasets
bnds = c_data.find({"type": "boundary", "options.group_class": "actual"})


active_iso3_list = config.release_gadm.values() + config.other_gadm

print active_iso3_list

# for each boundary dataset get boundary tracker
for bnd in bnds:

    print "\n"
    print bnd['options']['group'] + ' tracker'

    print "active flag: " + str(bnd["active"])

    is_active = 0

    # manage active state for gadm boundaries based on config settings
    # do not process inactive boundaries
    if "gadm_info" in bnd:

        print bnd["gadm_info"]["iso3"]
        is_active_gadm = bnd["gadm_info"]["iso3"].upper() in active_iso3_list

        print "active gadm: "  + str(is_active_gadm)

        if bnd["active"] == 0 and is_active_gadm:
            print "setting active"
            c_data.update_one({"name": bnd["name"]}, {"$set":{"active": 1}})
            is_active = 1

        elif bnd["active"] == 1 and not is_active_gadm:
            print "setting inactive"
            c_data.update_one({"name": bnd["name"]}, {"$set":{"active": 0}})
            continue


    if not is_active and bnd["active"] == 0:
        print "inactive"
        continue


    # ---------------------------------

    print 'processing...'

    c_bnd = db[bnd["options"]["group"]]

    # ---------------------------------


    # add each non-boundary dataset item to boundary tracker collection with
    #   "unprocessed" flag if it is not already in collection
    # (no longer done in add gadm/release)
    dsets = c_data.find({"type": {"$ne": "boundary"}, "active": 1})
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
        print '\tchecking ' + match['name'] + ' dataset'

        # boundary base and type
        bnd_base = bnd['base'] +"/"+ bnd["resources"][0]["path"]
        bnd_type = bnd['type']

        meta = c_data.find({'name':match['name']})[0]

        if "active" in meta and meta["active"] == 0:
            c_bnd.update_one({"name": match['name']},
                             {"$set": {"status": -3}}, upsert=False)
            continue

        # dataset base and type
        dset_base = meta['base'] +"/"+ meta["resources"][0]["path"]
        dset_type = meta['type']

        result = False
        if meta['file_format'] in ["raster", "release"]:

            if bnd_type == "boundary" and dset_type == "raster":
                # python raster stats extract
                # bnd_geo = cascaded_union(
                #     [shape(shp) for shp in shapefile.Reader(bnd_base).shapes()])
                bnd_geo = cascaded_union([shape(shp.geometry)
                                          for shp in pygeoj.load(bnd_base)])

                extract = rs.zonal_stats(bnd_geo, dset_base, stats="min max")

                if extract[0]['min'] != extract[0]['max']:
                    result = True

            elif bnd_type == "boundary" and dset_type == "release":
                result = True

            else:
                print ("Error - Dataset type not yet supported (skipping " +
                       "dataset).\n")
                continue

            # check results and update tracker
            if result == True:
                c_bnd.update_one({"name": match['name']},
                                 {"$set": {"status": 1}}, upsert=False)
            else:
                c_bnd.update_one({"name": match['name']},
                                 {"$set": {"status": 0}}, upsert=False)

        # elif meta['format'] == "vector":

        #   if bnd_type == "boundary" and dset_type == "polydata":
        #       # shapely intersect
        #       bnd_geo = cascaded_union(
            #       [shape(shp) for shp in shapefile.Reader(bnd_base).shapes()])
    #           dset_geo = cascaded_union(
        #           [shape(shp) for shp in shapefile.Reader(dset_base).shapes()])

    #           intersect = bnd_geo.intersects(dset_geo)

        #       if intersect == True:
        #           result = True

        #   else:
        #       print ("Error - Dataset type not yet supported (skipping " +
            #          "dataset).\n")
        #       continue

        #   # check results and update tracker
        #   if result == True:
        #       c_bnd.update({"name": match['name']},
            #                {"$set": {"status": 1}}, upsert=False)
        #   else:
        #       c_bnd.update({"name": match['name']},
            #                {"$set": {"status": 0}}, upsert=False)

        else:
            # update tracker with error status for dataset and continue
            print ("Error - Invalid format for dataset \"" + match['name'] +
                  "\" in \"" + c_bnd + "\" tracker (skipping dataset).\n")
            c_bnd.update_one({"name": match['name']},
                             {"$set": {"status": -2}}, upsert=False)
            continue


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

