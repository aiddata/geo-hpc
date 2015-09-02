# search datasets based on boundary

import sys
import os
import pymongo
from osgeo import gdal,ogr,osr
# import shapefile
import pygeoj
from shapely.geometry import Point, shape, box
from shapely.ops import cascaded_union
import rasterstats as rs 

# user inputs
# in_trigger = sys.argv[1]

# check trigger that initiated search
# 

# connect to mongodb
client = pymongo.MongoClient()
db = client.asdf
c_data = db.data

# lookup all boundary datasets
bnds = c_data.find({"type": "boundary", "options.group_class": "actual"})

# for each boundary dataset get boundary tracker
for bnd in bnds:
    
    print 'processing ' + bnd['options']['group'] + ' tracker...'
    
    c_bnd = db[bnd["options"]["group"]]

    # get boundary bbox
    geo = bnd["spatial"]

    # lookup all unprocessed data in boundary tracker
    uprocs = c_bnd.find({"status": -1})

    # lookup unprocessed data in boundary tracker that intersect boundary (first stage search)
    matches = c_bnd.find({
                            "status": -1,
                            "$or": [
                                {
                                    "spatial": {
                                        "$geoIntersects": {
                                            "$geometry": geo
                                        }
                                    }
                                },
                                {
                                    "scale": "global"
                                }
                            ]               
                        })

    # for each unprocessed dataset in boundary tracker matched in first stage search (second stage search)
    # search boundary actual vs dataset actual
    for match in matches:
        print '\tchecking ' + match['name'] + ' dataset'

        # boundary base and type
        bnd_base = bnd['base'] +"/"+ bnd["resources"][0]["path"]
        bnd_type = bnd['type']

        meta = c_data.find({'name':match['name']})[0]

        # dataset base and type
        dset_base = meta['base'] +"/"+ meta["resources"][0]["path"]
        dset_type = meta['type'] 

        result = False
        if meta['file_format'] == "raster":

            if bnd_type == "boundary" and dset_type == "raster":
                # python raster stats extract
                # bnd_geo = cascaded_union([shape(shp) for shp in shapefile.Reader(bnd_base).shapes()])
                bnd_geo = cascaded_union([shape(shp.geometry) for shp in pygeoj.load(bnd_base)])

                extract = rs.zonal_stats(bnd_geo, dset_base, stats="min max")

                if extract[0]['min'] != extract[0]['max']:
                    result = True

            else:
                print "Error - Dataset type not yet supported (skipping dataset).\n"
                continue

            # check results and update tracker
            if result == True:
                c_bnd.update_one({"name": match['name']},{"$set": {"status": 1}}, upsert=False)
            else:
                c_bnd.update_one({"name": match['name']},{"$set": {"status": 0}}, upsert=False)

        # elif meta['format'] == "vector":

        #   if bnd_type == "boundary" and dset_type == "polydata":
        #       # shapely intersect
        #       bnd_geo = cascaded_union([shape(shp) for shp in shapefile.Reader(bnd_base).shapes()])
    #           dset_geo = cascaded_union([shape(shp) for shp in shapefile.Reader(dset_base).shapes()])
                
    #           intersect = bnd_geo.intersects(dset_geo)

        #       if intersect == True:
        #           result = True

        #   else:
        #       print "Error - Dataset type not yet supported (skipping dataset).\n"
        #       continue

        #   # check results and update tracker
        #   if result == True:
        #       c_bnd.update({"name": match['name']},{"$set": {"status": 1}}, upsert=False)
        #   else:
        #       c_bnd.update({"name": match['name']},{"$set": {"status": 0}}, upsert=False)

        else:
            # update tracker with error status for dataset and continue
            print "Error - Invalid format for dataset \"" + match['name'] + "\" in \"" + c_bnd + "\" tracker (skipping dataset).\n"
            c_bnd.update_one({"name": match['name']},{"$set": {"status": -2}}, upsert=False)
            continue


        # run third stage search on second stage matches
        # request actual vs dataset actual 
        # may only be needed for user point input files
        # 

        # update tracker for third stage search
        # 


    # update tracker for all unprocessed dataset not matching first stage search
    for uproc in uprocs:
        if uproc['status'] == -1:
            c_bnd.update_many({"name": uproc['name']},{"$set": {"status": 0}}, upsert=False)
