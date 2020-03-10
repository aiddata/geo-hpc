"""
search and index datasets based on boundary
according to whether the datasets were found to be
relevant for that particular boundary

boundary dataset status types:
 2  forced active (cannot be set inactive by automated processes)
 1  active (normal)
 0  inactive (normal)
-1  forced inactive (cannot be set active by automated processes)
-2  group forced inactive (no datasets in respective boundary group can be set
        active by automated processes, currently only applies to "actual"
        boundaries which define their group)

"""

# -----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

utils_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_dir)

from config_utility import BranchConfig

config = BranchConfig(branch=branch)
config.test_connection()

# -----------------------------------------------------------------------------

import geo_rasterstats as rs


# -------------------------------------
# initialize mpi

import mpi_utility
job = mpi_utility.NewParallel(capture=True)


# -------------------------------------
# verify good mongo connection on all processors

connect_status = job.comm.gather(
    (job.rank, config.connection_status, config.connection_error), root=0)

if job.rank == 0:
    connection_error = False
    for i in connect_status:
        if i[1] != 0:
            print ("mongodb connection error ({0} - {1}) "
                   "on processor rank {2})").format(i[1], i[2], [3])
            connection_error = True

    if connection_error:
        sys.exit()


job.comm.Barrier()


# -------------------------------------
# load libraries now that job is initialized

import time
import random
import itertools
import pymongo
import fiona
import rasterio
import numpy as np
from shapely.geometry import Point, shape, box
from shapely.ops import cascaded_union
from pprint import pprint

# -------------------------------------
# prepare mongo connections

client = config.client
c_asdf = client.asdf.data
db_trackers = client.trackers
db_releases = client.releases

# -------------------------------------


if job.rank == 0:

    # update active status for all boundaries based on config
    inactive_bnds_list = config.inactive_bnds
    print "Inactive boundaries list: {0}".format(inactive_bnds_list)

    c_asdf.update_many({"type": "boundary", "name": {"$in": inactive_bnds_list}, "active": 1},
                       {"$set":{"active": 0}})

    c_asdf.update_many({"type": "boundary", "name": {"$nin": inactive_bnds_list}, "active": 0},
                       {"$set":{"active": 1}})


    # lookup all boundary datasets that are the "actual" for their group
    bnds = list(c_asdf.find({
        "type": "boundary",
        "options.group_class": "actual",
        'active': {'$gte': -1}
    }))

    random.shuffle(bnds)

    def bnd_item(name):
        return {
            "name": name,
            "status": "waiting",
            "start": -1,
            "runtime": -1,
        }

    boundary_tracker = {i["name"]: bnd_item(i["name"]) for i in bnds}

    boundary_remaining = [i["name"] for i in bnds]
    boundary_running = []
    boundary_completed = []

else:
    # lookup all active non boundary dataset
    dsets = list(c_asdf.find({
        'type': {'$ne': 'boundary'},
        'active': {'$gte': 1}
    }, {
        'name': 1,
        'type': 1,
        'spatial': 1,
        'scale': 1
    }))




def tmp_general_init(self):
    pass


def tmp_master_init(self):
    # start job timer
    self.Ts = int(time.time())
    self.T_start = time.localtime()
    self.last_update = time.time()
    print '\n\n'
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)
    print '\n\n'


def tmp_master_process(self, worker_result):
    boundary_tracker[worker_result]["status"] = "complete"
    boundary_tracker[worker_result]["runtime"] = int(time.time()) - boundary_tracker[worker_result]["start"]
    boundary_running.remove(worker_result)
    boundary_completed.append(worker_result)
    print "\nBoundaries running ({}): \n{}".format(len(boundary_running), boundary_running)
    print "\nBoundaries remaining ({}): \n{}".format(len(boundary_remaining), boundary_remaining)
    mT_run = int(time.time() - self.Ts)
    self.last_update = time.time()
    print '\nCurrent Master Runtime: ' + str(mT_run//60) +'m '+ str(int(mT_run%60)) +'s'


def tmp_master_update(self):
    print "\nBoundaries running ({}): \n{}".format(len(boundary_running), boundary_running)
    print "\nBoundaries remaining ({}): \n{}".format(len(boundary_remaining), boundary_remaining)
    runtime = int(time.time() - self.Ts)
    print '\nCurrent Master Runtime: ' + str(runtime//60) +'m '+ str(int(runtime%60)) +'s'
    since_update = int(time.time() - self.last_update)
    print '\nTime since last update: ' + str(since_update//60) +'m '+ str(int(since_update%60)) +'s'


def tmp_master_final(self):

    # stop job timer
    T_run = int(time.time() - self.Ts)
    T_end = time.localtime()
    print '\n\n'
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)
    print 'End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end)
    print 'Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s'
    print '\n\n'
    pprint(boundary_tracker)


def tmp_worker_job(self, task_index, task_data):

    worker_start_time = int(time.time())

    worker_tagline = "Worker {0} | Task {1} - ".format(self.rank, task_index)
    print worker_tagline

    # for each boundary dataset get boundary tracker
    bnd = task_data

    print "\tTracker for: {0}".format(bnd['options']['group'])
    print "\t\tName: {0}".format(bnd["name"])
    print "\t\tActive: {0}".format(bnd["active"])


    # ---------------------------------

    print '\t\tInitializing and populating tracker...'

    if not bnd["options"]["group"] in db_trackers.collection_names():
        c_bnd = db_trackers[bnd["options"]["group"]]
        c_bnd.create_index("name")#, unique=True)
        c_bnd.create_index([("spatial", pymongo.GEOSPHERE)])
    else:
        c_bnd = db_trackers[bnd["options"]["group"]]

    # ---------------------------------


    # add each non-boundary dataset item to boundary tracker
    # collection with "unprocessed" flag if it is not already
    # in collection
    # (no longer done during ingest)

    for full_dset in dsets:
        dset = {
            'name': full_dset["name"],
            'type': full_dset["type"],
            'spatial': full_dset["spatial"],
            'scale': full_dset["scale"]
        }

        if c_bnd.find_one(dset) == None:
            dset['status'] = -1
            c_bnd.insert(dset)

    # ---------------------------------

    worker_tmp_runtime = int(time.time() - worker_start_time)
    print '\t\t\t...worker running for {}m {}s [#1]'.format(worker_tmp_runtime//60, int(worker_tmp_runtime%60))

    print '\t\tRunning relevance checks...'

    # lookup unprocessed data in boundary tracker that
    # intersect boundary (first stage search)

    search_status_list = [-1]

    if bnd["scale"] == "global":
        # NOTE: intersect/within at global (ie, >hemispehere)
        # may not work properly. using this as temp workaround
        # could potentially be impacting smaller datasets as well, not sure
        matches = list(c_bnd.find({
            "status": {"$in": search_status_list}
        }))
    else:
        matches = list(c_bnd.find({
            "status": {"$in": search_status_list},
            "$or": [
                {
                    "spatial": {
                        "$geoIntersects": {
                            "$geometry": bnd["spatial"]
                        }
                    }
                },
                # {
                #     "spatial": {
                #         "$geoWithin": {
                #             "$geometry": bnd["spatial"]
                #         }
                #     }
                # },
                {
                    "scale": "global"
                }
            ]
        }))

    print '\t\t{0} matches found'.format(len(matches))

    worker_tmp_runtime = int(time.time() - worker_start_time)
    print '\t\t\t...worker running for {}m {}s [#2]'.format(worker_tmp_runtime//60, int(worker_tmp_runtime%60))

    if len(matches) > 0:
        # boundary base and type
        bnd_path = os.path.join(bnd['base'], bnd["resources"][0]["path"])
        bnd_type = bnd['type']

        # bnd_geo = cascaded_union([shape(shp['geometry']) for shp in fiona.open(bnd_base, 'r')])

        with fiona.open(bnd_path, 'r') as bnd_src:
            minx, miny, maxx, maxy = bnd_src.bounds
            total_area = sum([shape(i['geometry']).area for i in bnd_src])


    # for each unprocessed dataset in boundary tracker matched in
    # first stage search (second stage search)
    # search boundary actual vs dataset actual
    for match in matches:
        print "\t\tChecking dataset: {0}".format(match['name'])

        meta_search = list(c_asdf.find({'name': match['name']}))

        if len(meta_search) == 0:
            print '\t\t\tCould not find dataset'
            c_bnd.update_one({"name": match['name']},
                             {"$set": {"status": -3}}, upsert=False)
            continue

        meta = meta_search[0]

        if "active" in meta and meta["active"] == 0:
            print '\t\t\tDataset inactive'
            c_bnd.update_one({"name": match['name']},
                             {"$set": {"status": -3}}, upsert=False)
            continue

        # dataset base and type
        test_resource = meta["resources"][0]["path"]
        if test_resource != "":
            dset_path = meta['base'] +"/"+ meta["resources"][0]["path"]
        else:
            dset_path = meta['base']

        dset_type = meta['type']

        result = False

        if bnd["scale"] == "global":
            print '\t\t\tGlobal boundary'
            result = True

        elif dset_type == "raster":
            # true extract takes too long and is too costly to run
            # use a simple test of sample points over boundary bounding box
            # to do a good enough check of whether the data is relevant to boundary

            raster_src = rasterio.open(dset_path)

            pixel_size = raster_src.meta['transform'][1]
            nodata = raster_src.meta['nodata']

            xsize = (maxx - minx) / pixel_size
            ysize = (maxy - miny) / pixel_size


            # -----
            # this section creates the sample of pixels within extents of boundary data
            # *** potential flaw here is that samples are only within the extet, but
            #     not necessarily within the actual boundary. For data such as islands
            #     which have small areas but cover large extents, and which are surrounded
            #     by nodata vals, this could be an issue

            # minimum ratio of valid pixels required
            valid_sample_thresh = 0.05
            # maximum number of pixels to test
            pixel_limit = 10000

            # init as > than limit to force one run of loop
            sampled_pixel_count = pixel_limit + 1
            step_size = pixel_size * 1

            while sampled_pixel_count > pixel_limit:
                xvals = np.arange(minx, maxx, step_size)
                yvals = np.arange(miny, maxy, step_size)
                samples = list(itertools.product(xvals, yvals))
                sampled_pixel_count = len(samples)
                # increase step size until sample pixel count is small enough
                step_size = pixel_size * 2

            # -----


            values = [val[0] for val in raster_src.sample(samples)]

            raster_src.close()

            clean_values = [i for i in values if i != nodata and i is not None]

            distinct_values = set(clean_values)

            # percent of samples resulting in clean value
            if len(clean_values) > len(samples)*valid_sample_thresh and len(distinct_values) > 1:
                result = True
            else:
                print '\t\t\tPixel check did not pass'


            # else:
            #     # python raster stats extract
            #     extract = rs.gen_zonal_stats(bnd_path, dset_path, stats="min max", limit=200000)

            #     for i in extract:
            #         if i['min'] != None or i['max'] != None:
            #             result = True
            #             break

        elif dset_type == "release":

            # iterate over active (premable, iso3) in
            # release_iso3 field of config
            for k, v in config.release_iso3.items():
                if match['name'].startswith(k.lower()):
                    if ("gadm_iso3" in bnd["extras"] and bnd["extras"]["gadm_iso3"].upper() in v) or ("iso3" in bnd["extras"] and bnd["extras"]["iso3"].upper() in v):
                        result = True

                    elif "global" in v:

                        bnd_coords = bnd['spatial']['coordinates']

                        bnd_minx = bnd_coords[0][0][0]
                        bnd_miny = bnd_coords[0][1][1]
                        bnd_maxx = bnd_coords[0][2][0]
                        bnd_maxy = bnd_coords[0][0][1]

                        loc_count = db_releases[match['name']].count({
                            'locations.longitude': {'$gte': bnd_minx, '$lte': bnd_maxx},
                            'locations.latitude': {'$gte': bnd_miny, '$lte': bnd_maxy}
                        })

                        print "\t\t\t{0} locations found".format(loc_count)
                        if loc_count > 0:
                            result = True



        # elif dset_type == "polydata":

        #   # shapely intersect
        #   bnd_geo = cascaded_union(
        #       [shape(shp) for shp in shapefile.Reader(bnd_path).shapes()])
        #   dset_geo = cascaded_union(
        #       [shape(shp) for shp in shapefile.Reader(dset_path).shapes()])

        #   intersect = bnd_geo.intersects(dset_geo)

        #   if intersect == True:
        #       result = True


        else:
            print ("\t\tError - Dataset type not yet supported (skipping)")
            c_bnd.update_one({"name": match['name']},
                             {"$set": {"status": -2}}, upsert=False)
            continue


        print '\t\t\tactive: {0}'.format(result)

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

    worker_tmp_runtime = int(time.time() - worker_start_time)
    print '\t\t\t...worker running for {}m {}s [#3]'.format(worker_tmp_runtime//60, int(worker_tmp_runtime%60))

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

    worker_tmp_runtime = int(time.time() - worker_start_time)
    print '\t\t\t...worker running for {}m {}s [#4]'.format(worker_tmp_runtime//60, int(worker_tmp_runtime%60))

    return bnd["name"]


def tmp_get_task_data(self, task_index, source):
    # task_data = self.task_list[task_index]
    # return task_data


    print ("Master - starting request search for Worker {0} "
           "(Task Index: {1})").format(
                source, task_index)

    task_name = bnds[task_index]["name"]

    boundary_tracker[task_name]["status"] = "running"
    boundary_tracker[task_name]["start"] = int(time.time())

    boundary_remaining.remove(task_name)
    boundary_running.append(task_name)

    return bnds[task_index]





# init / run job
if job.rank == 0:
    job.set_task_count(len(bnds))
    print("########## Running {} Tasks (Boundaries) ##########".format(len(bnds)))

job.set_general_init(tmp_general_init)
job.set_master_init(tmp_master_init)
job.set_master_process(tmp_master_process)

update_interval = 60*5
job.set_master_update(tmp_master_update)
job.set_update_interval(update_interval)

job.set_master_final(tmp_master_final)
job.set_worker_job(tmp_worker_job)
job.set_get_task_data(tmp_get_task_data)

job.run()
