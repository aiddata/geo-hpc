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

from config_utility import BranchConfig

config_attempts = 0
while True:
    config = BranchConfig(branch=branch)
    config_attempts += 1
    if config.connection_status == 0 or config_attempts > 5:
        break

# -----------------------------------------------------------------------------


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
import pymongo
import fiona
from shapely.geometry import Point, shape, box
from shapely.ops import cascaded_union
import rasterstats as rs


# -------------------------------------
# prepare mongo connections

# connect to mongodb
client = config.client
c_asdf = client.asdf.data
db_trackers = client.trackers

# -------------------------------------


if job.rank == 0:
    # lookup all boundary datasets that are the "actual" for their group
    bnds = list(c_asdf.find({
        "type": "boundary",
        "options.group_class": "actual"
    }))

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


# active_iso3_list = config.release_iso3.values() + config.other_iso3
# print "Active iso3 list: {0}".format(active_iso3_list)

inactive_iso3_list = config.inactive_iso3
print "Inactive iso3 list: {0}".format(inactive_iso3_list)



def tmp_general_init(self):
    pass


def tmp_master_init(self):
    # start job timer
    self.Ts = int(time.time())
    self.T_start = time.localtime()
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)


def tmp_master_process(self, worker_result):
    pass


def tmp_master_final(self):

    # stop job timer
    T_run = int(time.time() - self.Ts)
    T_end = time.localtime()
    print '\n\n'
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)
    print 'End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end)
    print 'Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s'
    print '\n\n'


def tmp_worker_job(self, task_index, task_data):

    worker_tagline = "Worker {0} | Task {1} - ".format(self.rank, task_index)

    # for each boundary dataset get boundary tracker
    bnd = task_data

    print "\n\tTracker for: {0}".format(bnd['options']['group'])
    print "\t\tdataset active status: {0}".format(bnd["active"])

    is_active = 0

    # manage active state for gadm boundaries based on config settings
    # do not process inactive boundaries
    if "extras" in bnd and "gadm_iso3" in bnd["extras"]:

        print "\t\tGADM iso3: {0}".format(bnd["extras"]["gadm_iso3"])
        # is_active_gadm = bnd["extras"]["gadm_iso3"].upper() in active_iso3_list
        is_active_gadm = bnd["extras"]["gadm_iso3"].upper() not in inactive_iso3_list

        print "\t\tGADM boundary is active: {0}".format(is_active_gadm)

        if is_active_gadm:
            print "\t\t\tsetting group active"
            c_asdf.update_many({"options.group": bnd["options"]["group"], "active": 0}, {"$set":{"active": 1}})
            is_active = 1

        elif not is_active_gadm:
            print "\t\t\tsetting group inactive"
            c_asdf.update_many({"options.group": bnd["options"]["group"], "active": 1}, {"$set":{"active": 0}})
            return


    if not is_active and bnd["active"] == 0:
        print "\t\tdataset inactive"
        return


    # ---------------------------------

    print '\t\tInitializing and populating tracker...'

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


    print '\t\tRunning relevance checks...'

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
        print "\t\tChecking dataset: {0}".format(match['name'])

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
                         "global" in v)):

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

    return


def tmp_get_task_data(self, task_index, source):
    # task_data = self.task_list[task_index]
    # return task_data


    print ("Master - starting request search for Worker {0} "
           "(Task Index: {1})").format(
                source, task_index)

    return bnds[task_index]



# init / run job
job.set_task_count(len(bnds))

job.set_general_init(tmp_general_init)
job.set_master_init(tmp_master_init)
job.set_master_process(tmp_master_process)
job.set_master_final(tmp_master_final)
job.set_worker_job(tmp_worker_job)
job.set_get_task_data(tmp_get_task_data)

job.run()



