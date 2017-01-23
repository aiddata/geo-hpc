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


# -------------------------------------
# initialize mpi

import mpi_utility
job = mpi_utility.NewParallel(capture=True, print_worker_log=False)


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
from copy import deepcopy
from warnings import warn


# -------------------------------------
# prepare mongo connections

client = config.client
c_asdf = client.asdf.data

if not 'extracts' in client.asdf.collection_names():
    c_extracts = client.asdf.extracts

    extract_indexes = [
        pymongo.IndexModel([('boundary', 1)]),
        pymongo.IndexModel([('data', 1)]),
        pymongo.IndexModel([('extract_type', 1)]),
        pymongo.IndexModel([('version', 1)]),
        pymongo.IndexModel([('classification', 1)])
    ]

    db.test.create_indexes(extract_indexes)

else:
    c_extracts = client.asdf.extracts


c_msr = client.asdf.msr
db_trackers = client.trackers

version = config.versions["extract-scripts"]


# -------------------------------------

if job.rank == 0:

    # lookup all boundary datasets
    boundaries = c_asdf.find({
        "type": "boundary",
        "active": {'$gte': 1}
    })

    # active_iso3_list = config.release_iso3.values() + config.other_iso3
    inactive_iso3_list = config.inactive_iso3

    # get boundary names
    bnds_info = {
        b['resources'][0]['name']:b for b in boundaries
        if not 'gadm_iso3' in b['extras']
        or ('gadm_iso3' in b['extras']
            # and b['extras']['gadm_iso3'].upper() in active_iso3_list)
            and b['extras']['gadm_iso3'].upper() not in inactive_iso3_list)

    }

    bnd_groups = {}
    for name, b in bnds_info.iteritems():
        # skip adm0
        if 'extras' in b and 'gadm_adm' in b['extras'] and b['extras']['gadm_adm'] == 0:
            continue
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

                for e in extract_types:

                    if e == "categorical":
                        category_map = raster['options']['category_map']

                        raster_extract_items += [
                            {
                                'boundary': b,
                                'data': r['name'],
                                'extract_type': e,
                                'category_map': category_map,
                                'version': version,
                                'classification': 'raster'
                            }
                            for r in raster['resources']
                            for b in group_bnds
                        ]

                    else:
                        raster_extract_items += [
                            {
                                'boundary': b,
                                'data': r['name'],
                                'extract_type': e,
                                'version': version,
                                'classification': 'raster'
                            }
                            for r in raster['resources']
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


    print ""
    print "Potential raster extracts: {0}".format(raster_total_count)
    print "Potential msr extracts: {0}".format(release_total_count)

    build_end_time = int(time.time())
    build_duration = build_end_time - build_start_time
    print "Time to build potential extract list: {0} seconds".format(build_duration)
    print ""



def tmp_general_init(self):
    self.count_added = 0


def tmp_master_init(self):
    # start job timer
    self.Ts = int(time.time())
    self.T_start = time.localtime()
    print '\n\n'
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)
    print '\n\n'


def tmp_master_process(self, worker_result):
    self.count_added += worker_result


def tmp_master_final(self):

    print "Count added: {0}".format(self.count_added)

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
    print worker_tagline

    # check if unique extract combinations exist in tracker
    # and add if they do not

    # build full doc
    ctime = int(time.time())

    i_full = deepcopy(task_data)
    i_full["status"] = 0
    i_full["generator"] = "auto"
    i_full["priority"] = -1

    i_full["submit_time"] = ctime
    i_full["update_time"] = ctime

    exists = c_extracts.update_one(task_data, {'$setOnInsert': i_full}, upsert=True)

    add_count = 0
    if exists.upserted_id != None:
        add_count = 1

    return add_count




# init jobs
job.set_general_init(tmp_general_init)
job.set_master_init(tmp_master_init)
job.set_master_process(tmp_master_process)
job.set_master_final(tmp_master_final)
job.set_worker_job(tmp_worker_job)



# run raster job

def tmp_get_task_data(self, task_index, source):
    # task_data = self.task_list[task_index]
    # return task_data


    print ("Master - starting request search for Worker {0} "
           "(Task Index: {1})").format(
                source, task_index)

    return raster_extract_items[task_index]


if job.rank == 0:
    job.set_task_count(len(raster_extract_items))

job.set_get_task_data(tmp_get_task_data)

if job.rank == 0:
    print "Checking raster extracts: {0}".format(raster_total_count)

job.run()

if job.rank == 0:
    print "Potential raster extracts: {0}".format(raster_total_count)



print '\n'

# run release job

def tmp_get_task_data(self, task_index, source):
    # task_data = self.task_list[task_index]
    # return task_data


    print ("Master - starting request search for Worker {0} "
           "(Task Index: {1})").format(
                source, task_index)

    return release_extract_items[task_index]


if job.rank == 0:
    job.set_task_count(len(release_extract_items))

job.set_get_task_data(tmp_get_task_data)

if job.rank == 0:
    print "Checking msr extracts: {0}".format(release_total_count)

job.run()

if job.rank == 0:
    print "Potential msr extracts: {0}".format(release_total_count)





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

#     "raster" : "timorlesteaims_geocodedresearchrelease_level1_
#                   v1_4_1_47c6a3c265e1e605708560e30fb2e1662238b18b",
#     "extract_type" : "reliability",

#     "status" : 0,
#     "classification" : "automated",
#     "priority" : -1

#     "submit_time" : 1449897663,
#     "update_time" : 1450383510,
# }

