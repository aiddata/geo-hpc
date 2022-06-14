#

import sys
import os
import traceback
import re
import errno
import time
from random import randint
from datetime import datetime
import types
import random

import pymongo
from bson import ObjectId


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

import extract_utility


# -------------------------------------
# initialize mpi

import mpi_utility
job = mpi_utility.NewParallel()


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
# get / check version

version = config.versions["extracts"]


# -------------------------------------
# prepare mongo connections

client = config.client
c_asdf = client.asdf.data
c_extracts = client.asdf.extracts
# c_features = client.asdf.features


# -------------------------------------
# prepare output path

general_output_base = os.path.join(
    config.branch_dir, 'outputs/extracts',  version.replace('.', '_'))

# -------------------------------------
# number of time mongo query/update for request can fail
# to find and update a request before giving up
#
#   (soemtimes multiple jobs will pull same request and
#   only one can be the first to update. if this happens 100
#   times, something else is probably going on)
job.search_limit = 100


# -------------------------------------
# interval at which to update `update_time` field for active extract tasks
# time in seconds

update_interval = 60*10


# -------------------------------------
# job type definitions for request queries

job.job_type = 'default'
if len(sys.argv) >= 3:
    job.job_type = sys.argv[2]


if job.job_type == 'default':
    # runs anything
    job.generator_list = None
    job.classification_list = None

elif job.job_type == 'det':
    job.generator_list = ['det']
    job.classification_list = None

elif job.job_type == 'raster':
    job.generator_list = None
    job.classification_list = ['raster']

elif job.job_type == 'msr':
    job.generator_list = None
    job.classification_list = ['msr']

elif 'errors' in job.job_type:
    job.generator_list = None
    job.classification_list = None


if job.rank == 0:
    print("running job type: {0}".format(job.job_type))



# -------------------------------------
# number of extracts requests to process per job

extract_limit = 200

if len(sys.argv) >= 4:
    extract_limit = int(sys.argv[3])


# limit of 0 = no limit
# limit of -1 means set limit to # workers (single cycle on cores)
if extract_limit == -1:
    extract_limit = job.size -1


# -------------------------------------
# pixel_limit definitions for request queries

job.pixel_limit = 250000

if len(sys.argv) >= 5:
    job.pixel_limit = int(sys.argv[4])


def tprint(x):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("{0} -- {1}".format(timestamp, x))


# =============================================================================
# =============================================================================

# define functions for parallel job instance

def tmp_general_init(self):
    pass


def tmp_master_init(self):
    self.potential_request_list = []
    # start job timer
    self.Ts = int(time.time())
    self.T_start = time.localtime()
    print('Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start))


def tmp_master_process(self, worker_result):
    pass


def tmp_master_update(self):
    update_time = int(time.time())
    for i in self.worker_log.values():
        ctd = i['current_task_data']
        if ctd is not None:
            # update status of item in extract queue
            update_extract = c_extracts.update_one({
                '_id': ObjectId(ctd['_id'])
            }, {
                '$set': {
                    'update_time': update_time
                }
            }, upsert=False)


def tmp_master_final(self):

    # stop job timer
    T_run = int(time.time() - self.Ts)
    T_end = time.localtime()
    print('\n\n')
    print('Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start))
    print('End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end))
    print('Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s')
    print('\n\n')


    # Ts2 = int(time.time())
    # T_start2 = time.localtime()
    # print 'Merge Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', T_start2)

    # merge_obj = extract_utility.MergeObject(input_json,
    #                                         os.path.dirname(input_json_path))
    # merge_obj.build_merge_list()
    # merge_obj.run_merge()

    # # stop job timer
    # T_run2 = int(time.time() - Ts2)
    # T_end2 = time.localtime()
    # print '\n\n'
    # print 'Merge Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', T_start2)
    # print 'Merge End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end2)
    # print 'Merge Runtime: ' + str(T_run2//60) +'m '+ str(int(T_run2%60)) +'s'


def tmp_worker_job(self, task_index, task_data):

    worker_tagline = "Worker {0} | Task {1} - ".format(self.rank, task_index)

    # tprint("{0} task approval received".format(worker_tagline))

    # request = self.extract_task_query()
    # if (isinstance(request, tuple) and len(request) == 3 and request[0] == "error"):
    #     extract_status = -3
    #     tprint("{0} task query error ({1})".format(worker_tagline, extract_status))
    #     return extract_status

    # tprint("{0} task found".format(worker_tagline))

    # task_data = prepare_extract_task(request)
    # tprint("{0} task prepared".format(worker_tagline))

    # =================================

    # inputs (see jobscript_template comments for detailed
    # descriptions of inputs)
    # * = managed by ExtractObject

    # absolute path of boundary file *
    bnd_absolute = task_data['bnd_absolute']

    # raster file or dataset directory *
    data_path = task_data['data_path']

    # extract type *
    extract_type = task_data['extract_type']

    # boundary, dataset and raster names
    bnd_name = task_data['bnd_name']
    dataset_name = task_data['dataset_name']
    data_name = task_data['data_name']

    # output directory
    output_base = task_data['output_base']

    # =================================

    exo = extract_utility.ExtractObject()

    exo.set_vector_path(bnd_absolute)

    exo.set_base_path(data_path)

    category_map = None
    if extract_type in ["categorical", "encoded"]:
        category_map = task_data['category_map']

    exo.set_extract_type(extract_type, category_map=category_map)


    # =================================

    output_dir = os.path.join(output_base, bnd_name, "cache", dataset_name)

    # creates directories
    try:
        os.makedirs(output_dir)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    # =================================

    # generate raster path
    raster = data_path


    # run extract

    tprint(("{0} running extract: "
           "\n\tvector: ({2}) {3}"
           "\n\traster: ({4}) {5}"
           "\n\tmethod: {6}").format(
                worker_tagline, None, bnd_name, bnd_absolute,
                data_name, raster, extract_type))


    run_data = exo.run_feature_extract(raster, pixel_limit=job.pixel_limit)


    # generate output path
    temporal = data_name[data_name.rindex('_')+1:]
    temporal = temporal if temporal != '' else 'na'
    file_name = '.'.join([dataset_name, temporal, exo._extract_type]) + ".csv"
    output = os.path.join(output_dir, file_name)

    run_data = exo.export_to_csv(run_data, output)


    # run_data = exo.export_to_db(
    #     stats = run_data,
    #     client = client,
    #     bnd_name = bnd_name,
    #     data_name = data_name,
    #     ex_method = extract_type,
    #     classification = task_data['classification'],
    #     ex_version = version
    # )


    try:
        Te_start = int(time.time())
        for _ in run_data: pass
        Te_run = int(time.time() - Te_start)

        extract_status = 1
        tprint(("{0} completed extract in {1} seconds"
               "\n\tvector: ({2}) {3}"
               "\n\traster: ({4}) {5}"
               "\n\tmethod: {6}").format(
                    worker_tagline, Te_run, bnd_name, bnd_absolute,
                    data_name, raster, extract_type))


    except MemoryError as e:
        extract_status = -2

        tprint(("{0} memory error ({1})"
               "\n\tvector: ({2}) {3}"
               "\n\traster: ({4}) {5}"
               "\n\tmethod: {6}").format(
                    worker_tagline, extract_status, bnd_name, bnd_absolute,
                    data_name, raster, extract_type))


    except Exception as e:
        extract_status = -1

        tprint(("{0} unknown error ({1})"
               "\n\tvector: ({2}) {3}"
               "\n\traster: ({4}) {5}"
               "\n\tmethod: {6}").format(
                    worker_tagline, extract_status, bnd_name, bnd_absolute,
                    data_name, raster, extract_type))


        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=2, file=sys.stdout)


    tprint(("{0} updating database for completed task").format(worker_tagline))

    # update status of item in extract queue
    update_extract = c_extracts.update_one({
        '_id': task_data['_id']
    }, {
        '$set': {
            'status': extract_status,
            'update_time': int(time.time()),
            'complete_time': int(time.time())
        }
    }, upsert=False)

    tprint(("{0} database updated for completed task").format(worker_tagline))

    return extract_status



def extract_task_query(self):

    search_attempt = 0
    request = 0
    while search_attempt < self.search_limit:
        # print 'Master - finding request:'

        search_query = {
            'status': 0
        }

        if self.generator_list:
            if len(self.generator_list) > 1:
                search_query['generator'] = {'$in': self.generator_list},
            else:
                search_query['generator'] = self.generator_list[0]

        if self.classification_list:
            if len(self.classification_list) > 1:
                search_query['classification'] = {'$in': self.classification_list},
            else:
                search_query['classification'] = self.classification_list[0]


        if 'errors' in job.job_type:
            attempt_max = int(job.job_type.split('_')[1])
            attempt_min = attempt_max - 5
            search_query['$and'] = [
                {'attempts': {'$gte': attempt_min}},
                {'attempts': {'$lt': attempt_max}}
            ]

        else:
            attempt_max = 5
            search_query['attempts'] = {'$lt': attempt_max}

        update_fields = {
            'status': 2,
            'update_time': int(time.time()),
            'start_time': int(time.time()),
            'complete_time': 0,
            'processor_name': job.processor_name,
        }


        request = c_extracts.find_one_and_update(
            search_query,
            { '$set': update_fields, '$inc': {'attempts': 1} },
            sort=[("priority", -1), ("submit_time", 1)]
        )

        if request is not None:
            break

        search_attempt += 1
        print('Failed Search Attempt: {0}'.format(search_attempt))
        time.sleep(random.randint(1, 10))


    if search_attempt == job.search_limit:
        # print "error updating request status in mongodb (attempting to continue)"
        return ("error", "pass", "error updating request status in mongodb (attempting to continue)")

    elif request is None:
        # print 'no jobs found in queue'
        return ("error", "empty", "no jobs found in queue")

    else:
        return request



job.extract_task_query = types.MethodType(extract_task_query, job)


def prepare_extract_task(request):
    task = {}

    task['_id'] = request['_id']

    task['bnd_name'] = request['boundary']

    bnd_info = c_asdf.find_one(
        {'name': task['bnd_name']},
        {'base': 1, 'resources': 1})

    task['bnd_absolute'] = (bnd_info['base'] + '/' +
                            bnd_info['resources'][0]['path'])

    task['data_name'] = request['data']

    task['classification'] = request['classification']

    if request['classification'] == 'msr':

        rname = request['data']
        rdataset = rname[:rname.rindex('_')]
        rhash = rname[rname.rindex('_')+1:]

        task['dataset_name'] = rdataset
        task['data_path'] = os.path.join(
            config.branch_dir, "outputs/msr/done",
            rdataset, rhash, "raster.tif")
        task['file_mask'] = "None"

    else:

        task['dataset_name'] = request['data'][:request['data'].rindex("_")]
        # print request['data']
        # print task['dataset_name']

        data_info = c_asdf.find_one({'resources.name': request['data']})
            # {'name': 1, 'base': 1, 'file_mask':1, 'resources': 1})


        task['dataset_name'] = data_info['name']


        tmp_resource_path = [
            j['path'] for j in data_info['resources']
            if j['name'] == request['data']
        ][0]

        if tmp_resource_path == '':
            task['data_path'] = data_info['base']
        else:
            task['data_path'] = data_info['base'] + '/'+ tmp_resource_path



        task['file_mask'] = data_info['file_mask']


    task['extract_type'] = request['extract_type']
    if request['extract_type'] in ['categorical', 'encoded']:
        task['category_map'] = data_info['extras']['category_map']

    task['output_base'] = general_output_base

    return task


def tmp_get_task_data(self, task_index, source):
    # task_data = self.task_list[task_index]
    # return task_data

    # tprint("Master - approving search for Worker {0} | Task {1}".format(source, task_index))
    # return task_index

    tprint("Master - starting search for Worker {0} | Task {1}".format(source, task_index))
    request = self.extract_task_query()
    if (isinstance(request, tuple) and len(request) == 3 and request[0] == "error"):
        return request
    task = prepare_extract_task(request)
    return task


# init / run job
job.set_task_count(extract_limit)

job.set_general_init(tmp_general_init)
job.set_master_init(tmp_master_init)
job.set_master_process(tmp_master_process)
job.set_master_update(tmp_master_update)
job.set_update_interval(update_interval)
job.set_master_final(tmp_master_final)
job.set_worker_job(tmp_worker_job)
job.set_get_task_data(tmp_get_task_data)

job.run()

