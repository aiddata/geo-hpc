#

import sys
import os
import re
import errno
import time
import pymongo
import json
import hashlib

import extract_utility

import mpi_utility

job = mpi_utility.NewParallel()


# -----------------------------------------------------------------------------

# import sys
# import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
sys.path.insert(0, config_dir)

import config_utility

config = config_utility.BranchConfig(branch=branch)

# -------------------------------------


# check mongodb connection
if config.connection_status != 0:
    sys.exit("connection status error: " + str(config.connection_error))


# -----------------------------------------------------------------------------


def get_version():
    vfile = os.path.join(os.path.dirname(__file__), "_version.py")
    with open(vfile, "r") as vfh:
        vline = vfh.read()
    vregex = r"^__version__ = ['\"]([^'\"]*)['\"]"
    match = re.search(vregex, vline, re.M)
    if match:
        return match.group(1)
    else:
        raise RuntimeError("Unable to find version string in {}.".format(vfile))


tmp_v1 = config.versions["extract-scripts"]
tmp_v2 = get_version()

if tmp_v1 == tmp_v2:
    version = tmp_v1
else:
    raise Exception("Config and src versions do not match")


general_output_base = ('/sciclone/aiddata10/REU/outputs/' + branch +
                       '/extracts/' + version.replace('.', '_'))


default_extract_limit = 20
# default_time_limit = 5
# default_extract_minimum = 1


# =============================================================================
# =============================================================================

# if something:
#   check config/input/request for extract_limit parameter
#   extract_limit = some val
# else:
#   use default
extract_limit = default_extract_limit

# limit of 0 = no limit
# limit of -1 means set limit to # workers (single cycle on cores)
if extract_limit == -1:
    extract_limit = job.size -1

client = pymongo.MongoClient(config.server)
c_asdf = client[config.asdf_db].data
c_extracts = client[config.extracts_db].extracts
c_features = client[config.features_db].features

# -----------------------------------------------------------------------------

# build extract list

extract_list = []

for i in range(extract_limit):
    if job.rank == 0:

        print 'starting request search'
        search_limit = 5
        search_attempt = 0

        while search_attempt < search_limit:

            print 'finding request:'
            find_request = c_extracts.find_one({
                'status': 0
            }, sort=[("priority", -1), ("submit_time", 1)])

            print find_request

            if find_request is None:
                request = None
                break

            request_accept = c_extracts.update_one({
                '_id': find_request['_id'],
                'status': find_request['status']
            }, {
                '$set': {
                    'status': 2,
                    'update_time': int(time.time())
                }
            })

            print request_accept.raw_result

            if (request_accept.acknowledged and
                    request_accept.modified_count == 1):
                request = find_request
                break

            search_attempt += 1

            print 'looking for another request...'


        if search_attempt == search_limit:
            request = 'Error'
            break

        print 'request found'

    else:
        request = 0


    request = job.comm.bcast(request, root=0)

    if request is None:
        if i == 0:
            quit("no jobs found in queue")
        else:
            break

    elif request == 'Error':
        quit("error updating request status in mongodb")

    elif request == 0:
        quit("error getting request from master")

    extract_list.append(request)


# extract_list = client[config.extracts_db].extracts.find(
#     {'status':0}).sort([
#         ("priority", -1),
#         ("submit_time", 1)
#     ]).limit(extract_limit)


# -----------------------------------------------------------------------------

qlist = []

for i in extract_list:
    tmp = {}

    tmp['_id'] = i['_id']

    tmp['bnd_name'] = i['boundary']

    bnd_info = c_asdf.find(
        {'name': tmp['bnd_name']},
        {'base': 1, 'resources': 1}).limit(1)[0]

    tmp['bnd_absolute'] = (bnd_info['base'] + '/' +
                           bnd_info['resources'][0]['path'])


    tmp['raster_name'] = i['raster']

    tmp['classification'] = i['classification']

    if i['classification'] == 'msr':

        rname = i['raster']
        rdataset = rname[:rname.rindex('_')]
        rhash = rname[rname.rindex('_')+1:]

        tmp['data_name'] = rdataset
        tmp['data_absolute'] = os.path.join("/sciclone/aiddata10/REU/outputs/",
                                            branch , "msr", "done",
                                            rdataset, rhash, "raster.tif")
        tmp['file_mask'] = "None"

    else:

        tmp['data_name'] = i['raster'][:i["raster"].rindex("_")]
        print i['raster']
        print tmp['data_name']

        data_info = c_asdf.find(
            {'name': tmp['data_name']},
            {'name': 1, 'base': 1, 'file_mask':1, 'resources': 1}).limit(1)[0]

        tmp['data_absolute'] = (
            data_info['base'] + '/'+
            [
                j['path'] for j in data_info['resources']
                if j['name'] == i['raster']
            ][0])

        tmp['file_mask'] = data_info['file_mask']


    tmp['extract_type'] = i['extract_type']

    tmp['output_base'] = general_output_base

    if 'reliability' in i:
        tmp['reliability'] = i['reliability']
    else:
        tmp['reliability'] = False

    qlist.append(tmp)


# =============================================================================
# =============================================================================


def tmp_general_init(self):
    pass


def tmp_master_init(self):
    # start job timer
    self.Ts = int(time.time())
    self.T_start = time.localtime()
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)


def tmp_master_process(self, worker_data):
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


def tmp_worker_job(self, task_id):

    task = self.task_list[task_id]


    # =================================

    # inputs (see jobscript_template comments for detailed
    # descriptions of inputs)
    # * = managed by ExtractObject

    # absolute path of boundary file *
    bnd_absolute = task['bnd_absolute']

    # boundary name
    bnd_name = task['bnd_name']

    # folder which contains data (or data file) *
    data_absolute = task['data_absolute']

    data_name = task['data_name']

    raster_name = task['raster_name']

    # dataset mini_name
    # data_mini = task['data_mini']

    # # string containing year information *
    # year_string = task['years']

    # # file mask for dataset files *
    # file_mask = task['file_mask']

    # extract type *
    extract_type = task['extract_type']

    # output folder
    output_base = task['output_base']


    # =================================

    exo = extract_utility.ExtractObject()

    exo.set_vector_path(bnd_absolute)

    exo.set_base_path(data_absolute)
    exo.set_reliability(task['reliability'])

    # exo.set_years(year_string)

    # exo.set_file_mask(file_mask)
    exo.set_extract_type(extract_type)


    # =================================

    output_dir = (output_base + "/" + bnd_name + "/cache/" +
                  data_name +"/"+ exo._extract_type)

    # creates directories
    try:
        os.makedirs(output_dir)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


    # =================================

    # generate raster path
    raster = data_absolute

    # generate output path
    output = output_dir + "/" + raster_name + "_"

    output += exo._extract_options[exo._extract_type]

    # run extract
    print ('Worker ' + str(self.rank) + ' | Task ' + str(task_id) +
           ' - running extract: ' + output)

    run_data, run_statment = exo.run_extract(raster, output)

    print ('Worker ' + str(self.rank) + ' | Task ' + str(task_id) +
           ' - ' + run_statment)



    # update extract result database
    for idx, feat in enumerate(run_data):
        geom = feat['geometry']
        geom_hash = json_sha1_hash(geom)

        # if 'id' in feat:
        #     feature_id = feat['id']
        # else:
        #     feature_id = idx


        feature_properties = feat['properties']
        # feature_properties = {}

        if task['classification'] == 'msr' and task['reliability']:
            ex_method = 'msr'
            ex_value = {
                'sum': feat['properties']['exfield_sum'],
                'reliability': feat['properties']['exfield_reliability']
            }
        else:
            ex_method = extract_type
            ex_value = feat['properties']['exfield_' + extract_type]


        temporal = raster_name[raster_name.rindex('_')+1:]

        feature_extracts = [{
            'raster': raster_name,
            'dataset': data_name,
            'temporal': temporal,
            'method': ex_method,
            'version': version,
            'value': ex_value
        }]


        # check if geom / geom hash exists
        search = c_features.find_one({'hash': geom_hash})

        print "!!!"
        print search

        exists = search is not None
        if exists:

            extract_search_params = {
                'hash': geom_hash,
                'extracts.raster': raster_name,
                'extracts.method': ex_method,
                'extracts.version': version
            }

            extract_search = c_features.find_one(extract_search_params)
            extract_exists = extract_search is not None

            if extract_exists:
                search_params = extract_search_params,
                update_params = {
                    '$set': {'extracts.$': feature_extracts[0]}
                }

            else:
                search_params = {'hash': geom_hash}
                update_params = {
                    '$push': {'extracts': {'$each': feature_extracts}}
                }



            if not bnd_name in search['datasets']:
                # add dataset to datasets
                if not '$push' in update_params:
                    update_params['$push'] = {}
                if not '$set' in update_params:
                    update_params['$set'] = {}

                update_params['$push']['datasets'] = bnd_name
                update_params['$set']['properties.' + bnd_name] = feature_properties


            update = c_features.update_one(search_params, update_params)


        else:

            feature_insert = {
                'geometry': geom,
                'hash': geom_hash,
                'properties': {bnd_name: feature_properties},
                'datasets': [bnd_name],
                'extracts': feature_extracts
            }
            # insert
            insert = c_features.insert(feature_insert)



    # update status of item in extract queue
    update_extract = c_extracts.update_one({
        '_id': task['_id']
    }, {
        '$set': {
            'status': 1,
            'update_time': int(time.time())
        }
    }, upsert=False)

    return 0


def json_sha1_hash(hash_obj):
    hash_json = json.dumps(hash_obj,
                           sort_keys = True,
                           ensure_ascii=True,
                           separators=(', ',': '))
    hash_builder = hashlib.sha1()
    hash_builder.update(hash_json)
    hash_sha1 = hash_builder.hexdigest()
    return hash_sha1


# init / run job

job.set_task_list(qlist)

job.set_general_init(tmp_general_init)
job.set_master_init(tmp_master_init)
job.set_master_process(tmp_master_process)
job.set_master_final(tmp_master_final)
job.set_worker_job(tmp_worker_job)

job.run()

