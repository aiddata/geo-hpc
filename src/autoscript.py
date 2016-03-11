#

import sys
import os

import time
import pymongo

import extract_utility

import mpi_utility

job = mpi_utility.NewParallel()

# --------------------------------------------------

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


# --------------------------------------------------


default_extract_limit = 10
# default_time_limit = 5
# default_extract_minimum = 1


# ===========================================================================
# ===========================================================================

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

# client = pymongo.MongoClient('128.239.20.36')
client = pymongo.MongoClient(config.server)

asdf = client[config.asdf_db].data


# ---
extract_list = []

# ---

extract_list = client[config.det_db].extracts.find(
    {'status':0}).sort([
        ("priority", -1),
        ("submit_time", 1)
    ]).limit(extract_limit)


qlist = []

for i in extract_list:
    tmp = {}

    tmp['bnd_name'] = i['boundary']

    bnd_info = asdf.find(
        {'name': tmp['bnd_name']},
        {'base': 1, 'resources': 1}).limit(1)[0]

    tmp['bnd_absolute'] = (bnd_info['base'] + '/'
        + bnd_info['resources'][0]['path'])

    tmp['data_mini' = i['raster'].split('_')[0]


    data_info = asdf.find(
        {'options.mini_name': tmp['data_mini']},
        {'name': 1, 'base': 1, 'file_mask':1, 'resources': 1}).limit(1)[0]

    tmp['data_name'] = data_info['name']
    tmp['data_absolute'] = (data_info['base'] + '/'+
        [
            j['path'] for j in data_info['resources']
            if j['name'] == i['raster']
        ][0])

    tmp['file_mask'] = data_info['file_mask']

    tmp['extract_type'] = i['extract_type']

    tmp['extract_method'] = 'python'
    tmp['output_base'] = '/sciclone/aiddata10/REU/extracts'

    qlist.append(tmp)


# ===========================================================================
# ===========================================================================


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

    Ts2 = int(time.time())
    T_start2 = time.localtime()
    print 'Merge Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', T_start2)


    merge_obj = MergeObject(input_json, os.path.dirname(input_json_path))
    merge_obj.build_merge_list()
    merge_obj.run_merge()


    # stop job timer
    T_run2 = int(time.time() - Ts2)
    T_end2 = time.localtime()
    print '\n\n'
    print 'Merge Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', T_start2)
    print 'Merge End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end2)
    print 'Merge Runtime: ' + str(T_run2//60) +'m '+ str(int(T_run2%60)) +'s'


def tmp_worker_job(self, task_id):

    task = self.task_list[task_id]


    # ==================================================


    # inputs (see jobscript_template comments for detailed
    # descriptions of inputs)
    # * = managed by ExtractObject

    # extract method *
    extract_method = task['extract_method']

    # absolute path of boundary file *
    bnd_absolute = task['bnd_absolute']

    # boundary name
    bnd_name = task['bnd_name']

    # folder which contains data (or data file) *
    data_absolute = task['data_absolute']

    # dataset mini_name
    data_mini = task['data_mini']

    # # string containing year information *
    # year_string = task['years']

    # file mask for dataset files *
    file_mask = task['file_mask']

    # extract type *
    extract_type = task['extract_type']

    # output folder
    output_base = task['output_base']


    # ==================================================


    exo = extract_utility.ExtractObject()

    exo.set_extract_method(extract_method)
    exo.set_vector_path(bnd_absolute)

    # exo.set_base_path(data_base)
    # exo.set_years(year_string)

    # exo.set_file_mask(file_mask)
    exo.set_extract_type(extract_type)


    # ==================================================


    output_dir = (output_base + "/" + bnd_name + "/cache/"
        + data_name +"/"+ exo._extract_type)

    # creates directories
    try:
        os.makedirs(output_dir)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    # ==================================================


    # generate raster path
    raster = data_absolute

    # # generate output path
    # output = (output_base + "/extracts/" + bnd_name + "/cache/"
    #     + data_name +"/"+ extract_type + "/extract_"
    #     + '_'.join([str(e) for e in item[0]]))
    output = (output_dir + "/" + data_mini + "_"
        + ''.join([str(e) for e in item[0]])
        + exo._extract_options[exo._extract_type])

    # run extract
    print ('Worker ' + str(self.rank) + ' | Task ' + str(task_id)
        + ' - running extract: ' + output)
    run_status, run_statment = exo.run_extract(raster, output)

    if run_status == 0:
        print ('Worker ' + str(self.rank) + ' | Task ' + str(task_id)
            + ' - ' + run_statment)
    else:
        raise Exception(run_statment)

    return 0


# init / run job

job.set_task_list(qlist)

job.set_general_init(tmp_general_init)
job.set_master_init(tmp_master_init)
job.set_master_process(tmp_master_process)
job.set_master_final(tmp_master_final)
job.set_worker_job(tmp_worker_job)

job.run()
