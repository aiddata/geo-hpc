
# could get dataset info from asdf instead of using json

# could get boundary info from asdf instead of manual input
# with optional override for non-asdf boundaries


import sys
import os
import errno
import json
import time
import math
import random

import subprocess as sp

from collections import OrderedDict

import extract_utility

# =============================================================================
# =============================================================================
# load job and datasets json

job_json_path = sys.argv[1]

if not os.path.isfile(job_json_path):
    sys.exit("builder.py has terminated : invalid job json path")

job_json_path = os.path.abspath(job_json_path)
job_dir = os.path.dirname(job_json_path)

job_file = open(job_json_path, 'r')
job_json = json.load(job_file, object_pairs_hook=OrderedDict)
job_file.close()

base_dir = os.path.dirname(os.path.abspath(__file__))

datasets_file = open(base_dir + '/datasets.json','r')
datasets_json = json.load(datasets_file)
datasets_file.close()


# =============================================================================
# =============================================================================
# validate and assess requested data

# prompt to continue function
def user_prompt_bool(question):
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}

    while True:
        sys.stdout.write(str(question) + " [y/n] \n> ")
        choice = raw_input().lower()

        if choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " +
                             "(or 'y' or "" 'n').\n")


job_summary = OrderedDict()
job_summary['summary'] = OrderedDict()
job_summary['datasets'] = []


required_options = ["bnd_absolute", "bnd_name", "extract_type",
                    "output_base", "run_hours", "years"]

missing_defaults = [i for i in required_options
                    if i not in job_json['defaults'].keys()]


if len(missing_defaults) > 0:
    print ("builder.py warning: required option(s) missing from defaults " +
           "(" + str(missing_defaults) + ")")


msr_count = 0
for dataset_options in job_json['data']:

    tmp_config = {}

    if 'type' in dataset_options and dataset_options['type'] == 'msr':

        msr_fields = ['branch', 'release', 'hash']
        if any([i not in dataset_options for i in msr_fields]):
            if user_prompt_bool("MSR dataset required options not found. " +
                                "Ignore dataset and continue? [y/n]"):
                continue
            else:
                sys.exit("builder.py has terminated : user's request.")


        dataset_name = (dataset_options['release'] + '_' +
                        dataset_options['hash'])

        print dataset_name

        msr_dir = ('/sciclone/aiddata10/REU/outputs/' +
                   dataset_options['branch'] + '/msr/done/' +
                   dataset_options['release'] + '/' +
                   dataset_options['hash'])

        if (not os.path.isdir(msr_dir) or
                not os.path.isfile(msr_dir + '/raster.tif')):

            if user_prompt_bool("MSR dataset not found. " +
                                "Ignore dataset and continue? [y/n]"):
                continue
            else:
                sys.exit("builder.py has terminated : user's request.")


        if ('reliability' in dataset_options and
                dataset_options['reliability'] in [True, 'True', 1]):

            if not os.path.isfile(msr_dir + '/unique.geojson'):

                if user_prompt_bool("MSR reliability geojson not found. " +
                                        "Run without reliability calcs? [y/n]"):
                    tmp_config['reliability'] = False

                else:
                    sys.exit("builder.py has terminated : user's request.")
            else:
                tmp_config['reliability'] = True

        else:
            tmp_config['reliability'] = False


        tmp_config['name'] = dataset_name
        tmp_config['data_base'] = msr_dir + '/raster.tif'
        tmp_config['data_mini'] = 'msr_' + str(msr_count)
        tmp_config["file_mask"] = "None"

        msr_count += 1

    else:
        dataset_name = dataset_options['name']
        print dataset_name

        tmp_config['reliability'] = False

        # make sure dataset exists in datasets_json
        try:
            dataset_info = datasets_json[dataset_name]

            for k in dataset_info.keys():
                tmp_config[k] = dataset_info[k]

        except KeyError:

            dataset_fields = ['data_base', 'data_name', 'data_mini', 'file_mask']
            if all([i in dataset_options for i in dataset_fields]):

                for j in dataset_fields:
                    tmp_config[j] = dataset_options[j]

            elif user_prompt_bool("Dataset ("+str(dataset_name) + ") not found " +
                                "in dataset json. Ignore dataset and continue? " +
                                "[y/n]"):
                continue
            else:
                sys.exit("builder.py has terminated : user's request.")


    if any([i not in dataset_options for i in missing_defaults]):
        sys.exit("builder.py has terminated : required option(s) missing " +
                 "from both dataset default options.")


    # gather all relevant options
    for k in required_options:
        if k in dataset_options:
            tmp_config[k] = dataset_options[k]
        else:
            tmp_config[k] = job_json['defaults'][k]


    # ==================================================

    # init / setup extract and generate qlist

    exo = extract_utility.ExtractObject(builder=True)

    exo.set_vector_path(tmp_config['bnd_absolute'])

    exo.set_base_path(tmp_config['data_base'])
    exo.set_reliability(tmp_config['reliability'])

    exo.set_years(tmp_config['years'])

    exo.set_file_mask(tmp_config['file_mask'])

    if tmp_config['extract_type'] == "categorical":
        exo.set_extract_type(tmp_config['extract_type'], tmp_config['categories'])
    else:
        exo.set_extract_type(tmp_config['extract_type'])


    qlist = exo.gen_data_list()


    print len(qlist)
    print qlist

    tmp_info = OrderedDict()
    tmp_info['name'] = dataset_name
    tmp_info['info'] = {}
    tmp_info['info']['count'] = len(qlist)
    tmp_info['info']['individual_run_time'] = tmp_config['run_hours']
    tmp_info['info']['serial_run_time'] = (
        len(qlist) * float(tmp_config['run_hours']))
    tmp_info['settings'] = tmp_config
    tmp_info['qlist'] = qlist

    if len(qlist) == 0:
        if user_prompt_bool("No data found for dataset " +
                            "("+str(dataset_name) + ") and given year " +
                            "string ("+ str(tmp_config['years']) +"). " +
                            "Ignore dataset and continue?"):
            continue
        else:
            sys.exit("builder.py has terminated: user's request.")



    # add job_summary entry for dataset with tmp_info
    job_summary['datasets'].append(tmp_info)


# =============================================================================
# =============================================================================
# determine node specifications for job

total_count = sum([i['info']['count'] for i in job_summary['datasets']]) + 1
max_individual_run_time = max(
    [i['info']['individual_run_time'] for i in job_summary['datasets']])

# job_summary['summary']['count'] = total_count
# job_summary['summary']['max_individual_run_time'] = max_individual_run_time
# job_summary['summary']['distribution'] = [(i['info']['count'], i['info']['individual_run_time']) for i in job_summary['datasets']]
# job_summary['summary']['serial_run_time'] = sum([i['info']['serial_run_time'] for i in job_summary['datasets']])
# job_summary['summary']['weighted_serial_run_time'] = sum([i['info']['individual_run_time']*i['info']['count'] for i in job_summary['datasets']]) / total_count


def get_ppn(value, node_type):
    tmp_default = ppn_defaults[node_type]

    if value > tmp_default:
        sys.exit("builder.py has terminated : (get_ppn) invalid ppn")
    elif value > 0:
        tmp_ppn = value
    else:
        tmp_ppn = tmp_default

    return tmp_ppn


node_spec_reference = {
    'xeon': ['xeon:compute', 'c10', 'c10a', 'c11', 'c11a'],
    'vortex': ['vortex:compute', 'c18a', 'c18b'],
    'vortex-alpha': ['c18c']
}

ppn_defaults = {
    'xeon': 8,
    'vortex': 12,
    'vortex-alpha': 16
}


node_spec = job_json["config"]["node_spec"]
max_node_count = job_json["config"]["max_node_count"]
ppn_override = job_json["config"]["ppn_override"]

if node_spec in node_spec_reference.keys():
    node_spec = node_spec + ':compute'

node_type = None
for n in node_spec_reference.keys():
    if node_spec in node_spec_reference[n]:
        node_type = n
        break

if node_type == None:
    sys.exit("builder.py has terminated : invalid node spec")


ppn = get_ppn(ppn_override, node_type)


# adjust node count if needed (reduce from max nodes if possible)
full_cycle_node_count = math.ceil( total_count / float(ppn) )
# if full_cycle_node_count < max_node_count:
#     node_count = full_cycle_node_count
# else:
#     node_count = max_node_count
node_count = max_node_count


# optimize node calcs
#

# simple example of optimization
# scenario:
# - dataset #1 - temporally invariant (1 extract) estimated at 10 hours
# - dataset #2 - yearly with 10 years (10 extracts) estimated at
#                   1 hour each (10 hours total)
# - max node count of 2, no ppn override, default ppn of 8
# current resource use:
#   two nodes will be requested to run the 11 combined extracts
#   even though all but 1 processor will be idle after the first hour
# optimized resource use:
#   single node will be requested since the 10 yearly extracts can run
#   in multiple cycles on a reduced number of processors in the time
#   it will take the single temporally invariant extract to complete

# optimization notes
# - obviously this gets more complicated as we add in more datasets.
# - estimated run times may be very rough or not even offer enough
#   differentiation to make meaningful optimizations


np = node_count * ppn

if total_count < np:
    np = total_count



if job_json['config']['walltime_override']:
    try:
        run_hours = int(job_json['config']['walltime'])

        if run_hours < 1:
            sys.exit("builder.py has terminated : walltime must be at " +
                     "least 1 hour")

    except:
        sys.exit("builder.py has terminated : invalid walltime provided")

else:
    run_hours = int(math.ceil(
        math.ceil(total_count / np) * max_individual_run_time))
    if run_hours < 1:
        run_hours = 1


if run_hours > 180:
    sys.exit("builder.py has terminated : job walltime cannot exceed 180 " +
             "hours")


# =============================================================================
# =============================================================================
# prep job json

# user prefix (used in hpc job name)
user_prefix = job_json["config"]["user_prefix"]

# general component of hpc job name for  all hpc job that will be generate
job_name = job_json["config"]["job_name"]

# folder where jobscripts go
batch_name = job_json["config"]["batch_name"]

batch_dir = job_dir +'/ioe/'+ batch_name

# --------------------------------------------------

Ts = str(time.time())

tmp_time = time.localtime()
job_summary['summary']['id'] = (time.strftime('%Y%m%d', tmp_time) +'_'+
                                str(int(time.time())) +'_'+
                                '{0:05d}'.format(int(random.random() * 10**5)))
job_summary['summary']['created'] = time.strftime('%Y-%m-%d  %H:%M:%S', tmp_time)
job_summary['summary']['results'] = {}

# job_summary['summary']['jobscript'] = job_name + '_' + job_summary['summary']['id']
# job_summary['summary']['json'] = job_name + '_' + job_summary['summary']['id'] + '.json'


# add job summary to job json
job_json['job'] = job_summary
# job_json['job'] = {}
# job_json['jobs'][Ts] = job_summary

output_dir = batch_dir +"/"+ job_summary['summary']['id'] +"_"+ job_name

output_json_path = output_dir +'/config.json'


# =============================================================================
# =============================================================================
# build jobscript

lines = []

lines.append('#!/bin/tcsh')
lines.append('#PBS -N '+user_prefix+':ex:'+job_name)
lines.append('#PBS -l nodes='+str(int(node_count))+':'+node_spec+':'+
             'ppn='+str(int(ppn)))
lines.append('#PBS -l walltime='+str(int(run_hours))+':00:00')
lines.append('#PBS -q alpha')
lines.append('#PBS -j oe')
lines.append('')

# tmp_config['years'] = tmp_config['years'].replace(":", "_to_")
# tmp_config['years'] = tmp_config['years'].replace("|", "_and_")
# tmp_config['years'] = tmp_config['years'].replace("!", "_not_")


lines.append('')
lines.append('cd $PBS_O_WORKDIR')
lines.append('')

args = base_dir + '/runscript.py ' + output_json_path

if node_type == "xeon":
    lines.append('mvp2run -m cyclic -c ' + str(int(np)) +' python-mpi '+ args)
elif node_type in ["vortex", "vortex-alpha"]:
    # lines.append('mpirun --mca mpi_warn_on_fork 0 -np ' +
    #               str(int(np)) +' python-mpi '+ args)
    lines.append('mpirun --map-by node -np ' + str(int(np)) +' python-mpi '+ args)



# jobscripts must have newline at end of file
lines.append('')

output = '\n'.join(lines)


# =============================================================================
# =============================================================================
# final checks, display job summary, confirm job submission

if len(job_summary.keys()) == 0:
    sys.exit("builder.py has terminated: No data found for any dataset.")


# display job summary info
print 'Job Summary: '
print job_summary


# check if user wants to continue
if not user_prompt_bool("Submit job?"):
    sys.exit("builder.py has terminated: user's request.")


# =============================================================================
# =============================================================================
# create job json


# creates batch job output directory
try:
    os.makedirs(output_dir)
except OSError as exception:
    if exception.errno != errno.EEXIST:
        raise

# write json
output_job_file = open(output_json_path, "w")
output_job_file.write(json.dumps(job_json, indent = 4))
output_job_file.close()


# =============================================================================
# =============================================================================
# create directory, write jobscript, submit job

# qsub jobscript
def qsub(jobscript):
    try:
        # buildt command for Rscript
        cmd = "qsub " + jobscript
        print cmd

        # spawn new process for Rscript
        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

    except sp.CalledProcessError as err:
        print ">> subprocess error code:", err.returncode, '\n', err.output



output_jobscript_path = output_dir +'/jobscript'

jobscript_file = open(output_jobscript_path, 'w')
jobscript_file.write(output)
jobscript_file.close()

os.chdir(output_dir)


# submit job via qsub
qsub(output_jobscript_path)


