
import os
import errno
import json
import subprocess as sp

dir_base = os.path.dirname(os.path.abspath(__file__))

# could get this info from asdf instead of using json
dset_info = json.load(open(dir_base + '/datasets.json','r'))

# script_base = "/home/userz/Desktop"
script_base = "/sciclone/home00/sgoodman/work/extracts/gen3/extract"


# --------------------------------------------------

batch_name = "liberia"

project_id = "lbr"

# could get this info from asdf instead of manual input
# with optional override for non-asdf boundaries
general_info = {
    'bnd_name': 'liberia_districts_rev',
    'bnd_absolute': '/sciclone/aiddata10/REU/projects/liberia/shps/Liberia_districts_rev.shp',
    # 'bnd_name': 'liberia_clan_areas_rev',
    # 'bnd_absolute': '/sciclone/aiddata10/REU/projects/liberia/shps/Liberia_clan_areas_rev.shp',
    # 'bnd_name': 'liberia_grid',
    # 'bnd_absolute': '/sciclone/aiddata10/REU/projects/liberia/shps/Liberia_grid.shp',

    'output_base': '/sciclone/aiddata10/REU/extracts',
    'extract_method': 'rpy2'
}


# specify datasets
datasets = ["dist_to_all_rivers","dist_to_roads","srtm_elevation","srtm_slope","accessibility_map"]
# datasets = ["gpw_v3"]
# datasets = ["ndvi_max_mask_lt6k", "v4avg_lights_x_pct", "terrestrial_air_temperature_v4.01", "terrestrial_precipitation_v4.01"]

# all available datasets
# datasets = dset_info.keys()


node_count = 1
# node_count = 1
# node_count = 3


run_hours = 36

node_type = "xeon"
# node_type = "vortex" 

ppn_override = 1
# ppn_override = 3
# ppn_override = 0

# --------------------------------------------------


ppn_default = {
    'xeon': 8,
    'vortex': 12
}


if ppn_override > 0:
    ppn = ppn_override
else:
    ppn = ppn_default[node_type]



# creates batch job output directory
if not os.path.isdir(script_base +"/"+ batch_name):
    try:
        os.makedirs(script_base +"/"+ batch_name)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise



def qsub(jobscript):
    try:  

        # buildt command for Rscript
        cmd = "qsub " + jobscript
        print cmd

        # spawn new process for Rscript
        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

    except sp.CalledProcessError as sts_err:                                                                                                   
        print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output



for dset in datasets:

    lines = []

    inputs = dset_info[dset]

    lines.append('#!/bin/tcsh')
    lines.append('#PBS -N asg:ex:'+project_id+'-'+inputs['data_mini'])
    lines.append('#PBS -l nodes='+str(node_count)+':'+node_type+':compute'+':ppn='+str(ppn))
    lines.append('#PBS -l walltime='+str(run_hours)+':00:00')
    lines.append('#PBS -j oe')
    lines.append('')

    for k in general_info.keys():
        lines.append('set '+ str(k) + ' = "' + str(general_info[k]) + '"')

    for k in inputs.keys():
        lines.append('set '+ str(k) + ' = "' + str(inputs[k]) + '"')

    lines.append('')
    lines.append('cd $PBS_O_WORKDIR')
    # lines.append('cd ' + script_base +"/"+ batch_name)
    lines.append('')

    args = 'python-mpi ../runscript.py "$run_option" "$bnd_name" "$bnd_absolute" "$data_base" "$data_path" "$data_name" "$data_mini" "$file_mask" "$extract_type" "$output_base" "$extract_method"'
    
    if node_type == "xeon":
        lines.append('mvp2run -m cyclic ' + args)
    elif node_type == "vortex":
        np = node_count * ppn
        lines.append('mpirun --mca mpi_warn_on_fork 0 -np '+ str(np) +' '+ args)


    # jobscripts must have newline at end of file
    lines.append('')


    output = '\n'.join(lines)

    job_name = general_info['bnd_name'] +'_'+ inputs['data_mini'] + '_jobscript'
    out_file = script_base +'/'+ batch_name +'/'+ job_name
    open(out_file, 'w').write(output)


    # qsub
    os.chdir(script_base +"/"+ batch_name)
    # os.chdir(script_base)

    print os.getcwd()
    qsub(job_name)

    # move jobscript to batch folder once job has been queued
    #    
