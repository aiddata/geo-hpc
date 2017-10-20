#!/bin/bash

# responsible for running cron tasks
# manages creating logs for tasks by redirecting std_out/err
#
# input args
#   branch
#   job_class


branch=$1

job_class=$2

timestamp=$(date +%Y%m%d)

branch_dir="/sciclone/aiddata10/geo/${branch}"
src="${branch_dir}/source"


# cron tasks do not inherit pythonpath from user,
# so it needs to be specified here
export PYTHONPATH="${PYTHONPATH}:/usr/local/amd64/seoul/gcc/python-2.7.8/lib/python2.7/site-packages/:/sciclone/aiddata10/REU/py_libs/lib/python2.7/site-packages"


config_path=$src/geo-hpc/config.json


# -----------------------------------------------------------------------------


# verify $job_class is in config.json
is_valid_job_class=$(python -c "import json; print '$job_class' in json.load(open('$config_path', 'r'))['$branch']['jobs']")

if [ $is_valid_job_class = "False" ]; then
    echo "Invalid cron job class ($job_class)"
    exit 1
fi


mkdir -p "$branch_dir"/log/"$job_class"
bash "$src"/geo-hpc/tasks/build_jobs.sh "$branch" "$timestamp" "$job_class"  2>&1 | tee 1>>"$branch_dir"/log/"$job_class"/"$timestamp"."$job_class".log

