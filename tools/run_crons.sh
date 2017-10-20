#!/bin/bash


# responsible for running cron tasks
# manages creating logs for tasks by redirecting std_out/err
#
# input args
#   branch
#   cron task keyword


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



# case $job_class in
#     "update_repos")     mkdir -p "$branch_dir"/log/update_repos
#                         bash "$src"/geo-hpc/tasks/build_jobs.sh "$branch" "$timestamp" update_repos  2>&1 | tee 1>>"$branch_dir"/log/update_repos/"$timestamp".update_repos.log
#                         exit 0;;

#     "cleanup_repos")    mkdir -p "$branch_dir"/log/cleanup_repos
#                         bash "$src"/geo-hpc/tasks/build_jobs.sh "$branch" "$timestamp" cleanup_repos  2>&1 | tee 1>>"$branch_dir"/log/cleanup_repos/"$timestamp".cleanup_repos.log
#                         exit 0;;

#     "error_check")      mkdir -p "$branch_dir"/log/error_check
#                         bash "$src"/geo-hpc/tasks/build_jobs.sh "$branch" "$timestamp" error_check 2>&1 | tee 1>>"$branch_dir"/log/error_check/"$timestamp".error_check.log
#                         exit 0;;

#     "update_trackers")  mkdir -p "$branch_dir"/log/update_trackers
#                         bash "$src"/geo-hpc/tasks/build_jobs.sh "$branch" "$timestamp" update_trackers 2>&1 | tee 1>>"$branch_dir"/log/update_trackers/"$timestamp".update_trackers.log
#                         exit 0;;

#     "update_extracts")  mkdir -p "$branch_dir"/log/update_extracts
#                         bash "$src"/geo-hpc/tasks/build_jobs.sh "$branch" "$timestamp" update_extracts 2>&1 | tee 1>>"$branch_dir"/log/update_extracts/"$timestamp".update_extracts.log
#                         exit 0;;

#     "update_msr")       mkdir -p "$branch_dir"/log/update_msr
#                         bash "$src"/geo-hpc/tasks/build_jobs.sh "$branch" "$timestamp" update_msr 2>&1 | tee 1>>"$branch_dir"/log/update_msr/"$timestamp".update_msr.log
#                         exit 0;;

#     "det")              mkdir -p "$branch_dir"/log/det
#                         bash "$src"/geo-hpc/tasks/build_jobs.sh "$branch" "$timestamp" det 2>&1 | tee 1>>"$branch_dir"/log/det/"$timestamp".det.log
#                         exit 0;;

#     "msr")              mkdir -p "$branch_dir"/log/msr
#                         bash "$src"/geo-hpc/tasks/build_msr_job.sh "$branch" "$timestamp" msr 2>&1 | tee 1>>"$branch_dir"/log/msr/"$timestamp".msr.log
#                         exit 0;;

#     "extracts")         mkdir -p "$branch_dir"/log/extracts
#                         bash "$src"/geo-hpc/tasks/build_extract_job.sh "$branch" "$timestamp" extracts 2>&1 | tee 1>>"$branch_dir"/log/extracts/"$timestamp".extracts.log
#                         exit 0;;


#     *)                  echo "Invalid cron.";
#                         exit 1 ;;

# esac


