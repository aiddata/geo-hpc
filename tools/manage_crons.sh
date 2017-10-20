#!/bin/bash

# script to enable cron jobs for production / development branch scrips
#
# setup script will call this to initialize cron jobs
# and will be called again by update tasks if changes to this file are
# detected, indicating new tasks were added, or existing tasks modified
# (eg: task schedule changed)

# possible to do:
#   - script can also add/remove flocks on relevant scripts called by cron jobs
#   - admin can then run this after initial setup to disable/enable specific
#     tasks as needed
#   - ability to comment/uncomment all jobs with master/develop flag


# for each branch:

# file updates / admin tasks
# - load_repos.sh

# database tasks
# - update_extract_list.py
# - update_msr_list.py
# - clean stale entries out of dbs/collections (multiples? asdf data/trackers, extract, msr)
# - update extract/msr trackers from manual job cache?

# hpc job starts
# - extract job starter
# - msr job starter



branch=$1

action=$2


branch_dir="/sciclone/aiddata10/geo/${branch}"
src="${branch_dir}/source"

cron_tag='#geo-hpc'


config_path=$src/geo-hpc/config.json


# --------------------------------------------------

# backup crontab
backup_cron() {
    mkdir -p "$branch_dir"/backups/crontab.backup
    crontab -l > "$branch_dir"/backups/crontab.backup/$(date +%Y%m%d.%s)."$branch".crontab
}


# # get crons
# get_crontab() {
#     crontab -l
# }


# # set / update crons
# set_crontab() {
#     echo "$1" | crontab -
# }


init() {


    get_job_class() {
        val=$(python -c "import json; print json.load(open('$config_path', 'r'))['$branch']['jobs'].keys()[$1]")
        echo $val
    }

    get_cron_schedule() {
        val=$(python -c "import json; print json.load(open('$config_path', 'r'))['$branch']['jobs']['$job_class']['cron_schedule']")
        echo $val
    }

    x=$(python -c "import json; print len(json.load(open('$config_path', 'r'))['$branch']['jobs'])")

    for ((i=0;i<$x;i+=1)); do

        job_class=$(get_job_class $i)
        cron_schedule=$(get_cron_schedule $job_class)

        cron_string="$cron_scheudle"' bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' '"$job_class"' #geo-hpc'
        crontab -l | grep -v 'run_crons.*'"$branch"'.*'"$job_class" | { cat; echo "$cron_string"; } | crontab -

    done



    # # update_repos
    # update_repos_cron='*/10 * * * * bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' update_repos #geo-hpc'
    # crontab -l | grep -v 'run_crons.*'"$branch"'.*update_repos' | { cat; echo "$update_repos_cron"; } | crontab -

    # # cleanup_repos
    # cleanup_repos_cron='0 23 * * * bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' cleanup_repos #geo-hpc'
    # crontab -l | grep -v 'run_crons.*'"$branch"'.*cleanup_repos' | { cat; echo "$cleanup_repos_cron"; } | crontab -

    # # build_error_check_job - manage errors from jobs
    # build_error_check_job_cron='*/5 * * * * bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' build_error_check_job #geo-hpc'
    # crontab -l | grep -v 'run_crons.*'"$branch"'.*build_error_check_job' | { cat; echo "$build_error_check_job_cron"; } | crontab -

    # # build_update_trackers_job - index tracker update
    # build_update_trackers_job_cron='0 * * * * bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' build_update_trackers_job #geo-hpc'
    # crontab -l | grep -v 'run_crons.*'"$branch"'.*build_update_trackers_job' | { cat; echo "$build_update_trackers_job_cron"; } | crontab -

    # # build_update_extracts_job - extract queue update
    # build_update_extracts_job_cron='0 23 * * * bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' build_update_extracts_job #geo-hpc'
    # crontab -l | grep -v 'run_crons.*'"$branch"'.*build_update_extracts_job' | { cat; echo "$build_update_extracts_job_cron"; } | crontab -

    # # build_update_msr_job - msr queue update
    # build_update_msr_job_cron='0 23 * * * bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' build_update_msr_job #geo-hpc'
    # crontab -l | grep -v 'run_crons.*'"$branch"'.*build_update_msr_job' | { cat; echo "$build_update_msr_job_cron"; } | crontab -

    # # build_det_job
    # build_det_job_cron='*/1 * * * * bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' build_det_job #geo-hpc'
    # crontab -l | grep -v 'run_crons.*'"$branch"'.*build_det_job' | { cat; echo "$build_det_job_cron"; } | crontab -

    # # build_msr_job
    # build_msr_job_cron='*/1 * * * * bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' build_msr_job #geo-hpc'
    # crontab -l | grep -v 'run_crons.*'"$branch"'.*build_msr_job' | { cat; echo "$build_msr_job_cron"; } | crontab -

    # # build_extract_job
    # build_extracts_job_cron='*/2 * * * * bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' build_extracts_job #geo-hpc'
    # crontab -l | grep -v 'run_crons.*'"$branch"'.*build_extracts_job' | { cat; echo "$build_extracts_job_cron"; } | crontab -


}


# activate() {}

# deactivate() {}

# lock() {}

# unlock() {}


# --------------------------------------------------

case $action in
    "init")     backup_cron; $action; exit 0;;
    *)          echo "Invalid input."; exit 1 ;;
esac
