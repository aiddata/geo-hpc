#!/bin/bash

# script to enable cron jobs for production / development branch scrips
#
# jobs are defined by config.json
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
        echo "$val"
    }

    x=$(python -c "import json; print len(json.load(open('$config_path', 'r'))['$branch']['jobs'])")

    echo "Adding crons..."
    for ((i=0;i<$x;i+=1)); do

        job_class=$(get_job_class $i)
        cron_schedule=$(get_cron_schedule $job_class)

        cron_string="$cron_schedule"' bash '"$src"'/geo-hpc/tools/run_crons.sh '"$branch"' '"$job_class"' #geo-hpc'

        echo "$cron_string"
        crontab -l | grep -v 'run_crons.*'"$branch"'.*'"$job_class" | { cat; echo "$cron_string"; } | crontab -

    done

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
