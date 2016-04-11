#!/bin/bash

# script to enable cron jobs for production / development branch scrips

# script can also add/remove flocks on relevant scripts called by cron jobs

# setup script will call this to initialize cron jobs
# admin can then run this after initial setup to disable/enable as needed

# ability to comment/uncomment all jobs with master/develop flag


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

# det queue processing
# - prep.py
# - processing.py



branch=$1

action=$2


src="${HOME}"/active/"$branch"

cron_tag='#asdf'

# --------------------------------------------------

# backup crontab
backup_cron() {
    mkdir -p "$src"/../crontab.backup
    crontab -l > "$src"/../crontab.backup/$(date +%Y%m%d.%s)."$branch".crontab
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

    # shell_line="SHELL=/bin/bash"
    # echo "$shell_line" | { cat; crontab -l | grep -v "$shell_line"; }| crontab -


    # setup update_repos.sh cronjob
    # update_repos_base='0 4-23/6 * * *'
    # update_repos_script='bash '"$src"'/asdf/src/tools/update_repos.sh '"$branch"
    # update_repos_log='2>&1 | tee 1>'"$src"'/log/update_repos/`date +%s`.update_repos.log'
    # update_repos_cron="$update_repos_base $update_repos_script $update_repos_log $cron_tag"
    # crontab -l | grep -v 'update_repos.*'"$branch" | { cat; echo "$update_repos_cron"; } | crontab -

    update_repos_cron='*/10 * * * * bash '"$src"'/asdf/src/tools/cron_wrapper.sh '"$branch"' update_repos #asdf'
    crontab -l | grep -v 'cron_wrapper.*'"$branch"'.*update_repos' | { cat; echo "$update_repos_cron"; } | crontab -


    # setup build_update_job.sh cronjob
    # build_update_job_base='0 0 * * * '
    # build_update_job_script='bash '"$src"'/asdf/src/tools/build_update_job.sh '"$branch"' `date +%s`'
    # build_update_job_log='2>&1 | tee 1>'"$src"'/log/db_updates/`date +%s`.db_updates.log'
    # build_update_job_cron="$build_update_job_base $build_update_job_script $build_update_job_log $cron_tag"
    # crontab -l | grep -v 'build_update_job.*'"$branch" | { cat; echo "$build_update_job_cron"; } | crontab -

    db_updates_cron='0 4-23/4 * * * bash '"$src"'/asdf/src/tools/cron_wrapper.sh '"$branch"' db_updates #asdf'
    crontab -l | grep -v 'cron_wrapper.*'"$branch"'.*db_updates' | { cat; echo "$db_updates_cron"; } | crontab -


    cleanup_repos_cron='25 0 * * * bash '"$src"'/asdf/src/tools/cron_wrapper.sh '"$branch"' cleanup_repos #asdf'
    crontab -l | grep -v 'cron_wrapper.*'"$branch"'.*cleanup_repos' | { cat; echo "$cleanup_repos_cron"; } | crontab -

}


# activate() {}

# deactivate() {}

# lock() {}

# unlock() {}


# --------------------------------------------------

case $action in
    "init")     $backup_cron; $action; exit 0;;
    *)          echo "Invalid input."; exit 1 ;;
esac
