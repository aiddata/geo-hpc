#!/bin/bash

branch=$1
cron=$2

src="${HOME}"/active/"$branch"

timestamp=$(date +%Y%m%d)

# make sure log directories exist
# mkdir -p "$src"/log/{update_repos,cleanup_repos,db_updates,msr}

export PYTHONPATH="${PYTHONPATH}:/usr/local/amd64/seoul/gcc/python-2.7.8/lib/python2.7/site-packages/:/sciclone/aiddata10/REU/py_libs/lib/python2.7/site-packages"

case $cron in
    "update_repos")         mkdir -p "$src"/log/update_repos
                            bash "$src"/asdf/src/tools/update_repos.sh "$branch" 2>&1 | tee 1>>"$src"/log/update_repos/"$timestamp".update_repos.log
                            exit 0;;

    "cleanup_repos")        mkdir -p "$src"/log/cleanup_repos
                            bash "$src"/asdf/src/tools/cleanup_repos.sh "$branch" 2>&1 | tee 1>>"$src"/log/cleanup_repos/"$timestamp".cleanup_repos.log
                            exit 0;;

    "db_updates")           mkdir -p "$src"/log/db_updates
                            bash "$src"/asdf/src/tools/build_db_updates_job.sh "$branch" "$timestamp" 2>&1 | tee 1>>"$src"/log/db_updates/"$timestamp".db_updates.log
                            exit 0;;

    "build_msr_job")        mkdir -p "$src"/log/msr
                            bash "$src"/asdf/src/tools/build_msr_job.sh "$branch" "$timestamp" 2>&1 | tee 1>>"$src"/log/msr/"$timestamp".msr.log
                            exit 0;;

    "build_extract_job")    mkdir -p "$src"/log/extract
                            bash "$src"/asdf/src/tools/build_extract_job.sh "$branch" "$timestamp" 2>&1 | tee 1>>"$src"/log/extract/"$timestamp".extract.log
                            exit 0;;

    *)                      echo "Invalid cron.";
                            exit 1 ;;
esac

