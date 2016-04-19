#!/bin/bash

branch=$1
cron=$2

src="${HOME}"/active/"$branch"

timestamp=$(date +%Y%m%d)

# make sure log directories exist
mkdir -p "$src"/log/{update_repos,cleanup_repos,db_updates}


case $cron in
    "update_repos")     bash "$src"/asdf/src/tools/update_repos.sh "$branch" 2>&1 | tee 1>>"$src"/log/update_repos/"$timestamp".update_repos.log
                        exit 0;;

    "cleanup_repos")    bash "$src"/asdf/src/tools/cleanup_repos.sh "$branch" 2>&1 | tee 1>>"$src"/log/cleanup_repos/"$timestamp".cleanup_repos.log
                        exit 0;;

    "db_updates")       bash "$src"/asdf/src/tools/build_db_updates_job.sh "$branch" "$timestamp" 2>&1 | tee 1>>"$src"/log/db_updates/"$timestamp".db_updates.log
                        exit 0;;

    "build_msr_job")    bash "$src"/asdf/src/tools/build_msr_job.sh "$branch" "$timestamp" 2>&1 | tee 1>>"$src"/log/msr/"$timestamp".msr.log
                        exit 0;;

    *)                  echo "Invalid cron.";
                        exit 1 ;;
esac

