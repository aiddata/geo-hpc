#!/bin/bash

cron=$1
branch=$2

src="${HOME}"/active/"$branch"

case $cron in
    "update_repos")     bash "$src"/asdf/src/tools/update_repos.sh "$branch" 2>&1 | tee 1>"$src"/log/update_repos/$(date +%s).update_repos.log
                        exit 0;;

    "db_updates")       bash "$src"/asdf/src/tools/build_update_job.sh "$branch" $(date +%s) 2>&1 | tee 1>"$src"/log/db_updates/$(date +%s).db_updates.log 
                        exit 0;;
    
    *)                  echo "Invalid cron."; 
                        exit 1 ;;
esac 



