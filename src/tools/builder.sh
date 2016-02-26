#!/bin/bash

# used to initialize portions of asdf
# manages setup of both production and development branch files


# server=$1
branch=$1

# timestamp=$(date +%s)

echo -e "\n"
# echo Building on server: "$server"
echo Starting build for branch: "$branch"
# echo Timestamp: "$timestamp"
echo -e "\n"


# setup branch directory
src="${HOME}"/active/"$branch"

rm -rf "$src"

mkdir -p "$src"/{tmp,git,latest,log/{db_updates,update_repos}}
#,'jobs',tasks}

cd "$src"


# clone tmp asdf for init scripts
git clone -b "$branch" https://github.com/itpir/asdf tmp/asdf

# run load_repos.sh
bash "$src"/tmp/asdf/src/tools/load_repos.sh "$branch" #2>&1 | tee "$src"/log/load_repos/$(date +%s).load_repos.log

# clean up tmp asdf
rm -rf "$src"/tmp/asdf


# --------------------------------------------------
# replace with running manage_crons.sh script later
#

# backup crontab
mkdir -p "$src"/../crontab.backup
crontab -l > "$src"/../crontab.backup/$(date +%Y%m%d.%s)."$branch".crontab


# setup update_repos.sh cronjob
update_repos_base='0 4-23/6 * * * bash '"$src"'/asdf/src/tools/update_repos.sh'
update_repos_cron="$update_repos_base"' '"$server"' '"$branch"' 2>&1 | tee 1>'"$src"'/log/update_repos/'$(date +%s)'.update_repos.log #asdf'
crontab -l | grep -v 'update_repos.*'"$branch" | { cat; echo "$update_repos_cron"; } | crontab -


# setup build_update_job.sh cronjob
build_update_job_cron='0 0 * * * bash '"$src"'/asdf/src/tools/build_update_job.sh '"$branch"' 2>&1 | tee 1>'"$src"'/log/db_updates/'$(date +%s)'.db_updates.log #asdf'
crontab -l | grep -v 'build_update_job.*'"$branch" | { cat; echo "$build_update_job_cron"; } | crontab -



# --------------------------------------------------
# other setup?
# 

