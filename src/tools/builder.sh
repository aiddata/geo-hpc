#!/bin/bash

# used to initialize portions of asdf

# manages setup of both production and development branch files
# NOTE: this script must be manually replaced/updated if changes to setup.sh are made


# get server/branch inputs from user

server=$1
branch=$2

timestamp=$(date +%s)

echo -e "\n"
echo Building on server: "$server"
echo Loading branch: "$branch"
echo Timestamp: "$timestamp"
echo -e "\n"




# setup branch directory

src="${HOME}"/active/"$branch"

rm -rf "$src"

mkdir -p "$src"/{latest,'jobs',tmp,tasks,log/{db_updates,load_repos}}


# setup load_repos.sh cronjob and run load_repos.sh for first time

cd "$src"/tmp

if [[ $server == "hpc" ]]; then
    git clone -b "$branch" https://github.com/itpir/asdf
else
    git clone -b "$branch" http://github.com/itpir/asdf
fi


# cp  "$src"/tmp/asdf/src/tools/load_repos.sh "$src"/tasks/load_repos.sh
# cp  "$src"/tmp/asdf/src/tools/build_update_job.sh "$src"/tasks/build_update_job.sh



bash "$src"/asdf/src/tools/load_repos.sh "$server" "$branch" 2>1 | tee "$src"/log/load_repos/$(date +%s).load_repos.log



rm -rf "$src"/tmp/asdf





mkdir -p "$src"/../crontab.backup
crontab -l > "$src"/../crontab.backup/$(date +%Y%m%d.%s)."$branch".crontab


# --------------------------------------------------
# replace with running manage_crons.sh script later
#

load_repos_base='0 4-23/6 * * * bash '"$src"'/asdf/src/tools/load_repos.sh'
load_repos_cron="$load_repos_base"' '"$server"' '"$branch"' 2>1 | tee 1>'"$src"'/log/load_repos/'$(date +%s)'.load_repos.log #asdf'
crontab -l | grep -v 'load_repos.*'"$branch" | { cat; echo "$load_repos_cron"; } | crontab -


build_update_job_cron='0 0 * * * bash '"$src"'/asdf/src/tools/build_update_job.sh '"$branch"' 2>1 | tee 1>'"$src"'/log/db_updates/'$(date +%s)'.db_updates.log #asdf'
crontab -l | grep -v 'build_update_job.*'"$branch" | { cat; echo "$build_update_job_cron"; } | crontab -

# --------------------------------------------------





# other setup?
# 


