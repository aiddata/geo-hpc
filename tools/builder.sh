#!/bin/bash

# used to initialize portions of geo-hpc
# manages setup for specified branch
#   clears any existing files created for branch
#   loads repos
#   creates cron tasks
#   adds config options to mongo

# should be called automatically by using the more generic setup.sh
#
# input args
#   branch


# server=$1
branch=$1

# timestamp=$(date +%s)

echo -e "\n"
# echo Building on server: "$server"
echo 'Starting build for branch: '"$branch"
# echo Timestamp: "$timestamp"
echo -e "\n"


# setup branch directory
branch_dir="/sciclone/aiddata10/geo/${branch}"
src="${branch_dir}/source"

if [[ -d ${src} ]] && [[ $2 == "--overwrite" ]]; then
    find "$src" -type f -exec rm -rf "{}" \;
    find "$src" -type d -exec rm -rf "{}" \;
fi

if [[ -d ${src} ]];then
    echo "Warning: Source directory exists for branch (${branch_dir})"
    echo "    Use --overwrite option to rebuild source for branch"
    echo "    (Other directories in branch will not be removed)"
    exit 1
fi


mkdir -p "$src"/{git,latest}


# create temp dir, clone geo-hpc for init scripts
tmp=$(mktemp -d)
git clone -b "$branch" https://github.com/itpir/geo-hpc "$tmp"/geo-hpc

# need this so load_repo.sh can find repos_list.txt
ln -sfn $tmp "$src"/geo-hpc


# run load_repos.sh
bash "$tmp"/geo-hpc/tools/load_repos.sh "$branch" #2>&1 | tee "$src"/log/load_repos/$(date +%s).load_repos.log

# setup crons using manage_crons.sh script
bash "$tmp"/geo-hpc/tools/manage_crons.sh "$branch" init


# add basic config info to mongo (for det, etc.)
# does not update when config changes
#   may change later, currently just needs basic branch name/server info
python "$tmp"/geo-hpc/tools/add_config_to_mongo.py "$branch"


# clean up tmp geo-hpc
rm -rf "$tmp"



# initialize other directories in case they do not exist

mkdir -p "${backup_dir"/log

backup_dir="${branch_dir}"/backups
mkdir -p "${backup_dir}"/{mongodb_backups,mongodb_logs,mongodb_cron_logs}


output_dir="${branch_dir}"/outputs
mkdir -p "${backup_dir}"/{det,extracts,msr}


find "${branch_dir}" -type d -exec chmod u=rwx,g=rwxs,o=rx {} +
find "${branch_dir}" -type f -exec chmod u=rw,g=rw,o=r {} +
