#!/bin/bash

# used to initialize portions of asdf
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
src="${HOME}"/active/"$branch"

# rm -rf "$src"
find "$src" -type f -exec rm -rf "{}" \;
find "$src" -type d -exec rm -rf "{}" \;

mkdir -p "$src"/{tmp,git,latest,log}
#,'jobs',tasks}

cd "$src"


# clone tmp asdf for init scripts
git clone -b "$branch" https://github.com/itpir/asdf tmp/asdf
ln -sfn tmp/asdf "$src"/asdf

# run load_repos.sh
bash "$src"/tmp/asdf/src/tools/load_repos.sh "$branch" #2>&1 | tee "$src"/log/load_repos/$(date +%s).load_repos.log


# setup crons using manage_crons.sh script
bash "$src"/tmp/asdf/src/tools/manage_crons.sh "$branch" init



# add basic config info to mongo (for det, etc.)
# does not update when config changes
#   may change later, currently just needs basic branch name/server info
python "$src"/tmp/asdf/src/tools/add_config_to_mongo.py "$branch"



# clean up tmp asdf
rm -rf "$src"/tmp/asdf

