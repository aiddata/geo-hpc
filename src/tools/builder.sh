#!/bin/bash

# used to initialize portions of asdf
# manages setup of both production and development branch files


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

mkdir -p "$src"/{tmp,git,latest,log/{db_updates,update_repos}}
#,'jobs',tasks}

cd "$src"


# clone tmp asdf for init scripts
git clone -b "$branch" https://github.com/itpir/asdf tmp/asdf

# run load_repos.sh
bash "$src"/tmp/asdf/src/tools/load_repos.sh "$branch" #2>&1 | tee "$src"/log/load_repos/$(date +%s).load_repos.log


# setup crons using manage_crons.sh script 
bash "$src"/tmp/asdf/src/tools/manage_crons.sh "$branch" init



# other setup?
# 


# clean up tmp asdf
rm -rf "$src"/tmp/asdf

