#!/bin/bash

# clones repos from github for specified branch
# removes existing files for repos if they exists
#
# gets list of repos from repo_list.txt
#
# also creates copy of repo in "latest" dir and
# creates symlinks in branch's root dir
#
# input args
#   branch


# server=$1
branch=$1

# timestamp=$(date +%s)
timestamp=$(date +%Y%m%d.%s)

echo -e "\n"
# echo Building on server: "$server"
echo Loading repos for branch: "$branch"
echo Timestamp: "$timestamp"
echo -e "\n"


branch_dir="/sciclone/aiddata10/geo/${branch}"
src="${branch_dir}/source"


cd "$src"/git


get_repo() {

    echo -e "\n"
    echo Loading repo: "$repo"

    # git clone -b "$branch" https://github.com/"$orgrepo" "$repo"

    wget https://github.com/"$orgrepo"/archive/"$branch".zip
    unzip -o "$repo"-"$branch".zip
    mv "$repo"-"$branch" "$repo"


    cp -r "$repo" "$src"/latest/"$timestamp"."$repo"

    ln -sfn "$src"/latest/"$timestamp"."$repo" "$src"/"$repo"

}


repo_list=($(cat "$src"/geo-hpc/repo_list.txt))

for orgrepo in ${repo_list[*]}; do
    repo=$(basename ${orgrepo})
    get_repo
done


echo -e "\n"
echo 'Done'
echo -e "\n"

