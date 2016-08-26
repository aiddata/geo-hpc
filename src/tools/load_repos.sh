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


src="${HOME}"/active/"$branch"

# rm -rf "$src"/git
find "$src"/git -type f -exec rm -rf "{}" \;
find "$src"/git -type d -exec rm -rf "{}" \;

mkdir "$src"/git
cd "$src"/git


get_repo() {

    echo -e "\n"
    echo Loading repo: "$repo"

    git clone -b "$branch" https://github.com/"$orgrepo" "$repo"

    cp -r "$repo" "$src"/latest/"$timestamp"."$repo"

    ln -sfn "$src"/latest/"$timestamp"."$repo" "$src"/"$repo"

}


repo_list=($(cat "$src"/asdf/src/tools/repo_list.txt))

for orgrepo in ${repo_list[*]}; do
    repo=$(basename ${orgrepo})
    get_repo
done


echo -e "\n"
echo 'Done'
echo -e "\n"

