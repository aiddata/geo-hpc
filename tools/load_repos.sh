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

    if [ ! -z "${repo// }" ] && [ -d "$repo" ]; then
        rm -rf "$repo"
    fi

    git clone -b "$branch" git@github.com:"$orgrepo" "$repo"

    if [[ "$repo" == "public_datasets" && "$branch" != "master" ]]; then
        git clone -b "master" git@github.com:"$orgrepo" "$repo"
    fi

    # ---
    # wget https://github.com/"$orgrepo"/archive/"$branch".zip -O tmp_"$repo".zip

    # if [[ "$repo" == "public_datasets" && "$branch" != "master" ]]; then
    #     wget https://github.com/"$orgrepo"/archive/master.zip -O tmp_"$repo".zip
    # fi

    # unzip -o tmp_"$repo".zip -d tmp_"$repo"
    # mv tmp_"$repo"/* "$repo"

    # rm -r tmp_"$repo"
    # rm tmp_"$repo".zip
    # ---

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

