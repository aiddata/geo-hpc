#!/bin/bash

# clone repos from github


# server=$1
branch=$1

timestamp=$(date +%s)

echo -e "\n"
# echo Building on server: "$server"
echo Loading repos for branch: "$branch"
echo Timestamp: "$timestamp"
echo -e "\n"


src="${HOME}"/active/"$branch"

rm -rf "$src"/git
mkdir "$src"/git
cd "$src"/git


get_repo() {

    echo -e "\n"
    echo Loading repo: "$repo"

    git clone -b "$branch" https://github.com/itpir/"$repo" "$repo"

    cp -r "$repo" "$src"/latest/"$timestamp"."$repo"

    ln -sfn "$src"/latest/"$timestamp"."$repo" "$src"/"$repo"

}


repo_list=($(cat "$src"/asdf/src/tools/repo_list.txt))

for repo in ${repo_list[*]}; do 
    get_repo
done


# remove old repos from latest
echo -e "\n"
echo 'Cleaning up old repos...'

find "$src"/latest -mindepth 1 -maxdepth 1 -type d | grep -v "$timestamp" | xargs rm -rf

echo 'Done'
echo -e "\n"

