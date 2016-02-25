#!/bin/bash

# makes sure the latest versions of repos are downloaded
# should be called periodically from cronjob (cronjob may be added automatically during setup)

server=$1
branch=$2

echo -e "\n"
echo Building on server: "$server"
echo Loading branch: "$branch"


src="${HOME}"/active/"$branch"

cd "$src"/latest
# rm -rf asdf

timestamp=$(date +%s)


get_repo() {

    if [[ $server == "hpc" ]]; then
        git clone -b "$branch" https://github.com/itpir/"$repo" "$timestamp"."$repo"
    else
        git clone -b "$branch" http://github.com/itpir/"$repo" "$timestamp"."$repo"
    fi

    ln -sfn "$src"/latest/"$timestamp"."$repo" "$src"/"$repo"



}


repo='asdf'
get_repo


old_hash=$(md5sum "$src"/load_repos.sh | awk '{ print $1 }')
new_hash=$(md5sum "$src"/latest/"$timestamp"."$repo"/src/tools/load_repos.sh | awk '{ print $1 }')


if [[ "$old_hash" != "$new_hash" ]]; then

    echo "Found new load_repos.sh ..."
    cp  "$src"/asdf/src/tools/load_repos.sh "$src"/load_repos.sh
    bash "$src"/load_repos.sh "$server" "$branch"

else

    # cd "$src"
    # rm -rf extract-scripts

    # if [[ $server == "hpc" ]]; then
    #     git clone -b "$branch" https://github.com/itpir/extract-scripts
    # else
    #     git clone -b "$branch" http://github.com/itpir/extract-scripts
    # fi

    repo='extract-scripts'
    get_repo


    # cd "$src"
    # rm -rf mean-surface-rasters

    # if [[ $server == "hpc" ]]; then
    #     git clone -b "$branch" https://github.com/itpir/mean-surface-rasters
    # else
    #     git clone -b "$branch" http://github.com/itpir/mean-surface-rasters
    # fi

    repo='mean-surface-rasters'
    get_repo


    # cd "$src"
    # rm -rf det-module

    # if [[ $server == "hpc" ]]; then
    #     git clone -b "$branch" https://github.com/itpir/det-module
    # else
    #     git clone -b "$branch" http://github.com/itpir/det-module
    # fi

    repo='det-module'
    get_repo



    # remove old repos from latest
    find "$src"/latest -type d -maxdepth 1 | grep -v "$timestamp" | xargs -0 rm

fi
