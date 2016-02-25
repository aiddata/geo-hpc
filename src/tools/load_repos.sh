#!/bin/bash

# makes sure the latest versions of repos are downloaded
# should be called periodically from cronjob (cronjob may be added automatically during setup)

server=$1
branch=$2

echo -e "\n"
echo Building on server: "$server"
echo Loading branch: "$branch"
echo -e "\n"


src="${HOME}"/active/"$branch"

cd "$src"/latest
# rm -rf asdf

timestamp=$(date +%s)


get_repo() {

    echo -e "\n"
    echo Loading repo: "$repo"

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

    echo -e "\n"
    echo "Found new load_repos.sh ..."
    cp  "$src"/asdf/src/tools/load_repos.sh "$src"/load_repos.sh
    bash "$src"/load_repos.sh "$server" "$branch"

else

    repo_list=(
        'extract-scripts'
        'mean-surface-rasters'
        'det-module'
    )

    for repo in ${repo_list[*]}; do 
        # echo $repo
        get_repo
    done


    # remove old repos from latest
    echo -e "\n"
    echo 'Cleaning up old repos...'

    find "$src"/latest -mindepth 1 -maxdepth 1 -type d | grep -v "$timestamp" | xargs rm -rf

    echo 'Done'
    echo -e "\n"

fi
