#!/bin/bash

# makes sure the latest versions of repos are pulled from github
# should be called periodically from cronjob (cronjob may be added automatically during setup)

# server=$1
branch=$1

timestamp=$(date +%s)

echo -e "\n"
# echo Building on server: "$server"
echo Updating repos for branch: "$branch"
echo Timestamp: "$timestamp"
echo -e "\n"


src="${HOME}"/active/"$branch"

rm -rf "$src"/git
mkdir "$src"/git
cd "$src"/git


get_hash() {
    echo $(md5sum $1 | awk '{ print $1 }')
}


check_repo() {

    echo -e "\n"
    echo Checking repo: "$repo"

    if [ "$repo" = 'asdf' ]; then
        old_repo_hash=$(get_hash "$src"/git/asdf/src/tools/repo_list.sh)
        old_load_hash=$(get_hash "$src"/git/asdf/src/tools/load_repos.sh)
        old_update_hash=$(get_hash "$src"/git/asdf/src/tools/update_repos.sh)
    fi

    update_status=$(bash gitupdate.sh "$src"/git/"$repo")

    echo Status for repo ( "$repo" ): "$update_status"


    if [ $(echo "$update_status" | grep 'Update complete') ]; then

        echo Completing update for repo: "$repo"

        if [ "$repo" = 'asdf' ]; then
            new_repo_hash=$(get_hash "$src"/git/asdf/src/tools/repo_list.sh)
            new_load_hash=$(get_hash "$src"/git/asdf/src/tools/load_repos.sh)
            new_update_hash=$(get_hash "$src"/git/asdf/src/tools/update_repos.sh)

            if [ "$old_repo_hash" != "$new_repo_hash" ] | [ "$old_load_hash" != "$new_load_hash" ]; then
                echo -e "\n"
                echo "Found new load_repos.sh ..."
                bash "$src"/git/asdf/src/tools/load_repos.sh "$branch"
                exit 0 
            else
                if [ "$old_update_hash" != "$new_update_hash" ]; then
                    echo -e "\n"
                    echo "Found new update_repos.sh ..."
                    bash "$src"/git/asdf/src/tools/update_repos.sh "$branch"
                    exit 0 
                fi
            fi

        fi

        cp -r "$repo" "$src"/latest/"$timestamp"."$repo"

        ln -sfn "$src"/latest/"$timestamp"."$repo" "$src"/"$repo"
    fi
}


repo_list=($(cat "$src"/asdf/src/tools/repo_list.txt))

for repo in ${repo_list[*]}; do 
    check_repo
done


# remove old repos from latest
echo -e "\n"
echo 'Cleaning up old repos...'

find "$src"/latest -mindepth 1 -maxdepth 1 -type d | grep -v "$timestamp" | xargs rm -rf

echo 'Done'
echo -e "\n"

