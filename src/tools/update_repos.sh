#!/bin/bash

# makes sure the latest versions of repos are pulled from github
# should be called periodically from cronjob (cronjob may be added automatically during setup)

# server=$1
branch=$1

# timestamp=$(date +%s)
timestamp=$(date +%Y%m%d.%s)

echo '=================================================='
# echo Building on server: "$server"
echo Updating repos for branch: "$branch"
echo Timestamp: $(date)("$timestamp")
echo -e "\n"


src="${HOME}"/active/"$branch"

# rm -rf "$src"/git
# mkdir "$src"/git
cd "$src"/git



check_repo() {

    echo 'Checking repo: '"$repo"

    if [ "$repo" = 'asdf' ]; then
        old_manage_cron_hash=$(md5sum "$src"/git/asdf/src/tools/manage_crons.sh | awk '{ print $1 }')
        old_repo_hash=$(md5sum "$src"/git/asdf/src/tools/repo_list.txt | awk '{ print $1 }')
        old_load_hash=$(md5sum "$src"/git/asdf/src/tools/load_repos.sh | awk '{ print $1 }')
        old_update_hash=$(md5sum "$src"/git/asdf/src/tools/update_repos.sh | awk '{ print $1 }')
    fi

    update_status=$(bash "$src"/git/asdf/src/tools/gitupdate.sh "$src"/git/"$repo")

    echo 'Status for repo ( '"$repo"' ): '"$update_status"

    if echo "$update_status" | grep -q 'Update complete'; then

        echo 'Completing update for repo: '"$repo"


        cp -r "$repo" "$src"/latest/"$timestamp"."$repo"
        ln -sfn "$src"/latest/"$timestamp"."$repo" "$src"/"$repo"


        # for i in "$src"/latest/*; do
        #     echo "$i"
            
        #     if echo "$i" | grep -q "$repo"; then
        #         if echo "$i" | grep -q -v "$timestamp"; then

        #             echo 'Cleaning up old '"$repo"' repo...'
        #             find "$i" -type f -exec rm -rf "{}" \;
        #             find "$i" -type d -exec rm -rf "{}" \;

        #         fi
        #     fi
        # done

        if [ "$repo" = 'asdf' ]; then
            new_manage_cron_hash=$(md5sum "$src"/git/asdf/src/tools/manage_crons.sh | awk '{ print $1 }')
            new_repo_hash=$(md5sum "$src"/git/asdf/src/tools/repo_list.txt | awk '{ print $1 }')
            new_load_hash=$(md5sum "$src"/git/asdf/src/tools/load_repos.sh | awk '{ print $1 }')
            new_update_hash=$(md5sum "$src"/git/asdf/src/tools/update_repos.sh | awk '{ print $1 }')

            if [ "$old_manage_cron_hash" != "$new_manage_cron_hash" ]; then
                echo -e "\n"
                echo "Updating crons ..."
                bash "$src"/git/asdf/src/tools/manage_crons.sh "$branch" init
            fi

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

        echo -e "\n"


    fi
}


repo_list=($(cat "$src"/asdf/src/tools/repo_list.txt))

for repo in ${repo_list[*]}; do 
    check_repo
done

echo 'Done'
echo -e "\n"

