#!/bin/bash

# makes sure the latest versions of repos are pulled from github
# should be called periodically from cronjob
#
# reruns specific scripts when changes in certain files
# are detected
#
# input args:
#   branch
#   force_update (optional, any input for this var will enable force_update clauses)


branch=$1

# optional
force_update=$2

# timestamp=$(date +%s)
timestamp=$(date +%Y%m%d.%s)

echo Updating repos for branch: "$branch"
echo Timestamp: $(date) '('"$timestamp"')'
echo -e "\n"


branch_dir="/sciclone/aiddata10/geo/${branch}"
src="${branch_dir}/source"


# -----------------------------------------------------------------------------


check_repo() {

    echo -e "\n"
    echo 'Checking repo: '"$repo"

    # make sure repo exists
    # run load repos if it does not
    if [ ! -d "$src"/"$repo" ]; then
        echo -e "\n"
        echo "Found missing repo. Loading repos..."
        bash "$src"/git/geo-hpc/tools/load_repos.sh "$branch"
        exit 0
    fi

    if [ "$repo" = 'geo-hpc' ]; then
        old_repo_hash=$(md5sum "$src"/git/geo-hpc/repo_list.txt | awk '{ print $1 }')
        old_load_hash=$(md5sum "$src"/git/geo-hpc/tools/load_repos.sh | awk '{ print $1 }')
        old_update_hash=$(md5sum "$src"/git/geo-hpc/tasks/update_repos.sh | awk '{ print $1 }')
    fi

    update_status=$(bash "$src"/git/geo-hpc/tools/gitupdate.sh "$src"/git/"$repo" "$branch")

    echo 'Status for repo ( '"$repo"' ): '"$update_status"

    valid_update=$(echo "$update_status" | grep 'Update complete')
    if [[ "$valid_update" || "$repo" = 'geo-hpc' && "$force_update" ]]; then

        echo 'Completing update for repo: '"$repo"

        cp -r "$src"/git/"$repo" "$src"/latest/"$timestamp"."$repo"
        ln -sfn "$src"/latest/"$timestamp"."$repo" "$src"/"$repo"

        if [ "$repo" = 'geo-hpc' ]; then

            new_repo_hash=$(md5sum "$src"/git/geo-hpc/repo_list.txt | awk '{ print $1 }')
            new_load_hash=$(md5sum "$src"/git/geo-hpc/tools/load_repos.sh | awk '{ print $1 }')
            new_update_hash=$(md5sum "$src"/git/geo-hpc/tasks/update_repos.sh | awk '{ print $1 }')

            echo -e "\n"

            echo "Updating config db ..."
            python "$src"/git/geo-hpc/tools/add_config_to_mongo.py "$branch"

            echo "Updating featured datasets ..."
            python "$src"/git/geo-hpc/tools/update_featured_datasets.py "$branch"

            echo "Updating crons ..."
            bash "$src"/git/geo-hpc/tools/manage_crons.sh "$branch" init

            if [ "$old_repo_hash" != "$new_repo_hash" ] || [ "$old_load_hash" != "$new_load_hash" ]; then
                echo -e "\n"
                echo "Found new load_repos.sh or repo list..."
                bash "$src"/git/geo-hpc/tools/load_repos.sh "$branch"
                exit 0

            elif [ "$old_update_hash" != "$new_update_hash" ]; then
                echo -e "\n"
                echo "Found new update_repos.sh ..."
                bash "$src"/git/geo-hpc/tasks/update_repos.sh "$branch" force
                exit 0

            fi

        fi

    fi

}


# -----------------------------------------------------------------------------


if [ "$force_update" ];then
    echo "Running forced update"
fi


repo_list=($(cat "$src"/geo-hpc/repo_list.txt))

for orgrepo in ${repo_list[*]}; do
    repo=$(basename ${orgrepo})
    check_repo
done

# create symlink from geo-hpc/utils/geo_rasterstats to source code dir of rasterstats repo
rs_src="${src}/python-rasterstats/src/rasterstats"
rs_dst="${src}/geo-hpc/utils/geo_rasterstats"
rm -r ${rs_dst}
ln -s ${rs_src} ${rs_dst}

echo -e "\n"
echo "Setting permissions..."

# make sure permissions are set
find "${src}" -type d ! -perm u=rwx,g=rxs,o=rx -exec chmod u=rwx,g=rxs,o=rx {} +
find "${src}" -type f ! -perm u=rw,g=r,o=r -exec chmod u=rw,g=r,o=r {} +



