#!/bin/bash

# cleanup old repos stored in the "latest" directory
#   only remove repos over 48 hours old
#   always keep at least the ~5 latest versions
#
# should be called periodically from cronjob
#
# input args
#   branch


branch=$1

# timestamp=$(date +%s)
timestamp=$(date +%Y%m%d.%s)

echo '=================================================='
# echo Building on server: "$server"
echo Cleaning up old repos for branch: "$branch"
echo Timestamp: $(date) '('"$timestamp"')'
echo -e "\n"


src="${HOME}"/active/"$branch"


cd "$src"/latest


repo_list=($(cat "$src"/asdf/src/repo_list.txt))

today=$(date +%Y%m%d)
yesterday=$(date -d "yesterday" +%Y%m%d)



for orgrepo in ${repo_list[*]}; do
    repo=$(basename ${orgrepo})

    echo Cleaning up repo: "$repo"

    tmp_rm_list=$(find "$src"/latest -mindepth 1 -maxdepth 1 -type d | grep "$repo"$ | sort -nr | tail -n +6 | grep -v "$today\|$yesterday")

    for i in ${tmp_rm_list[*]}; do

        find "$i" -type f -exec rm -rf "{}" \;
        find "$i" -type d -exec rm -rf "{}" \;

    done
    echo -e "\n"

done


echo 'Done'
echo -e "\n"
