#!/bin/bash

# cleanup old repos stored in the "latest" directory
# should be called periodically from cronjob (cronjob may be added automatically during setup)


branch=$1

# timestamp=$(date +%s)
timestamp=$(date +%Y%m%d.%s)

echo -e "\n"
# echo Building on server: "$server"
echo Cleaning up old repos for branch: "$branch"
echo Timestamp: "$timestamp"
echo -e "\n"


src="${HOME}"/active/"$branch"


cd "$src"/latest


# only remove repos over 48 hours old
# always keep at least the ~5 latest versions

repo_list=($(cat "$src"/asdf/src/tools/repo_list.txt))

for repo in ${repo_list[*]}; do 


    for i in "$src"/latest/*; do
        echo "$i"
        
        if echo "$i" | grep -q "$repo"; then

            echo "cool"
            # if echo "$i" | grep -q -v "$timestamp"; then

            #     echo 'Cleaning up old '"$repo"' repo...'
            #     find "$i" -type f -exec rm -rf "{}" \;
            #     find "$i" -type d -exec rm -rf "{}" \;

            # fi
        
        fi

    done


done





echo -e "\n"
echo 'Done'
echo -e "\n"
