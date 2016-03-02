#!/bin/bash


branch=$1
timestamp=$2
src=$3

output_path=$(mktemp -p "$src"/log/db_updates/tmp)

echo 'Timestamp: '$timestamp >> "$output_path"
echo 'Job id: '"$PBS_JOBID" >> "$output_path"

echo -e "\n *** Running update_trackers.py... \n" >> "$output_path"
python $src/asdf/src/tools/update_trackers.py "$branch" 2>&1 | tee 1>> "$output_path"

echo -e "\n *** Running update_extract_queue.py... \n"  >> "$output_path"
python $src/asdf/src/tools/update_extract_queue.py "$branch" 2>&1 | tee 1>> "$output_path"

echo -e "\n *** Running update_msr_queue.py... \n" >> "$output_path"
python $src/asdf/src/tools/update_msr_queue.py "$branch" 2>&1 | tee 1>> "$output_path"

echo -e "\nDone \n" >> "$output_path"

cat "$output_path" >> "$src"/log/db_updates/"$timestamp".db_updates.log

rm "$output_path"
