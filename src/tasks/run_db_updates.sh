#!/bin/bash

# called by job created by build_db_updates_job.sh
# running different databased update scripts for various
# portions of asdf (core functions, maintenance, module specific tasks, etc.)
#
# input args
#   branch
#   timestamp
#
# NOTE:
# If something with this script breaks, remove manually specifying src
# and go back to using src from input args.
#
# If this does not break, remove src input arg and remove it being passed
# to this script in build_db_updates_job.sh
#
# Cannot remember if there was some odd reason it needed to be passed
# instead of just built in this script


branch=$1
timestamp=$2

src="${HOME}"/active/"$branch"


# output_path=$(mktemp -p "$src"/log/db_updates/tmp)

echo 'Timestamp: '$timestamp #>> "$output_path"
echo 'Job: '"$PBS_JOBID" #>> "$output_path"

echo -e "\n *** Running update_trackers.py... \n" #>> "$output_path"
python $src/asdf/src/tasks/update_trackers.py "$branch" #2>&1 | tee 1>> "$output_path"

echo -e "\n *** Running update_extract_queue.py... \n"  #>> "$output_path"
python $src/asdf/src/tasks/update_extract_queue.py "$branch" #2>&1 | tee 1>> "$output_path"

echo -e "\n *** Running update_msr_queue.py... \n" #>> "$output_path"
python $src/asdf/src/tasks/update_msr_queue.py "$branch" #2>&1 | tee 1>> "$output_path"

echo -e "\n" #>> "$output_path"
echo $(date) #>> "$output_path"
echo -e "\nDone \n" #>> "$output_path"

# cat "$output_path" >> "$src"/log/db_updates/"$timestamp".db_updates.log

# rm "$output_path"


JOBID=$(echo $PBS_JOBID | sed 's/[.].*$//')

printf "%0.s-" {1..40}
echo -e "\n"
cat ${HOME}/ax-dbu-$branch.o$JOBID >> $src/log/db_updates/$timestamp.db_updates.log
rm ${HOME}/ax-dbu-$branch.o$JOBID
