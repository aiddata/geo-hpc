#!/bin/bash

# called by job created by build_det_job.sh
# running different queue processing script for det
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
# to this script in build_det_job.sh
#
# Cannot remember if there was some odd reason it needed to be passed
# instead of just built in this script


branch=$1
timestamp=$2
src=$3

src="${HOME}"/active/"$branch"


# output_path=$(mktemp -p "$src"/log/det/tmp)

echo 'Timestamp: '$timestamp #>> "$output_path"
echo 'Job: '"$PBS_JOBID" #>> "$output_path"

echo -e "\n *** Running det queue processing... \n" #>> "$output_path"
python $src/det-module/queue/processing.py "$branch" #2>&1 | tee 1>> "$output_path"

echo -e "\n" #>> "$output_path"
echo $(date) #>> "$output_path"
echo -e "\nDone \n" #>> "$output_path"

# cat "$output_path" >> "$src"/log/det/"$timestamp".det.log

# rm "$output_path"


JOBID=$(echo $PBS_JOBID | sed 's/[.].*$//')

printf "%0.s-" {1..40}
echo -e "\n"
cat ${HOME}/ax-det-$branch.o$JOBID >> $src/log/det/$timestamp.det.log
rm ${HOME}/ax-det-$branch.o$JOBID
