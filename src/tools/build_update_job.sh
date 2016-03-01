#!/bin/bash


branch=$1

timestamp=$2

echo '=================================================='
echo Building db update job for branch: "$branch"
echo Timestamp: $(date) '('"$timestamp"')'
echo -e "\n"

src="${HOME}"/active/"$branch"

job_path=$(mktemp)

cat <<EOF >> "$job_path"

#!/bin/tcsh
#PBS -N asdf-update
#PBS -l nodes=1:c18c:ppn=1
#PBS -l walltime=180:00:00
#PBS -o $src/log/db_updates/$timestamp.db_updates.log
#PBS -j oe

echo 'Job id: '"$PBS_JOBID"

echo -e "\n *** Running update_trackers.py... \n"
python $src/asdf/src/tools/update_trackers.py $branch

echo -e "\n *** Running update_extract_list.py... \n"
python $src/asdf/src/tools/update_extract_list.py $branch

echo -e "\n *** Running update_msr_list.py... \n"
python $src/asdf/src/tools/update_msr_list.py $branch

EOF


qsub "$job_path"

rm "$job_path"

