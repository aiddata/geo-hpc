#!/bin/bash


branch=$1

timestamp=$(date +%s)


src="${HOME}"/active/"$branch"

job_path="$src"/tasks/tmp_update_db_job

cat <<EOF > "$job_path"

#!/bin/tcsh
#PBS -N asdf-update
#PBS -l nodes=1:xeon:compute:ppn=1
#PBS -l walltime=1:00:00
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

