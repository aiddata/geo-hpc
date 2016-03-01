#!/bin/bash


branch=$1

timestamp=$2


echo '=================================================='
echo Running db_updates job builder for branch: "$branch"
echo Timestamp: $(date) '('"$timestamp"')'
echo -e "\n"


# check if job needs to be run 
echo 'Checking for existing db_updates job (asdf-update-'"$branch"')...'
qstat -nu $USER

if qstat -nu $USER | grep -q 'asdf-update-'"$branch"; then

    echo "Existing job found"
    echo -e "\n"

else

    echo "No existing job found."
    echo "Building job..."

    src="${HOME}"/active/"$branch"

    mkdir -p "$src"/log/db_updates/jobs

    job_path=$(mktemp)

    output_path=$(mktemp -p "$src"/log/db_updates/jobs)


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use cat <<- EOF to strip leading tabs )

cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N asdf-update-$branch
#PBS -l nodes=1:c18c:ppn=1
#PBS -l walltime=180:00:00
#PBS -o $(mktemp)
#PBS -j oe

echo 'Timestamp: '$timestamp >> '$output_path
echo 'Job id: $PBS_JOBID" >> '$output_path

echo -e "\n *** Running update_trackers.py... \n" >> '$output_path
python $src/asdf/src/tools/update_trackers.py $branch 2>&1 | tee 1>>$output_path

echo -e "\n *** Running update_extract_list.py... \n"  >> '$output_path
python $src/asdf/src/tools/update_extract_list.py $branch 2>&1 | tee 1>>$output_path

echo -e "\n *** Running update_msr_list.py... \n" >> '$output_path
python $src/asdf/src/tools/update_msr_list.py $branch 2>&1 | tee 1>>$output_path

cat $output_path >> $src/log/db_updates/$timestamp.db_updates.log

EOF


    qsub "$job_path"

    rm "$job_path"

fi
