#!/bin/bash


branch=$1

# timestamp=$2
timestamp=$(date +%Y%m%d.%s)


echo '=================================================='
echo Running mean-surface-rasters job builder for branch: "$branch"
echo Timestamp: $(date) #'('"$timestamp"')'
echo -e "\n"


# check if job needs to be run 
echo 'Checking for existing msr job (asdf-msr-'"$branch"')...'
qstat -nu $USER

if qstat -nu $USER | grep -q 'asdf-msr-'"$branch"; then

    echo "Existing job found"
    echo -e "\n"

else

    echo "No existing job found."
    echo "Building job..."

    src="${HOME}"/active/"$branch"

    mkdir -p "$src"/log/msr

    job_path=$(mktemp)


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use cat <<- EOF to strip leading tabs )

cat <<EOF >> "$job_path"

#!/bin/tcsh
#PBS -N asdf-msr-$branch
#PBS -l nodes=2:c18c:ppn=16
#PBS -l walltime=180:00:00
#PBS -o $src/log/msr/$timestamp.msr.log
#PBS -j oe

echo 'Job id: '"$PBS_JOBID"

echo -e "\n *** Running mean-surface-rasters autoscript.py... \n"
mpirun --mca mpi_warn_on_fork 0 -np 32 python-mpi $src/asdf/src/tools/update_trackers.py $branch

EOF


    qsub "$job_path"

    rm "$job_path"

fi
