#!/bin/bash


branch=$1

# timestamp=$2
timestamp=$(date +%Y%m%d.%s)


echo '=================================================='
echo Running extract-scripts job builder for branch: "$branch"
echo Timestamp: $(date) #'('"$timestamp"')'
echo -e "\n"


# check if job needs to be run
echo 'Checking for existing extract job (asdf-extract-'"$branch"')...'
/usr/local/torque-2.3.7/bin/qstat -nu $USER

if /usr/local/torque-2.3.7/bin/qstat -nu $USER | grep -q 'asdf-extract-'"$branch"; then

    echo "Existing job found"
    echo -e "\n"

else

    src="${HOME}"/active/"$branch"

    echo "No existing job found."

    echo "Checking for items in extract queue..."
    queue_status=$(python "$src"/asdf/src/tools/check_extract_queue.py "$branch")

    if [ "$queue_status" = "error" ]; then
        echo '... error connecting to extract queue'
        exit 1
    fi

    if [ "$queue_status" = "empty" ]; then
        echo '... extract queue empty'
        exit 0
    fi

    if [ "$queue_status" = "ready" ]; then
        echo '... items found in queue'
        echo -e "\n"
    fi

    echo "Building job..."


    mkdir -p "$src"/log/extract
    #/jobs

    job_path=$(mktemp)


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use cat <<- EOF to strip leading tabs )

cat <<EOF >> "$job_path"

#!/bin/tcsh
#PBS -N asdf-extract-$branch
#PBS -l nodes=4:c18c:ppn=16
#PBS -l walltime=180:00:00
#PBS -q alpha
#PBS -j oe
#PBS -o $src/log/extract/$timestamp.extract.log

echo -e "\nJob id: $PBS_JOBID"

echo -e "\n *** Running extract-scripts autoscript.py... \n"
mpirun --mca mpi_warn_on_fork 0 -np 32 python-mpi $src/extract-scripts/src/autoscript.py $branch $timestamp

EOF


    # cd "$src"/log/extract/jobs
    /usr/local/torque-2.3.7/bin/qsub "$job_path"

    echo "Running job..."
    echo -e "\n"

    rm "$job_path"

fi
