#!/bin/bash


branch=$1

timestamp=$2
# timestamp=$(date +%Y%m%d.%s)

echo '=================================================='
echo Running mean-surface-rasters job builder for branch: "$branch"
echo Timestamp: $(date) #'('"$timestamp"')'
echo -e "\n"


# check if job needs to be run
echo 'Checking for existing msr job (ax-msr-'"$branch"')...'
/usr/local/torque-2.3.7/bin/qstat -nu $USER

if /usr/local/torque-2.3.7/bin/qstat -nu $USER | grep -q 'ax-msr-'"$branch"; then

    echo "Existing job found"
    echo -e "\n"

else

    src="${HOME}"/active/"$branch"

    echo "No existing job found."

    echo "Checking for items in msr queue..."
    queue_status=$(python "$src"/asdf/src/tools/check_msr_queue.py "$branch")


    if [ "$queue_status" != "ready" ]; then

        if [ "$queue_status" = "error" ]; then
            echo '... error connecting to msr queue'
            exit 1
        fi

        if [ "$queue_status" = "empty" ]; then
            echo '... msr queue empty'
            exit 0
        fi

        echo '... unknown error checking msr queue'
        exit 2

    fi

    echo '... items found in queue'
    echo -e "\n"

    echo "Building job..."


    mkdir -p "$src"/log/msr/jobs

    job_path=$(mktemp)


    nodes=7
    ppn=16
    total=$(($nodes * $ppn))


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use cat <<- EOF to strip leading tabs )


#PBS -o $src/log/msr/$timestamp.msr.log

cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N ax-msr-$branch
#PBS -l nodes=$nodes:c18c:ppn=$ppn
#PBS -l walltime=180:00:00
#PBS -q alpha
#PBS -j oe

echo -e "\nJob id: $PBS_JOBID"

echo -e "\n *** Running mean-surface-rasters autoscript.py... \n"
mpirun --mca mpi_warn_on_fork 0 -np $total python-mpi $src/mean-surface-rasters/src/autoscript.py $branch $timestamp

EOF

    cd "$src"/log/msr/jobs
    /usr/local/torque-2.3.7/bin/qsub "$job_path"

    echo "Running job..."
    echo -e "\n"

    rm "$job_path"

fi
