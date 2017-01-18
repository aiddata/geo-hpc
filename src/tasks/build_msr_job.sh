#!/bin/bash

# automated script called by cron_wrapper.sh
#
# required input:
#   branch
#   timestamp
#
# builds job that runs tasks for mean-surface-raster module
# job runs python-mpi script autoscript.py from mean-surface-rasters repo
#
# only allows 1 job of this type to run at a time
# utilizes qstat grep to search for standardized job name to
# determine if job is already running
#
# job name format: ax-msr-<branch>


branch=$1

timestamp=$2

jobtime=$(date +%H%M%S)


# check if job needs to be run
qstat=$(/usr/local/torque-6.0.2/bin/qstat -nu $USER)
job_count=$(echo "$qstat" | grep 'ax-msr-'"$branch" | wc -l)

# if echo "$qstat" | grep -q 'ax-msr-'"$branch"; then

# change this # to be 1 less than desired number of jobs
if [[ $job_count -gt 0 ]]; then

    printf "%0.s-" {1..40}
    echo -e "\n"

    echo [$(date) \("$timestamp"."$jobtime"\)] Max number of jobs running
    echo "$qstat"
    echo -e "\n"

else

    src="${HOME}"/active/"$branch"

    job_dir="$src"/log/msr/jobs
    mkdir -p $job_dir

    shopt -s nullglob
    for i in "$job_dir"/*.job; do
        cat "$i"
        rm "$i"

        printf "%0.s-" {1..80}
        echo -e "\n"
    done

    echo [$(date) \("$timestamp"."$jobtime"\)] Job opening found.

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

    echo "Building job #"$job_count"..."

    job_path=$(mktemp)

    nodes=1
    ppn=16
    total=$(($nodes * $ppn))


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use `cat <<- EOF` to strip leading tabs )

cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N ax-msr-$branch
#PBS -l nodes=$nodes:c18c:ppn=$ppn
#PBS -l walltime=24:00:00
#PBS -j oe
#PBS -o $src/log/msr/jobs/$timestamp.$jobtime.msr.job
#PBS -V

echo -e "\n *** Running mean-surface-rasters autoscript.py... \n"
mpirun --mca mpi_warn_on_fork 0 --map-by node -np $total python-mpi $src/mean-surface-rasters/src/autoscript.py $branch $timestamp

EOF

    # cd "$src"/log/msr/jobs
    /usr/local/torque-6.0.2/bin/qsub "$job_path"

    echo "Running job..."
    echo -e "\n"

    rm "$job_path"

fi
