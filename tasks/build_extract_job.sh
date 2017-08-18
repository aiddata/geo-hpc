#!/bin/bash

# automated script called by cron_wrapper.sh
#
# required input:
#   branch
#   timestamp
#
# builds job that runs tasks for extract-scripts module
# job runs python-mpi script autoscript.py from extract-scripts repo
#
# only allows 1 job of this type to run at a time
# utilizes qstat grep to search for standardized job name to
# determine if job is already running
#
# job name format: ax-ex-<branch>


branch=$1

timestamp=$2

jobtime=$(date +%H%M%S)


branch_dir="/sciclone/aiddata10/geo/${branch}"
src="${branch_dir}/source"


# check if job needs to be run
qstat=$(/usr/local/torque-6.0.2/bin/qstat -nu $USER)
job_count=$(echo "$qstat" | grep 'ax-ex-'"$branch" | wc -l)

# if echo "$qstat" | grep -q 'ax-ex-'"$branch"; then

# change this # to be 1 less than desired number of jobs
if [[ $job_count -gt 3 ]]; then

    printf "%0.s-" {1..40}
    echo -e "\n"

    echo [$(date) \("$timestamp"."$jobtime"\)] Max number of jobs running

    echo "$qstat"
    echo -e "\n"

else

    job_type=default
    # if [[ $job_count -eq 2 ]]; then
    #     job_type=det
    # fi


    job_dir="$branch_dir"/log/extract/jobs
    mkdir -p $job_dir

    shopt -s nullglob
    for i in "$job_dir"/*.job; do
        cat "$i"
        rm "$i"

        printf "%0.s-" {1..80}
        echo -e "\n"
    done


    echo [$(date) \("$timestamp"."$jobtime"\)] Job opening found.

    echo "Checking for items in extract queue..."
    queue_status=$(python "$src"/geo-hpc/tools/check_extract_queue.py "$branch")


    if [ "$queue_status" != "ready" ]; then

        if [ "$queue_status" = "error" ]; then
            echo '... error connecting to extract queue'
            exit 1
        fi

        if [ "$queue_status" = "empty" ]; then
            echo '... extract queue empty'
            exit 0
        fi

        echo '... unknown error checking extract queue'
        exit 2

    fi

    echo '... items found in queue'
    echo -e "\n"

    echo "Building job #"$job_count" ("$job_type" job)..."

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
#PBS -N ax-ex-$branch
#PBS -l nodes=$nodes:c18c:ppn=$ppn
#PBS -l walltime=48:00:00
#PBS -j oe
#PBS -o $branch_dir/log/extract/jobs/$timestamp.$jobtime.extract.job
#PBS -V

echo -e "\n *** Running extract-scripts autoscript.py... \n"
mpirun --mca mpi_warn_on_fork 0 --map-by node -np $total python-mpi $src/geo-hpc/tasks/extract_runscript.py $branch $job_type

EOF

    # cd "$branch_dir"/log/extract/jobs
    /usr/local/torque-6.0.2/bin/qsub "$job_path"

    echo "Running job..."
    echo -e "\n"

    rm "$job_path"

fi


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------


job_count=$(echo "$qstat" | grep 'ax-ex2-'"$branch" | wc -l)

# if echo "$qstat" | grep -q 'ax-ex-'"$branch"; then

# change this # to be 1 less than desired number of jobs
if [[ $job_count -gt 11 ]]; then

    printf "%0.s-" {1..40}
    echo -e "\n"

    echo [$(date) \("$timestamp"."$jobtime"\)] Max number of jobs running

    echo "$qstat"
    echo -e "\n"

else

    job_type=default
    # if [[ $job_count -eq 2 ]]; then
    #     job_type=det
    # fi


    job_dir="$branch_dir"/log/extract/jobs
    mkdir -p $job_dir

    shopt -s nullglob
    for i in "$job_dir"/*.job; do
        cat "$i"
        rm "$i"

        printf "%0.s-" {1..80}
        echo -e "\n"
    done


    echo [$(date) \("$timestamp"."$jobtime"\)] Job opening found.

    echo "Checking for items in extract queue..."
    queue_status=$(python "$src"/geo-hpc/tools/check_extract_queue.py "$branch")


    if [ "$queue_status" != "ready" ]; then

        if [ "$queue_status" = "error" ]; then
            echo '... error connecting to extract queue'
            exit 1
        fi

        if [ "$queue_status" = "empty" ]; then
            echo '... extract queue empty'
            exit 0
        fi

        echo '... unknown error checking extract queue'
        exit 2

    fi

    echo '... items found in queue'
    echo -e "\n"

    echo "Building job #"$job_count" ("$job_type" job)..."

    job_path=$(mktemp)

    nodes=1
    ppn=12
    total=$(($nodes * $ppn))


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use `cat <<- EOF` to strip leading tabs )

cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N ax-ex2-$branch
#PBS -l nodes=$nodes:c18a:ppn=$ppn
#PBS -l walltime=48:00:00
#PBS -j oe
#PBS -o $branch_dir/log/extract/jobs/$timestamp.$jobtime.extract.job
#PBS -V

echo -e "\n *** Running extract job... \n"
mpirun --mca mpi_warn_on_fork 0 --map-by node -np $total python-mpi $src/geo-hpc/tasks/extract_runscript.py $branch $job_type

EOF

    # cd "$branch_dir"/log/extract/jobs
    /usr/local/torque-6.0.2/bin/qsub "$job_path"

    echo "Running job..."
    echo -e "\n"

    rm "$job_path"

fi

