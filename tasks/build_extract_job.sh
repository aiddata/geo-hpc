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
# job name format: geo-ex-<branch>


branch=$1

timestamp=$2

jobtime=$(date +%H%M%S)


branch_dir="/sciclone/aiddata10/geo/${branch}"
src="${branch_dir}/source"


walltime="180:00:00"


torque_path=/usr/local/torque-6.1.1.1/bin


# check if job needs to be run
qstat=$($torque_path/qstat -nu $USER)


# -----------------------------------------------------------------------------

# generic vortex-alpha job
jobname=ex
nodespec=c18c
max_jobs=6
nodes=1
ppn=16

build_job

# ----------------------------------------

# priority only vortex-alpha job
jobname=ex1
nodespec=c18c
max_jobs=1
nodes=1
ppn=16

build_job

# ----------------------------------------

# generic vortex (c18a) job
jobname=ex2
nodespec=c18a
max_jobs=0
nodes=1
ppn=12

build_job


# -----------------------------------------------------------------------------


build_job() {

    active_jobs=$(echo "$qstat" | grep 'geo-$jobname-'"$branch" | wc -l)

    # if echo "$qstat" | grep -q 'geo-$jobname-'"$branch"; then

    if [[ $active_jobs -ge $max_jobs ]]; then

        printf "%0.s-" {1..40}
        echo -e "\n"

        echo [$(date) \("$timestamp"."$jobtime"\)] Max number of jobs running

        echo "$qstat"
        echo -e "\n"

    else

        # job_type=default
        # if [[ $active_jobs -eq 2 ]]; then
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
            job_type=default
            if [ $jobname == "ex1"]; then
                echo '... no priority tasks found'
                exit 0
            fi

        elif [ "$queue_status" = "det" ]; then
            job_type=det

        elif [ "$queue_status" = "empty" ]; then
            echo '... extract queue empty'
            exit 0

        elif [ "$queue_status" = "error" ]; then
            echo '... error connecting to extract queue'
            exit 1

        else
            echo '... unknown error checking extract queue'
            exit 2

        fi

        echo '... items found in queue'
        echo -e "\n"

        echo "Building <"$jobname"> job #"$active_jobs" ("$job_type" job)..."

        job_path=$(mktemp)

        total=$(($nodes * $ppn))


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use `cat <<- EOF` to strip leading tabs )

cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N geo-$jobname-$branch
#PBS -l nodes=$nodes:$nodespec:ppn=$ppn
#PBS -l walltime=$walltime
#PBS -j oe
#PBS -o $branch_dir/log/extract/jobs/$timestamp.$jobtime.extract.job
#PBS -V

echo -e "\n *** Running extract-scripts autoscript.py... \n"
mpirun -mca orte_base_help_aggregate 0 --mca mpi_warn_on_fork 0 --map-by node -np $total python-mpi $src/geo-hpc/tasks/extract_runscript.py $branch $job_type $nodespec
# mpirun --mca mpi_warn_on_fork 0 --map-by node -np $total python-mpi $src/geo-hpc/tasks/extract_runscript.py $branch $job_type $nodespec

EOF

        # cd "$branch_dir"/log/extract/jobs
        $torque_path/qsub "$job_path"

        echo "Running job..."
        echo -e "\n"

        rm "$job_path"

    fi


}


