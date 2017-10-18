#!/bin/bash

# automated script called by cron_wrapper.sh
#
# required input:
#   branch
#   timestamp
#
# builds job that runs extract tasks



branch=$1

timestamp=$2

jobtime=$(date +%H%M%S)


branch_dir="/sciclone/aiddata10/geo/${branch}"
src="${branch_dir}/source"

torque_path=/usr/local/torque-6.1.1.1/bin

# check if job needs to be run
qstat=$($torque_path/qstat -nu $USER)


# -----------------------------------------------------------------------------


build_job() {

    echo Preparing $jobname job...

    active_jobs=$(echo "$qstat" | grep 'geo-'"$jobname"'-'"$branch" | wc -l)

    if [[ $active_jobs -ge $max_jobs ]]; then

        printf "%0.s-" {1..40}
        echo -e "\n"

        echo [$(date) \("$timestamp"."$jobtime"\)] Max number of jobs running
        echo "$qstat"
        echo -e "\n"

    else

        job_dir="$branch_dir"/log/extract/jobs
        mkdir -p $job_dir

        shopt -s nullglob
        for i in "$job_dir"/*.job; do
            cat "$i"
            rm "$i"

            printf "%0.s-" {1..80}
            echo -e "\n"
        done


        echo [$(date) \("$timestamp"."$jobtime"\)] Job opening found

        echo "Checking for items in queue..."
        queue_status=$(python "$src"/geo-hpc/tools/check_extract_queue.py "$branch")


        if [ "$queue_status" = "ready" ]; then
            job_type=default
            if [ $jobname = "ex1" ]; then
                echo '... no priority tasks found'
                exit 0
            fi

        elif [ "$queue_status" = "det" ]; then
            job_type=det

        elif [ "$queue_status" = "empty" ]; then
            echo '... queue empty'
            exit 0

        elif [ "$queue_status" = "error" ]; then
            echo '... error connecting to queue'
            exit 1

        else
            echo '... unknown error checking queue'
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

echo -e "\n *** Running $jobname job... \n"
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


# -----------------------------------------------------------------------------

# load job settings from config json and
# build jobs using those settings

config_path=$src/geo-hpc/config.json
job_class=extracts

get_val() {
    jobnumber=$1
    field=$2
    val=$(python -c "import json; print json.load(open('$config_path', 'r'))['$branch']['jobs']['$job_class'][$jobnumber]['$field']")
    echo $val
}

x=$(python -c "import json; print len(json.load(open('$config_path', 'r'))['$branch']['jobs']['$job_class'])")

for ((i=0;i<$x;i+=1)); do
    jobname=$(get_val $i jobname)
    nodespec=$(get_val $i nodespec)
    max_jobs=$(get_val $i max_jobs)
    nodes=$(get_val $i nodes)
    ppn=$(get_val $i ppn)
    walltime=$(get_val $i walltime)

    build_job
done

