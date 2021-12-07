#!/bin/bash

# automated script called by cron_wrapper.sh
#
# builds jobs for specified tasks
#
# required input:
#   branch
#   timestamp
#   job_class - which job to build
#
# job name format: geo-<jobname>-<branch>


branch=$1

timestamp=$2

job_class=$3

jobtime=$(date +%H%M%S)


branch_dir="/sciclone/aiddata10/geo/${branch}"
src="${branch_dir}/source"

torque_path=/usr/local/torque-6.1.1.1/bin

# check if job needs to be run
qstat=$($torque_path/qstat -nu $USER)


config_path=$src/geo-hpc/config.json

source $HOME/.bashrc

# -----------------------------------------------------------------------------


check_scheduler() {

    test1=$(${torque_path}/qmgr -c 'p s' | grep scheduling)

    fail1=False
    # if [[ $test1 == "set server scheduling = False" ]];then
    #     fail1=True
    # fi

    # NOTE: maui only runs on vortex
    # test2=$(ps -fu maui | wc -l)

    # fail2=False
    # if [[ $test2 == 1 ]];then
    #     fail2=True
    # fi

    test3a=$(showres 2> >(wc -l))
    test3b=$(showres 2> >(grep ERROR | wc -l))

    fail3=False
    if [[ $test3a == 2 && $test3b == 2 ]];then
        fail3=True
    fi

    fail=False
    # if [[ $fail1 == True || $fail2 == True || $fail3 == True ]]; then
    if [[ $fail1 == True || $fail3 == True ]]; then
        echo "Scheduler is offline."
        fail=True
        exit 1
    fi

}


clean_jobs() {

    job_dir="$branch_dir"/log/$job_class/jobs
    mkdir -p $job_dir

    shopt -s nullglob
    for i in "$job_dir"/*.job; do
        echo -e "\n"

        cat "$i"
        rm "$i"

        echo -e "\n"
        printf "%0.s-" {1..40}
    done

}


build_job() {

    echo "Preparing $jobname job..."

    # ----------------------------------------

    if [[ $job_class = "extracts" ]]; then
        echo "Checking for items in queue..."
        queue_status=$(python "$src"/geo-hpc/tools/check_extract_queue.py "$branch" $job_type)

    elif [[ $job_class = "msr" ]]; then
        echo "Checking for items in queue..."
        queue_status=$(python "$src"/geo-hpc/tools/check_msr_queue.py "$branch")

    else
        queue_status="none"

    fi


    if [[ $queue_status = "none" ]]; then
        :

    elif [[ $queue_status = "ready" ]]; then
        echo '... items found in queue'

    elif [[ $queue_status = "empty" ]]; then
        echo '... queue empty'
        return 0

    elif [[ $queue_status = "error" ]]; then
        echo '... error connecting to queue'
        return 1

    elif [[ $queue_status = "invalid" ]]; then
        echo '... error due to invalid input'
        return 2

    else
        echo '... unknown error'
        return 3

    fi

    # ----------------------------------------

    active_jobs=$(echo "$qstat" | grep 'geo-'"$jobname"'-'"$branch" | wc -l)

    if [[ $active_jobs -ge $max_jobs ]]; then

        echo "Max number of jobs running"
        echo "$qstat"

    else

        echo "Job opening found"
        echo "Building <"$jobname"> job #"$active_jobs" ("$job_type" job)..."

        job_path=$(mktemp)


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
#PBS -o $branch_dir/log/$job_class/jobs/$timestamp.$jobtime.$job_class.job
##PBS -V

date

echo "*** Running $jobname job..."
echo Timestamp: $timestamp
echo Job: "\$PBS_JOBID"

echo $cmd

$cmd

EOF

        out=$($torque_path/qsub "$job_path")

        echo "Running job..."
        echo "$out"

        rm "$job_path"

    fi

}


# -----------------------------------------------------------------------------


# always clean up old job outputs first
clean_jobs

# make sure scheduler is online before submitting any jobs
check_scheduler

# test mongo connection before building job
db_conn_status=$(python ${src}/geo-hpc/utils/config_utility.py $branch)
if [[ $db_conn_status != "success" ]]; then
    echo -e "\n"
    echo [$(date) \("$timestamp"."$jobtime"\)]
    echo -e "\n"

    echo "Error connecting to mongodb"
    exit 1
fi


# load job settings from config json and
# build jobs using those settings

get_val() {
    jobnumber=$1
    field=$2
    val=$(python -c "import json; print json.load(open('$config_path', 'r'))['$branch']['jobs']['$job_class']['tasks'][$jobnumber]['$field']")
    echo "$val"
}

x=$(python -c "import json; print len(json.load(open('$config_path', 'r'))['$branch']['jobs']['$job_class']['tasks'])")

for ((i=0;i<$x;i+=1)); do

    echo -e "\n"
    echo [$(date) \("$timestamp"."$jobtime"\)]
    echo -e "\n"

    # only modified for relevant jobs
    job_type=default

    jobname=$(get_val $i jobname)
    nodespec=$(get_val $i nodespec)
    max_jobs=$(get_val $i max_jobs)
    nodes=$(get_val $i nodes)
    ppn=$(get_val $i ppn)
    walltime=$(get_val $i walltime)
    cmd=$(get_val $i cmd)

    if [[ $job_class = "extracts" ]]; then
        job_type=$(get_val $i job_type)
        extract_limit=$(get_val $i extract_limit)
        pixel_limit=$(get_val $i pixel_limit)
    fi

    total=`expr $nodes \* $ppn`
    cmd=$(eval "echo $cmd")

    if [[ $nodespec = "local" ]]; then
        $cmd
    else
        build_job
    fi

    echo -e "\n"
    printf "%0.s-" {1..40}
    echo -e "\n"

done
