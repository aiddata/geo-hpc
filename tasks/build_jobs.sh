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
# only allows 1 job of each job_class type to run at a time
# utilizes qstat grep to search for standardized job name to
# determine if job is already running
#
# job name format: geo-<job_name>-<branch>


branch=$1

timestamp=$2

job_class=$3

jobtime=$(date +%H%M%S)


branch_dir="/sciclone/aiddata10/geo/${branch}"
src="${branch_dir}/source"

torque_path=/usr/local/torque-6.1.1.1/bin

# check if job needs to be run
qstat=$($torque_path/qstat -nu $USER)


# -----------------------------------------------------------------------------


case "$job_class" in

    error_check)
        job_name=ec
        nodespec=c18x
        max_jobs=1
        nodes=1
        ppn=1
        walltime=48:00:00
        cmd="python ${src}/geo-hpc/tasks/check_errors.py ${branch}"
        ;;

    update_trackers)
        job_name=upt
        nodespec=c18c
        max_jobs=1
        nodes=1
        ppn=16
        walltime=48:00:00
        cmd="mpirun --mca mpi_warn_on_fork 0 --map-by node -np 16 python-mpi ${src}/geo-hpc/tasks/update_trackers.py ${branch}"
        ;;

    update_extracts)
        job_name=upe
        nodespec=c18c
        max_jobs=1
        nodes=1
        ppn=16
        walltime=48:00:00
        cmd="mpirun --mca mpi_warn_on_fork 0 --map-by node -np 16 python-mpi ${src}/geo-hpc/tasks/update_extract_queue.py ${branch}"
        ;;

    update_msr)
        job_name=upm
        nodespec=c18x
        max_jobs=1
        nodes=1
        ppn=1
        walltime=48:00:00
        cmd="mpirun --mca mpi_warn_on_fork 0 -np 1 python-mpi ${src}/geo-hpc/tasks/update_msr_queue.py ${branch}"
        ;;

    det)
        job_name=det
        nodespec=c18x
        max_jobs=1
        nodes=1
        ppn=1
        walltime=48:00:00
        cmd="mpirun --mca mpi_warn_on_fork 0 -np 1 python-mpi ${src}/geo-hpc/tasks/geoquery_request_processing.py ${branch}"
        ;;

    *)  echo "Invalid build_db_updates_job job_class.";
        exit 1 ;;
esac


# -----------------------------------------------------------------------------


clean_jobs() {

    job_dir="$branch_dir"/log/"$job_class"/jobs
    mkdir -p $job_dir

    shopt -s nullglob
    for i in "$job_dir"/*.job; do
        echo -e "\n"

        cat "$i"
        rm "$i"

        echo -e "\n"
        printf "%0.s-" {1..80}
    done

}


build_job() {

    echo "Preparing $job_class job..."

    active_jobs=$(echo "$qstat" | grep 'geo-'"$job_name"'-'"$branch" | wc -l)

    if [[ $active_jobs -ge $max_jobs ]]; then

        echo "Max number of jobs running"
        echo "$qstat"

    else

        echo "Job opening found"

        echo "Building job..."

        job_path=$(mktemp)

        total=$(($nodes * $ppn))


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use `cat <<- EOF` to strip leading tabs )


cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N geo-$job_name-$branch
#PBS -l nodes=$nodes:$nodespec:ppn=$ppn
#PBS -l walltime=$walltime
#PBS -j oe
#PBS -o $branch_dir/log/$job_class/jobs/$timestamp.$jobtime.$job_class.job
#PBS -V

echo "\n"
date
echo "\n"

echo -e "\n *** Running $job_name job... \n"
echo Timestamp: $timestamp
echo Job: "\$PBS_JOBID"
echo "\n"

$cmd

echo "\n"
date
echo "\nDone \n"

EOF

        $torque_path/qsub "$job_path"

        echo "Running job..."

        rm "$job_path"

    fi

}

# -----------------------------------------------------------------------------

# always clean up old job outputs first
clean_jobs


echo -e "\n"
echo [$(date) \("$timestamp"."$jobtime"\)]
echo -e "\n"

build_job

echo -e "\n"
printf "%0.s-" {1..40}
