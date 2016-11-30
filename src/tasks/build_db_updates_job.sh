#!/bin/bash

# automated script called by cron_wrapper.sh
#
# required input:
#   branch
#   timestamp
#
# builds job that updates trackers
# job runs the bash script update_trackers_script.sh
#
# only allows 1 job of this type to run at a time
# utilizes qstat grep to search for standardized job name to
# determine if job is already running
#
# job name format: ax-upt-<branch>


branch=$1

timestamp=$2

task=$3

jobtime=$(date +%H%M%S)



case "$task" in

    update_trackers)
        short_name=upt ;;

    update_extract)
        short_name=upe ;;

    update_msr)
        short_name=upm ;;

    *)  echo "Invalid build_db_updates_job task.";
        exit 1 ;;
esac



# check if job needs to be run
qstat=$(/usr/local/torque-6.0.2/bin/qstat -nu $USER)

if echo "$qstat" | grep -q 'ax-'"$short_name"'-'"$branch"; then

    printf "%0.s-" {1..40}
    echo -e "\n"

    echo [$(date) \("$timestamp"."$jobtime"\)] Existing job found
    echo "$qstat"
    echo -e "\n"

else

    printf "%0.s-" {1..80}
    echo -e "\n"

    src="${HOME}"/active/"$branch"

    job_dir="$src"/log/"$task" #/jobs
    mkdir -p $job_dir


    echo [$(date) \("$timestamp"."$jobtime"\)] No existing job found.
    echo "Building job..."

    job_path=$(mktemp)


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use `cat <<- EOF` to strip leading tabs )


# this job uses `-k` instead of `-j` and `-o`
#   this will place job oe file in user home dir and
#   make it available on execution hosts
# `-V` will export env from qsub to batch env (e.g., $PBS_JOBID)
#
# these options allow us to keep the full job output and then
# concatenate with the main log file (PBS would overwrite existing
# main log if using `-o` to specify it via qsub options)

#PBS -o $src/log/$task/jobs/$timestamp.$jobtime.$task.job


cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N ax-$short_name-$branch
#PBS -l nodes=1:c18c:ppn=1
#PBS -l walltime=24:00:00
#PBS -j oe
#PBS -k oe
#PBS -V

bash $src/asdf/src/tasks/run_db_updates.sh $branch $timestamp $task

EOF

    # cd "$src"/log/"$task"/jobs
    /usr/local/torque-6.0.2/bin/qsub "$job_path"

    echo "Running job..."
    echo -e "\n"

    rm "$job_path"

fi
