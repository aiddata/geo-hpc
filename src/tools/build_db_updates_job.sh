#!/bin/bash

# automated script called by cron_wrapper.sh
#
# required input:
#   branch
#   timestamp
#
# builds job that run maintenance tasks on database
# job runs the bash script db_updates_script.sh
#
# only allows 1 job of this type to run at a time
# utilizes qstat grep to search for standardized job name to
# determine if job is already running
#
# job name format: ax-dbu-<branch>


branch=$1

timestamp=$2

jobtime=$(date +%H%M%S)


# check if job needs to be run
qstat=$(/usr/local/torque-6.0.2/bin/qstat -nu $USER)

if echo "$qstat" | grep -q 'ax-dbu-'"$branch"; then

    printf "%0.s-" {1..40}
    echo -e "\n"

    echo [$(date) \("$timestamp"."$jobtime"\)] Existing job found
    echo "$qstat"
    echo -e "\n"

else

    printf "%0.s-" {1..80}
    echo -e "\n"

    src="${HOME}"/active/"$branch"

    job_dir="$src"/log/db_updates #/jobs
    mkdir -p $job_dir


    echo [$(date) \("$timestamp"."$jobtime"\)] No existing job found.
    echo "Building job..."

    job_path=$(mktemp)


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use `cat <<- EOF` to strip leading tabs )

cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N ax-dbu-$branch
#PBS -l nodes=1:c18c:ppn=1
#PBS -l walltime=24:00:00
#PBS -k oe
#PBS -j oe
#PBS -o $src/log/db_updates/jobs/$timestamp.$jobtime.db_updates.job
#PBS -V

bash $src/asdf/src/tools/db_updates_script.sh $branch $timestamp $src

EOF

    # cd "$src"/log/db_updates/jobs
    /usr/local/torque-6.0.2/bin/qsub "$job_path"

    echo "Running job..."
    echo -e "\n"

    rm "$job_path"

fi
