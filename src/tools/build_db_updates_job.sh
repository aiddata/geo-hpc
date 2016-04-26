#!/bin/bash


branch=$1

timestamp=$2

jobtime=$(date +%H%M%S)

printf "%0.s-" {1..80}


# check if job needs to be run
qstat=$(/usr/local/torque-2.3.7/bin/qstat -nu $USER)

if echo "$qstat" | grep -q 'ax-update-'"$branch"; then

    echo [$(date) \("$timestamp"."$jobtime"\)] Existing job found
    echo "$qstat"
    echo -e "\n"

else

    src="${HOME}"/active/"$branch"

    job_dir="$src"/log/db_updates/jobs
    mkdir -p $job_dir

    # updated=0
    # shopt -s nullglob
    # for i in "$job_dir"/*.job; do
    #     updated=1
    #     cat "$i"
    #     rm "$i"
    # done

    # if [ "$updated" == 1 ]; then
    #     printf "%0.s-" {1..80}
    #     printf "%0.s-" {1..80}
    # fi

    echo [$(date) \("$timestamp"."$jobtime"\)] No existing job found.
    echo "Building job..."

    job_path=$(mktemp)


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use cat <<- EOF to strip leading tabs )

cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N ax-update-$branch
#PBS -l nodes=1:c18c:ppn=1
#PBS -l walltime=180:00:00
#PBS -q alpha
#PBS -j oe
#PBS -o $src/log/db_updates/jobs/$timestamp.$jobtime.db_updates.job

bash $src/asdf/src/tools/db_updates_script.sh $branch $timestamp $src

cat $src/log/db_updates/jobs/$timestamp.$jobtime.db_updates.job >> $src/log/db_updates/$timestamp.db_updates.log

EOF

    # cd "$src"/log/db_updates/jobs
    /usr/local/torque-2.3.7/bin/qsub "$job_path"

    echo "Running job..."
    echo -e "\n"

    rm "$job_path"

fi
