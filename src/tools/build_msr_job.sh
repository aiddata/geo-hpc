#!/bin/bash


branch=$1

timestamp=$2

jobtime=$(date +%H%M%S)


# check if job needs to be run
qstat=$(/usr/local/torque-2.3.7/bin/qstat -nu $USER)

if echo "$qstat" | grep -q 'ax-msr-'"$branch"; then

    printf "%0.s-" {1..40}
    echo -e "\n"

    echo [$(date) \("$timestamp"."$jobtime"\)] Existing job found
    echo "$qstat"
    echo -e "\n"

else

    src="${HOME}"/active/"$branch"

    job_dir="$src"/log/msr/jobs
    mkdir -p $job_dir

    updated=0
    shopt -s nullglob
    for i in "$job_dir"/*.job; do
        updated=1
        cat "$i"
        rm "$i"
    done

    if [ "$updated" == 1 ]; then
        printf "%0.s-" {1..80}
        echo -e "\n"
    fi

    echo [$(date) \("$timestamp"."$jobtime"\)] No existing job found.

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

    job_path=$(mktemp)

    nodes=7
    ppn=16
    total=$(($nodes * $ppn))


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use cat <<- EOF to strip leading tabs )

cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N ax-msr-$branch
#PBS -l nodes=$nodes:c18c:ppn=$ppn
#PBS -l walltime=24:00:00
#PBS -q alpha
#PBS -j oe
#PBS -o $src/log/msr/jobs/$timestamp.$jobtime.msr.job
#PBS -V

echo -e "\n *** Running mean-surface-rasters autoscript.py... \n"
mpirun -np $total python -m mpi4py $src/mean-surface-rasters/src/autoscript.py $branch $timestamp

EOF

    # cd "$src"/log/msr/jobs
    /usr/local/torque-2.3.7/bin/qsub "$job_path"

    echo "Running job..."
    echo -e "\n"

    rm "$job_path"

fi
