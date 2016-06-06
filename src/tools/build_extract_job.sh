#!/bin/bash


branch=$1

timestamp=$2

jobtime=$(date +%H%M%S)


# check if job needs to be run
qstat=$(/usr/local/torque-2.3.7/bin/qstat -nu $USER)

if echo "$qstat" | grep -q 'ax-ex-'"$branch"; then

    printf "%0.s-" {1..40}
    echo -e "\n"

    echo [$(date) \("$timestamp"."$jobtime"\)] Existing job found
    echo "$qstat"
    echo -e "\n"

else

    src="${HOME}"/active/"$branch"

    job_dir="$src"/log/extract/jobs
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

    echo "Checking for items in extract queue..."
    queue_status=$(python "$src"/asdf/src/tools/check_extract_queue.py "$branch")


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

    echo "Building job..."

    job_path=$(mktemp)

    nodes=2
    ppn=4
    total=$(($nodes * $ppn))


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use cat <<- EOF to strip leading tabs )

cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N ax-ex-$branch
#PBS -l nodes=$nodes:c18c:ppn=$ppn
#PBS -l walltime=24:00:00
#PBS -q alpha
#PBS -j oe
#PBS -o $src/log/extract/jobs/$timestamp.$jobtime.extract.job
#PBS -V

echo -e "\n *** Running extract-scripts autoscript.py... \n"
mpirun --mca mpi_warn_on_fork 0 --map-by node -np $total python-mpi $src/extract-scripts/src/autoscript.py $branch $timestamp

EOF

    # cd "$src"/log/extract/jobs
    /usr/local/torque-2.3.7/bin/qsub "$job_path"

    echo "Running job..."
    echo -e "\n"

    rm "$job_path"

fi
