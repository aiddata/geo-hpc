#!/bin/bash


branch=$1

timestamp=$2


echo '=================================================='
echo Running db_updates job builder for branch: "$branch"
echo Timestamp: $(date) '('"$timestamp"')'
echo -e "\n"


# check if job needs to be run 
echo 'Checking for existing db_updates job (asdf-update-'"$branch"')...'
qstat -nu $USER

if qstat -nu $USER | grep -q 'asdf-update-'"$branch"; then

    echo "Existing job found"
    echo -e "\n"

else

    echo "No existing job found."
    echo "Building job..."

    src="${HOME}"/active/"$branch"

    mkdir -p "$src"/log/db_updates/{tmp,'jobs'}

    job_path=$(mktemp)

    output_path=$(mktemp -p "$src"/log/db_updates/tmp)


# NOTE: just leave this heredoc unindented
#   sublime text is set to indent with spaces
#   heredocs can only be indented with true tabs
#   (can use cat <<- EOF to strip leading tabs )

cat <<EOF >> "$job_path"
#!/bin/tcsh
#PBS -N asdf-update-$branch
#PBS -l nodes=1:c18c:ppn=1
#PBS -l walltime=180:00:00
#PBS -j oe

bash $src/asdf/src/tools/db_updates_script.sh $branch $timestamp $output_path $src 

EOF

    cd "$src"/log/db_updates/jobs
    qsub "$job_path"

    echo "Running job..."
    echo -e "\n" 
    
    rm "$job_path"

fi
