#!/bin/bash

# used to initialize portions of asdf

# manages setup of both production and development branch files
# NOTE: this script must be manually replaced/updated if changes to setup.sh are made


# get server/branch inputs from user

echo -e "\n"
while true; do
    echo "Input server [hpc / web]:"
    read REPLY
    case $REPLY in
        "hpc")  server="$REPLY"; break ;;
        "web")  server="$REPLY"; break ;;
        *)      echo "Invalid input."; continue ;;
    esac 
done  


echo -e "\n"
while true; do
    echo "Input development branch [master / develop]:"
    read REPLY
    case $REPLY in
        "master")   branch="$REPLY"; break ;;
        "develop")  branch="$REPLY"; break ;;
        *)          echo "Invalid input."; continue ;;
    esac 
done  

echo -e "\n"


# setup branch directory

src="${HOME}"/active/"$branch"

rm -rf "$src"

mkdir -p "$src"/{latest,'jobs',tmp,tasks,log/{db_updates,load_repos}}


# setup load_repos.sh cronjob and run load_repos.sh for first time

cd "$src"/tmp

if [[ $server == "hpc" ]]; then
    git clone -b "$branch" https://github.com/itpir/asdf
else
    git clone -b "$branch" http://github.com/itpir/asdf
fi


cp  "$src"/tmp/asdf/src/tools/load_repos.sh "$src"/tasks/load_repos.sh

rm -rf "$src"/tmp/asdf


mkdir -p "$src"/../crontab.backup
crontab -l > "$src"/../crontab.backup/$(date +%Y%m%d.%s)."$branch".crontab


# --------------------------------------------------
# replace with running manage_crons.sh script later
#

load_repos_base='0 4-23/6 * * * bash '"$src"'/tasks/load_repos.sh'
load_repos_cron="$load_repos_base"' '"$server"' '"$branch"' 2>$1 | tee 1>'"$src"/log/load_repos/$(date +%s).load_repos.log
crontab -l | grep -v 'load_repos.*'"$branch" | { cat; echo "$load_repos_cron"; } | crontab -

# --------------------------------------------------


bash "$src"/tasks/load_repos.sh "$server" "$branch" 2>$1 | tee "$src"/log/load_repos/$(date +%s).load_repos.log



# other setup
 
# create config file
# PLACEHOLDER:
#   could be used by jobs to grab server/other info depending on whether it is production/dev
# touch "$src"/../config.json

timestamp=$(date +%s)

cat <<EOF > "$src"/tasks/update_db_job

#!/bin/tcsh
#PBS -N asdf-update
#PBS -l nodes=1:vortex:ppn=1
#PBS -l walltime=180:00:00
#PBS -o "$timestamp".db_updates.log
#PBS -j oe

src="$src"/log/db_updates

mkdir -p "$src"
cd "$src"

python /sciclone/home00/sgoodman/active/develop/asdf/src/tools/update_trackers.py
python /sciclone/home00/sgoodman/active/develop/asdf/src/tools/update_extract_list.py
python /sciclone/home00/sgoodman/active/develop/asdf/src/tools/update_msr_list.py

EOF
