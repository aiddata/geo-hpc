#!/bin/bash

# used to initialize portions of asdf
# use bash setup.sh --dev for dev environment (no options needed for production enviornment)


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

src="${HOME}"/active/"$branch"


# setup load_repos.sh cronjob and run load_repos.sh for first time
rm -rf "$src"/tmp
mkdir -p "$src"/tmp
cd "$src"/tmp

if [[ $server == "hpc" ]]; then
    git clone -b "$branch" https://github.com/itpir/asdf
    # git pull https://github.com/itpir/asdf develop
    # git pull git@github.com:itpir/asdf.git develop
else
    git clone -b "$branch" http://github.com/itpir/asdf
    # git pull https://github.com/itpir/asdf master
    # git pull git@github.com:itpir/asdf.git master
fi


cp  "$src"/tmp/asdf/src/tools/load_repos.sh "$src"/load_repos.sh

rm -rf "$src"/tmp


mkdir -p "$src"/../crontab.backup
crontab -l > "$src"/../crontab.backup/$(date +%Y%m%d.%s)."$branch".crontab


# replace with running edit_crons.sh script later
load_repos_base='0 1 * * * '"$src"'/load_repos.sh'
load_repos_cron="$load_repos_base"' '"$server"' '"$branch"
crontab -l | grep -v 'load_repos.*'"$branch" | { cat; echo "$load_repos_cron"; } | crontab -


cd "$src"
bash load_repos.sh "$server" "$branch"



# other setup
 
# create config file
# PLACEHOLDER:
#   could be used by jobs to grab server/other info depending on whether it is production/dev
# touch "$src"/../config.json
