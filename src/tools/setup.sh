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


# setup load_repos.sh cronjob and run load_repos.sh for first time
rm -rf ~/active/tmp
mkdir -p ~/active/tmp
cd ~/active/tmp

if [[ $server == "hpc" ]]; then
    git clone -b "$branch" https://github.com/itpir/asdf
    # git pull https://github.com/itpir/asdf develop
    # git pull git@github.com:itpir/asdf.git develop
else
    git clone -b "$branch" http://github.com/itpir/asdf
    # git pull https://github.com/itpir/asdf master
    # git pull git@github.com:itpir/asdf.git master
fi


cp  ~/active/tmp/asdf/src/tools/load_repos.sh ~/active/load_repos.sh

rm -rf ~/active/tmp


mkdir -p ~/crontab.backup
crontab -l > ~/crontab.backup/$(date +%Y%m%d).crontab

load_repos_base='0 1 * * * ~/active/load_repos.sh'
load_repos_cron="$load_repos_base"' '"$server"' '"$branch"

crontab -l | grep -v 'load_repos.sh' | { cat; echo "$load_repos_cron"; } | crontab -


cd ~/active
bash load_repos.sh "$server" "$branch"



# setup other cronjobs
# 


