#!/bin/bash

# used to initialize portions of asdf


# setup load_repos.sh cronjob and run load_repos.sh for first time
rm -rf ~/active/tmp
mkdir -p ~/active/tmp
cd ~/active/tmp

git clone https://github.com/itpir/asdf

cp  ~/active/tmp/asdf/src/tools/load_repos.sh ~/active/load_repos.sh

rm -rf ~/active/tmp

mkdir -p ~/crontab.backup
crontab -l > ~/crontab.backup/$(date +%Y%m%d).crontab

load_repos_cron="0 1 * * * ~/active/load_repos.sh"

cron_exists=$(crontab -l 2>/dev/null | grep -xF "$load_repos_cron" 2>/dev/null)

if [ ! "$cron_exists" ]; then
    crontab -l | { cat; echo "$load_repos_cron"; } | crontab -
fi

cd ~/active
bash load_repos.sh


# setup other cronjobs
# 
