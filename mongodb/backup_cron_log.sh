#!/bin/sh

# this script should be placed in:
#   /opt/aiddata/backup_cron_log.sh
#
# with a crontab set:
#   1 0 * * * bash /opt/aiddata/backup_cron_log.sh BRANCH
# where BRANCH is either "master" or "develop"
#
# requires ssh key be setup on server for aiddatageo

timestamp=$(date +%Y%m%d_%H%M%S)

echo -e "\n"
echo "Backing up cron log (${timestamp})"


branch=$1

if [[ $branch == "" ]]; then
    echo "No branch specified"
    exit 1
fi


backup_name=cron_log.${timestamp}

src_dir=/opt/aiddata/
dst_dir=/sciclone/aiddata10/geo/${branch}/backups/mongodb_cron_logs

cron_log_src=${src_dir}/cron_log.log
cron_log_dst=${dst_dir}/${backup_name}

rsync ${cron_log_src} aiddatageo@vortex.sciclone.wm.edu:${cron_log_dst}

ssh aiddatageo@vortex.sciclone.wm.edu "chmod g=rw,o=r ${cron_log_dst}"

rm $cron_log_src
