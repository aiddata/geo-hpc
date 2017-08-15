#!/bin/sh

# this script should be placed in:
#   /opt/aiddata/db_backup_script.sh
#
# with a crontab set:
#   1 1 * * * bash /opt/aiddata/db_backup_script.sh BRANCH
# where BRANCH is either "master" or "develop"
#
# requires ssh key be setup on server for aiddatageo

branch=$1

if [[ $branch == "" ]]; then
    exit 1
fi

backup_root=/opt/aiddata

timestamp=`date +%Y%m%d_%H%M%S`
tmp_backup=$backup_root/mongodump_$timestamp

mongodump -o $tmp_backup

rsync -r --delete $tmp_backup/ aiddatageo@vortex.sciclone.wm.edu:data20/mongodb_backups/$branch/$timestamp

rm -r $tmp_backup
