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

timestamp=`date +%Y%m%d_%H%M%S`

backup_dir=/sciclone/aiddata10/geo/"${branch}"/backups/mongodb_backups

# compresses individual items then archives
# example mongorestor:
#   mongorestore --gzip --archive=backup.archive
# for details see:
#   https://www.mongodb.com/blog/post/archiving-and-compression-in-mongodb-tools

output="${backup_dir}/${timestamp}.archive"


mongodump --gzip --archive | ssh aiddatageo@vortex.sciclone.wm.edu \
"mkdir -p $dirname(${output}) && cat - > ${output} && chmod g=rw,o=r ${output}"

