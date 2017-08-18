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

output=/sciclone/aiddata10/geo/"${branch}"/backups/mongodb_backups/"${timestamp}".archive


# compresses individual items then archives
# example mongorestor:
#   mongorestore --gzip --archive=backup.archive
# for details see:
#   https://www.mongodb.com/blog/post/archiving-and-compression-in-mongodb-tools

mongodump --gzip --archive | ssh aiddatageo@vortex.sciclone.wm.edu \
"cat - > ${output} && chmod g=rw,o=r ${output}"

