#!/bin/sh

# this script should be placed in:
#   /opt/aiddata/backup_mongo_db.sh
#
# with a crontab set:
#   1 1 * * * bash /opt/aiddata/backup_mongo_db.sh BRANCH >> /opt/aiddata/cron_log.log
# where BRANCH is either "master" or "develop"
#
# requires ssh key be setup on server for aiddatageo

timestamp=`date +%Y%m%d_%H%M%S`

echo -e "\n"
echo "Backing up mongo db (${timestamp})"


branch=$1

if [[ $branch == "" ]]; then
    echo "No branch specified"
    exit 1
fi


backup_name="${timestamp}".archive

dst_dir=/sciclone/aiddata10/geo/"${branch}"/backups/mongodb_backups/

output=${dst_dir}/${backup_name}

# compresses individual items then archives
# example mongorestor:
#   mongorestore --gzip --archive=backup.archive
# for details see:
#   https://www.mongodb.com/blog/post/archiving-and-compression-in-mongodb-tools

mongodump --gzip --archive | ssh aiddatageo@vortex.sciclone.wm.edu \
"cat - > ${output} && chmod g=rw,o=r ${output}"

