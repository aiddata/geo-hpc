#!/bin/sh

# this script should be placed in:
#   /opt/aiddata/backup_mongo_db.sh
#
# with a crontab set:
#   1 1 * * * bash /opt/aiddata/backup_mongo_db.sh BRANCH 2>&1 | tee 1>>/opt/aiddata/cron_log.log
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


dst_dir=/sciclone/aiddata10/geo/"${branch}"/backups/mongodb_backups/


# compresses individual items then archives
# example mongorestore:
#   mongorestore --gzip --archive=backup.archive
# for details see:
#   https://www.mongodb.com/blog/post/archiving-and-compression-in-mongodb-tools


asdf_output=${dst_dir}/${timestamp}.asdf.archive

mongodump --db asdf --excludeCollection features --gzip --archive | ssh aiddatageo@vortex.sciclone.wm.edu \
"cat - > ${asdf_output} && chmod g=rw,o=r ${asdf_output}"



trackers_output=${dst_dir}/${timestamp}.trackers.archive

mongodump --db trackers --gzip --archive | ssh aiddatageo@vortex.sciclone.wm.edu \
"cat - > ${trackers_output} && chmod g=rw,o=r ${trackers_output}"



releases_output=${dst_dir}/${timestamp}.releases.archive

mongodump --db releases --gzip --archive | ssh aiddatageo@vortex.sciclone.wm.edu \
"cat - > ${releases_output} && chmod g=rw,o=r ${releases_output}"

