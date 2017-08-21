#!/bin/sh

# this script should be placed in:
#   /opt/aiddata/backup_mongo_log.sh
#
# with a crontab set:
#   1 0 * * * bash /opt/aiddata/backup_mongo_log.sh BRANCH 2>&1 | tee 1>>/opt/aiddata/cron_log.log
# where BRANCH is either "master" or "develop"
#
# requires ssh key be setup on server for aiddatageo
#
# make sure /etc/mongod.conf has:
# 	systemLog.logAppend: true
#	systemLog.logRotate: reopen
#
# if mongo was installed by Puppet
# when IT setup VM, they will need to
# update it through Puppet (restart
# mongod before changes take effect)

timestamp=$(date +%Y%m%d_%H%M%S)


echo -e "\n"
echo "Backing up mongo log (${timestamp})"


branch=$1

if [[ $branch == "" ]]; then
    echo "No branch specified"
    exit 1
fi

backup_name=mongodb.log.${timestamp}

src_dir=/opt/aiddata/
dst_dir=/sciclone/aiddata10/geo/${branch}/backups/mongodb_logs

mongo_log_src=${src_dir}/${backup_name}
mongo_log_dst=${dst_dir}/${backup_name}


cp /var/log/mongodb/mongodb.log ${mongo_log_src}

# rotate log
mongo <<EOF
use admin
db.runCommand( { logRotate : 1 } )
quit()
EOF


rsync ${mongo_log_src} aiddatageo@vortex.sciclone.wm.edu:${mongo_log_dst}

ssh aiddatageo@vortex.sciclone.wm.edu "chmod g=rw,o=r ${mongo_log_dst}"

rm $mongo_log_src

