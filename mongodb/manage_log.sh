#!/bin/sh

# this script should be placed in:
#   /opt/aiddata/manage_log.sh
#
# with a crontab set:
#   1 0 * * * bash /opt/aiddata/manage_log.sh BRANCH
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

branch=$1

if [[ $branch == "" ]]; then
    exit 1
fi

backup_root=/opt/aiddata

# copy log
timestamp=$(date +%Y%m%d_%H%M%S)
tmp_name=mongodb.log.${timestamp}
tmp_log=${backup_root}/${tmp_name}
cp /var/log/mongodb/mongodb.log ${tmp_log}

# rotate log
mongo <<EOF
use admin
db.runCommand( { logRotate : 1 } )
quit()
EOF

output=/sciclone/aiddata10/geo/backups/mongodb_logs/${branch}/${tmp_name}

rsync ${tmp_log} aiddatageo@vortex.sciclone.wm.edu:${output}
ssh aiddatageo@vortex.sciclone.wm.edu "mkdir -p $(dirname ${output} && chmod g+rw,o+r ${output}"

rm $tmp_log
