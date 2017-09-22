

# modify ulimits for nproc and nfile
# https://stackoverflow.com/a/39506150

sudo bash -c 'cat <<EOF >> /etc/systemd/system.conf
DefaultLimitNOFILE=65536
EOF'

sudo bash -c 'cat <<EOF >> /etc/security/limits.conf
*    soft nofile 64000
*    hard nofile 64000
root soft nofile 64000
root hard nofile 64000
EOF'

sudo bash -c 'cat <<EOF >> /etc/pam.d/common-session
session required pam_limits.so
EOF'

sudo bash -c 'cat <<EOF >> /etc/pam.d/common-session-noninteractive
session required pam_limits.so
EOF'

# sudo bash -c 'cat <<EOF >> /etc/rc.local
# sysctl -w net.core.somaxconn=65535
# echo never > /sys/kernel/mm/transparent_hugepage/enabled
# EOF'

# sudo bash -c 'cat <<EOF >> /etc/sysctl.conf
# vm.overcommit_memory = 1
# EOF'



# -----------------------------------------------------------------------------


# monit setup
#
# usage:
# https://mmonit.com/monit/documentation/monit.html#GENERAL-OPERATION
#
# to disable start up if not using
#   sudo update-rc.d monit disable
#
# 9/22 - setting the nproc/nfile properly seems to have fixed the only
#        thing that has been causing crashes, so monit is probably not
#        needed and I am just disabling it on server for now.
#        I think puppet actually auto starts mongod on reboot,
#        though it doesn't seem to restart mongod on crash. Maybe worth
#        asking IT about that if need arises in future.

sudo apt-get install -y monit


sudo cp mongod_monit.conf /etc/monit/conf.d/
sudo chmod 700 /etc/monit/conf.d/mongod_monit.conf


# start monit for error checking
sudo monit -d 10 -c /etc/monit/conf.d/mongod_monit.conf
