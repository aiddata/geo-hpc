# monit usage:
# https://mmonit.com/monit/documentation/monit.html#GENERAL-OPERATION


sudo apt-get install -y monit


sudo cp mongod_monit.conf /etc/monit/conf.d/
sudo chmod 700 /etc/monit/conf.d/mongod_monit.conf


# start monit for error checking
sudo monit -d 10 -c /etc/monit/conf.d/mongod_monit.conf
