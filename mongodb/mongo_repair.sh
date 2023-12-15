#!/bin/sh

# commands to fix mongo after crash
# try each option based on conditions in comments, and/or sequentially
# restart using sudo service mongod restart after each option attempted

# to check log for errors
tail /var/log/mongodb/mongodb.log


# OPTION 1 - if log shows error with mongod.pid, first try
sudo touch /var/run/mongod.pid
sudo chown mongodb:mongodb /var/run/mongod.pid

# OPTION 2 - if issue with lock...
sudo rm /var/lib/mongodb/mongod.lock

# OPTION 3
# only repair if needed, may take a while to run on larger
# collection like features db
sudo mongod --repair --dbpath /var/lib/mongodb --storageEngine wiredTiger
sudo chown -R mongodb:mongodb /var/lib/mongodb

# restart mongo
sudo service mongod restart
