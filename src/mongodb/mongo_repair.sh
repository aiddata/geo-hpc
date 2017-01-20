#!/bin/sh

# commands to fix mongo after crash

sudo rm /var/lib/mongodb/mongod.lock

# only repair if needed, may take a while to run on larger
# collection like features db
sudo mongod --repair --dbpath /var/lib/mongodb --storageEnginer wiredTiger

# files need proper ownership
sudo chown -R mongodb:mongodb /var/lib/mongodb

sudo service mongod restart

