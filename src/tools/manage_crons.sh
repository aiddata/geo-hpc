#!/bin/bash

# script to enable cron jobs for production / development branch scrips

# script can also add/remove flocks on relevant scripts called by cron jobs

# setup script will call this to initialize cron jobs
# admin can then run this after initial setup to disable/enable as needed

# ability to comment/uncomment all jobs with master/develop flag


# for each branch:

# file updates / admin tasks
# - load_repos.sh

# database tasks
# - update_extract_list.py
# - update_msr_list.py
# - clean stale entries out of dbs/collections (multiples? asdf data/trackers, extract, msr)
# - update extract/msr trackers from manual job cache?

# hpc job starts
# - extract job starter
# - msr job starter

# det queue processing
# - prep.py
# - processing.py




# activate() {}

# deactivate() {}

# lock() {}

# unlock() {}
