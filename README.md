# aiddata spatial data framework


## Setup

### basic sciclone environment
- update config scripts for sciclone that define environment variables (e.g., PYTHONPATH) and load necessary modules (see src/sciclone dir). reload files (`source <file>` or logout to load changes
- install python package if needed (unlikely these will ever get wiped, but list is in sciclone/pip_list.txt) see sciclone/scipip for pip install
- make sure HPC account being used is set as priority user on for vortex-alpha nodes (HPC staff can do this)
- make sure HPC servers have necessary ports open for mongodb, gmail


## database server
- have IT open mongodb ports for geo.aiddata.wm.edu and all HPC servers (prod and dev servers, where applicable)
- update mongod.conf
- copy db_backup_script.sh and add cron (see comments in script for details)


## asdf setup
- run `bash setup.sh <branch>`

## asdf dataset ingest
- see ingest dir for specifics on ingesting datasets (related resources in asdf-datasets repo)


