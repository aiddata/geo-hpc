# AidData's Geo Framework on William and Mary's High Performance Computing cluster, SciClone


## Setup

### basic sciclone environment
- update config scripts for sciclone that define environment variables (e.g., PYTHONPATH) and load necessary modules (see src/sciclone dir). reload files (`source <file>` or logout to load changes
- install python package if needed (unlikely these will ever get wiped, but list is in sciclone/pip_list.txt) see sciclone/scipip for pip install
- make sure HPC account being used is set as priority user on for vortex-alpha nodes (HPC staff can do this)
- make sure HPC servers have necessary ports open for mongodb, gmail


### database server
- have IT open mongodb ports for geo.aiddata.wm.edu and all HPC servers (prod and dev servers, where applicable)
- update mongod.conf
- copy db_backup_script.sh and add cron (see comments in script for details)


### asdf setup
- run `bash setup.sh <branch>`

### ingest
- utilizes data and ingest files from asdf-datasets repo
- see ingest dir for specifics on ingesting datasets (related resources in asdf-datasets repo)



## Components

### mean-surface-rasters

Originally built based on a fork of [monte-carlo-rasters](https://github.com/itpir/monte-carlo-rasters) release [v0.2.0](https://github.com/itpir/monte-carlo-rasters/releases/tag/v0.2.0)

### extract-scripts

todo


### geoquery-queue

todo



## Related Repos / Resources

Dataset preparation and ingest:
- https://github.com/itpir/asdf-datasets

Private components (website source and related config files):
- https://github.com/itpir/geo-query
- https://github.com/itpir/geo-core

Related:
- https://github.com/itpir/geoMatch
- https://github.com/itpir/geoML
- https://github.com/itpir/geoDash
- https://github.com/itpir/geoValuate

Previous repos for components of the Geo Framework which were combined in this repo:
- https://github.com/itpir/asdf
- https://github.com/itpir/mean-surface-rasters
- https://github.com/itpir/extract-scripts
- https://github.com/itpir/det-module

SciClone Resources:
https://github.com/itpir/aiddata-sciclone
