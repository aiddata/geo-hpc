# AidData's Geo Framework
Core components of AidData's Geo Framework running on William and Mary's SciClone High Permance Computing cluster.

http://geo.aiddata.wm.edu

## Setup

### core hpc config
- make sure HPC account being used is set as priority user on for vortex-alpha nodes (HPC staff can do this)
- make sure HPC servers have necessary ports open for mongodb, gmail

### prepare database server
- have IT open mongodb ports for geo.aiddata.wm.edu and all HPC servers (prod and dev servers, where applicable)
- update mongod.conf
- copy db_backup_script.sh and add cron (see comments in script for details)


### prepare sciclone environment
- install home directory environment scripts (copy from `sciclone` in this repo to your sciclone account home directory, or extract from the `home_backups` dir in `/sciclone/aiddata10/geo`)
- load necessary modules by logging out and back in to sciclone or using `. ~/.cshrc` and `. ~/.cshrc.rhel6-opteron`
- install python packages: `pip install --user -r pip_list.txt`
- add ssh key from your sciclone account to `aiddatageo` github (approve rsa key first time manually)

### initialize framework
- run `bash setup.sh <branch>`


## Ingest Datasets
- utilizes data and ingest files from asdf-datasets repo
- see ingest dir for specifics on ingesting datasets (related resources in asdf-datasets repo)


## Components

### Tasks

todo


### Tools

todo


### Utils

todo


### Assets

static files/resources used by other scripts (e.g., images, text templates, pdfs)


### Scr

Scratch - misc scripts and tools not critical to running geo framework


## Tests

py-test based tests for testing geo utiltiies and components


## extract-scripts

Tools for manually running batch extract jobs


## Related Repos / Resources

Wiki - Additional details about Geo Framework and Sciclone:
- https://github.com/itpir/geo-hpc/wiki

Dataset preparation and ingest:
- https://github.com/itpir/geo-datasets

Website source and related config files (Private repos):
- https://github.com/itpir/geo-query
- https://github.com/itpir/geo-core

Related:
- https://github.com/itpir/geo-portal
- https://github.com/itpir/geoMatch
- https://github.com/itpir/geoML
- https://github.com/itpir/geoDash
- https://github.com/itpir/geoValuate

Previous repos for components of the Geo Framework which were combined in this repo:
- https://github.com/itpir/asdf
- https://github.com/itpir/mean-surface-rasters
- https://github.com/itpir/extract-scripts
- https://github.com/itpir/det-module

