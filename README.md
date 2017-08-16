# AidData's Geo Framework
### on William and Mary's High Performance Computing cluster, SciClone


## Setup

### prepare sciclone environment
- update config scripts for sciclone that define environment variables (e.g., PYTHONPATH) and load necessary modules (see src/sciclone dir). reload files (`source <file>` or logout to load changes
- install python package if needed (unlikely these will ever get wiped, but list is in sciclone/pip_list.txt) see sciclone/scipip for pip install
- make sure HPC account being used is set as priority user on for vortex-alpha nodes (HPC staff can do this)
- make sure HPC servers have necessary ports open for mongodb, gmail


### prepare database server
- have IT open mongodb ports for geo.aiddata.wm.edu and all HPC servers (prod and dev servers, where applicable)
- update mongod.conf
- copy db_backup_script.sh and add cron (see comments in script for details)


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


## Tests

py-test based tests for testing geo utiltiies and components


## extract-scripts

Tools for manually running batch extract jobs


## Related Repos / Resources

Dataset preparation and ingest:
- https://github.com/itpir/asdf-datasets

Website source and related config files (Private repos):
- https://github.com/itpir/geo-query
- https://github.com/itpir/geo-core

SciClone Resources:
- https://github.com/itpir/aiddata-sciclone

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

