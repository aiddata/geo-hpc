# extract-scripts 
__gen3.2__

scripts for preparing, extracting and working with datasets on sciclone


--------------------------------------------------
--------------------------------------------------

## file overview

### extract/

__extract.py__  

__builder.py__
automatically create and run jobs using a given jobs json

__extract_utility.py__
3rd generation fully integrated extract script which can be used for datasets with any temporal type  
- includes: year, year month, year day, temporally invariant

__runscript.py__
stuff

__merge.py__
script (temporally agnostic) for use with gen 3 sciclone extract outputs


### tests/

stuff
(be sure to run "build_test_datasets.sh" in tests/data folder before running dev tests)


--------------------------------------------------
--------------------------------------------------

## how it works

- create job config json (see below for details on creating this)
- run builder and give it your job config json
- builder validates your config, searches for available datasets, clones your config file and adds information that will be used by main script, then finally creates a jobscript and submits the job on the HPC
- when your HPC job starts the main runscript reads in the cloned config file which now includes details on how to run extracts
- the run script then runs all the extracts and created a merged csv for each unique boundary used for extracts within your job
- you can also independently run a merge using a json of the same format as your original config json to get specific merges to meet your needs after the extracts have finished. this just uses a normal python script and does not require running an HPC job.


--------------------------------------------------
--------------------------------------------------

## job config json guide


### config

__general information__

batch name, job name, user prefix

__job resources and runtime__

max nodes, ppn override, walltime override and walltime

current resource management
- currently minimal resource management is automated, mostly based on user inputs
- job config json includes max nodes (and optional ppn) you are willing to wait on
- if job is small enough that it does not require all nodes to complete in a single cycle, the number of nodes requested will be reduced

future plans for optimization
- adjusts based on estimated runtimes of individual extract jobs
- required reasonable estimation of runtimes and optimization algorithm

### defaults

__required fields__

required fields must be present in defaults if they are not specified in **every** dataset options (see below for details on dataset specific options). the builder script will provide a warning if required fields are missing from the defaults object.

### data

__required fields__
name: name must match dataset name in datasets.json (eventually will match to names in asdf)

__overriding default fields__

any of the required fields from the defaults section may be modified here. changes apply to the dataset they are specified within only. if a required field is excluding from the defaults, it must be included in every dataset's options or an error will occur


