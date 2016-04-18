guidelines for the aiddata spatial data framework


--------------------------------------------------
## data preprocessing and asdf ingestion prep

- any data that requires preprocessing should have a copy of the original data in /sciclone/aiddata10/REU/raw
- include preprocessing scripts in the asdf-datasets git repo in data-prep folder
- all datasets must be projected to WGS84 (EPSG4326) before entering into asdf
- final version of datasets should be moved the appropriate location in /sciclone/aiddata10/REU/data (see structure below)

data folder structure:

boundaries
    <dataset>

rasters
    external
        global
            <dataset>
        regional
            <dataset>

    internal
        msr
            <dataset> (research release)
                <hash> (actual data)

releases
    <dataset> (container)
        <dataset> (actual)


--------------------------------------------------
## asdf ingestion

- user inputting data into asdf are responsible for managing unique names which properly identify the dataset (this includes incorporating version into name for datasets which exist in multiple versions - the version field does not handle this)


### boundaries

requirement
- every boundary file has its own dataset
- all boundary files must be uploaded individually

recommendations
- adm boundary file dataset names should be: <country ISO3>_adm<adm #> (eg: NPL_adm0 - note: this will always be converted to all lowercase)
- country groups for adm boundary datasets should be the lowercase country name (eg: nepal) with spaces replaces with underscores (eg: timor_leste)
- an "actual" boundary for each group should always be added before other boundaries are added to group, though it is not required
- groups without an actual will not be usable until an actual is added and indexed by the system
- boundary datasets do not officially become a group (eg: for asdf dataset spatial indexing via tracker collections, in the DET interface) until they have an actual associated with them


### rasters (external)

manual logging (interface) / automatic logging (using json - not yet available):

- the file structure of processed data in /sciclone/aiddata10/REU/data should follow the standard of grouping the lowest level of temporal data in the same folder (eg: .../dataset/year/{month01.ext, month02.ext, ...)

- temporal information for datasets/files in data folder must be retrievable from the path to files and/or file names
- files must all include at least 4 digit years
- exception made for temporally invariant data which is designated using a "file_mask" input of "None"

- acceptable temporal data types for DET use include
    - temporally invariant
    - year
- other temporal data types which may be added to database but are NOT valid for DET use include:
    - year, month
    - year, month, day of month (with additional day range option)
    - year, day of year (with additional day range option)

- file names / paths should be consistent for all files in a dataset
- all files in a raster dataset must have the same bounds and resolution

- processed data must be logged before it can be used in any application (creates datapackage.json and inserts into database)


- data being input into the framework for DET use must be either yearly or temporally invariant
- datasets with resources spanning multiple years (eg: 2004-2005,2006-2007,...) must either be reduced to a single year each (eg: start year) and a notation made in the dataset description OR each resource should be added as its own dataset
- for data with a large range and small resource year spans (eg: 1900-2000 with resource for every 2 years) we suggest reducing each resource to a single year so it can be included in a single dataset
- data with large year spans (eg: 1900-2000 with resource every 50 years) can be added as individual datasets


### releases

if version of "asdf-releases" changes due to
    A) pure internal (non data impacting, eg: change logistical field in database collection) reasons: do not update "mean-surface-rasters" version
    B) changes which impact data (eg: fix bug which was causing donor fields with some character to be imported wrong) then "mean-surface-rasters" version must be incremented to reflect data change


### rasters (internal)

- this is automatically populated via msr module
- data is not logged in asdf:data collection, only det:msr collection
- see msr section of modules below


--------------------------------------------------
## modules and data use


### extracts

extract cache

- shared between det-module extracts and sciclone extracts
- script to update det-module cache database for all files in cache (allows running sciclone extracts to populate det cache)
- det always outputs extracts into /sciclone/aiddata10/REU/extracts and sciclone extracts can as well


- overview: /sciclone/aiddata10/REU/extracts/<bnd_name>/cache/<data_name>/<extract_type>/{extracts, ...}

- top level folder in this directory is based on boundary names within the asdf
- when running extracts through sciclone, make sure to use correct name if boundary exists in asdf
- within each boundary folder, there is a cache folder for extracts and an optional shps folder for storing boundary files that do not exist in asdf

- inside the cache folder, datasets are specified by their name in the asdf or by their folder name in /sciclone/aiddata10/REU/data
- if dataset does not exist in /sciclone/aiddata10/REU/data make sure to use a unique name that will not interfere with existing extracts (it is not suggested to store these extracts in this location)

- each dataset folder contains subfolders which specify the extract type (mean, max, sum, etc.)
- actual extracts files exist in a flat file structure within the extract type subfolder


projects folder

- used for storing project specific extracts for data that is not in asdf (as well as other project data not related to extracts)
- separate merge script can be run on extract cache to generate merges as needed which can be output to project folder or other location


### msr

filter formats:
- minimal filter for msr (hash)
    + eg: if field is "All" drop field from filter and do not run filter for that field
    + move all filters to "filters" object in "options" object
    + all basic filter keys should be the same string literal as actual field name used in filter (years filter would be example of an exception)

