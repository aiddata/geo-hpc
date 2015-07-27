# extract-scripts

scripts for preparing, extracting and working with datasets on sciclone

--------------------------------------------------
--------------------------------------------------

## year_extract
generic extract scripts for datasets identified by year

## year_month_extract
generic extract scripts for datasets identified by year and month

## year_day_extract
generic extract scripts for datasets identified by year and day of year

## single_extract
generic extract scripts for temporally invariant data



## data prep 
scripts for preprocessing raw data

- ltdr  
   preprocessing for ltdr ndvi data

- ndvi_mosaic  
   Scripts for creating a job on the Sciclone cluster which preprocesses/mosaics contemporary GIMMS NDVI data in parallel

- historic_ndvi  
   Scripts for creating a job on the Sciclone cluster to process historic GIMMS NDVI data (1981-2003)

- atap  
   creates rasters from raw atap data



## data post
postprocessing scripts for working with extract data

- coming soon



## old
old scripts (most in process of being updated, moved or deprecated)


- ndvi_original  
    Contains original Rscripts for downloading GIMMS NDVI data, preprocessing, creating mosaic and extracting (serial scripts)

- ndvi_extract  
    Scripts for creating a job on the Sciclone cluster to run extracts on mosaic outputs of contemporary data

- historic_ndvi_extract  
    Scripts for creating a job on the Sciclone cluster to run extracts on processed historic data

- ndvi_merge  
    Scripts for creating a job on the Sciclone cluster to merge extract outputs for both contemporary and historic data (change variable in runscripts.py to choose data type)

- atap_extract  
    Scripts for creating a job on the Sciclone cluster to run extracts on atap (air temperature and precipitation) datasets

- atap_merge  
    Scripts for creating a job on the Sciclone cluster to merge extract outputs for atap datasets (change variable in runscripts.py to choose air temp or precip)

- utility  
    Various scripts for managing output data, local tasks or anything not requiring a full Sciclone job.
    - _ndvi_max.R_ : gets yearly maximum from merged ndvi extract outputs
    - _gpw_extract.R_ : Rscript for running local extract on GPWv3 data
    - _gpw_extract_merge.R_ : merge gpw extract
    - _generic_extract.R_ : generic extract Rscript which takes user inputs
    - _other scripts_ : see script comments for details

