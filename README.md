# extract-scripts

##ndvi_mosaic
Scripts for creating a job on the Sciclone cluster which preprocesses/mosaics contemporary GIMMS NDVI data in parallel

##ndvi_extract
Scripts for creating a job on the Sciclone cluster to run extracts on mosaic outputs of contemporary data

##historic_ndvi_prep
Scripts for creating a job on the Sciclone cluster to process historic GIMMS NDVI data (1981-2003)

##historic_ndvi_extract
Scripts for creating a job on the Sciclone cluster to run extracts on processed historic data

##ndvi_merge
Scripts for creating a job on the Sciclone cluster to merge extract outputs for both contemporary and historic data (change variable in runscripts.py to choose data type)

##atap_extract
Scripts for creating a job on the Sciclone cluster to run extracts on atap (air temperature and precipitation) datasets

##atap_merge
Scripts for creating a job on the Sciclone cluster to merge extract outputs for atap datasets (change variable in runscripts.py to choose air temp or precip)

##utility
Various scripts for managing output data, local tasks or anything not requiring a full Sciclone job.
- _ndvi_max.R_ : gets yearly maximum from merged ndvi extract outputs
- _gpw_extract.R_ : Rscript for running local extract on GPWv3 data
- _gpw_extract_merge.R_ : merge gpw extract
- _generic_extract.R_ : generic extract Rscript which takes user inputs
- _atap_grid.sh_ :  generate rasters from origin atap data
- _other scripts_ : see script comments for details

##ndvi_original  
Contains original Rscripts for downloading GIMMS NDVI data, preprocessing, creating mosaic and extracting (serial scripts)
