#!/bin/bash

# process historic gimms ndvi data
# prereqs: GDAL (current version - 1.11.2)

# called by python mpi4py script
# inputs: 1) year, 2) month, 3) file

# internal variables:
# force 		- [bool] whether to force overwriting of existing mosaics
# GDAL_CACHEMAX - [int] environmental variable used to set the amount of memory GDAL is allowed to use
# myuser 		- [str] HPC account username used for writing to /local/scr on node

year=$1
month=$2
file=$3

force=1

# export GDAL_CACHEMAX=8191
# export GDAL_CACHEMAX=10921
# export GDAL_CACHEMAX=12287
# export GDAL_CACHEMAX=16383
export GDAL_CACHEMAX=22527

# update to use user's /local/scr directory on node
myuser="sgoodman"

# input and output directories
in_dir="/sciclone/data20/aiddata/REU/raw/historic_gimms_ndvi"
tmp_dir="/local/scr/"${myuser}"/REU/data/historic_gimms_ndvi/"${year}/${month}
out_dir="/sciclone/data20/aiddata/REU/data/historic_gimms_ndvi/"${year}/${month}

# clean up tmp directory if it exists
\rm -f -r "$tmp_dir"/*

# make input and tmp directories
mkdir -p "$in_dir"
mkdir -p "$tmp_dir"

process_in="$in_dir"/"$file"
process_tmp="$tmp_dir"/`echo $file | sed s/.asc/.tif/`
process_act="$out_dir"/`echo $file | sed s/.asc/.tif/`

if [[ $force -eq 1 || ! -f "$process_act" ]]; then
	
	# remove output file it already exists
	\rm -f "$process_act"

	# process
	# everything < 0 grouped into nodata value of -99
	gdal_calc.py -A "$process_in" --outfile="$process_tmp" --calc="A*(A>=0)+(255)*(A<0)" --NoDataValue=255 --co COMPRESS=LZW --co TILED=YES --co BIGTIFF=YES

	# make output directory and move from tmp_dir to out_dir
	mkdir -p "$out_dir"
	mv "$process_tmp" "$process_act"

fi

# clean up tmp_dir
\rm -f -r "$tmp_dir"/*

# echo year_day that was processed
echo "$year"_"$month"
