#!/bin/bash

# mosaic daily GIMMS NDVI from MODIS Terra
# prereqs: GDAL (currently using 1.9.0 - plans to upgrade to 1.11.2)

# called by python mpi4py script
# inputs: 1) year, 2) day

# internal variables:
# force 		- [bool] whether to force overwriting of existing mosaics
# GDAL_CACHEMAX - [int] environmental variable used to set the amount of memory GDAL is allowed to use
# myuser 		- [str] HPC account username used for writing to /local/scr on node

year=$1
day=$2

force=1

# export GDAL_CACHEMAX=8191
# export GDAL_CACHEMAX=10921
# export GDAL_CACHEMAX=12287
# export GDAL_CACHEMAX=16383
export GDAL_CACHEMAX=22527

# update to use user's /local/scr directory on node
myuser="sgoodman"

# input and output directories
in_dir="/sciclone/data20/aiddata/REU/raw/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"${year}/${day}
tmp_dir="/local/scr/"${myuser}"/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"${year}/${day}
out_dir="/sciclone/data20/aiddata/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"${year}/${day}

# clean up tmp directory if it exists
\rm -f -r "$tmp_dir"/*

# make input and tmp directories
mkdir -p "$in_dir"
mkdir -p "$tmp_dir"/unzip
mkdir -p "$tmp_dir"/prep

# move gzipped files to tmp dir
cd "$in_dir"
cp *.gz "$tmp_dir"

# gunzip and process individual frames
cd "$tmp_dir"
for a in *.gz; do

	# unzip
	z="$tmp_dir"/unzip/`echo $a | sed s/.gz//`
	gunzip -c $a > $z

	# process frame
	prep_tmp="$tmp_dir"/prep/`echo $a | sed s/.gz//`
	gdal_calc.py -A "$z" --outfile="$prep_tmp" --calc="A*(A<=250)+(255)*(A>250)" --NoDataValue=255

done


# get a file name for the output mosiac
# just use the name of the first tif you find in the unzip dir, except without xNNyNN tile name
cd "$tmp_dir"/prep
outname=$( find . -type f -iregex ".*[.]tif" | sed -n "1p" | sed "s:x[0-9]\+y[0-9]\+[.]::g" | sed "s:^[.]/::g;s:^:mosaic.:g" )


# build mosaic
mosaic_tmp="$tmp_dir"/tmp_"$outname"
mosaic_act="$out_dir"/"$outname"
if [[ $force -eq 1 || ! -f "$mosaic_act" ]]; then
	
	# remove tmp and output mosaic if they exist
	\rm -f "$mosaic_act"

	# merge processed frames into compressed geotiff mosaic
	# nodata value of 255
	gdal_merge.py -of GTiff -init 255 -n 255 -a_nodata 255 -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES *.tif -o "$mosaic_tmp"

	# make output directory and move from tmp_dir to out_dir
	mkdir -p "$out_dir"
	mv "$mosaic_tmp" "$mosaic_act"

fi

# clean up tmp_dir
\rm -f -r "$tmp_dir"/*

# echo year_day that was processed
echo "$year"_"$day"
