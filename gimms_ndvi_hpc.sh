#!/bin/bash

# mosaic daily GIMMS NDVI from MODIS Terra
# prereqs: GDAL (currently using 1.9.0 - plans to upgrade to 1.11.2)

# called by python mpi4py script
# inputs: 1) year, 2) day

year=$1
day=$2

force=1

# export GDAL_CACHEMAX=8191
# export GDAL_CACHEMAX=10921
# export GDAL_CACHEMAX=12287
export GDAL_CACHEMAX=16383


# update to use user's /local/scr directory on node
myuser="sgoodman"

# input and output directories
in_dir="/sciclone/data20/aiddata/REU/raw/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"${year}/${day}
tmp_dir="/local/scr/"${myuser}"/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"${year}/${day}
out_dir="/sciclone/data20/aiddata/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"${year}/${day}

# make directories
mkdir -p "$in_dir"/unzip
mkdir -p "$tmp_dir"
mkdir -p "$out_dir"


cd "$in_dir"

# gunzip each gzipped file to "unzip" directory if not already gunzipped
# process individual frames
for a in *.gz; do
	z="$in_dir"/unzip/`echo $a | sed s/.gz//`
	if [[ ! -f  $z ]]; then
		gunzip -c $a > $z
	fi

	prep_tmp="$tmp_dir"/`echo $a | sed s/.gz//`
	\rm -f "$prep_tmp"
	gdal_calc.py -A "$z" --outfile="$prep_tmp" --calc="A*(A<=250)+(255)*(A>250)" --NoDataValue=255

done


# get a file name for the output mosiac
# just use the name of the first tif you find in the unzip dir, except without xNNyNN tile name
cd unzip
outname=$( find . -type f -iregex ".*[.]tif" | sed -n "1p" | sed "s:x[0-9]\+y[0-9]\+[.]::g" | sed "s:^[.]/::g;s:^:mosaic.:g" )


# build mosaic
cd "$tmp_dir"
mosaic_tmp="$tmp_dir"/tmp_"$outname"
mosaic_act="$out_dir"/"$outname"
if [[ $force -eq 1 || ! -f "$mosaic_act" ]]; then
	
	\rm -f "$mosaic_tmp"
	\rm -f "$mosaic_act"

	gdal_merge.py -of GTiff -init 255 -n 255 -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES *.tif -o "$mosaic_tmp"

	# move from tmp_dir to out_dir
	mv "$mosaic_tmp" "$mosaic_act"

	# clean up tmp_dir
	\rm -f "$tmp_dir"/*

fi


echo "$year"_"$day"
