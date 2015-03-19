#!/bin/bash

# mosaic daily GIMMS NDVI from MODIS Terra
# prereqs: GDAL (currently using 1.9.0 - plans to upgrade to 1.11.2)

# called by python mpi4py script
# inputs: 1) year, 2) day

year=$1
day=$2

# input and output directories
in_dir="/sciclone/data20/aiddata/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"$year/$day
out_dir="/sciclone/data20/aiddata/REU/processed/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"$year/$day

# make and go to input unzip directory
mkdir -p "$in_dir"/unzip
cd "$in_dir"

# gunzip each gzipped file to "unzip" directory if not already gunzipped
for a in *.gz; do 
	z="$in_dir"/unzip/`echo $a | sed s/.gz//`
	if [[ ! -f  $z ]]; then
		gunzip -c $a > $z
	fi
done

# make output (and prep) directory
# mkdir -p "$out_dir"/prep

# prep each tif
# multiply all values by 0.004
# set values less than 0 or greater than 250 to NA value (255)
cd unzip
# for f in *; do
# 	calc_in="$in_dir"/unzip/"$f"
# 	calc_out="$out_dir"/prep/"$f"
# 	gdal_calc.py -A $calc_in --outfile=$calc_out --calc="A*0.004" --calc="A*(A>=0)+(-9999)*(A<0)" --calc="A*(A<=250)+(-9999)*(A>250)" --NoDataValue=-9999
# done

mkdir -p "$out_dir"

# get a file name for the output mosiac
# just use the name of the first tif you find in the dir, except without xNNyNN tile name

outname=$( find . -type f -iregex ".*[.]tif" | sed -n "1p" | sed "s:x[0-9]\+y[0-9]\+[.]::g" | sed "s:^[.]/::g;s:^:mosaic.:g" )

# data source
# /sciclone/data20/aiddata/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/2000/249/unzip



# METHOD: tif mosaic from original tifs -> calc on mosaic tif -> output tif
# add tmp files and checks to see if process was interrupted (tmp exists, actual does not)
# skips if tmp does not exist and actual does (assumed process completed succesfully)

# build mosaic
pre_tmp="$out_dir"/tmp_raw_"$outname"
pre_act="$out_dir"/raw_"$outname"
if [[ -f "$pre_tmp" || ! -f "$pre_act" ]]; then
	
	\rm -f "$pre_tmp"
	\rm -f "$pre_act"

	gdal_merge.py -of GTiff -init 255 -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES *.tif -o "$pre_tmp"

	mv "$pre_tmp" "$pre_act"

fi

# # process mosaic
# post_tmp="$out_dir"/tmp_"$outname"
# post_act="$out_dir"/"$outname"
# if [[ -f "$post_tmp" || ! -f "$post_act" ]]; then
	
# 	\rm -f "$post_tmp"
# 	\rm -f "$post_act"

# 	gdal_calc.py -A "$pre_act" --outfile="$post_tmp" --calc="A*(A<=250)+(255)*(A>250)" --NoDataValue=255

# 	mv "$post_tmp" "$post_act"

# fi



# METHOD: vrt mosaic from original tifs -> calc on mosaic vrt -> output tif

# outmosaic="$out_dir"/vrtraw_"$outname"
# gdalbuildvrt "$outmosaic" *.tif

# outcalc="$out_dir"/"$outname"
# gdal_calc.py -A "$outmosaic" --outfile="$outcalc" --calc="A*(A<=250)+(255)*(A>250)" --NoDataValue=255 --co COMPRESS=LZW --co TILED=YES --co BIGTIFF=YES



# METHOD: vrt mosaic from original tifs -> add compression instructions to vrt -> calc on vrt mosaic -> output tif

# outvrt="$out_dir"/vrt_"$outname"
# gdalbuildvrt "$outvrt" *.tif

# outtrans="$out_dir"/trans_"$outname"
# gdal_translate -of VRT -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES "$outvrt" "$outtrans"

# outcalc="$out_dir"/calc_"$outname"
# gdal_calc.py -A "$outtrans" --outfile="$outcalc" --calc="A*(A<=250)+(255)*(A>250)" --NoDataValue=255



# METHOD: vrt mosaic from original tifs -> add compression and float32 instructions to vrt -> calc (0-1) on vrt -> output tif

# outmosaic="$out_dir"/1_"$outname"
# gdalbuildvrt "$outmosaic" *.tif

# outtranslate="$out_dir"/2_"$outname"
# gdal_translate -of "VRT" -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES -ot "Float32" "$outmosaic" "$outtranslate"

# outcalc="$out_dir"/3_"$outname"
# gdal_calc.py -A "$outtranslate" --outfile="$outcalc" --calc="A*0.004" --calc="A*(A>=0)+(-9999)*(A<0)" --calc="A*(A<=250)+(-9999)*(A>250)" --NoDataValue=-9999 



echo "$year"_"$day"
