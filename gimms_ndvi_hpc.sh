#!/bin/bash

# mosaic daily GIMMS NDVI from MODIS Terra
# prereqs: GDAL

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

# mosaic
# first get a file name for the output mosiac
# just use the name of the first tif you find in the dir, except without xNNyNN tile name
# cd "$out_dir"/prep

outname=$( find . -type f -iregex ".*[.]tif" | sed -n "1p" | sed "s:x[0-9]\+y[0-9]\+[.]::g" | sed "s:^[.]/::g;s:^:mosaic.:g" )

# /sciclone/data20/aiddata/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/2000/249/unzip




outmosaic="$out_dir"/raw_"$outname"
gdal_merge.py -of GTiff -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES *.tif -o $outmosaic

outcalc="$out_dir"/"$outname"
gdal_calc.py -A "$outmosaic" --outfile="$outcalc" --calc="A*(A<=250)+(255)*(A>250)" --NoDataValue=255




# outmosaic="$out_dir"/vrtraw_"$outname"
# gdalbuildvrt "$outmosaic" *.tif

# outcalc="$out_dir"/"$outname"
# gdal_calc.py -A "$outmosaic" --outfile="$outcalc" --calc="A*(A<=250)+(255)*(A>250)" --NoDataValue=255 --co COMPRESS=LZW --co TILED=YES --co BIGTIFF=YES




# outvrt="$out_dir"/vrt_"$outname"
# gdalbuildvrt "$outvrt" *.tif

# outtrans="$out_dir"/trans_"$outname"
# gdal_translate -of VRT -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES "$outvrt" "$outtrans"

# outcalc="$out_dir"/calc_"$outname"
# gdal_calc.py -A "$outtrans" --outfile="$outcalc" --calc="A*(A<=250)+(255)*(A>250)" --NoDataValue=255




# # create merged vrt
# outmosaic="$out_dir"/1_"$outname"
# gdalbuildvrt "$outmosaic" *.tif

# # gdal_translate on byte vrt to output float vrt (and compress)
# outtranslate="$out_dir"/2_"$outname"
# gdal_translate -of "VRT" -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES -ot "Float32" "$outmosaic" "$outtranslate"

# # gdal_calc on vrt and output tif
# outcalc="$out_dir"/3_"$outname"
# gdal_calc.py -A "$outtranslate" --outfile="$outcalc" --calc="A*0.004" --calc="A*(A>=0)+(-9999)*(A<0)" --calc="A*(A<=250)+(-9999)*(A>250)" --NoDataValue=-9999 




echo "$year"_"$day"
