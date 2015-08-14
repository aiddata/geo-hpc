#!/bin/bash

# prep shapefiles for loading into cartodb

# copy shapefiles into new directory and zip directory

base=/home/userx/Desktop/output/
for y in "$base"/*; do 
	year=`echo $y | sed 's/.*\///'`
	for d in "$base"/"$year"/*; do
		day=`echo $d | sed 's/.*\///'`
		echo "$base"/"$year"/"$day"/extract_"$year"_"$day".prj
		
		# rm -rf "$base"/"$year"/"$day"/extract_"$year"_"$day"
		mkdir -p "$base"/"$year"/"$day"/extract_"$year"_"$day"

		for f in "$base"/"$year"/"$day"/*; do
			file=`echo $f | sed 's/.*\///'`
			ext=`echo $f | sed 's/.*\.//'`

			if [[ "$ext" != "csv" ]]; then
				echo "$base"/"$year"/"$day"/extract_"$year"_"$day"/"$file"

				cp $f "$base"/"$year"/"$day"/extract_"$year"_"$day"/"$file"
			fi
		done
		zip "$base"/"$year"/"$day"/extract_"$year"_"$day"
	done
done