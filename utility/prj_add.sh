#!/bin/bash

base=/home/userx/Desktop/output/
for y in "$base"/*; do 
	year=`echo $y | sed 's/.*\///'`
	for d in "$base"/"$year"/*; do
		day=`echo $d | sed 's/.*\///'`
		echo "$base"/"$year"/"$day"/extract_"$year"_"$day".prj
		cp /home/userx/Desktop/proj.prj "$base"/"$year"/"$day"/extract_"$year"_"$day".prj
	done
done