#!/bin/bash


sensor=$1
year=$2
day=$3
filename=$4

force=1


# update to use user's /local/scr directory on node
myuser="sgoodman"

# input and output directories
in_dir="/sciclone/data20/aiddata/REU/raw/ltdr.nascom.nasa.gov/allData/Ver4/"${year}/${day}
tmp_dir="/local/scr/"${myuser}"/REU/data/ltdr.nascom.nasa.gov/allData/Ver4/"${year}/${day}
out_dir="/sciclone/data20/aiddata/REU/data/ltdr.nascom.nasa.gov/allData/Ver4/"${year}/${day}




for s in "$base"/*;do 
	sensor_major=`echo $s | sed 's/.*\///'`

	for y in "$s"/*;do 
		year=`echo $y | sed 's/.*\///'`

		for f in "$y"/*;do 
			file=`echo "$f" | sed 's/.*\///'`

			product=
			year=
			day=
			sensor_minor=

			if [[ "$product" == "AVH13C1" ]];then 

				#
				
			fi

		done

	done

done





