#!/bin/bash

# download night time light (v4) raw tar files

# dir=/sciclone/aiddata10/REU/raw/v4avg_lights_x_pct
dir=/home/userz/globus-data/raw/v4avg_lights_x_pct
mkdir -p ${dir}
cd ${dir}

# z=(F101992 F101993 
#     F121994 F121995 F121996 
#     F141997 F141998 F141999 F152000 F152001 F152002 F152003
#     F162004 F162005 F162006 F162007 F162008 F162009)

# for i in ${z[*]}; do 

#     echo $i
#     file="http://ngdc.noaa.gov/eog/data/web_data/v4avg_lights_x_pct/"${i}".v4b.avg_lights_x_pct.tar"
#     echo $file
#     wget -c -N $file

# done


z=(F182010 F182011 F182012 F182013)

for i in ${z[*]}; do 

    echo $i
    file="http://ngdc.noaa.gov/eog/data/web_data/v4avg_lights_x_pct/"${i}".v4c.avg_lights_x_pct.tar"
    echo $file
    wget -c -N $file

done