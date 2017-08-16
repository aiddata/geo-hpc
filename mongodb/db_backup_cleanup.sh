#!/bin/sh

# unfinished script


backup_root=/sciclone/aiddata10/REU/backups/mongodb_backups

backup_name=`date +%Y%m%d_%s`

dst=$backup_root/$backup_name

mkdir -p $dst

mongodump -o $dst




backup_history=$(find $backup_root -mindepth 1 -maxdepth 1 -type d | sort -nr | # DEFINE WHAT WE WANT TO REMOVE # )

for i in ${backup_history=[*]}; do

    find "$i" -type f -exec rm -rf "{}" \;
    find "$i" -type d -exec rm -rf "{}" \;

done

