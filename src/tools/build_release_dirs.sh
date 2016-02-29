#!/bin/bash

# useful for releases in asdf 
# since they already have datapackage in root dir
# we need to create another folder for new datapackage to live in

# creates dir of same name inside another dir 
# and moves all existing files into new dir

# cd to parent first

for i in *; do 
    mkdir $i/$i; 
    for j in $i/*; do 
        if [[ $j != $i/$i ]]; then 
            mv $j $i/$i; 
        fi; 
    done; 
done


# single line
# for i in *; do mkdir $i/$i; for j in $i/*; do if [[ $j != $i/$i ]]; then mv $j $i/$i; fi; done; done

