#!/bin/bash

mkdir -p ~/active/{asdf,extract-scripts,mean-surface-rasters}

cd ~/active/asdf
if [ ! -d .git ]; then
   git clone -b develop https://github.com/itpir/asdf
else
   git pull origin develop
fi


old_hash=$(md5sum ~/active/load_repos.sh)
new_hash=$(md5sum ~/active/asdf/src/tools/load_repos.sh)
echo $old_hash
echo $new_hash

if [[ "$old_hash" != "$new_hash" ]]; then

    echo "Found new load_repos.sh ..."
    cp  ~/active/asdf/src/tools/load_repos.sh ~/active/load_repos.sh
    bash ~/active/load_repos.sh

else

    cd ~/active/extract-scripts
    if [ ! -d .git ]; then
        git clone -b develop https://github.com/itpir/extract-scripts
    else
       git pull origin master
    fi


    cd ~/active/mean-surface-rasters
    if [ ! -d .git ]; then
        git clone -b develop https://github.com/itpir/mean-surface-rasters
    else
       git pull origin master 
    fi

fi
