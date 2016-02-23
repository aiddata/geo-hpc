#!/bin/bash

mkdir -p ~/active/{asdf,extract-scripts,mean-surface-rasters}

cd ~/active/asdf
if [ ! -d .git ]; then
   git clone -b develop https://github.com/itpir/asdf
else
   git pull origin develop
fi


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

