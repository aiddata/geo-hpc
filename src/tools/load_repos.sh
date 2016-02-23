#!/bin/bash

# makes sure the latest versions of repos are downloaded
# should be called periodically from cronjob (cronjob may be added automatically during setup)

branch=$1
echo '$branch'

mkdir -p ~/active/{asdf,extract-scripts,mean-surface-rasters}

cd ~/active/asdf
if [ ! -d .git ]; then
    git init
    git pull https://github.com/itpir/asdf '${branch}'
else
    git checkout '${branch}'
    git pull origin '${branch}'
fi


old_hash=$(md5sum ~/active/load_repos.sh | awk '{ print $1 }')
new_hash=$(md5sum ~/active/asdf/src/tools/load_repos.sh | awk '{ print $1 }')


if [[ "$old_hash" != "$new_hash" ]]; then

    echo "Found new load_repos.sh ..."
    cp  ~/active/asdf/src/tools/load_repos.sh ~/active/load_repos.sh
    bash ~/active/load_repos.sh '${branch}'

else

    cd ~/active/extract-scripts
    if [ ! -d .git ]; then
        git init
        git pull https://github.com/itpir/extract-scripts '${branch}'
    else
        git checkout '${branch}'
        git pull origin '${branch}'
    fi


    cd ~/active/mean-surface-rasters
    if [ ! -d .git ]; then
        git init
        git pull https://github.com/itpir/mean-surface-rasters '${branch}'
    else
        git checkout '${branch}'
        git pull origin '${branch}' 
    fi

fi
