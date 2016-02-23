#!/bin/bash

# makes sure the latest versions of repos are downloaded
# should be called periodically from cronjob (cronjob may be added automatically during setup)

branch=$1
echo Loading branch: "$branch"

mkdir -p ~/active/{asdf,extract-scripts,mean-surface-rasters}


load_repo() {
    cd ~/active
    if [ ! -d "${active_repo}" ] || [ ! -d "${active_repo}"/.git ]; then
        rm -rf "${active_repo}"
        git clone https://github.com/itpir/"${active_repo}"
    fi

    cd asdf
    git checkout "${branch}"
    git pull origin "${branch}"

}

# load asdf
# cd ~/active
# if [ ! -d asdf ] || [ ! -d asdf/.git ]; then
#     git clone https://github.com/itpir/asdf
# fi

# cd asdf
# git checkout '${branch}'
# git pull origin '${branch}'
active_repo='asdf'
load_repo


old_hash=$(md5sum ~/active/load_repos.sh | awk '{ print $1 }')
new_hash=$(md5sum ~/active/asdf/src/tools/load_repos.sh | awk '{ print $1 }')


if [[ "$old_hash" != "$new_hash" ]]; then

    echo "Found new load_repos.sh ..."
    cp  ~/active/asdf/src/tools/load_repos.sh ~/active/load_repos.sh
    bash ~/active/load_repos.sh "${branch}"

else

    # load extract-scripts
    # cd ~/active
    # if [ ! -d extract-scripts ] || [ ! -d extract-scripts/.git ]; then
    #     rm -rf extract-scripts
    #     git clone https://github.com/itpir/extract-scripts
    # fi

    # cd extract-scripts
    # git checkout '${branch}'
    # git pull origin '${branch}'
    active_repo='extract-scripts'
    load_repo

    # load mean-surface-rasters
    # cd ~/active
    # if [ ! -d mean-surface-rasters ] || [ ! -d mean-surface-rasters/.git ]; then
    #     rm -rf extract-scripts
    #     git clone https://github.com/itpir/extract-scripts
    # fi
  
    # cd mean-surface-rasters
    # git checkout '${branch}'
    # git pull origin '${branch}' 
    active_repo='mean-surface-raster'
    load_repo

fi
