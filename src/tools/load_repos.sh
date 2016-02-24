#!/bin/bash

# makes sure the latest versions of repos are downloaded
# should be called periodically from cronjob (cronjob may be added automatically during setup)

server=$1
branch=$2

echo -e "\n"
echo Building on server: "$server"
echo Loading branch: "$branch"


# mkdir -p ~/active/{asdf,extract-scripts,mean-surface-rasters}


# load_repo() {
#     cd ~/active
#     rm -rf "$active_repo"
#     git clone -b "$branch" http://github.com/itpir/"$active_repo" 
# }

# load_repo() {
#     cd ~/active
#     if [ ! -d "$active_repo" ] || [ ! -d "$active_repo"/.git ]; then
#         rm -rf "$active_repo"
#         mkdir -p "$active_repo"
#         cd "$active_repo"
#         git clone http://github.com/itpir/"$active_repo" 
#         # git pull git@github.com:itpir/"${active_repo}".git "${branch}"

#     fi

#     cd ~/active/"$active_repo"
#     git checkout "${branch}"
#     git pull "${branch}"

# }

# load asdf
# cd ~/active
# if [ ! -d asdf ] || [ ! -d asdf/.git ]; then
#     git clone https://github.com/itpir/asdf
# fi

# cd asdf
# git checkout '${branch}'
# git pull origin '${branch}'

# active_repo='asdf'
# load_repo


src="${HOME}"/active/"$branch"

cd "$src"
rm -rf asdf

# git clone -b "$branch" http://github.com/itpir/asdf


if [[ $server == "hpc" ]]; then
    git clone -b "$branch" https://github.com/itpir/asdf
    # git pull https://github.com/itpir/asdf develop
    # git pull git@github.com:itpir/asdf.git develop
else
    git clone -b "$branch" http://github.com/itpir/asdf
    # git pull https://github.com/itpir/asdf master
    # git pull git@github.com:itpir/asdf.git master
fi





old_hash=$(md5sum "$src"/load_repos.sh | awk '{ print $1 }')
new_hash=$(md5sum "$src"/asdf/src/tools/load_repos.sh | awk '{ print $1 }')


if [[ "$old_hash" != "$new_hash" ]]; then

    echo "Found new load_repos.sh ..."
    cp  "$src"/asdf/src/tools/load_repos.sh "$src"/load_repos.sh
    bash "$src"/load_repos.sh "$branch"

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

    # active_repo='extract-scripts'
    # load_repo


    cd "$src"
    rm -rf extract-scripts
    # git clone -b "$branch" http://github.com/itpir/extract-scripts 


    if [[ $server == "hpc" ]]; then
        git clone -b "$branch" https://github.com/itpir/extract-scripts
    else
        git clone -b "$branch" http://github.com/itpir/extract-scripts
    fi




    # load mean-surface-rasters
    # cd ~/active
    # if [ ! -d mean-surface-rasters ] || [ ! -d mean-surface-rasters/.git ]; then
    #     rm -rf extract-scripts
    #     git clone https://github.com/itpir/extract-scripts
    # fi
  
    # cd mean-surface-rasters
    # git checkout '${branch}'
    # git pull origin '${branch}' 

    # active_repo='mean-surface-raster'
    # load_repo


    cd "$src"
    rm -rf mean-surface-rasters
    # git clone -b "$branch" http://github.com/itpir/mean-surface-rasters

    if [[ $server == "hpc" ]]; then
        git clone -b "$branch" https://github.com/itpir/mean-surface-rasters
    else
        git clone -b "$branch" http://github.com/itpir/mean-surface-rasters
    fi

fi
