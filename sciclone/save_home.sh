#!/bin/bash

pip freeze > ${HOME}/pip_list.txt

base_path=/sciclone/aiddata10/geo/home_backups


if [[ ! -d ${base_path} ]]; then
    mkdir -p ${base_path}
    chmod 775 ${base_path}
fi

timestamp=$(date +%Y%m%d%H%M%S)

output_path=${base_path}/${USER}_${timestamp}_home_copy.tar.gz

tar czvf ${output_path} check_oe.sh  save_home.sh  privatemodules  pip_list.txt .cshrc .cshrc.rhel6-opteron

chmod 644 ${output_path}

rm ${HOME}/pip_list.txt


# to extract:
#     tar -xvf /sciclone/aiddata10/geo/home_backups/<USER>_<TIMESTAMP>_home_copy.tar.gz

# to install pip list:
#     pip install --user -r pip_list.txt
