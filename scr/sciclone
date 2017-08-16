#!/bin/bash

# script to mount / unmount sciclone/aiddata10

# requires 'sshfs' package be installed
# sudo apt-get install sshfs

action=$1
user=$2

src_addr="vortex.sciclone.wm.edu"
src_dir="/sciclone/aiddata10"
mnt_dir="/sciclone/aiddata10"

if [[ $action != "mount" && $action != "unmount" ]]; then
    echo "  Must specify whether you want to mount or unmount SciClone."
    exit 1
fi

if [[  $action == "mount" && $user == "" ]]; then
    echo "  Must provide your SciClone account name."
    exit 1
fi


if [[ $action == "mount" ]]; then

    if [[ ! -d "$mnt_dir" ]]; then
        sudo mkdir -p "$mnt_dir"
    fi

    echo "Removing old mount if it exists..."
    sudo umount "$mnt_dir"
    sudo chown $USER: /sciclone/aiddata10

    echo "Mounting..."
    sudo sshfs -o allow_other "$user"@"$src_addr":"$src_dir" "$mnt_dir"

elif [[ $action == "unmount" ]]; then

    echo "Removing mount..."
    sudo umount "$mnt_dir"

fi
