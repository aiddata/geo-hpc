#!/bin/bash

# used to get and run geo-hpc builder
# NOTE: this script must be manually replaced/updated if changes to setup.sh are made

# user may pass  "--overwrite" as option which will be passed on to builder.sh

# get server/branch inputs from user

# echo -e "\n"
# while true; do
#     echo "Input server [hpc / web]:"
#     read REPLY
#     case $REPLY in
#         "hpc")  server="$REPLY"; break ;;
#         "web")  server="$REPLY"; break ;;
#         *)      echo "Invalid input."; continue ;;
#     esac
# done

while true; do
    echo -e "\n"
    echo "Input development branch [master / develop]:"
    read REPLY
    case $REPLY in
        "master")   branch="$REPLY"; break ;;
        "develop")  branch="$REPLY"; break ;;
        *)          echo "Invalid input."; continue ;;
    esac
done


# create temp dir, clone geo-hpc, run builder, then clean up
tmp=$(mktemp -d)

# git clone -b "$branch" https://github.com/itpir/geo-hpc "$tmp"/geo-hpc

wget -O "$tmp"/"$branch".zip https://github.com/itpir/geo-hpc/archive/"$branch".zip
unzip "$tmp"/"$branch".zip -d "$tmp"
mv "$tmp"/geo-hpc-"$branch" "$tmp"/geo-hpc

bash "$tmp"/geo-hpc/tools/builder.sh "$branch" $1
rm -rf "$tmp"
