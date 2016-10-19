#!/bin/bash

# used to get and run asdf builder
# NOTE: this script must be manually replaced/updated if changes to setup.sh are made


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

echo -e "\n"
while true; do
    echo "Input development branch [master / develop]:"
    read REPLY
    case $REPLY in
        "master")   branch="$REPLY"; break ;;
        "develop")  branch="$REPLY"; break ;;
        *)          echo "Invalid input."; continue ;;
    esac 
done  

# create temp dir, clone asdf, run builder, then clean up
tmp=$(mktemp -d)
git clone -b "$branch" https://github.com/itpir/asdf "$tmp"/asdf
bash "$tmp"/asdf/src/tools/builder.sh "$branch"
rm -rf "$tmp"
