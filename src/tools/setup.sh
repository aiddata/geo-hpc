#!/bin/bash

# used to initialize portions of asdf

# manages setup of both production and development branch files
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

echo -e "\n"


tmp=$(mktemp -d)

cd "$tmp"

if [[ $server == "hpc" ]]; then
    git clone -b "$branch" https://github.com/itpir/asdf
else
    git clone -b "$branch" http://github.com/itpir/asdf
fi

bash "$tmp"/asdf/src/tools/setup.sh "$server" "$branch"

rm -r "$tmp"

