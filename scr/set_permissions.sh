#!/bin/bash

if [ -z "$1" ]; then
    path="."
    echo "Setting permissions for "$(pwd)
else
    path=$1
    echo "Setting permissions for "$path
fi

echo "Confirm: [y/n]"
while true; do
    read REPLY
    case $REPLY in
        "y")   break ;;
        "n")   exit 0 ;;
        *)     echo "Invalid input. Please use [y/n]:"; continue ;;
    esac
done

find "${path}" -type d -exec chmod u=rwx,g=rwxs,o=rx {} +
find "${path}" -type f -exec chmod u=rw,g=rw,o=r {} +
