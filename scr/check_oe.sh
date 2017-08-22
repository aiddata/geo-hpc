#!/bin/bash

# node id (eg: vx18)
node=$1

# job id (eg: 1481006)
job=$2

# check std output/error file while job is running
# rsh "$node" cat /local/scr/TORQUE/spool/"$job".typhoon.sciclone.wm.edu.OU
rsh "$node" cat /var/local/torque/spool/"$job".*.sciclone.wm.edu.OU
