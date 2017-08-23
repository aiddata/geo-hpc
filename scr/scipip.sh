#!/bin/bash

# scipip
# pip wrapper to install package to aiddata10 central location
#
# some packages may have dependencies that require an HPC admin to
# install them globally on sciclone or may need require loading
# a hpc modules (e.g., numpy) - see .cshrc.rhel6-opteron config
# file for modules being loaded or use `module list` and `module avail`
# to see what modules you currently have loaded and available
#
# input arg is python package name, same as with normal pip usage
# if additional options are required, run manually
# (this is just a simple wrapper to save time)

pip install --install-option="--prefix=/sciclone/aiddata10/REU/py_libs" $1
