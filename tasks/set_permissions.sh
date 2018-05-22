#!/bin/bash

branch=$1

find "${HOME}" -type d -exec chmod u=rwx,g=rxs,o=rx {} +
find "${HOME}" -type f -exec chmod u=rw,g=r,o=r {} +

find "/sciclone/aiddata10/geo/${branch}" -type d -exec chmod u=rwx,g=rxs,o=rx {} +
find "/sciclone/aiddata10/geo/${branch}" -type f -exec chmod u=rw,g=r,o=r {} +
