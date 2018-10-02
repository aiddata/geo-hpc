#!/bin/bash

branch=$1

find "${HOME}" -type d -exec chmod u=rwx,g=rxs,o= {} +
find "${HOME}" -type f -exec chmod u=rw,g=,o= {} +

find "${HOME}/privatemodules" -type d -exec chmod u=rwx,g=rxs,o=rx {} +
find "${HOME}/privatemodules" -type f -exec chmod u=rw,g=r,o=r {} +

find "${HOME}/usr" -type d -exec chmod u=rwx,g=rxs,o=rx {} +
find "${HOME}/usr" -type f -exec chmod u=rw,g=r,o=r {} +

find "/sciclone/aiddata10/geo/${branch}" -type d ! -perm u=rwx,g=rxs,o=rx -exec chmod u=rwx,g=rxs,o=rx {} +
find "/sciclone/aiddata10/geo/${branch}" -type f ! -perm u=rw,g=r,o=r -exec chmod u=rw,g=r,o=r {} +
