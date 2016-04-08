# add dataset to system
#   - generate metadata for dataset resources
#   - create datapackage
#   - update mongo database


import sys
import os
import datetime
import json
from collections import OrderedDict

from resource_utility import ResourceTools
from mongo_utility import MongoUpdate


# -----------------------------------------------------------------------------

script = os.path.basename(sys.argv[0])
version = "0.4"
generator = "auto"

# -----------------------------------------------------------------------------


def quit(reason):

    # do error log stuff
    #

    # output error logs somewhere
    #

    # if auto, move job file to error location
    #

    sys.exit("add_release.py: terminating script - " + str(reason) + "\n")


# update mongo class instance
update_db = MongoUpdate()

# init data package
dp = OrderedDict()

# get release base path
if len(sys.argv) > 1:

    path = sys.argv[1]

    if os.path.isdir(path):
        dp['base'] = path
    else:
        quit("Invalid base directory provided.")

else:
    quit("no base directory provided")


# optional arg
# mainly for user to specify manual run
if len(sys.argv) > 2:
    if sys.argv[2] in ['auto', 'manual']:
        generator = sys.argv[2]
    else:
        quit("Invalid additional inputs")


# exit if too many args
if len(sys.argv) > 3:
        quit("Invalid inputs arguments count")


# remove trailing slash from path
if dp["base"].endswith("/"):
    dp["base"] = dp["base"][:-1]


dp["asdf_date_added"] = str(datetime.date.today())
dp["asdf_date_updated"] = str(datetime.date.today())
dp["asdf_script"] = script
dp["asdf_version"] = version
dp["asdf_generator"] = generator

dp["type"] = "release"
dp["file_format"] = "release"
dp["file_extension"] = ""
dp["file_mask"] = "None"

# get release datapackage
release_path = dp["base"] + '/datapackage.json'
release_package =  json.load(open(release_path, 'r'))

for f in release_package.keys():

    if f not in ['resources', 'extras']:
        rkey = f.replace (" ", "_").lower()
        dp[f] = release_package[f]

    elif f == 'extras':
        for g in release_package['extras']:
            rkey = g['key'].replace (" ", "_").lower()
            dp[g['key']] = g['value']


# -----------------------------------------------------------------------------

# resource utils class instance
ru = ResourceTools()

print "\nProcessing temporal..."

# set temporal using release datapackage
ru.temporal["name"] = dp['Temporal Name']
ru.temporal["format"] = "%Y"
ru.temporal["type"] = "year"
ru.temporal["start"] = dp['Temporal Start']
ru.temporal["end"] = dp['Temporal End']


# -------------------------------------
print "\nProcessing spatial..."

# get extemt
geo_ext = ru.release_envelope(dp['base'] + "/data/locations")

# clip extents if they are outside global bounding box
for c in range(len(geo_ext)):

    if geo_ext[c][0] < -180:
        geo_ext[c][0] = -180
    elif geo_ext[c][0] > 180:
        geo_ext[c][0] = 180

    if geo_ext[c][1] < -90:
        geo_ext[c][1] = -90
    elif geo_ext[c][1] > 90:
        geo_ext[c][1] = 90


# display datset info to user
print "Dataset bounding box: ", geo_ext

# check bbox size
xsize = geo_ext[2][0] - geo_ext[1][0]
ysize = geo_ext[0][1] - geo_ext[1][1]
tsize = abs(xsize * ysize)

scale = "regional"
if tsize >= 32400:
    scale = "global"

dp["scale"] = scale

# spatial
ru.spatial = {
    "type": "Polygon",
    "coordinates": [ [
        geo_ext[0],
        geo_ext[1],
        geo_ext[2],
        geo_ext[3],
        geo_ext[0]
    ] ]
}


# -------------------------------------
print '\nProcessing resources...'

resource_tmp = {
    "name": dp['name'],
    "bytes": 0,
    "path": "",
    "start": ru.temporal['start'],
    "end": ru.temporal['end'],
    "reliability": False
}

resource_order = ["name", "path", "bytes", "start", "end", "reliability"]
resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)
ru.resources.append(resource_tmp)


# -------------------------------------
# add temporal, spatial and resources info

dp["temporal"] = ru.temporal
dp["spatial"] = ru.spatial
dp["resources"] = ru.resources


# -----------------------------------------------------------------------------
# database update(s) and datapackage output

print "\nFinal datapackage..."
print dp

quit("!!!")

# update mongo
print "\nWriting datapackage to mongo..."

core_update_status = update_db.update_core(dp)

tracker_update_status = update_db.update_trackers(dp)

# create mongodb for dataset
ru.release_to_mongo(dp['name'], dp['base'])

print "\nDone.\n"

