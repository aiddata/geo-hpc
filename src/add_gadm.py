# add dataset to system
#   - generate metadata for dataset resources
#   - create datapackage
#   - update mongo database

# -----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
sys.path.insert(0, config_dir)

from config_utility import *

config = BranchConfig(branch=branch)

# -------------------------------------

# check mongodb connection
if config.connection_status != 0:
    sys.exit("connection status error: " + str(config.connection_error))

# -----------------------------------------------------------------------------


import datetime
import json
import pymongo
import fiona

from resource_utility import ResourceTools


# -----------------------------------------------------------------------------

parent = os.path.dirname(os.path.abspath(__file__))
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

    sys.exit("add_gadm.py: Terminating script - "+str(reason)+"\n")


# init data package
dp = {}

# get release base path
if len(sys.argv) > 2:

    path = sys.argv[2]

    if os.path.isdir(path):
        dp['base'] = path
    else:
        quit("Invalid base directory provided.")

else:
    quit("No base directory provided")


# add version input here
if len(sys.argv) > 3:

    gadm_version = sys.argv[3]

    try:
        gadm_version = float(gadm_version)
    except:
        quit("Invalid GADM version provided.")

else:
    quit("No GADM version provided")


# optional arg
# mainly for user to specify manual run
if len(sys.argv) > 4:
    if sys.argv[4] in ['auto', 'manual']:
        generator = sys.argv[4]
    else:
        quit("Invalid additional inputs")


# exit if too many args
if len(sys.argv) > 5:
        quit("Invalid inputs arguments count")


# remove trailing slash from path
if dp["base"].endswith("/"):
    dp["base"] = dp["base"][:-1]


dp["asdf"] = {}
dp["asdf"]["date_added"] = str(datetime.date.today())
dp["asdf"]["date_updated"] = str(datetime.date.today())
dp["asdf"]["script"] = script
dp["asdf"]["version"] = version
dp["asdf"]["generator"] = generator

dp["type"] = "boundary"
dp["file_format"] = "vector"
dp["file_extension"] = "geojson"
dp["file_mask"] = "None"

# -------------------------------------


gadm_name = os.path.basename(dp["base"])

gadm_iso3 = gadm_name[:3]
gadm_adm = gadm_name[4:]

gadm_lookup_path = parent + '/gadm_iso3.json'
gadm_lookup =  json.load(open(gadm_lookup_path, 'r'))

gadm_country = gadm_lookup[gadm_iso3]

dp["name"] = (gadm_iso3.lower() + "_" + gadm_adm.lower() + "_gadm" +
             str(gadm_version).replace('.', ''))

dp["title"] = (gadm_country + " " + gadm_adm.upper() +
              " Boundary - GADM " + str(gadm_version))

dp["description"] = "PLACEHOLDER"

dp["version"] = gadm_version


dp["options"] = {}
dp["options"]["group"] = (gadm_iso3.lower() + "_gadm" +
                         str(gadm_version).replace('.', ''))

dp["extras"] = {}

dp["extras"]["citation"] = "Global Administrative Areas (GADM) http://www.gadm.org."
dp["extras"]["sources_web"] = "http://www.gadm.org"
dp["extras"]["sources_name"] = "Global Administrative Areas (GADM)"

dp["extras"]["gadm_country"] = gadm_country
dp["extras"]["gadm_iso3"] = gadm_iso3
dp["extras"]["gadm_adm"] = int(gadm_adm[-1:])
dp["extras"]["gadm_name"] = "PLACEHOLDER"

try:
    dp["options"]["group_title"] = "{0} GADM {1}".format(gadm_country,
                                                         gadm_version)
except Exception as e:
    print gadm_country
    print gadm_version
    raise Exception(e)

    
# v = ValidationTools()

# probably do not need this
# run check on group to prep for group_class selection
# v.run_group_check(dp['options']['group'])


# boundary group
if "adm0" in gadm_name.lower():
     dp["options"]["group_class"] = "actual"
else:
     dp["options"]["group_class"] = "sub"


dp["active"] = 0


# -----------------------------------------------------------------------------

# resource utils class instance
ru = ResourceTools()


# find all files with file_extension in path
for root, dirs, files in os.walk(dp["base"]):
    for fname in files:

        fname = os.path.join(root, fname)

        file_check = ru.run_file_check(fname, dp["file_extension"])

        if file_check == True and not fname.endswith('simplified.geojson'):
            ru.file_list.append(fname)


if len(ru.file_list) == 0:
    quit("No vector file found in " + dp["base"])

elif len(ru.file_list) > 1:
    quit("Boundaries must be submitted individually.")


# -------------------------------------
print "\nProcessing temporal..."

# temporally invariant dataset
ru.temporal["name"] = "Temporally Invariant"
ru.temporal["format"] = "None"
ru.temporal["type"] = "None"
ru.temporal["start"] = 10000101
ru.temporal["end"] = 99991231


# -------------------------------------
print "\nProcessing spatial..."


# iterate over files to get bbox and do basic spatial validation (mainly make
# sure rasters are all same size)
f = ru.file_list[0]

# boundary datasets can be multiple files (for administrative zones)
print f
geo_ext = ru.vector_envelope(f)

convert_status = ru.add_ad_id(f)
if convert_status == 1:
     quit("Error adding ad_id to boundary file & outputting geojson.")



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
# get generic spatial data for rasters
# something else for vectors?
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

print f

# resources
# individual resource info
resource_tmp = {}

# path relative to datapackage.json
resource_tmp["path"] = f[f.index(dp["base"]) + len(dp["base"]) + 1:]


# get adm unit name for country and add to gadm info and description
tmp_feature = fiona.open(
    os.path.join(dp["base"], resource_tmp["path"]), 'r').next()

if gadm_adm.lower() == "adm0":
    dp["extras"]["gadm_name"] = "Country"
else:
    dp["extras"]["gadm_name"] = tmp_feature['properties']['ENGTYPE_'+ gadm_adm[-1:]]

dp["description"] = "GADM Boundary File for {0} ({1}) in {2}.".format(
    gadm_adm.upper(), dp["extras"]["gadm_name"], gadm_country)

# file size
resource_tmp["bytes"] = os.path.getsize(f)

resource_tmp["name"] = dp["name"]

# file date range
resource_tmp["start"] = 10000101
resource_tmp["end"] = 99991231

# reorder resource fields
# resource_order = ["name", "path", "bytes", "start", "end"]
# resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)

# update main list
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

# json_out = '/home/userz/Desktop/summary.json'
# json_handle = open(json_out, 'w')
# json.dump(dp, json_handle, sort_keys=False, indent=4,
#           ensure_ascii=False)

# quit("!!!")

# update mongo
print "\nWriting datapackage to system..."

# connect to database and asdf collection
client = pymongo.MongoClient(config.server)
db_asdf = client.asdf
db_tracker = client.trackers


gadm_col_str = "data"
# gadm_col_str = "gadm" + str(gadm_version).replace('.', '')

# prep collection if needed
if not gadm_col_str in db_asdf.collection_names():
    c_data = db_asdf[gadm_col_str]

    c_data.create_index("base", unique=True)
    c_data.create_index("name", unique=True)
    c_data.create_index([("spatial", pymongo.GEOSPHERE)])

else:
    c_data = db_asdf[gadm_col_str]


# update core
# try:
c_data.replace_one({"base": dp["base"]}, dp, upsert=True)
print "successful core update"
# except:
#      quit("Error updating core.")


# create/initialize tracker
# try:

if dp["options"]["group_class"] == "actual":

    # drop boundary tracker if exists
    if dp["options"]["group"] in db_tracker.collection_names():
        db_tracker.drop_collection(dp["options"]["group"])

    # create new boundary tracker collection
    c_bnd = db_tracker[dp["options"]["group"]]
    c_bnd.create_index("name", unique=True)
    # c_bnd.create_index("base", unique=True)
    c_bnd.create_index([("spatial", pymongo.GEOSPHERE)])

    # # add each non-boundary dataset item to new boundary collection with "unprocessed" flag
    # dsets = c_data.find({"type": {"$ne": "boundary"}})
    # for full_dset in dsets:
    #     dset = {
    #         'name': full_dset["name"],
    #         'spatial': full_dset["spatial"],
    #         'scale': full_dset["scale"],
    #         'status': -1
    #     }
    #     c_bnd.insert(dset)

    print "successful tracker creation"


# except:
#      quit("Error updating tracker.")



print "\nadd_gadm.py: Done.\n"

