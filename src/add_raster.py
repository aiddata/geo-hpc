# add dataset to system
#   - validate options
#   - scan and validate dataset resources
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
from collections import OrderedDict
import pymongo

from validation_utility import ValidationTools
from resource_utility import ResourceTools
from mongo_utility import MongoUpdate


# -----------------------------------------------------------------------------

script = os.path.basename(sys.argv[0])
version = "0.4.1"
generator = "manual"

# -----------------------------------------------------------------------------

# connect to database and asdf collection
client = pymongo.MongoClient(config.server)
c_asdf = client.asdf.data
db_tracker = client.trackers


# validate class instance
v = ValidationTools(client)

# update mongo class instance
update_db = MongoUpdate(client)


# resource utils class instance
ru = ResourceTools()





# -----------------------------------------------------------------------------


def quit(reason):

    # do error log stuff
    #

    # output error logs somewhere
    #

    # if auto, move job file to error location
    #

    sys.exit("Terminating script - "+str(reason)+"\n")



path = sys.argv[2]

if os.path.isfile(path):
    input_data = json.load(open(path, 'r'), object_pairs_hook=OrderedDict)
else:
    quit("invalid input file")


dp = {}
dp["asdf"] = {}
dp["asdf"]["date_added"] = str(datetime.date.today())
dp["asdf"]["date_updated"] = str(datetime.date.today())
dp["asdf"]["script"] = script
dp["asdf"]["version"] = version
dp["asdf"]["generator"] = generator


required_core_fields = [
    "base", "type", "file_format", "file_extension", "file_mask",
    "name", "title", "description", "version"
]

missing_core_fields = [i for i in required_core_fields
                       if i not in input_data]

if len(missing_core_fields) > 0:
    quit("Missing core fields ({0})".format(missing_core_fields))


# validate base path
dp["base"] = input_data["base"]

if not os.path.isdir(dp['base']):
    quit("Invalid or no base directory provided.")

# remove trailing slash from path to prevent multiple unique
# path strings to same dir
if dp["base"].endswith("/"):
    dp["base"] = dp["base"][:-1]


base_exists = c_asdf.find_one({'base': dp["base"]}) is not None

if base_exists:
    quit("Data entry with base exists")


# validate name
dp["name"] = input_data["name"]
valid_name = v.name(dp["name"])
if not valid_name[0]:
    quit(valid_name[2] + " ({0})".format(dp["name"]))



# validate type and set file_format
dp["type"] = input_data["type"]

if dp["type"] != "raster":
    quit("Invalid type ({0}), must be raster".format(dp["type"]))

dp["file_format"] = "raster"



# validate file extension (validation depends on file format)
dp["file_extension"] = input_data["file_extension"]

if not v.file_extension(dp["file_extension"])[0]:
    quit("Invalid file extension ({0}). Valid extensions: {1}".format(
         dp["file_extension"],
         v.types["file_extensions"][dp["file_format"]]))





# validate title, description and version
dp["title"] = str(input_data["title"])
dp["description"] = str(input_data["description"])
dp["version"] = str(input_data["version"])



# validate options for raster

if not "options" in input_data:
    quit("Missing options lookup")


require_options = ["resolution", "extract_types", "factor", "variable_description", "mini_name"]

missing_options = [i for i in required_options
                   if i not in input_data]

if len(missing_options) > 0:
    quit("Missing fields from options lookup ({0})".format(missing_options))


dp["options"] = {}

# resolution (in decimal degrees)
dp["resolution"] = input_data["resolution"]
if not v.factor():
    quit()

# extract_types (multiple, separate your input with commas)
dp["extract_types"] = input_data["extract_types"]
"Valid extract types (" + ', '.join(v.types["extracts"]) + ") [separate your input with commas]"
v.extract_types

# multiplication factor (if needed, defaults to 1 if blank)
dp["factor"] = input_data["factor"]
v.factor

# Description of the variable (units, range, etc.)
dp["variable_description"] = input_data["variable_description"]
v.string

# mini name
dp["mini_name"] = input_data["mini_name"]
"must be 4 characters and unique"
v.mini_name



# extras
if not "extras" in input_data
    print("Although fields in extras are not required, it may contain commonly "
          "used field which should be added whenever possible "
          "(example: sources_web field)")
    dp["extras"] = {}

elif not isinstance(input_data["extras"], dict):
    quit("Invalid instance of extras ({0}) of type: {1}".format(
        input_data["extras"], type(input_data["extras"])))
else:
    dp["extras"] = input_data["extras"]


# -----------------------------------------------------------------------------
# resource scan and validation

# find all files with file_extension in path
for root, dirs, files in os.walk(dp["base"]):
    for file in files:
        file = os.path.join(root, file)
        file_check = ru.run_file_check(file, dp["file_extension"])


# -----------------------------------------------------------------------------
# temporal info

# validate file_mas
def validate_file_mask(vmask):

    # designates temporally invariant dataset
    if vmask == "None":
        return True, vmask, None

    # test file_mask for first file in file_list
    test_date_str = ru.run_file_mask(vmask, ru.file_list[0], dp["base"])
    valid_date = ru.validate_date(test_date_str)
    if valid_date[0] == False:
        return False, None, valid_date[1]

    return True, vmask, None



# file mask identifying temporal attributes in path/file names

# "file_mask"
# validate_file_mask



if dp["file_mask"] == "None":

    # temporally invariant dataset
    ru.temporal["name"] = "Temporally Invariant"
    ru.temporal["format"] = "None"
    ru.temporal["type"] = "None"

elif len(ru.file_list) > 0:

    # name for temporal data format
    ru.temporal["name"] = "Date Range"
    ru.temporal["format"] = "%Y%m%d"
    ru.temporal["type"] = ru.get_date_range(ru.run_file_mask(
        dp["file_mask"], ru.file_list[0], dp["base"]))[2]

    # day range for each file (eg: MODIS 8 day composites)
    if "day_range" in v.data:
        generic_input("open", "day_range", "File day range? (Must be integer)", v.day_range)

else:
    print("Warning: file mask given but no resources were found")
    ru.temporal["name"] = "Unknown"
    ru.temporal["format"] = "Unknown"
    ru.temporal["type"] = "Unknown"


# -----------------------------------------------------------------------------
# spatial info

print "\nChecking spatial data ("+dp["file_format"]+")..."


# iterate over files to get bbox and do basic spatial validation
# (mainly make sure rasters are all same size)
f_count = 0
for f in ru.file_list:

    # get basic geo info from each file
    geo_ext = ru.raster_envelope(f)
    # get full geo info from first file
    if f_count == 0:
        base_geo = geo_ext

        f_count += 1

    # exit if basic geo does not match
    if base_geo != geo_ext:
        quit("Raster bounding box does not match")



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
    print ("This dataset has a bounding box larger than a hemisphere "
           "and will be treated as a global dataset. If this is not a "
           "global (or near global) dataset you may want to turn it into "
           "multiple smaller datasets and ingest them individually.")

dp["scale"] = scale


# set spatial
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


# -----------------------------------------------------------------------------
# resource info

print '\nProcessing resources...'


for f in ru.file_list:
    print f

    # resources
    # individual resource info
    resource_tmp = {}

    # path relative to datapackage.json
    resource_tmp["path"] = f[f.index(dp["base"]) + len(dp["base"]) + 1:]


    # file size
    resource_tmp["bytes"] = os.path.getsize(f)

    if dp["file_mask"] != "None":
        # temporal
        # get unique time range based on dir path / file names

        # get data from mask
        date_str = ru.run_file_mask(dp["file_mask"], resource_tmp["path"])

        validate_date_str = ru.validate_date(date_str)

        if not validate_date_str[0]:
            quit(validate_date_str[1])


        if "day_range" in dp:
            range_start, range_end, range_type = ru.get_date_range(date_str, dp["day_range"])
        else:
            range_start, range_end, range_type = ru.get_date_range(date_str)

        # name (unique among this dataset's resources - not same name as dataset)
        resource_tmp["name"] = (dp["name"] +"_"+
                                date_str["year"] + date_str["month"] + date_str["day"])

    else:
        range_start = 10000101
        range_end = 99991231

        resource_tmp["name"] = dp["name"] + "_none"


    # file date range
    resource_tmp["start"] = range_start
    resource_tmp["end"] = range_end

    # reorder resource fields
    resource_order = ["name", "path", "bytes", "start", "end"]
    resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)

    # update main list
    ru.resources.append(resource_tmp)


    # update dataset temporal info
    if not ru.temporal["start"] or range_start < ru.temporal["start"]:
        ru.temporal["start"] = range_start
    elif not ru.temporal["end"] or range_end > ru.temporal["end"]:
        ru.temporal["end"] = range_end


# -----------------------------------------------------------------------------
# add temporal, spatial and resources info

dp["temporal"] = ru.temporal
dp["spatial"] = ru.spatial
dp["resources"] = ru.resources


# -----------------------------------------------------------------------------
# database update(s) and datapackage output

print "\nFinal datapackage..."
print dp


# update mongo
print "\nWriting datapackage to system..."

core_update_status = update_db.update_core(dp)

tracker_update_status = update_db.update_trackers(dp,
                                                  v.new_boundary,
                                                  v.update_geometry,
                                                  update_data_package)


print "\nDone.\n"
