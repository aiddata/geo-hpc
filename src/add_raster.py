# ingest raster dataset to asdf
#   - create/initialize document
#   - validate options
#   - scan and validate dataset resources
#   - generate metadata for dataset resources
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
    data = json.load(open(path, 'r'))
else:
    quit("invalid input file")


required_core_fields = [
    "base", "type", "file_extension", "file_mask",
    "name", "title", "description", "version"
]

missing_core_fields = [i for i in required_core_fields
                       if i not in data]

if len(missing_core_fields) > 0:
    quit("Missing core fields ({0})".format(missing_core_fields))




update = False
if len(sys.argv) == 4 and sys.argv[3] == "update":
    print ("Warning: currently, updates will completely replace the existing "
           "dataset.")
    update = True


doc = {}
doc["asdf"] = {}
doc["asdf"]["script"] = script
doc["asdf"]["version"] = version
doc["asdf"]["generator"] = generator
doc["asdf"]["date_updated"] = str(datetime.date.today())
if not update:
    doc["asdf"]["date_added"] = str(datetime.date.today())


# validate base path
valid_base, base_exists = v.base(data["base"], update)

if not valid_base.isvalid:
    quit(valid_base.error)

doc["base"] = valid_base.value
base_exists = valid_base.data['exists']


# validate name
valid_name, name_exists = v.name(data["name"], update)

if not valid_name.isvalid:
    quit(valid_name.error)

doc["name"] = valid_name.value
name_exists = valid_name.data['exists']


if update and not base_exists and not name_exists:
    quit(("Update option specified but no dataset with base path "
          "({0}) or name ({1}) was found").format(doc["base"], doc["name"]))


elif update and base_exists and name_exists:

    base_id = str(valid_base.data['_id'])
    name_id = str(valid_name.data['_id'])

    if base_id != name_id:
        quit("Update option specified but identifying fields (base and name) "
             "belong to different existing datasets."
             "\n\tBase: {0}\n\tName: {1}".format(doc["base"], doc["name"]))




# validate type and set file_format
valid_type = v.data_type(data["type"])

if not valid_type.isvalid:
    quit(valid_type.error)

doc["type"] = valid_type.value
doc["file_format"] = valid_type.data["file_format"]


if doc["type"] != "raster":
    quit("Invalid type ({0}), must be raster.".format(doc["type"]))




# validate file extension (validation depends on file format)
valid_extension = v.file_extension(data["file_extension"], doc["file_format"])

if not valid_extension.isvalid:
    quit(valid_extension.error)

doc["file_extension"] = valid_extension.value



# validate title, description and version
doc["title"] = str(data["title"])
doc["description"] = str(data["description"])
doc["version"] = str(data["version"])


# -----------------------------------------------------------------------------
# validate options for raster

if not "options" in data:
    quit("Missing options lookup")


require_options = ["resolution", "extract_types", "factor",
                   "variable_description", "mini_name"]

missing_options = [i for i in required_options
                   if i not in data]

if len(missing_options) > 0:
    quit("Missing fields from options lookup ({0})".format(missing_options))


doc["options"] = {}

# resolution (in decimal degrees)
valid_resolution = v.factor(data["resolution"])

if not valid_resolution.isvalid:
    quit(valid_resolution.error)

doc["resolution"] = valid_resolution.value


# multiplication factor (if needed, defaults to 1 if blank)
valid_factor = v.factor(data["factor"])

if not valid_factor.isvalid:
    quit(valid_factor.error)

doc["factor"] = valid_factor.value

# ***
# if factor changes, any extracts adjust with old factor need to be removed
# ***


# extract_types (multiple, separate your input with commas)
valid_extract_types = v.extract_types(data["extract_types"])

if not valid_extract_types.isvalid:
    quit(valid_extract_types.error)

doc["extract_types"] = valid_extract_types.value


# Description of the variable (units, range, etc.)
doc["variable_description"] = str(data["variable_description"])


# mini name (4 valid chars and unique across datasets)
valid_mini_name = v.mini_name(data["mini_name"], update)

if not valid_mini_name.isvalid:
    quit(valid_mini_name.error)

doc["mini_name"] = valid_mini_name.value
mini_name_exists = valid_mini_name.data['exists']

if update and mini_name_exists:
    base_id = str(valid_base.data['_id'])
    mini_name_id = str(valid_mini_name.data['_id'])

    if base_id != mini_name_id:
        quit("Mini name ({0}) already used for another "
             "dataset".format(doc["mini_name"]))


# -----------------------------------------------------------------------------
# extras
if not "extras" in data:
    print("Although fields in extras are not required, it may contain "
          "commonly used field which should be added whenever possible "
          "(example: sources_web field)")
    doc["extras"] = {}

elif not isinstance(data["extras"], dict):
    quit("Invalid instance of extras ({0}) of type: {1}".format(
        data["extras"], type(data["extras"])))
else:
    doc["extras"] = data["extras"]


# -----------------------------------------------------------------------------
# resource scan and validation

# find all files with file_extension in path
for root, dirs, files in os.walk(doc["base"]):
    for file in files:
        file = os.path.join(root, file)
        file_check = file.endswith('.' + doc["file_extension"])
        if file_check == True:
            ru.file_list.append(file)


# -----------------------------------------------------------------------------
# temporal info

def validate_file_mask(vmask):
    """Validate a file mask"""

    # designates temporally invariant dataset
    if vmask == "None":
        return True, None

    # test file_mask for first file in file_list
    test_date_str = ru.run_file_mask(vmask, ru.file_list[0], doc["base"])
    valid_date = ru.validate_date(test_date_str)

    if valid_date[0] == False:
        return False, valid_date[1]

    return True, None


# file mask identifying temporal attributes in path/file names
valid_file_mask = validate_file_mask(data["file_mask"])

if valid_file_mask[0]:
    doc["file_mask"] = data["file_mask"]
else:
    quit(valid_file_mask[1])


if doc["file_mask"] == "None":

    # temporally invariant dataset
    ru.temporal["name"] = "Temporally Invariant"
    ru.temporal["format"] = "None"
    ru.temporal["type"] = "None"

elif len(ru.file_list) > 0:

    # name for temporal data format
    ru.temporal["name"] = "Date Range"
    ru.temporal["format"] = "%Y%m%d"
    ru.temporal["type"] = ru.get_date_range(ru.run_file_mask(
        doc["file_mask"], ru.file_list[0], doc["base"]))[2]

    # day range for each file (eg: MODIS 8 day composites)
    # if "day_range" in v.data:
        # "day_range", "File day range? (Must be integer)", v.day_range

else:
    quit("Warning: file mask given but no resources were found")
    # ru.temporal["name"] = "Unknown"
    # ru.temporal["format"] = "Unknown"
    # ru.temporal["type"] = "Unknown"


# -----------------------------------------------------------------------------
# spatial info

print "\nChecking spatial data ("+doc["file_format"]+")..."


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

doc["scale"] = scale


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

    # path relative to base
    resource_tmp["path"] = f[f.index(doc["base"]) + len(doc["base"]) + 1:]


    # file size
    resource_tmp["bytes"] = os.path.getsize(f)

    if doc["file_mask"] != "None":
        # temporal
        # get unique time range based on dir path / file names

        # get data from mask
        date_str = ru.run_file_mask(doc["file_mask"], resource_tmp["path"])

        validate_date_str = ru.validate_date(date_str)

        if not validate_date_str[0]:
            quit(validate_date_str[1])


        if "day_range" in doc:
            range_start, range_end, range_type = ru.get_date_range(
                date_str, doc["day_range"])
        else:
            range_start, range_end, range_type = ru.get_date_range(date_str)

        # name (unique among this dataset's resources, 
        # not same name as dataset name)
        resource_tmp["name"] = (doc["name"] +"_"+
                                date_str["year"] + 
                                date_str["month"] + 
                                date_str["day"])

    else:
        range_start = 10000101
        range_end = 99991231

        resource_tmp["name"] = doc["name"] + "_none"


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

doc["temporal"] = ru.temporal
doc["spatial"] = ru.spatial
doc["resources"] = ru.resources


# -----------------------------------------------------------------------------
# update mongo

print "\nFinal document..."
from pprint import pprint
pprint(doc)


# print "\nWriting document to mongo..."

# core_update_status = update_db.update_core(doc)

# tracker_update_status = update_db.update_trackers(doc,
#                                                   v.new_boundary,
#                                                   v.update_geometry,
#                                                   update_data_package)


# print "\nDone.\n"
