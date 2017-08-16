'''

!!!
!!! WARNING THIS SCRIPT IS OUTDATED AND NOT FUNCTIONAL
!!!

'''


# add dataset to system
#   - validate options
#   - scan and validate dataset resources
#   - generate metadata for dataset resources
#   - create datapackage
#   - update mongo database

import sys
import os
import datetime
import json
from collections import OrderedDict

from ingest_validation import ValidationTools
from ingest_resources import ResourceTools
from prompt_utility import PromptKit
from mongo_utility import MongoUpdate

# --------------------------------------------------


# validate class instance
v = ValidationTools()

# prompt class instance
p = PromptKit()

# update mongo class instance
update_db = MongoUpdate()


# --------------------------------------------------
# functions


def quit(reason):

    # do error log stuff
    #

    # output error logs somewhere
    #

    # if auto, move job file to error location
    #

    sys.exit("Terminating script - "+str(reason)+"\n")


def write_data_package():
    dp_out = data_package
    # print "dp out"
    # print dp_out.keys()

    if "_id" in dp_out.keys():
        del dp_out['_id']

    json.dump(dp_out, open(data_package["base"] + "/datapackage.json", 'w'), indent=4)


def update_datapackage_val(var_str, val, opt=0):
    if not opt:
        data_package[var_str] = val
    else:
        data_package["options"][var_str] = val






script = os.path.basename(sys.argv[0])
version = "0.4.1"
generator = "manual"

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



path = sys.argv[2]

if os.path.isfile(path):
    v.data = json.load(open(path, 'r'), object_pairs_hook=OrderedDict)


# check datapackage exists for path
dp_exists, tmp_data = v.datapackage_exists(v.data["base"])

if not "base" in v.data or not os.path.isdir(v.data['base']):
    quit("Invalid or no base directory provided.")





# init base for new datapackage and overwrite old base in case datapackage moved
data_package["base"] = v.data["base"]

# remove trailing slash from path
if data_package["base"].endswith("/"):
    data_package["base"] = data_package["base"][:-1]
    v.data["base"] = data_package["base"]





# "type"
# ', '.join(v.types["data"])
# v.data_type


# "id": "name",
# "in_2": v.name

# "id": "title",
# "in_2": v.string

# "id": "version",
# "in_2": v.string

# "id": "description",
# "in_2": v.string



for f in flist_core:
    generic_input(f["type"], f["id"], f["in_1"], f["in_2"])



# --------------------
# dependent inputs

# file format (raster or vector)

if data_package["type"] == "boundary":
    data_package["file_format"] = "vector"

else:
    quit("Invalid dataset type")


v.update_file_format(data_package["file_format"])


# file extension (validation depends on file format)
generic_input("open", "file_extension", "Primary file extension of data in dataset? (" + ', '.join(v.types["file_extensions"][data_package["file_format"]])+ ")", v.file_extension)



# boundary info
if data_package["type"] == "boundary":
    # boundary group
    generic_input("open", "group", "Boundary group? (eg. country name for adm boundaries) [leave blank if boundary has no group]", v.group, opt=True)

    # run check on group to prep for group_class selection
    v.run_group_check(data_package['options']['group'])


    # boundary class
    # only a single actual may exist for a group
    if v.is_actual:
        data_package["options"]["group_class"] = "actual"

    elif v.actual_exists[data_package["options"]["group"]]:
        # force sub if actual exists
        data_package["options"]["group_class"] = "sub"

    else:
        generic_input("open", "group_class", "Group class? (" + ', '.join(v.types["group_class"]) + ")", v.group_class, opt=True)

        if data_package["options"]["group_class"] == "actual" and (not v.group_exists or not v.actual_exists[data_package["options"]["group"]]):
            v.new_boundary = True


# --------------------
# option to review inputs

if interface and p.user_prompt_bool("Would you like to review your inputs?"):
    for x in data_package.keys():
        print x + " : \n\t" + str(data_package[x])

    # option to quit or continue
    if not p.user_prompt_bool("Continue with these inputs?"):
        quit("User request - rejected inputs.")


# --------------------------------------------------
# resource scan and validation


# resource utils class instance
ru = ResourceTools()

# find all files with file_extension in path
for root, dirs, files in os.walk(data_package["base"]):
    for file in files:

        file = os.path.join(root, file)

        file_check = ru.run_file_check(file, data_package["file_extension"])

        if file_check == True and not file.endswith('simplified.geojson'):
            ru.file_list.append(file)


# --------------------------------------------------
# temporal info

# validate file_mas
def validate_file_mask(vmask):

    # designates temporally invariant dataset
    if vmask == "None":
        return True, vmask, None

    # test file_mask for first file in file_list
    test_date_str = ru.run_file_mask(vmask, ru.file_list[0], data_package["base"])
    valid_date = ru.validate_date(test_date_str)
    if valid_date[0] == False:
        return False, None, valid_date[1]

    return True, vmask, None




data_package["file_mask"] = "None"


# temporally invariant dataset
ru.temporal["name"] = "Temporally Invariant"
ru.temporal["format"] = "None"
ru.temporal["type"] = "None"



# --------------------------------------------------
# spatial info

print "\nChecking spatial data ("+data_package["file_format"]+")..."


# iterate over files to get bbox and do basic spatial validation (mainly make sure rasters are all same size)
f_count = 0
for f in ru.file_list:

    # boundary datasets can be multiple files (for administrative zones)
    if data_package["type"] == "boundary" and f_count == 0:
        print f
        geo_ext = ru.vector_envelope(f)

        convert_status = ru.add_ad_id(f)
        if convert_status == 1:
             quit("Error adding ad_id to boundary file and outputting geojson.")


        if data_package["file_extension"] == "shp":

            # update file list
            ru.file_list[0] = os.path.splitext(ru.file_list[0])[0] + ".geojson"

            # update extension
            data_package["file_extension"] = "geojson"

            # remove shapefile
            for z in os.listdir(os.path.dirname(ru.file_list[0])):
                if os.path.isfile(data_package["base"] +"/"+ z) and not z.endswith(".geojson") and not z.endswith("datapackage.json"):
                    print "deleting " + data_package["base"] +"/"+ z
                    os.remove(data_package["base"] +"/"+ z)


        f_count += 1


    elif data_package["type"] == "boundary" and f_count > 0:
        quit("Boundaries must be submitted individually.")

    else:
        # - run something similar to ru.vector_envelope
        # - instead of polygons in adm files (or some "other" boundary file(s)) we are
        #   checking polygons in files in list
        # - create new ru.vector_list function which calls ru.vector_envelope
        #
        #  geo_ext = ru.vector_list(ru.file_list)
        quit("Only accepting boundary vectors at this time.")




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
    # prompt to continue
    if interface and not p.user_prompt_bool("This dataset has a bounding box larger than a hemisphere and will be treated as a global dataset. If this is not a global (or near global) dataset you may want to turn it into multiple smaller datasets. Do you want to continue?"):
        quit("User request - rejected global bounding box.")

data_package["scale"] = scale

# prompt to continue
if interface and not p.user_prompt_bool("Continue with this bounding box?"):
    quit("User request - rejected bounding box.")



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


# if updating an existing boundary who is the actual for a group
# warn users when the new geometry does not match the existing geometry
# continuing will force the boundary tracker database to be dumped
# all datasets that were in the tracker database will need to be reindexed
if update_data_package and data_package["type"] == "boundary" and data_package["options"]["group_class"] == "actual" and ru.spatial != data_package["spatial"]:
    v.update_geometry = True
    if interface and not p.user_prompt_bool("The geometry of your boundary does not match the existing geometry, do you wish to continue? (Warning: This will force a dump of the existing tracker database and all datasets in it will need to be reindexed)"):
        quit("User request - boundary geometry change.")

elif update_data_package and data_package["type"] != "boundary" and ru.spatial != data_package["spatial"]:
    v.update_geometry = True
    if interface and not p.user_prompt_bool("The geometry of your dataset does not match the existing geometry, do you wish to continue? (Warning: This dataset will need to be reindexed in all trackers)"):
        quit("User request - dataset geometry change.")


# --------------------------------------------------
# resource info

print '\nProcessing resources...'


for f in ru.file_list:
    print f

    # resources
    # individual resource info
    resource_tmp = {}

    # path relative to datapackage.json
    resource_tmp["path"] = f[f.index(data_package["base"]) + len(data_package["base"]) + 1:]


    # file size
    resource_tmp["bytes"] = os.path.getsize(f)

    if data_package["file_mask"] != "None":
        # temporal
        # get unique time range based on dir path / file names

        # get data from mask
        date_str = ru.run_file_mask(data_package["file_mask"], resource_tmp["path"])

        validate_date_str = ru.validate_date(date_str)

        if not validate_date_str[0]:
            quit(validate_date_str[1])


        if "day_range" in data_package:
            range_start, range_end, range_type = ru.get_date_range(date_str, data_package["day_range"])
        else:
            range_start, range_end, range_type = ru.get_date_range(date_str)

        # name (unique among this dataset's resources - not same name as dataset)
        resource_tmp["name"] = data_package["name"] +"_"+ date_str["year"] + date_str["month"] + date_str["day"]

    else:
        range_start = 10000101
        range_end = 99991231

        resource_tmp["name"] = data_package["name"] + "_none"


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


# --------------------------------------------------
# add temporal, spatial and resources info

data_package["temporal"] = ru.temporal
data_package["spatial"] = ru.spatial
data_package["resources"] = ru.resources


# --------------------------------------------------
# database update(s) and datapackage output

print "\nFinal datapackage..."
print data_package


# update mongo
print "\nWriting datapackage to system..."

core_update_status = update_db.update_core(data_package)

tracker_update_status = update_db.update_trackers(data_package, v.new_boundary, v.update_geometry, update_data_package)

# if mongo updates were successful:
if core_update_status == 0:
    # create datapackage
    write_data_package()


print "\nDone.\n"
