# add dataset to system
#   - generate metadata for dataset resources
#   - create datapackage
#   - update mongo database


import sys
import os
import datetime
import json
# from collections import OrderedDict

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

    sys.exit("Terminating script - "+str(reason)+"\n")


# update mongo class instance
update_db = MongoUpdate()

# init data package
# dp = OrderedDict()
dp = {}

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

dp["type"] = "boundary"
dp["file_format"] = "vector"
dp["file_extension"] = "shp"
dp["file_mask"] = "None"

# -------------------------------------

# v = ValidationTools()

gadm_default = {
  "name": "npl_adm4_gadm28",
  "title": "GADM_NAME GADM_ADM Boundary - GADM 2.8",
  "description": "GADM Boundary File for GADM_ADM in GADM_NAME.",
  "version": "2.8",
  "citation": "Global Administrative Areas (GADM) http://www.gadm.org.",
  "sources_web": "http://www.gadm.org",
  "sources_name": "Global Administrative Areas (GADM)"
}


gadm_name = os.path.basename(dp["base"])

gadm_iso3 = gadm_name[:3]
gadm_adm = gadm_name[4:]

gadm_lookup_path = 'gadm_iso3.json'
gadm_lookup =  json.load(open(gadm_lookup_path, 'r'))

gadm_country = gadm_lookup[gadm_iso3]


for i in gadm_default:
    tmp_field = gadm_default[i]
    tmp_field = gadm_default[i].replace("GADM_NAME", gadm_country)
    tmp_field = gadm_default[i].replace("GADM_ADM", gadm_adm.upper())
    dp[i] = tmp_field


dp["options"]["group"] = gadm_name.replace(" ", "_").lower()


# probably do not need this
# run check on group to prep for group_class selection
# v.run_group_check(dp['options']['group'])


# boundary group
dp["options"] = {}
if "adm0" in gadm_name:
     dp["options"]["group_class"] = "actual"
else:
     dp["options"]["group_class"] = "sub"



# -----------------------------------------------------------------------------

# resource utils class instance
ru = ResourceTools()


# find all files with file_extension in path
for root, dirs, files in os.walk(dp["base"]):
    for file in files:

        file = os.path.join(root, file)

        file_check = ru.run_file_check(file, dp["file_extension"])

        if file_check == True and not file.endswith('simplified.geojson'):
            ru.file_list.append(file)


# -------------------------------------
print "\nProcessing temporal..."

# temporally invariant dataset
ru.temporal["name"] = "Temporally Invariant"
ru.temporal["format"] = "None"
ru.temporal["type"] = "None"


# -------------------------------------
print "\nProcessing spatial..."


# iterate over files to get bbox and do basic spatial validation (mainly make sure rasters are all same size)
f_count = 0
for f in ru.file_list:

    # boundary datasets can be multiple files (for administrative zones)
    if f_count == 0:
        print f
        geo_ext = ru.vector_envelope(f)

        convert_status = ru.add_ad_id(f)
        if convert_status == 1:
             quit("Error adding ad_id to boundary file and outputting geojson.")


        if dp["file_extension"] == "shp":

            # update file list
            ru.file_list[0] = os.path.splitext(ru.file_list[0])[0] + ".geojson"

            # update extension
            dp["file_extension"] = "geojson"

            # remove shapefile
            for z in os.listdir(os.path.dirname(ru.file_list[0])):
                if os.path.isfile(dp["base"] +"/"+ z) and not z.endswith(".geojson") and not z.endswith("datapackage.json"):
                    print "deleting " + dp["base"] +"/"+ z
                    os.remove(dp["base"] +"/"+ z)


        f_count += 1


    elif f_count > 0:
        quit("Boundaries must be submitted individually.")





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

for f in ru.file_list:
    print f

    # resources
    # individual resource info
    resource_tmp = {}

    # path relative to datapackage.json
    resource_tmp["path"] = f[f.index(dp["base"]) + len(dp["base"]) + 1:]

    resource_tmp["reliability"] = False

    # file size
    resource_tmp["bytes"] = os.path.getsize(f)

    range_start = 10000101
    range_end = 99991231

    resource_tmp["name"] = dp["name"]

    # file date range
    resource_tmp["start"] = range_start
    resource_tmp["end"] = range_end

    # reorder resource fields
    # resource_order = ["name", "path", "bytes", "start", "end", "reliability"]
    # resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)

    # update main list
    ru.resources.append(resource_tmp)

    # update dataset temporal info
    if not ru.temporal["start"] or range_start < ru.temporal["start"]:
        ru.temporal["start"] = range_start
    elif not ru.temporal["end"] or range_end > ru.temporal["end"]:
        ru.temporal["end"] = range_end


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
print "\nWriting datapackage to system..."

core_update_status = update_db.update_core(dp)

# tracker_update_status = update_db.update_trackers(dp, v.new_boundary, v.update_geometry, update_data_package)
tracker_update_status = update_db.update_trackers(dp)

print "\nDone.\n"
