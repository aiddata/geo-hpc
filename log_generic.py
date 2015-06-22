# add dataset to system 
#   - validate options
#   - scan and validate dataset resources
#   - generate metadata for dataset resources
#   - create datapackage
#   - update mongo database

import sys
import os
# import re
# import copy

import datetime
import calendar
from dateutil.relativedelta import relativedelta

from collections import OrderedDict
import json

# import pymongo
# from osgeo import gdal,ogr,osr

from log_validate import validate
from log_prompt import prompts
from log_resources import resource_utils
# from log_mongo import update_mongo


# --------------------------------------------------


script = os.path.basename(sys.argv[0])
version = "0.1"
generator = "manual"

# validate class instance
v = validate()

# prompt class instance
p = prompts()


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
    json.dump(data_package, open(data_package["base"] + "/datapackage.json", 'w'), indent=4)


def init_datapackage(dp=0, init=0, update=0, clean=0, fields=0):

    dp = 0 if type(dp) != type(OrderedDict()) else dp

    init = 0 if init != 1 else init
    update = 0 if update != 1 else update
    clean = 0 if clean != 1 else clean
    fields = 0 if type(fields) != type(OrderedDict) else fields

    # init - rebuild from scratch
    # update - update core fields and make sure all current fields exist
    # clean - run update but also remove any outdated fields


    if not dp or init:
        init = 1
        update = 1

    if clean:
        update = 1

    if init:
        dp = OrderedDict()
        dp["date_added"] = str(datetime.date.today())

    if update:
        dp["date_updated"] = str(datetime.date.today())
        dp["datapackage_script"] = script
        dp["datapackage_version"] = version
        dp["datapackage_generator"] = generator
        dp["maintainers"] =  [
            {
                "web": "http://aiddata.org", 
                "name": "AidData", 
                "email": "info@aiddata.org"
            }
        ]
        dp["publishers"] = [
            {
                "web": "http://aiddata.org", 
                "name": "AidData", 
                "email": "info@aiddata.org"
            }
        ]

        # make sure all current datapackage fields exist
        # iterate over keys in fields list
        for k in v.fields:
            # if current key does not exist: add empty
            if k not in dp.keys():
                dp[k] = v.fields[k]["default"]

        # clean fields in an existing datapackages
        if clean:
            # iterate over keys in dp
            for k in dp.keys():
                # if clean: delete key if outdated
                if k not in v.fields:
                    del dp[k]

    return dp


def generic_input(input_type, update, var_str, in_1, in_2):

    if interface:
        if update:
            user_update = p.user_prompt_bool("Update dataset "+var_str+"? (\"" + str(data_package[var_str]) + "\")")

        if not update or update and user_update:
            
            if input_type == "open":
                v.data[var_str] = p.user_prompt_open(in_1, in_2)
                data_package[var_str] = v.data[var_str]

            elif input_type == "loop":
                v.data[var_str] = p.user_prompt_loop(in_1, in_2)
                data_package[var_str] = v.data[var_str]

        # elif update and not user_update:
            # validate anyway - in case validation function changed
            # force to enter new input if needed
            # 

            # data_package[var_str] = v.data[var_str]


    else:
        if input_type == "open" and in_2:

            if not var_str in v.data:
                v.data[var_str] = ""

            check_result = in_2(v.data[var_str])
            
            if type(check_result) != type(True) and len(check_result) == 3:
                valid, answer, error = check_result
            else:
                valid = check_result
                answer = v.data[var_str]
                error = None

            if error != None:
                error = " ("+error+")"
            else:
                error = ""

            if not valid:
                quit("Bad automated input " + error)

            data_package[var_str] = answer

        else:
            data_package[var_str] = v.data[var_str]


# --------------------------------------------------
# user inputs


if len(sys.argv) > 1 and sys.argv[1] == "auto":

    generator = "auto"


    try:

        path = sys.argv[2]

        if os.path.isfile(path):
            v.data = json.load(open(path, 'r'), object_pairs_hook=OrderedDict)

    except:
        # move mcr output to error folder
        # 

        quit("Bad inputs.")


interface = False
if generator == "manual":
    interface = True
    v.interface = True


# --------------------------------------------------
# prompts



# base path
# get base path
if interface:
    v.data["base"] = p.user_prompt_open("Absolute path to root directory of dataset? (eg: /mnt/sciclone-aiddata/REU/data/path/to/dataset)", v.is_dir)

elif not "base" in v.data:
    quit("No datapackage path given.")


# check datapackage.json exists at path 
if interface and os.path.isfile(v.data["base"]+"/datapackage.json"):
    # true: update protocol
    clean_data_package = p.user_prompt_bool("Remove outdated fields (if they exist) from existing datapackage?")

    data_package = json.load(open(v.data["base"]+"/datapackage.json", 'r'), object_pairs_hook=OrderedDict)
    data_package = init_datapackage(dp=data_package, update=1, clean=clean_data_package)
    update_data_package = True

    # quit("Datapackage already exists.")

else:
    # false: creation protocol
    data_package = init_datapackage()
    data_package["base"] = v.data["base"]
    update_data_package = False

# print data_package


# --------------------
# independent inputs

flist = [
    {   
        "id": "name",
        "type": "open",
        "in_1": "Dataset name? (must be unique from existing datasets)", 
        "in_2": v.name
    },
    {   
        "id": "title",
        "type": "open",
        "in_1": "Dataset title?", 
        "in_2": v.string
    },
    {   
        "id": "version",
        "type": "open",
        "in_1": "Dataset version?", 
        "in_2": v.string
    },
    {   
        "id": "sources",
        "type": "loop",
        "in_1": {"name":"","web":""}, 
        "in_2": ("Enter source ", "Add another source?")
    },
    {   
        "id": "source_link",
        "type": "open",
        "in_1": "Generic link for dataset?", 
        "in_2": v.string
    },
    {   
        "id": "licenses",
        "type": "open",
        "in_1": "Id of license(s) for dataset? (" + ', '.join(v.types["licenses"]) + ") [separate your input with commas]",
        "in_2": v.license_types
    },
    {   
        "id": "citation",
        "type": "open",
        "in_1": "Dataset citation?", 
        "in_2": v.string
    },
    {   
        "id": "short",
        "type": "open",
        "in_1": "A short description of the dataset?", 
        "in_2": v.string
    },
    {   
        "id": "variable_description",
        "type": "open",
        "in_1": "Description of the variable used in this dataset (units, range, etc.)?", 
        "in_2": v.string
    },
    {   
        "id": "type",
        "type": "open",
        "in_1": "Type of data in dataset? (" + ', '.join(v.types["data"])+ ")", 
        "in_2": v.data_type
    }
]

for f in flist:
    generic_input(f["type"], update_data_package, f["id"], f["in_1"], f["in_2"])


# --------------------
# dependent inputs

# file format (raster or vector)
if data_package["type"] == "raster":
    data_package["file_format"] = data_package["type"]
else:
    data_package["file_format"] = "vector"


v.update_file_format(data_package["file_format"])

# file extension (validation depends on file format)
generic_input("open", update_data_package, "file_extension", "Primary file extension of data in dataset? (" + ', '.join(v.types["file_extensions"][data_package["file_format"]])+ ")", v.file_extension)

# raster info
if data_package["type"] == "raster":
    # extract_types (multiple)
    generic_input("open", update_data_package, "extract_types", "Valid extract types for data in dataset? (" + ', '.join(v.types["extracts"]) + ") [separate your input with commas]", v.extract_types)

    # factor
    generic_input("open", update_data_package, "factor", "Dataset multiplication factor? (if needed. defaults to 1 if blank)", v.factor)


# --------------------
# option to review inputs

if interface and p.user_prompt_bool("Would you like to review your inputs?"):
    for x in data_package.keys():
        print x + " : \n\t" + str(data_package[x])

    # option to quit or continue
    if not p.user_prompt_bool("Continue with these inputs?"):
        quit("User request.")



# print data_package


# --------------------------------------------------
# resource scan and validation


# option to rerun file checks for manual script
if update_data_package and interface and not p.user_prompt_bool("Run resource checks?"):
    write_data_package()
    quit("User request.")


# resource utils class instance
ru = resource_utils()


# find all files with file_extension in path
for root, dirs, files in os.walk(data_package["base"]):
    for file in files:

        file = os.path.join(root, file)

        file_check = ru.run_file_check(file, data_package["file_extension"])

        if file_check == True:
            ru.file_list.append(file)


# iterate over files to get bbox and do basic spatial validation (mainly make sure rasters are all same size)
f_count = 0
for f in ru.file_list:

    if data_package["file_format"] == "raster":

        # get basic geo info from each file
        geo_ext = ru.raster_envelope(f)

        # get full geo info from first file
        if f_count == 0:
            base_geo = geo_ext

            # check bbox size
            xsize = geo_ext[2][0] - geo_ext[1][0]
            ysize = geo_ext[0][1] - geo_ext[1][1]
            tsize = abs(xsize * ysize)

            scale = "regional"
            if tsize >= 32400:
                scale = "global"
                # prompt to continue
                if interface and not p.user_prompt_bool("This dataset has a bounding box larger than a hemisphere and will be treated as a global dataset. If this is not a global (or near global) dataset you may want to clip it into multiple smaller datasets. Do you want to continue?"):
                    quit("User request.")

            data_package["scale"] = scale

            # display datset info to user
            print "Dataset bounding box: ", geo_ext

            # prompt to continue
            if interface and not p.user_prompt_bool("Continue with this bounding box?"):
                quit("User request.")

            f_count += 1


        # exit if basic geo does not match
        if base_geo != geo_ext:
            quit("Geography does not match")


    # vector datasets should always be just a single file
    elif data_package["file_format"] == 'vector' and f_count == 0:
        if data_package["type"] == "boundary":
            geo_ext = ru.vector_envelope(f)

        else:
            # run something similar to ru.vector_envelope
            # instead of polygons in one file we are checking polygons in files in list
            # create new ru.vector_list function which calls ru.vector_envelope
            # geo_ext = ru.vector_list(ru.file_list)
            quit("Only accepting boundary vectors at this time.")

    else:
        quit("File format error.")





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


# --------------------------------------------------
# temporal data and resource meta information


# *** WARNING ***
# this section used to determine temporal 
# data and gather information on individual 
# files is designed custom for each dataset


def run_file_mask(fmask, fname, fbase=0):

    if fbase and fname.startswith(fbase):
        fname = fname[fname.index(fbase) + len(fbase) + 1:]

    output = {
        "year": "".join([x for x,y in zip(fname, fmask) if y == 'Y' and x.isdigit()]),
        "month": "".join([x for x,y in zip(fname, fmask) if y == 'M' and x.isdigit()]),
        "day": "".join([x for x,y in zip(fname, fmask) if y == 'D' and x.isdigit()])
    }

    return output


def validate_date(date_obj):
    # year is always required
    if date_obj["year"] == "":
        return False, "No year found for data."

    # full 4 digit year required
    elif len(date_obj["year"]) != 4:
        return False, "Invalid year."

    # months must always use 2 digits 
    elif date_obj["month"] != "" and len(date_obj["month"]) != 2:
        return False, "Invalid month."

    # days of month (day when month is given) must always use 2 digits
    elif date_obj["month"] != "" and date_obj["day"] != "" and len(date_obj["day"]) != 2:
        return False, "Invalid day of month."

    # days of year (day when month is not given) must always use 3 digits
    elif date_obj["month"] == "" and date_obj["day"] != "" and len(date_obj["day"]) != 3:
        return False, "Invalid day of year."

    return True, None


# validate file_mas
def validate_file_mask(vmask):

    # test file_mask for first file in file_list
    test_date_str = run_file_mask(vmask, ru.file_list[0], data_package["base"])
    valid_date = validate_date(test_date_str) 
    if valid_date[0] == False:
        return False, None, valid_date[1]

    return True, vmask, None


def get_date_range(date_obj, drange=0):
    # year, day of year (7)
    if date_obj["month"] == "" and len(date_obj["day"]) == 3:  
        tmp_start = datetime.datetime(int(date_obj["year"]),1,1) + datetime.timedelta(int(date_obj["day"])-1)
        tmp_end = tmp_start + relativedelta(days=drange)

    # year, month, day (8)
    if date_obj["month"] != "" and len(date_obj["day"]) == 2:   
        tmp_start = datetime.datetime(int(date_obj["year"]), int(date_obj["month"]), int(date_obj["day"]))
        tmp_end = tmp_start + relativedelta(days=drange)

    # year, month (6)
    if date_obj["month"] != "" and date_obj["day"] == "":   
        tmp_start = datetime.datetime(int(date_obj["year"]), int(date_obj["month"]), 1)
        month_range = calendar.monthrange(int(date_obj["year"]), int(date_obj["month"]))[1]
        tmp_end = datetime.datetime(int(date_obj["year"]), int(date_obj["month"]), month_range)

    # year (4)
    if date_obj["month"] == "" and date_obj["day"] == "":   
        tmp_start = datetime.datetime(int(date_obj["year"]), 1, 1)
        tmp_end = datetime.datetime(int(date_obj["year"]), 12, 31)

    return int(datetime.datetime.strftime(tmp_start, '%Y%m%d')), int(datetime.datetime.strftime(tmp_end, '%Y%m%d'))


# name for temporal data format
ru.temporal["name"] = "Date Range"
ru.temporal["format"] = "%Y%m%d"

# file mask identifying temporal attributes in path/file names
generic_input("open", update_data_package, "file_mask", "File mask? Use Y for year, M for month, D for day (include full path relative to base)\nExample: YYYY/MM/xxxx.xxxxxxDD.xxxxx.xxx", validate_file_mask)
print data_package["file_mask"]

# day range for each file (eg: MODIS 8 day composites) 
use_day_range = False
if interface:
    use_day_range = user_prompt_bool("Set a day range for each file (not used if data is yearly/monthly)?")

if use_day_range or "day_range" in v.data:
    generic_input("open", update_data_package, "day_range", "File day range? (Must be integer)", v.day_range)


for f in ru.file_list:
    print f

    # resources
    # individual resource info
    resource_tmp = {}

    # path relative to datapackage.json
    resource_tmp["path"] = f[f.index(data_package["base"]) + len(data_package["base"]) + 1:]

    # file size
    resource_tmp["bytes"] = os.path.getsize(f)


    # temporal
    # get unique time range based on dir path / file names

    # get data from mask
    date_str = run_file_mask(v.data["file_mask"], resource_tmp["path"])

    validate_date_str = validate_date(date_str)

    if not validate_date_str[0]:
        quit(validate_date_str[1])


    if "day_range" in data_package:
        range_start, range_end = get_date_range(date_str, data_package["day_range"])

    else: 
        range_start, range_end = get_date_range(date_str)


    # name (unique among this dataset's resources - not same name as dataset)
    resource_tmp["name"] = data_package["name"] +"_"+ date_str["year"] + date_str["month"] +date_str["day"]

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




data_package["temporal"] = ru.temporal
data_package["spatial"] = ru.spatial
data_package["resources"] = ru.resources



print "\n\n\n"
print data_package
write_data_package()



# --------------------------------------------------
# database update(s) and datapackage output


# build dictionary for mongodb insert
mdata = {
    
    # resource spatial
    "loc": ru.spatial,

    # datapackage
    "short": data_package["short"],
    "type": data_package["type"],
    "file_format": data_package["file_format"],

    # exists but need to add
    "scale": data_package["scale"],

    # path to parent datapackage dir
    "datapackage_path": data_package["base"],
    # parent datapackage name (ie: group field)
    "datapackage_name": data_package["name"],

    # generation info
    "datapackage_script": data_package["datapackage_script"],
    "datapackage_version": data_package["datapackage_version"],
    "datapackage_generator": data_package["datapackage_generator"],

    # any other version info or other stuff we might need?
    # 
    
    # file specific info from resource_tmp
    "name": resource_tmp["name"],
    "path": resource_tmp["path"],
    "bytes": resource_tmp["bytes"],
    "start": resource_tmp["start"],
    "end": resource_tmp["end"]

}

# update mongo
# 
# from log_mongo import update_mongo
# update mongo class instance
# update_db = update_mongo()


# create datapackage
# 

