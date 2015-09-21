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

from log_validate import validate
from log_prompt import prompts
from log_resources import resource_utils
from log_mongo import update_mongo

# --------------------------------------------------

script = os.path.basename(sys.argv[0])
version = "0.2"
generator = "manual"

# validate class instance
v = validate()

# prompt class instance
p = prompts()

# update mongo class instance
update_db = update_mongo()


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

            # clean options?
            # 

    return dp


def update_datapackage_val(var_str, val, opt=0):
    if not opt:
        data_package[var_str] = val
    else:
        data_package["options"][var_str] = val


def check_update(var_str, opt=0):
    if interface and update_data_package:
        if not opt:
            update = p.user_prompt_bool("Update dataset "+var_str+"? (\"" + str(data_package[var_str]) + "\")")
            return update
        else:
            update = p.user_prompt_bool("Update dataset "+var_str+"? (\"" + str(data_package["options"][var_str]) + "\")")
            return update
    elif interface:
        return True
    
    return False


def generic_input(input_type, var_str, in_1, in_2, opt=0):

    # if no value is given for automated input, use default
    # default value will still have to pass validation later
    if not interface:
        if not opt and not var_str in v.data:
            v.data[var_str] = v.fields[var_str]["default"]
        elif opt and not var_str in v.data['options']:
            v.data["options"][var_str] = v.fields['options'][v.data['type']][var_str]["default"]

    # if interface and datapackage exists, check if user wants to update
    # defaults to false for new datasets or automated inputs
    user_update = check_update(var_str, opt)

    # set value to run validation on if user chooses not
    # to update and existing field
    update_val = None
    if update_data_package and not user_update:
        if not opt:
            update_val = v.data[var_str]
        else:
            update_val = v.data["options"][var_str]

    print ">"
    print update_val
    print update_data_package
    print user_update
    print ">"

    if input_type == "open":
        v.data[var_str] = p.user_prompt_open(in_1, in_2, (user_update, update_val))

    elif input_type == "loop":
        v.data[var_str] = []
        c = 1

        if user_update:
            while p.user_prompt_bool(in_1 +" #"+str(c)+"?"):
                tmp_loop_obj = {}
                for i in in_2.keys():
                    tmp_loop_obj[i] = p.user_prompt_open(in_1 +" "+str(c)+" "+str(i)+":", in_2[i], (1, None))

                v.data[var_str].append(tmp_loop_obj)
                c += 1
        else:
            for x in range(len(update_val)):
                tmp_loop_obj = {}
                for i in in_2.keys():
                    tmp_loop_obj[i] = p.user_prompt_open(in_1 +" "+str(c)+" "+str(i)+":", in_2[i], (0, update_val[x][i]))

                v.data[var_str].append(tmp_loop_obj)
                c += 1

    update_datapackage_val(var_str, v.data[var_str], opt)


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


dp_exists = False
interface = False
if generator == "manual":
    interface = True
    v.interface = True
    p.interface = True


# --------------------------------------------------
# prompts and independent inputs


# base path
# get base path
if interface:
    v.data["base"] = p.user_prompt_open("Absolute path to root directory of dataset? (eg: /sciclone/aiddata10/REU/data/path/to/dataset)", v.is_dir, (1, None))

    if "REU/data/boundaries" in v.data["base"] and not p.user_prompt_bool("Warning: boundary files will be modified/overwritten/deleted during process. Make sure you have a backup. Continue?"):
        quit("User request - boundary backup.")

    # check datapackage exists for path 
    dp_exists, tmp_data = v.datapackage_exists(v.data["base"])

    print "dpexists:"
    print dp_exists
    print "--"

    if dp_exists:
        v.data = tmp_data

elif not "base" in v.data or not os.path.isdir(v.data['base']):
    quit("Invalid or no base directory provided.")


if interface and dp_exists:

    # true: update protocol
    clean_data_package = p.user_prompt_bool("Remove outdated fields (if they exist) from existing datapackage?")

    data_package = init_datapackage(dp=v.data, update=1, clean=clean_data_package)
    update_data_package = True

else:
    # false: creation protocol
    data_package = init_datapackage()
    update_data_package = False


# init base for new datapackage and overwrite old base in case datapackage moved
data_package["base"] = v.data["base"]

# remove trailing slash from path
if data_package["base"].endswith("/"):
    data_package["base"] = data_package["base"][:-1]
    v.data["base"] = data_package["base"]


# dataset type
generic_input("open", "type", "Type of data in dataset? (" + ', '.join(v.types["data"])+ ")", v.data_type)


flist_core = [
    {
        "type": "open",
        "id": "name",
        "in_1": "Dataset name? (must be unique from existing datasets)", 
        "in_2": v.name
    },
    {   
        "type": "open",
        "id": "title",
        "in_1": "Dataset title?", 
        "in_2": v.string
    },
    {   
        "type": "open",
        "id": "version",
        "in_1": "Dataset version?", 
        "in_2": v.string
    },
    {   
        "type": "loop",
        "id": "sources",
        "in_1": "Add source", 
        "in_2": {"name": v.string, "web": v.string}
    },
    {   
        "type": "open",
        "id": "source_link",
        "in_1": "Generic link for dataset?", 
        "in_2": v.string
    },
    {   
        "type": "open",
        "id": "licenses",
        "in_1": "Id of license(s) for dataset? (" + ', '.join(v.types["licenses"]) + ") [separate your input with commas]",
        "in_2": v.license_types
    }
]

flist_additional = [
    {   
        "type": "open",
        "id": "citation",
        "in_1": "Dataset citation?", 
        "in_2": v.string
    },
    {   
        "type": "open",
        "id": "short",
        "in_1": "A short description of the dataset?", 
        "in_2": v.string
    }
]
    
# print v.data

if data_package["type"] in ['boundary', 'raster']:
    for f in flist_core:
        generic_input(f["type"], f["id"], f["in_1"], f["in_2"])

elif data_package["type"] == 'release':
    # get release datapackage
    release_package =  json.load(open(data_package["base"]+"/"+os.path.basename(data_package["base"])+'/datapackage.json', 'r'))

    # copy fields
    for f in flist_core:
        data_package[f["id"]] = release_package[f["id"]]


for f in flist_additional:
    generic_input(f["type"], f["id"], f["in_1"], f["in_2"])


# --------------------
# dependent inputs

# file format (raster or vector)
if data_package["type"] == "raster":
    data_package["file_format"] = "raster"

elif data_package["type"] == "boundary":
    data_package["file_format"] = "vector"

elif data_package["type"] == "release":
    data_package["file_format"] = "release"

else:
    quit("Invalid dataset type")


v.update_file_format(data_package["file_format"])


if data_package["file_format"] == "vector" or data_package["file_format"] == "raster":
    # file extension (validation depends on file format)
    generic_input("open", "file_extension", "Primary file extension of data in dataset? (" + ', '.join(v.types["file_extensions"][data_package["file_format"]])+ ")", v.file_extension)
else:
    data_package["file_extension"] = ""


# raster info
if data_package["type"] == "raster":

    # resolution
    generic_input("open", "resolution", "Dataset resolution? (in degrees)", v.factor, opt=True)

    # extract_types (multiple)
    generic_input("open", "extract_types", "Valid extract types for data in dataset? (" + ', '.join(v.types["extracts"]) + ") [separate your input with commas]", v.extract_types, opt=True)

    # factor
    generic_input("open", "factor", "Dataset multiplication factor? (if needed. defaults to 1 if blank)", v.factor, opt=True)

    # variable description
    generic_input("open", "variable_description", "Description of the variable used in this dataset (units, range, etc.)?", v.string, opt=True)

    # mini name
    generic_input("open", "mini_name", "Dataset mini name? (must be 4 characters and unique from existing datasets)", v.mini_name, opt=True)


# boundary info
elif data_package["type"] == "boundary":
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


# print data_package


# --------------------------------------------------
# resource scan and validation


# option to rerun file checks for manual script
if update_data_package and interface and not p.user_prompt_bool("Run resource checks?"):

    # update mongo
    update_status = update_db.update_core(data_package)

    # if mongo updates were successful:
    if update_status == 0:
        # create datapackage
        write_data_package()

    quit("User request - update completed but without resource run")


# resource utils class instance
ru = resource_utils()


if data_package["file_format"] in ['raster', 'vector']:

    # find all files with file_extension in path
    for root, dirs, files in os.walk(data_package["base"]):
        for file in files:

            file = os.path.join(root, file)

            file_check = ru.run_file_check(file, data_package["file_extension"])

            if file_check == True and not file.endswith('simplified.geojson'):
                ru.file_list.append(file)

# elif data_package["file_format"] == "release":
#     ru.file_list.append("datapackage.json")



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


if data_package["type"] == 'raster':
    # file mask identifying temporal attributes in path/file names
    generic_input("open", "file_mask", "File mask? Use Y for year, M for month, D for day (include full path relative to base) [use \"None\" for temporally invariant data]\nExample: YYYY/MM/xxxx.xxxxxxDD.xxxxx.xxx", validate_file_mask)
    # print data_package["file_mask"]
else:
    data_package['file_mask'] = ""


if data_package["file_format"] == 'release':

    # set temporal using release datapackage
    ru.temporal["name"] = "Date Range"
    ru.temporal["format"] = "%Y"
    ru.temporal["type"] = "year"
    print type(release_package)
    print release_package
    ru.temporal["start"] = release_package['temporal'][0]['start']
    ru.temporal["end"] = release_package['temporal'][0]['end']


elif data_package["file_mask"] == "None":

    # temporally invariant dataset
    ru.temporal["name"] = "Temporally Invariant"
    ru.temporal["format"] = "None"
    ru.temporal["type"] = "None"

elif len(ru.file_list) > 0:
    
    # name for temporal data format
    ru.temporal["name"] = "Date Range"
    ru.temporal["format"] = "%Y%m%d"
    ru.temporal["type"] = ru.get_date_range(ru.run_file_mask(data_package["file_mask"], ru.file_list[0], data_package["base"]))[2]

    # day range for each file (eg: MODIS 8 day composites) 
    use_day_range = False
    if interface:
        use_day_range = p.user_prompt_bool("Set a day range for each file (not used if data is yearly/monthly)?")

    if use_day_range or "day_range" in v.data:
        generic_input("open", "day_range", "File day range? (Must be integer)", v.day_range)

else:
    print("Warning: file mask given but no resources were found")
    ru.temporal["name"] = "Unknown"
    ru.temporal["format"] = "Unknown"
    ru.temporal["type"] = "Unknown"


# --------------------------------------------------
# spatial info

print "\nChecking spatial data ("+data_package["file_format"]+")..."

if data_package["file_format"] == "raster":

    # iterate over files to get bbox and do basic spatial validation (mainly make sure rasters are all same size)
    f_count = 0
    for f in ru.file_list:

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
                    quit("User request - rejected global bounding box.")

            data_package["scale"] = scale

            # display datset info to user
            print "Dataset bounding box: ", geo_ext

            # prompt to continue
            if interface and not p.user_prompt_bool("Continue with this bounding box?"):
                quit("User request - rejected bounding box.")

            f_count += 1

        # exit if basic geo does not match
        if base_geo != geo_ext:
            quit("Raster bounding box does not match")


elif data_package["file_format"] == 'vector':

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


elif data_package["file_format"] == 'release':

    # get extemt
    geo_ext = ru.release_envelope(data_package['base'] +"/"+ os.path.basename(data_package['base']) + "/data/locations")


else:
    quit("Invalid file format.")



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

if data_package["file_format"] in ['raster', 'vector']:

    for f in ru.file_list:
        print f

        # resources
        # individual resource info
        resource_tmp = {}

        # path relative to datapackage.json
        resource_tmp["path"] = f[f.index(data_package["base"]) + len(data_package["base"]) + 1:]

        # check for reliability geojson
        # should only be present for rasters generated using mean surface script
        if data_package["type"] == "raster":
            resource_tmp["reliability"] = False
            reliability_file = data_package["base"] +"/"+ resource_tmp["path"][:-len(data_package["file_extension"])] + "geojson"
            if os.path.isfile(reliability_file):
                resource_tmp["reliability"] = True


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

            resource_tmp["name"] = data_package["name"]


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


elif data_package["file_format"] == "release":

    resource_tmp = {
        "name":data_package['name'],
        "bytes":0,
        "path":data_package['name'],
        "start":ru.temporal['start'],
        "end":ru.temporal['end']
    }

    resource_order = ["name", "path", "bytes", "start", "end"]
    resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)
    ru.resources.append(resource_tmp)


# --------------------------------------------------
# add temporal, spatial and resources info

data_package["temporal"] = [ru.temporal]
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


# if release dataset, create mongodb for dataset
if data_package['file_format'] == 'release':
    ru.release_to_mongo(data_package['name'], data_package['base'] +"/"+ os.path.basename(data_package['base']))

# call/do ckan stuff eventually
# 

print "\nDone.\n"
