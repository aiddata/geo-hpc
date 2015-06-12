# add dataset to geo mongodb

import sys
import os
import re
import copy
from datetime import datetime,date
from collections import OrderedDict
import json
import pymongo
from osgeo import gdal,ogr,osr

from log_validate import validate
from log_prompt import prompts

# --------------------------------------------------

script = os.path.basename(sys.argv[0])
version = "0.1"
generator = "manual"

# validate class instance
v = validate()

# prompt class instance
p = prompts()

# --------------------------------------------------
# user inputs

if len(sys.argv) > 1 and sys.argv[1] == "auto":

    generator = "auto"


    try:

        path = sys.argv[2]

        if os.path.isfile(path):
            v.data = json.load(open(path), object_pairs_hook=OrderedDict)


    except:
        sys.stdout.write("Bad inputs.\n")



interface = False
if generator == "manual":
    interface = True


# --------------------------------------------------
# functions


def init_datapackage(dp=0, init=0, update=0, clean=0, fields=0):

    dp = 0 if type(dp) != type(OrderedDict) else dp

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
        dp["date_added"] = str(date.today())

    if update:
        dp["date_updated"] = str(date.today())
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

        # iterate over keys in fields list
        for k in v.fields:
            # if current key does not exist: add empty
            if k not in dp.keys():
                dp[k] = v.fields[k]

        if clean:
            # iterate over keys in dp
            for k in dp.keys():
                # if clean: delete key if outdated
                if k not in v.fields:
                    del dp[k]

    return dp


# --------------------------------------------------
# prompts


# base path
# get base path
if interface:
    v.data["base"] = p.user_prompt_open("Absolute path to root directory of dataset? (eg: /mnt/sciclone-aiddata/REU/data/path/to/dataset)", v.is_dir, v.error["is_dir"])

# check datapackage.json exists at path 
if os.path.isfile(v.data["base"]+"/datapackage.json"):
    # true: update protocol
    clean_data_package = p.user_prompt_bool("Remove outdated fields (if they exist) from existing datapackage?")

    data_package = json.load(open(v.data["base"]+"/datapackage.json"), object_pairs_hook=OrderedDict)
    data_package = init_datapackage(dp=data_package, update=1, clean=clean_data_package)
    update_data_package = True

    quit("Datapackage already exists.")

else:
    # false: creation protocol
    data_package = init_datapackage()
    data_package["base"] = v.data["base"]
    update_data_package = False



def generic_input(input_type, update, var_str, in_1, in_2, in_3):

    if interface:
        if update:
            user_update = p.user_prompt_bool("Update dataset "+var_str+"? (\"" + str(data_package[var_str]) + "\")")

        if not update or update and user_update:
            
            if input_type == "open":
                v.data[var_str] = p.user_prompt_open(in_1, in_2, in_3)
                data_package[var_str] = v.data[var_str]

            elif input_type == "loop":
                v.data[var_str] = p.user_prompt_loop(in_1, in_2, in_3)
                data_package[var_str] = v.data[var_str]

        # elif update and not user_update:
            # validate anyway - in case validation function changed
            # force to enter new input if needed

    else:
        if input_type == "open" and in_2:

            check_result = in_2(v.data[var_str])
            
            if type(check_result) != type(True) and len(check_result) == 2:
                valid, answer = check_result
            else:
                valid = check_result
                answer = v.data[var_str]

            if not valid:
                p.quit("Bad automated input")

            data_package[var_str] = answer

        else:
            data_package[var_str] = v.data[var_str]


# --------------------
# independent inputs

flist = [
    {   
        "id": "name",
        "type": "open",
        "in_1": "Dataset name? (must be unique from existing datasets)", 
        "in_2": v.name, 
        "in_3": v.error["name"]
    },
    {   
        "id": "title",
        "type": "open",
        "in_1": "Dataset title?", 
        "in_2": 0, 
        "in_3": 0
    },
    {   
        "id": "version",
        "type": "open",
        "in_1": "Dataset version?", 
        "in_2": 0, 
        "in_3": 0
    },
    {   
        "id": "sources",
        "type": "loop",
        "in_1": {"name":"","web":""}, 
        "in_2":  "Enter source ", 
        "in_3": "Add another source?"
    },
    {   
        "id": "source_link",
        "type": "open",
        "in_1": "Generic link for dataset?", 
        "in_2": 0, 
        "in_3": 0
    },
    {   
        "id": "licenses",
        "type": "open",
        "in_1": "Id of license(s) for dataset? (" + ', '.join(v.types["licenses"]) + ") [separate your input with commas]",
        "in_2": v.license_types, 
        "in_3": v.error["license_types"]
    },
    {   
        "id": "citation",
        "type": "open",
        "in_1": "Dataset citation?", 
        "in_2": 0, 
        "in_3": 0
    },
    {   
        "id": "short",
        "type": "open",
        "in_1": "A short description of the dataset?", 
        "in_2": 0, 
        "in_3": 0
    },
    {   
        "id": "variable_description",
        "type": "open",
        "in_1": "Description of the variable used in this dataset (units, range, etc.)?", 
        "in_2": 0, 
        "in_3": 0
    },
    {   
        "id": "type",
        "type": "open",
        "in_1": "Type of data in dataset? (" + ', '.join(v.types["data"])+ ")", 
        "in_2": v.data_type, 
        "in_3": v.error["data_type"]
    }
]

for f in flist:
    generic_input(f["type"], update_data_package, f["id"], f["in_1"], f["in_2"], f["in_3"])


# --------------------
# dependent inputs

# file format (raster or vector)
if v.data["type"] == "raster":
    v.data["file_format"] = v.data["type"]
else:
    v.data["file_format"] = "vector"

v.update_file_format(v.data["file_format"])

# file extension (validation depends on file format)
generic_input("open", update_data_package, "file_extension", "Primary file extension of data in dataset? (" + ', '.join(v.types["file_extensions"][v.data["file_format"]])+ ")", v.file_extension, v.error["file_extension"])

# raster info
if v.data["type"] == "raster":
    # extract_types (multiple)
    generic_input("open", update_data_package, "extract_types", "Valid extract types for data in dataset? (" + ', '.join(v.types["extracts"]) + ") [separate your input with commas]", v.extract_types, v.error["extract_types"])

    # factor
    generic_input("open", update_data_package, "factor", "Dataset multiplication factor? (if needed, use 1 otherwise)", v.factor, v.error["factor"])


# --------------------
# option to review inputs

if interface and p.user_prompt_bool("Would you like to review your inputs?"):
    for x in data_package.keys():
        print x + " : \n\t" + str(data_package[x])

    # option to quit or continue
    if not p.user_prompt_bool("Continue with these inputs?"):
        quit("User request.")




print data_package

quit("!!!")


# --------------------------------------------------


# get bounding box
# http://gis.stackexchange.com/questions/57834/how-to-get-raster-corner-coordinates-using-python-gdal-bindings

def GetExtent(gt,cols,rows):
    ''' Return list of corner coordinates from a geotransform

        @type gt:   C{tuple/list}
        @param gt: geotransform
        @type cols:   C{int}
        @param cols: number of columns in the dataset
        @type rows:   C{int}
        @param rows: number of rows in the dataset
        @rtype:    C{[float,...,float]}
        @return:   coordinates of each corner
    '''
    ext=[]
    xarr=[0,cols]
    yarr=[0,rows]

    for px in xarr:
        for py in yarr:
            x=gt[0]+(px*gt[1])+(py*gt[2])
            y=gt[3]+(px*gt[4])+(py*gt[5])
            ext.append([x,y])
            # print x,y
        yarr.reverse()
    return ext


def ReprojectCoords(coords,src_srs,tgt_srs):
    ''' Reproject a list of x,y coordinates.

        @type geom:     C{tuple/list}
        @param geom:    List of [[x,y],...[x,y]] coordinates
        @type src_srs:  C{osr.SpatialReference}
        @param src_srs: OSR SpatialReference object
        @type tgt_srs:  C{osr.SpatialReference}
        @param tgt_srs: OSR SpatialReference object
        @rtype:         C{tuple/list}
        @return:        List of transformed [[x,y],...[x,y]] coordinates
    '''
    trans_coords=[]
    transform = osr.CoordinateTransformation( src_srs, tgt_srs)
    for x,y in coords:
        x,y,z = transform.TransformPoint(x,y)
        trans_coords.append([x,y])
    return trans_coords


def check_env(new, old):
    if len(new) == len(old) and len(new) == 4:
        # update envelope if polygon extends beyond bounds

        if new[0] < old[0]:
            old[0] = new[0]
        if new[1] > old[1]:
            old[1] = new[1]
        if new[2] < old[2]:
            old[2] = new[2]
        if new[3] > old[3]:
            old[3] = new[3]

    elif len(old) == 0 and len(new) == 4:
        # initialize envelope
        for x in new:
            old.append(x)

    else:
        sys.exit("Terminating script - Invalid polygon envelope.\n")

    return old


# --------------------------------------------------
# get bounding box


if in_format == 'raster':
	ds=gdal.Open(in_path)

	gt=ds.GetGeoTransform()
	cols = ds.RasterXSize
	rows = ds.RasterYSize
	ext=GetExtent(gt,cols,rows)

	src_srs=osr.SpatialReference()
	src_srs.ImportFromWkt(ds.GetProjection())
	#tgt_srs=osr.SpatialReference()
	#tgt_srs.ImportFromEPSG(4326)
	tgt_srs = src_srs.CloneGeogCS()

	geo_ext=ReprojectCoords(ext,src_srs,tgt_srs)

	# geo_ext = [[-155,50],[-155,-30],[22,-30],[22,50]]


elif in_format == 'vector':
	ds = ogr.Open(in_path)
	lyr_name = in_path[in_path.rindex('/')+1:in_path.rindex('.')]
	lyr = ds.GetLayerByName(lyr_name)
	env = []


	for feat in lyr:
		temp_env = feat.GetGeometryRef().GetEnvelope()
		env = check_env(temp_env, env)
		# print temp_env

	# env = [xmin, xmax, ymin, ymax]
	geo_ext = [[env[0],env[3]], [env[0],env[2]], [env[1],env[2]], [env[1],env[3]]]
	# print "final env:",env
	# print "bbox:",geo_ext

else:
    sys.exit("Terminating script - File format error.\n")


# check bbox size
xsize = geo_ext[2][0] - geo_ext[1][0]
ysize = geo_ext[0][1] - geo_ext[1][1]
tsize = abs(xsize * ysize)

in_scale = "regional"
if tsize >= 32400:
	in_scale = "global"
	# prompt to continue
	if not p.user_prompt_bool("This dataset has a bounding box larger than a hemisphere and will be treated as a global dataset. If this is not a global (or near global) dataset you may want to clip it into multiple smaller datasets. Do you want to continue?"):
	    sys.exit("Terminating script - User request.\n")


# display datset info to user
print "Dataset bounding box: ", geo_ext

# prompt to continue
if not p.user_prompt_bool("Continue with this bounding box?"):
    sys.exit("Terminating script - User request.\n")


# --------------------------------------------------
# update database


# connect to mongodb
client = pymongo.MongoClient()
db = client.daf
c_data = db.data

# loc is 2dsphere spatial index
# > db.data.createIndex( { loc : "2dsphere" } )
# path is unique index
# > db.data.createIndex( { path : 1 }, { unique: 1 } )
# name is unique index
# > db.data.createIndex( { name : 1 }, { unique: 1 } )

# build dictionary for mongodb insert
data = {
	"loc": { 
			"type": "Polygon", 
			"coordinates": [ [
				geo_ext[0],
				geo_ext[1],
				geo_ext[2],
				geo_ext[3],
				geo_ext[0]
			] ]
		},
	"name": in_name,
	"short": in_short,
	"type": in_type,
	"scale": in_scale,
	"format": in_format,
	"path": in_path,
	"start": int(in_start),
	"end": int(in_end)
    # ADD PATH TO FILES DATAPACKAGE
}

# insert 
try:
	c_data.insert(data)
except pymongo.errors.DuplicateKeyError, e:
    print e
    sys.exit("Terminating script - Dataset with same name or path exists.\n")


# check insert and notify user
vp = c_data.find({"path": in_path})
vn = c_data.find({"name": in_name})

if vp.count() < 1 or vn.count() < 1:
    sys.exit( "Error - No items with name or path found in database.\n")
elif vp.count() > 1 or vn.count() > 1:
	sys.exit( "Error - Multiple items with name or path found in database.\n")
else:
	print "Success - Item successfully inserted into database.\n"


# update/create boundary tracker(s)
# *** add error handling for all inserts (above and below) ***
# *** remove previous inserts if later insert fails, etc. ***

if in_type == "boundary":
	# if dataset is boundary
	# create new boundary tracker collection
	# each each non-boundary dataset item to new boundary collection with "unprocessed" flag
	dsets =  c_data.find({"type": {"$ne": "boundary"}})
	c_bnd = db[in_name]
	c_bnd.create_index("name", unique=True)
	for dset in dsets:
		dset['status'] = -1
		c_bnd.insert(dset)

else:
	# if dataset is not boundary
	# add dataset to each boundary collection with "unprocessed" flag
	bnds = c_data.find({"type": "boundary"},{"name": 1})
	dset = copy.deepcopy(data)
	dset['status'] = -1
	for bnd in bnds:
		c_bnd = db[bnd['name']]
		c_bnd.insert(dset)

