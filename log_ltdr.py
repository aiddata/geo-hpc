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

# --------------------------------------------------

script = os.path.basename(sys.argv[0])
version = "0.1"

# --------------------------------------------------
# user inputs

if len(sys.argv) > 1 and sys.argv[1] == "auto":

    try:

        # always use absolute paths
        # ***going to add check to make sure path is in REU/data/internal(?) folder***
        in_path = sys.argv[x]

        in_name = sys.argv[x]
        

        in_short = sys.argv[x]
        in_type = sys.argv[x]


        # start and end must be in YYYYMMDD format
        # for data without day use YYYYMM01
        # for data without month use YYYY0101
        # annual/monthly/daily data should use same start and end date
        # in_start = sys.argv[x]
        # in_end = sys.argv[x]

    except:
        sys.stdout.write("Bad inputs.\n")


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
        dp["maintainers"] =  [
            {
                "web": "http://aiddata.org", 
                "name": "AidData REU", 
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
                dp[k] = v.fields[d]

        if clean:
            # iterate over keys in dp
            for k in dp.keys():
                # if clean: delete key if outdated
                if k not in v.fields:
                    del dp[k]


    # dp = OrderedDict()

    # dp["datapackage_script"] = script
    # dp["datapackage_version"] = version

    # dp["date_updated"] = str(date.today())
    # dp["date_added"] = str(date.today())

    # dp["maintainers"] =  [
    #     {
    #         "web": "http://aiddata.org", 
    #         "name": "AidData REU", 
    #         "email": "info@aiddata.org"
    #     }
    # ]

    # dp["publishers"] = [
    #     {
    #         "web": "http://aiddata.org", 
    #         "name": "AidData", 
    #         "email": "info@aiddata.org"
    #     }
    # ]

    return dp


def quit(reason):
    sys.exit("Terminating script - "+str(reason)+"\n")


# prompt to continue function
def user_prompt_bool(question):

    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    
    while True:
        sys.stdout.write(str(question) + " [y/n] \n> ")
        choice = raw_input().lower()

        if choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


# open ended user prompt
def user_prompt_open(question, check=0, error=0):
    
    if error:
        error = " ("+error+")"
    else:
        error = ""

    while True:
        sys.stdout.write(question + " \n> ")
        answer = raw_input()

        use_answer = user_prompt_bool("Use the following? : \"" + str(answer) + "\"")

        if not use_answer:
            redo_answer = user_prompt_bool("Use a new answer [y] or exit [n]?")

            if not redo_answer:
               quit("No answer given at open prompt.")

        else:
            if check and not check(answer):
                redo_answer = user_prompt_bool("Invalid input" + error + ": Use a new answer [y] or exit [n]?")

                if not redo_answer:
                   quit("No answer given at open prompt.")

            else:        
                return answer



# --------------------------------------------------
# prompts


# validation functions, fields, etc.
class validate():

    def __init__(self):

        # base path
        self.dir_base = os.path.dirname(os.path.abspath(__file__))

        # available licenses
        self.licenses = json.load(open(self.dir_base +"/licenses.json"))

        # init file format
        self.file_format = ""

        # acceptable inputs for various fields (dataset types, vector formats, raster formats, etc.)
        self.types = {
            "licenses": self.licenses.keys(),
            "data": ['raster','polydata','document','point','multipoint','boundary'],
            "file_extensions": {
                "vector": ['geojson', 'shp'],
                "raster": ['tif', 'asc']
            },
            "extracts": ['mean', 'max']
        }

        # error messages to go with validation functions
        self.error = {
            "dir_base": "not a directory",
            "name": "name exists or is invalid",
            "license_types": "at least one license id given not in list of valid license ids",
            "data_type": "not in list of valid types",
            "file_extension": "not a valid primary file extension"
            "extract_types": "at least one extract type given not in list of valid extract types",
            "factor": "could not be converted to float"
        }

        # current datapackage fields
        self.fields = json.load(open(self.dir_base +"/fields.json"), object_pairs_hook=OrderedDict)

        # {
        #     "datapackage_script": "", 
        #     "datapackage_version": "", 
        #     "date_updated": "",
        #     "date_added": "",
        #     "maintainers": [], 
        #     "publishers": [], 
        #     "resources": [], 
        #     "temporal": [], 
        #     "base": "",
        #     "name": "", 
        #     "title": "", 
        #     "version": "", 
        #     "sources": [], 
        #     "source_link": "",
        #     "licenses": [], 
        #     "citation":"",
        #     "short": "",
        #     "variable_description": "",
        #     "type": "",
        #     "file_format": "", 
        #     "file_extension": "", 
        #     "extract_types": [],
        #     "factor": 1
        # }


    # -------------------------
    #  misc functions


    # set file format 
    def file_format(self, val):
        self.file_format = val


    # -------------------------
    # input validation functions


    # base path exists and is a directory
    def dir_base(self, val):
        return os.path.isdir(str(val))


    # check if name is unique and valid
    # def name(self, val):
        # val = re.sub('[^0-9a-zA-Z]+', '', val)
        # return 


    # each extract type in extract_types
    def license_types(self, val):
        vals = [x.strip(' ') for x in val.split(",")]
        valid = False not in [x in self.types["licenses"] for x in vals]
        return valid


    # type in types
    def data_type(self, val):
        return val in self.types["data"]


    # each extract type in extract_types
    def file_extension(self, val):
        return self.file_format in self.types["file_extensions"].keys() and val in self.types["file_extensions"][self.file_format]


    # each extract type in extract_types
    def extract_types(self, val):
        vals = [x.strip(' ') for x in val.split(",")]
        valid = False not in [x in self.types["extracts"] for x in vals]
        return valid


    # factor is a float
    def factor(self, val):
        try:
            float(val)
            return True
        except:
            return False 



# validate class instance
v = validate()


# prompt responses
p = {}

# base path
# get base path
p["base"] = user_prompt_open("Absolute path to root directory of dataset? (eg: /mnt/sciclone-aiddata/REU/data/path/to/dataset)", v.dir_base, v.error["dir_base"])
# check datapackage.json exists at path 
if os.path.isfile(p["base"]+"/datapackage.json"):
    # true: update protocol
    data_package = json.load(open(p["base"]+"/datapackage.json"), object_pairs_hook=OrderedDict)
    data_package = init_datapackage(data_package)

    # data_package["date_updated"] = str(date.today())
    quit("Datapackage already exists.")

else:
    # false: creation protocol
    data_package = init_datapackage()
    data_package["base"] = p["base"]


# name
# p["name"] = user_prompt_open("Dataset name? (must be unique from existing datasets)", v.name, v.error["name"])
# data_package["name"] = p["name"]

# title
p["title"] = user_prompt_open("Dataset title?")
data_package["title"] = p["title"]

# version
p["version"] = user_prompt_open("Dataset version?")
data_package["version"] = p["version"]

# sources (multiple)
# web and name for each
# 

# source_link
p["source_link"] = user_prompt_open("Generic link for dataset?")
data_package["source_link"] = p["source_link"]

# licenses (multiple)
# use key for existing license or fill in details for new type
p["licenses"] = user_prompt_open("Id of license(s) for dataset? (" + ', '.join(v.types["licenses"]) + ") [separate your input with commas]", v.license_types, v.error["license_types"])
data_package["licenses"] = [v.licenses[x.strip(' ')] for x in p["license_types"].split(",")]

# citation
p["citation"] = user_prompt_open("Dataset citation?")
data_package["citation"] = p["citation"]

# short
p["short"] = user_prompt_open("A short description of the dataset?")
data_package["short"] = p["short"]

# variable_description
p["variable_description"] = user_prompt_open("Description of the variable used in this dataset (units, range, etc.)?")
data_package["variable_description"] = p["variable_description"]

# type
p["type"] = user_prompt_open("Type of data in dataset? (" + ', '.join(v.types["data"])+ ")", v.data_type, v.error["data_type"])
data_package["type"] = p["type"]

# file format (raster or vector)
if p["type"] == "raster":
    p["file_format"] = p["type"]
else:
    p["file_format"] = "vector"

v.file_format(p["file_format"])

# file extension
p["file_extension"] = user_prompt_open("Primary file extension of data in dataset? (" + ', '.join(v.types["file_extensions"][p["type"]])+ ")", v.file_extension, v.error["file_extension"])
data_package["file_extension"] = p["file_extension"]

# raster info
if p["type"] == "raster":
    # extract_types (multiple)
    p["extract_types"] = user_prompt_open("Valid extract types for data in dataset? (" + ', '.join(v.types["extracts"]) + ") [separate your input with commas]", v.extract_types, v.error["extract_types"])
    data_package["extract_types"] = [x.strip(' ') for x in p["extract_types"].split(",")]

    # factor
    p["factor"] = user_prompt_open("Dataset multiplication factor? (if needed, use 1 otherwise)", v.factor, v.error["factor"])
    data_package["factor"] = float(p["factor"])



print data_package

quit("!!!")



# --------------------------------------------------
# validate inputs


# lists of acceptable dataset types, vector formats and raster formats
# types = ['raster','polydata','document','point','multipoint','boundary']
# vectors = ['geojson', 'shp']
# rasters = ['tif', 'asc']

# make sure list has alphanumeric chars init
# ***may need to restrict this to only alphanumeric***
if re.sub('[^0-9a-zA-Z]+', '', in_name) == "" :
	sys.exit("Terminating script - You must include a valid 'name' input.\n")

# check acceptable types
if not in_type in types :
	sys.exit("Terminating script - Invalid 'type' input.\n")

# check file specified by input path exists
if not os.path.isfile(in_path):
	sys.exit("Terminating script - Input 'path' does not exit.\n")

# check file extensions and input format
file_ext = in_path[in_path.rindex(".")+1:]

if file_ext in vectors:
	in_format = "vector"
elif file_ext in rasters:
	in_format = "raster"
else:
	sys.exit("Terminating script - Invalid data format.")

# validate date range
if len(str(in_start)) == 8 and len(str(in_end)) == 8:

	try:
		int_start = int(in_start)
		int_end = int(in_end)
		date_start = datetime.strptime(str(in_start), '%Y%m%d')
		date_end = datetime.strptime(str(in_end), '%Y%m%d')
		if int_start > int_end:
			sys.exit("Terminating script - Invalid date inputs.")

	except:
		sys.exit("Terminating script - Invalid date inputs.")

else:
	sys.exit("Terminating script - Invalid date inputs.")


# display inputs
print "    Input Summary"
print "        Name: " + in_name
print "        Short: " + in_short
print "        Type: " + in_type
print "        Path: " + in_path
print "        Start: " + in_start
print "        End: " + in_end

# prompt to continue
if not user_prompt_bool("Continue with these inputs?"):
    sys.exit("Terminating script - User request.\n")

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
	if not user_prompt_bool("This dataset has a bounding box larger than a hemisphere and will be treated as a global dataset. If this is not a global (or near global) dataset you may want to clip it into multiple smaller datasets. Do you want to continue?"):
	    sys.exit("Terminating script - User request.\n")


# display datset info to user
print "Dataset bounding box: ", geo_ext

# prompt to continue
if not user_prompt_bool("Continue with this bounding box?"):
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

