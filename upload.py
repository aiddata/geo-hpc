# add dataset to geo mongodb

import sys
import os
import re
from datetime import datetime
import pymongo
from osgeo import gdal,ogr,osr

# user inputs
in_name = sys.argv[1]
in_short = sys.argv[2]
in_type = sys.argv[3]
in_path = sys.argv[4]

# start and end must be in YYYYMMDD format
in_start = sys.argv[5]
in_end = sys.argv[6]


# prompt to continue function
def user_prompt(question):

	valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    
	while True:
		sys.stdout.write(question + " [y/n] ")
		choice = raw_input().lower()

		if choice in valid:
			return valid[choice]
		else:
			sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


# validate inputs

types = ['raster','polydata','document','point','multipoint','boundary']
vectors = ['geojson', 'shp']
rasters = ['tif', 'asc']

if re.sub('[^0-9a-zA-Z]+', '', in_name) == "" :
	sys.exit("Terminating script - You must include a valid 'name' input.\n")

if not in_type in types :
	sys.exit("Terminating script - Invalid 'type' input.\n")

if not os.path.isfile(in_path):
	sys.exit("Terminating script - Input 'path' does not exit.\n")

file_ext = in_path[in_path.rindex(".")+1:]

if file_ext in vectors:
	in_format = "vector"
elif file_ext in rasters:
	in_format = "raster"
else:
	sys.exit("Terminating script - Invalid data format.")


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
if not user_prompt("Continue with these inputs?"):
    sys.exit("Terminating script - User request.\n")


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


if in_format == 'raster':
	raster = in_path
	# raster=r'somerasterfile.tif'
	ds=gdal.Open(raster)

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

	xsize = geo_ext[2][0] - geo_ext[1][0]
	ysize = geo_ext[0][1] - geo_ext[1][1]
	tsize = abs(xsize * ysize)

	in_scale = "regional"
	if tsize >= 32400:
		in_scale = "global"
		# prompt to continue
		if not user_prompt("This dataset has a bounding box larger than a hemisphere and will be treated as a global dataset. If this is not a global (or near global) dataset you may want to clip it into multiple smaller datasets. Do you want to continue?"):
		    sys.exit("Terminating script - User request.\n")



elif in_format == 'vector':
    sys.exit("Terminating script - No vectors yet.\n")

else:
    sys.exit("Terminating script - File format error.\n")



# display datset info to user
print "Dataset bounding box: ", geo_ext

# prompt to continue
if not user_prompt("Continue with this bounding box?"):
    sys.exit("Terminating script - User request.\n")



# connect to mongodb
client = pymongo.MongoClient()
db = client.daf
collection = db.data

# loc is 2dsphere spatial index
# > db.data.createIndex( { loc : "2dsphere" } )
# path is unique index
# > db.data.createIndex( { path : 1 }, { unique: 1 } )

# build dictionary for insert
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
}

# insert 
try:
	collection.insert(data)
except pymongo.errors.DuplicateKeyError, e:
    sys.exit("Terminating script - Dataset for file exists.\n")

    # print e


# check insert and notify user
v = collection.find({"path":in_path})

if v.count() < 1:
    print "Error - no items with path found in database."
elif v.count() > 1:
	print "Error - multiple items with path found in database."
else:
	print "Success - item successfully inserted into databse." 
