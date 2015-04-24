# runscript.py

import pandas as pd
import numpy as np
import os
import sys
import errno
# from sqlalchemy import create_engine
from copy import deepcopy
# from datetime import datetime
import time

import random
from shapely.geometry import Polygon, Point, shape, box
import shapefile

Ts = time.time()

# python /path/to/runscript.py nepal NPL 0.1
arg = sys.argv

try:
	country = sys.argv[1]
	abbr = sys.argv[2]
	pixel_size = float(sys.argv[3]) # 0.025

except:
	sys.exit("invalid inputs")

# examples of valid pixel sizes: 1.0, 0.5, 0.25, 0.2, 0.1, 0.05, 0.025, ...

if (1/pixel_size) != int(1/pixel_size):
	sys.exit("invalid pixel size: "+str(pixel_size))

# pixel size inverse
psi = 1/pixel_size

# -----------------------------------------------

# nodata value for output raster
nodata = -9999

# subset / filter
subset = "ALL"
# sector_codes = arg[5]
# type(sector_codes)

#number of iterations
iterations = 1
i_control = range(1, (int(iterations) + 1))

aid_field = "total_commitments"

# -----------------------------------------------

base = "/home/usery/mcr"

amp_path = base+"/"+country+"/data/projects.tsv"
loc_path = base+"/"+country+"/data/locations.tsv"

# -----------------------------------------------

# must start at and inlcude ADM0
# all additional ADM shps must be included so that adm_path index corresponds to adm level
adm_paths = []
adm_paths.append(base+"/"+country+"/shapefiles/ADM0/"+abbr+"_adm0.shp")
adm_paths.append(base+"/"+country+"/shapefiles/ADM1/"+abbr+"_adm1.shp")
adm_paths.append(base+"/"+country+"/shapefiles/ADM2/"+abbr+"_adm2.shp")

dir_working = base+"/"+country+"/working/"+country+"_"+subset+"_"+str(pixel_size)+"_"+str(iterations)+"-ITERATIONS"
dir_intermediate = dir_working+"/intermediate_rasters"
dir_final = dir_working+"/final_raster_products"


# create directories 

def make_dir(path):
	try: 
		os.makedirs(path)
	except OSError as exception:
		if exception.errno != errno.EEXIST:
			raise


make_dir(dir_working)
make_dir(dir_intermediate)
make_dir(dir_final)


# create point grid for country

# get adm0 bounding box
adm_shps = [shapefile.Reader(adm_path).shapes() for adm_path in adm_paths]

adm0 = shape(adm_shps[0][0])

(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = adm0.bounds
print (adm0_minx, adm0_miny, adm0_maxx, adm0_maxy)

import math
(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = (math.floor(adm0_minx*psi)/psi, math.floor(adm0_miny*psi)/psi, math.ceil(adm0_maxx*psi)/psi, math.ceil(adm0_maxy*psi)/psi)
print (adm0_minx, adm0_miny, adm0_maxx, adm0_maxy)

cols = np.arange(adm0_minx, adm0_maxx+pixel_size*0.5, pixel_size)
rows = np.arange(adm0_maxy, adm0_miny-pixel_size*0.5, -1*pixel_size)

print cols
print rows


# create grid based on output resolution (pixel size) 
op = {}

idx = 0
for r in rows:
	op[str(r)] = {}
	for c in cols:
		# build grid reference object
		op[str(r)][str(c)] = idx
		idx += 1

# initialize grid with 0
npa = np.zeros((int(idx+1),), dtype=np.int)


# initialize asc file output
asc = ""
asc += "NCOLS " + str(len(cols)) + "\n"
asc += "NROWS " + str(len(rows)) + "\n"

# asc += "XLLCORNER " + str(adm0_minx-pixel_size*0.5) + "\n"
# asc += "YLLCORNER " + str(adm0_miny-pixel_size*0.5) + "\n"

asc += "XLLCENTER " + str(adm0_minx) + "\n"
asc += "YLLCENTER " + str(adm0_miny) + "\n"

asc += "CELLSIZE " + str(pixel_size) + "\n"
asc += "NODATA_VALUE " + str(nodata) + "\n"



# ======================================================================================================



# check csv delim and return if valid type
def getCSV(path):
    if path.endswith('.tsv'):
        return pd.read_csv(path, sep='\t', quotechar='\"')
    elif path.endswith('.csv'):
        return pd.read_csv(path, quotechar='\"')
    else:
        sys.exit('getCSV - file extension not recognized.\n')

# read input csv files into memory
amp = getCSV(amp_path)
loc = getCSV(loc_path)


if not "project_id" in amp or not "project_id" in loc:
	sys.exit("project_id fields not found in amp or loc files")

amp["project_id"] = amp["project_id"].astype(str)
loc["project_id"] = loc["project_id"].astype(str)

# create projectdata by merging amp and location files by project_id
merged = loc.merge(amp, on='project_id')


code_field = "precision_code"
if not code_field in merged or not "longitude" in merged or not "latitude" in merged:
	sys.exit("required fields not found")



print "starting iterations ("+str(i_control)+" to be run)"



# mpi stuff
# for it in i_control:


print "iter "+str(1)+": starting"

# create dataframe for use in iterations
column_list = ['iter', 'id', 'ran_dollars']
dollar_table = pd.DataFrame(columns=column_list)

# create copy of merged project data
i_m = deepcopy(merged)

#add new column of random numbers (0-1)
i_m['ran_num'] = (pd.Series(np.random.random(len(i_m)))).values

#group merged table by project ID for the sum of each project ID's random numbers
grouped_random_series = i_m.groupby('project_id')['ran_num'].sum()


#create new empty dataframe
df_group_random = pd.DataFrame()

#add grouped random 'Series' to the newly created 'Dataframe' under a new grouped_random column
df_group_random['grouped_random'] = grouped_random_series

#add the series index, composed of project IDs, as a new column called project_ID
df_group_random['project_id'] = df_group_random.index


#now that we have project_ID in both the original merged 'Dataframe' and the new 'Dataframe' they can be merged
i_m = i_m.merge(df_group_random, on='project_id')

#calculate the random dollar ammount per point for each entry
i_m['random_dollars_pp'] = (i_m.ran_num / i_m.grouped_random) * i_m[aid_field]



print "iter "+str(1)+": random dollar calc complete"

print len(i_m)



# ======================================================================================================



agg_types = ["point","buffer","adm"]

lookup = {
	"1": {"type":"point","data":0},
	"2": {"type":"buffer","data":25000},
	"3": {"type":"adm","data":"2"},
	"4": {"type":"adm","data":"2"},
	"5": {"type":"buffer","data":25000},
	"6": {"type":"adm","data":"0"},
	"7": {"type":"adm","data":"0"},
	"8": {"type":"adm","data":"0"}
}


def getPolyWithin(item, polys):
	c = 0
	for shp in polys:
		tmp_shp = shape(shp)
		if item.within(tmp_shp):
			return tmp_shp

	return c


def inCountry(shp):
	return shp.within(adm0)


def getGeom(code, lon, lat):
	tmp_pnt = Point(lon, lat)
	
	if not inCountry(tmp_pnt):
		print "point not in country"
		return 0

	elif lookup[code]["type"] == "point":
		return tmp_pnt

	elif lookup[code]["type"] == "buffer":
		try:
			tmp_int = int(lookup[code]["data"])
			tmp_buffer = tmp_pnt.buffer(tmp_int)

			if inCountry(tmp_buffer):
				return tmp_buffer
			else:
				return tmp_buffer.intersection(adm0)

		except:
			print "buffer value could not be converted to int"
			return 0

	elif lookup[code]["type"] == "adm":
		try:
			tmp_int = int(lookup[code]["data"])
			return getPolyWithin(tmp_pnt, adm_shps[tmp_int])

		except:
			print "adm value could not be converted to int"
			return 0

	else:
		print "code type not recognized"
		return 0


def geomType(code):
	if str(code) in lookup:
		tmp_type = lookup[str(code)]["type"]
		return tmp_type

	else:
		print "code not recognized"
		return "None"


def geomVal(agg_type, code, lon, lat):
	if agg_type in agg_types:

		tmp_geom = getGeom(str(code), lon, lat)

		if tmp_geom != 0:
			return tmp_geom

		return "None"

	else:
		print "agg_type not recognized"
		return "None"



# add geom columns

i_m["agg_type"] = ["None"] * len(i_m)
i_m["agg_geom"] = ["None"] * len(i_m)

i_m.agg_type = i_m.apply(lambda x: geomType(x[code_field]), axis=1)
i_m.agg_geom = i_m.apply(lambda x: geomVal(x.agg_type, x[code_field], x.longitude, x.latitude), axis=1)

print "iter "+str(1)+": get geom complete"



i_mx = i_m.loc[i_m.agg_geom != "None"].copy(deep=True)

print "iter "+str(1)+": remove empty geoms complete"



INVALID_X = -9999
INVALID_Y = -9999

# add point gen function
def get_random_point_in_polygon(poly):
    (minx, miny, maxx, maxy) = poly.bounds
    p = Point(INVALID_X, INVALID_Y)
    px = 0
    while not poly.contains(p):
        p_x = random.uniform(minx, maxx)
        p_y = random.uniform(miny, maxy)
        p = Point(p_x, p_y)
    return p


# generate random point geom or use actual point
def addPt(agg_type, agg_geom):
	if agg_type == "point":
		return agg_geom
	else:
		tmp_rnd = get_random_point_in_polygon(agg_geom)
		return tmp_rnd


# add random points column to table
i_mx["rnd_pt"] = [0] * len(i_mx)
i_mx.rnd_pt = i_mx.apply(lambda x: addPt(x.agg_type, x.agg_geom), axis=1)

print "iter "+str(1)+": get rnd pts complete"


# round rnd_pts to match point grid
i_mx = i_mx.merge(i_mx.rnd_pt.apply(lambda s: pd.Series({'rnd_x':(round(s.x * psi) / psi), 'rnd_y':(round(s.y * psi) / psi)})), left_index=True, right_index=True)



# add commitment value for each rnd pt to grid value
for i in i_mx.iterrows():
	nx = str(i[1].rnd_x)
	ny = str(i[1].rnd_y)
	# print nx, ny, op[nx][ny]
	npa[op[ny][nx]] += int(i[1].random_dollars_pp)

print "iter "+str(1)+": add random_dollars_pp to op complete"


# add data to asc file output
# for r in rows:
# 	for c in cols:
# 		asc += str(op[str(r)][str(c)]) 
# 		asc += " "
# 	asc += "\n"
npa = np.char.mod('%f', npa)
asc += ' '.join(npa)


# write asc file
fout = open('/home/usery/mcr/'+country+'_output_'+str(pixel_size)+'_cc03.asc', 'w')
fout.write(asc)

print "iter "+str(1)+": update and write asc complete"

# print op
# print i_mx
# print len(i_mx)



# ======================================================================================================
# ======================================================================================================

Tloc = int(time.time() - Ts)
print '\t\tRuntime Loc: ' + str(Tloc//60) +'m '+ str(int(Tloc%60)) +'s'

# ======================================================================================================
# ======================================================================================================



# import os, sys, glob, fnmatch
# import numpy as np
# from osgeo import gdal

# gdal.AllRegister()

# arg = sys.argv

# #---------------------------------------
# inputfolder = arg[1]
# outputfolder = arg[2]
# count_wildcard = 'count*.tif'
# sum_wildcard = 'sum*.tif'
# #---------------------------------------

# if not os.path.exists(outputfolder):
#     os.makedirs(outputfolder)

# projectdir_count = os.path.join(inputfolder, count_wildcard)
# projectdir_sum = os.path.join(inputfolder, sum_wildcard)

# filecontents_count = glob.glob(projectdir_count)
# filecontents_sum = glob.glob(projectdir_sum)

# #---------------------------------------
# layers_count = []
# x = 0
# for raster in filecontents_count:
#     x += 1
#     ds = gdal.Open(raster)
#     ds_array = ds.GetRasterBand(1).ReadAsArray()
#     layers_count.append(ds_array)
#     (rowtest, columntest) = np.shape(ds_array)
#     if x > 1:
#         if (rowtest, columntest) != (rowtestlast, columntestlast):
#             print 'count iteration ' + str(x) + ' was shaped as (' + str(rowtest) + ',' + str(columntest) + ')'
#     rowtestlast, columntestlast = rowtest, columntest
# del rowtest, columntest

# stack_count = np.dstack(layers_count)

# std_count = np.std(stack_count, axis=2)
# mean_count = np.mean(stack_count, axis=2)

# #---------------------------------------

# (row, column) = np.shape(mean_count)

# filename= outputfolder + '/std_count.tif'
# currentraster= gdal.GetDriverByName('GTiff').Create(filename, column, row, 1, gdal.GDT_Float64)
# currentraster.GetRasterBand(1).WriteArray(std_count)
# currentraster = None
# del currentraster, filename

# filename= outputfolder + '/mean_count.tif'
# currentraster= gdal.GetDriverByName('GTiff').Create(filename, column, row, 1, gdal.GDT_Float64)
# currentraster.GetRasterBand(1).WriteArray(mean_count)
# currentraster = None
# del currentraster, filename

# #---------------------------------------
# layers_sum = []
# x = 0
# for raster in filecontents_sum:
#     x += 1
#     ds = gdal.Open(raster)
#     ds_array = ds.GetRasterBand(1).ReadAsArray()
#     layers_sum.append(ds_array)
#     (rowtest, columntest) = np.shape(ds_array)
#     if x > 1:
#         if (rowtest, columntest) != (rowtestlast, columntestlast):
#             print 'sum iteration ' + str(x) + ' was shaped as (' + str(rowtest) + ',' + str(columntest) + ')'
#     rowtestlast, columntestlast = rowtest, columntest

# stack_sum = np.dstack(layers_sum)

# std_sum = np.std(stack_sum, axis=2)
# mean_sum = np.mean(stack_sum, axis=2)

# #---------------------------------------

# (row, column) = np.shape(mean_sum)

# filename= outputfolder + '/std_sum.tif'
# currentraster= gdal.GetDriverByName('GTiff').Create(filename, column, row, 1, gdal.GDT_Float64)
# currentraster.GetRasterBand(1).WriteArray(std_sum)
# currentraster = None
# del currentraster, filename

# filename= outputfolder + '/mean_sum.tif'
# currentraster= gdal.GetDriverByName('GTiff').Create(filename, column, row, 1, gdal.GDT_Float64)
# currentraster.GetRasterBand(1).WriteArray(mean_sum)
# currentraster = None
# del currentraster, filename

# #---------------------------------------
# print 'complete'



# ======================================================================================================



# # get new corner points
# XMIN=$(echo "select st_xmin(a.geom)-0.5 from adm0 as a" | psql $db | sed '3q;d')
# XMAX=$(echo "select st_xmax(a.geom)+0.5 from adm0 as a" | psql $db | sed '3q;d')
# YMIN=$(echo "select st_ymin(a.geom)-0.5 from adm0 as a" | psql $db | sed '3q;d')
# YMAX=$(echo "select st_ymax(a.geom)+0.5 from adm0 as a" | psql $db | sed '3q;d')

# echo $XMIN, $XMAX, $YMIN, $YMAX

# # generate projection file
# # sudo apt-get install geotiff-bin
# listgeo -tfw $dir_intermediate'/count_1.tif'

# # copy projection file for each output file
# cp $dir_intermediate'/count_1.tfw' $dir_final'/mean_count.tfw'
# cp $dir_intermediate'/count_1.tfw' $dir_final'/std_count.tfw'
# cp $dir_intermediate'/count_1.tfw' $dir_final'/mean_sum.tfw'
# cp $dir_intermediate'/count_1.tfw' $dir_final'/std_sum.tfw'

# # reproject each output
# gdal_edit.py -a_srs EPSG:4326 $dir_final'/mean_count.tif'
# gdal_edit.py -a_srs EPSG:4326 $dir_final'/std_count.tif'
# gdal_edit.py -a_srs EPSG:4326 $dir_final'/mean_sum.tif'
# gdal_edit.py -a_srs EPSG:4326 $dir_final'/std_sum.tif'

# echo "upper left is "$XMIN" , "$YMAX
# echo "lower left is "$XMAX" , "$YMIN

# # clip final outputs to remove corner points
# # coordinates are backwards (ymin and ymax) because of something strange in the raster gen process that flips the ymin/ymax values
# gdal_translate -projwin $XMIN $YMIN $XMAX $YMAX $dir_final'/mean_count.tif' $dir_final'/'$country'_'$subset'_mean_count.tif'
# gdal_translate -projwin $XMIN $YMIN $XMAX $YMAX $dir_final'/std_count.tif' $dir_final'/'$country'_'$subset'_std_count.tif'
# gdal_translate -projwin $XMIN $YMIN $XMAX $YMAX $dir_final'/mean_sum.tif' $dir_final'/'$country'_'$subset'_mean_sum.tif'
# gdal_translate -projwin $XMIN $YMIN $XMAX $YMAX $dir_final'/std_sum.tif' $dir_final'/'$country'_'$subset'_std_sum.tif'

# # clean up
# rm $dir_final'/mean_count.tif'
# rm $dir_final'/std_count.tif'
# rm $dir_final'/mean_sum.tif'
# rm $dir_final'/std_sum.tif'

# echo '>>georeferencing complete'
# echo

# # clean up
# rm -rf $dir_intermediate


# ======================================================================================================



