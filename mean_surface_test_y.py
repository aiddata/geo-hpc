# 
# runscript.py
# 


# ====================================================================================================

from __future__ import print_function

# from mpi4py import MPI

import os
import sys
import errno
from copy import deepcopy
import time
import random
import math

import numpy as np
import pandas as pd
from shapely.geometry import Polygon, Point, shape, box
import shapefile


# ====================================================================================================
# general init


# mpi info
# comm = MPI.COMM_WORLD
# size = comm.Get_size()
# rank = comm.Get_rank()
# status = MPI.Status()

# absolute path to script directory
base = os.path.dirname(os.path.abspath(__file__))

# start time
Ts = time.time()

# python /path/to/runscript.py nepal NPL 0.1 10
arg = sys.argv

try:
	country = sys.argv[1]
	abbr = sys.argv[2]
	pixel_size = float(sys.argv[3]) # 0.025
	iterations = int(sys.argv[4]) # 2

	# iterations range
	i_control = range(int(iterations))

except:
	sys.exit("invalid inputs")

# examples of valid pixel sizes: 1.0, 0.5, 0.25, 0.2, 0.1, 0.05, 0.025, ...

if (1/pixel_size) != int(1/pixel_size):
	sys.exit("invalid pixel size: "+str(pixel_size))

# pixel size inverse
psi = 1/pixel_size


# --------------------------------------------------
# vars to be added as inputs

# nodata value for output raster
nodata = -9999

# subset / filter
subset = "all"
# sector_codes = arg[5]
# type(sector_codes)

aid_field = "total_commitments"


# --------------------------------------------------
# static vars that may be added as some type of input

# only use geocoded data
only_geocoded = True

# is geocoded field
is_geocoded = "is_geocoded"

# type definition for non geocoded projects
# either allocated at country level or ignored
not_geocoded = "country"


code_field = "precision_code"

agg_types = ["point", "buffer", "adm"]

lookup = {
	"1": {"type":"point","data":0},
	"2": {"type":"buffer","data":1},
	# "2": {"type":"buffer","data":25000},

	"3": {"type":"adm","data":"2"},
	"4": {"type":"adm","data":"2"},
	"5": {"type":"buffer","data":1},
	# "5": {"type":"buffer","data":25000},

	"6": {"type":"adm","data":"0"},
	"7": {"type":"adm","data":"0"},
	"8": {"type":"adm","data":"0"}
}


# ====================================================================================================
# functions


# check csv delim and return if valid type
def getCSV(path):
	if path.endswith('.tsv'):
		return pd.read_csv(path, sep='\t', quotechar='\"')
	elif path.endswith('.csv'):
		return pd.read_csv(path, quotechar='\"')
	else:
		sys.exit('getCSV - file extension not recognized.\n')


# get project and location data in path directory
# requires a field name to merge on and list of required fields
def getData(path, merge_id, field_ids):

	amp_path = path+"/projects.tsv"
	loc_path = path+"/locations.tsv"

	# make sure files exist
	# 

	# read input csv files into memory
	amp = getCSV(amp_path)
	loc = getCSV(loc_path)


	if not merge_id in amp or not merge_id in loc:
		sys.exit("getData - merge field not found in amp or loc files")

	amp[merge_id] = amp[merge_id].astype(str)
	loc[merge_id] = loc[merge_id].astype(str)

	# create projectdata by merging amp and location files by project_id
	if only_geocoded:
		tmp_merged = loc.merge(amp, on=merge_id)
		not_geocoded = "None"
	else:
		tmp_merged = loc.merge(amp, on=merge_id, how="left")

	if not "longitude" in tmp_merged or not "latitude" in tmp_merged:
		sys.exit("getData - latitude and longitude fields not found")

	for field_id in field_ids:
		if not field_id in tmp_merged:
			sys.exit("getData - required code field not found")

	return tmp_merged


# --------------------------------------------------


# defin tags enum
def enum(*sequential, **named):
    # source: http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
	enums = dict(zip(sequential, range(len(sequential))), **named)
	return type('Enum', (), enums)


# gets geometry type based on lookup table
def geomType(is_geo, code):

	try:
		is_geo = int(is_geo)

		if is_geo == 1: 
			if str(code) in lookup:
				tmp_type = lookup[str(code)]["type"]
				return tmp_type

			else:
				print("lookup code not recognized")
				return "None"

		elif is_geo == 0:
			return not_geocoded

		else:
			print("is_geocoded integer code not recognized: " + str(is_geo))
			return "None"

	except:
		return not_geocoded



# finds shape in set of polygons which arbitrary polygon is within
# returns 0 if item is not within any of the shapes
def getPolyWithin(item, polys):
	c = 0
	for shp in polys:
		tmp_shp = shape(shp)
		if item.within(tmp_shp):
			return tmp_shp

	return c


# checks if arbitrary polygon is within country (adm0) polygon
# depends on adm0
def inCountry(shp):
	return shp.within(adm0)


# build geometry for point based on code
# depends on lookup and adm0
def getGeom(code, lon, lat):
	tmp_pnt = Point(lon, lat)
	
	if not inCountry(tmp_pnt):
		print("point not in country")
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
			print("buffer value could not be converted to int")
			return 0

	elif lookup[code]["type"] == "adm":
		try:
			tmp_int = int(lookup[code]["data"])
			return getPolyWithin(tmp_pnt, adm_shps[tmp_int])

		except:
			print("adm value could not be converted to int")
			return 0

	else:
		print("code type not recognized")
		return 0


# returns geometry for point
def geomVal(agg_type, code, lon, lat):
	if agg_type in agg_types:

		tmp_geom = getGeom(str(code), lon, lat)

		if tmp_geom != 0:
			return tmp_geom

		return "None"

	elif agg_type == "country":

		return adm0

	else:
		print("agg_type not recognized: " + str(agg_type))
		return "None"


# --------------------------------------------------


# random point gen function
def get_random_point_in_polygon(poly):

	INVALID_X = -9999
	INVALID_Y = -9999

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


# ====================================================================================================

print("FINISHED FUNCTIONS")

# --------------------------------------------------
# load shapefiles

# must start at and inlcude ADM0
# all additional ADM shps must be included so that adm_path index corresponds to adm level
adm_paths = []
adm_paths.append(base+"/countries/"+country+"/shapefiles/ADM0/"+abbr+"_adm0.shp")
adm_paths.append(base+"/countries/"+country+"/shapefiles/ADM1/"+abbr+"_adm1.shp")
adm_paths.append(base+"/countries/"+country+"/shapefiles/ADM2/"+abbr+"_adm2.shp")


# --------------------------------------------------
# create point grid for country

# get adm0 bounding box
adm_shps = [shapefile.Reader(adm_path).shapes() for adm_path in adm_paths]

adm0 = shape(adm_shps[0][0])

# country bounding box
(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = adm0.bounds
# print( (adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) )

# bounding box rounded to pixel size (always increases bounding box size, never decreases)
(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = (math.floor(adm0_minx*psi)/psi, math.floor(adm0_miny*psi)/psi, math.ceil(adm0_maxx*psi)/psi, math.ceil(adm0_maxy*psi)/psi)
# print( (adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) )

# generate arrays of new grid x and y values
cols = np.arange(adm0_minx, adm0_maxx+pixel_size*0.5, pixel_size)
rows = np.arange(adm0_maxy, adm0_miny-pixel_size*0.5, -1*pixel_size)

# print cols
# print rows

# init grid reference object 
gref = {}
idx = 0
for r in rows:
	gref[str(r)] = {}
	for c in cols:
		# build grid reference object
		gref[str(r)][str(c)] = idx
		idx += 1


print("FINISHED GRID")

# ====================================================================================================
# data prep


# --------------------------------------------------
# load project data

merged = getData(base+"/countries/"+country+"/data", "project_id", (code_field, "project_location_id"))


# --------------------------------------------------
# filters

# apply filters to project data
# 

# --------------------------------------------------
# generate random dollars

# create dataframe for use in iterations
column_list = ['iter', 'id', 'ran_dollars']
dollar_table = pd.DataFrame(columns=column_list)

# create copy of merged project data
i_m = deepcopy(merged)

# add new column of random numbers (0-1)
i_m['ran_num'] = (pd.Series(np.random.random(len(i_m)))).values

# group merged table by project ID for the sum of each project ID's random numbers
grouped_random_series = i_m.groupby('project_id')['ran_num'].sum()


# get location count for each project
i_m['ones'] = (pd.Series(np.ones(len(i_m)))).values
grouped_location_count = i_m.groupby('project_id')['ones'].sum()

# create new empty dataframe
df_group_random = pd.DataFrame()

# add grouped random 'Series' to the newly created 'Dataframe' under a new grouped_random column
df_group_random['grouped_random'] = grouped_random_series
# add location count series to dataframe
df_group_random['location_count'] = grouped_location_count

# add the series index, composed of project IDs, as a new column called project_ID
df_group_random['project_id'] = df_group_random.index


# now that we have project_ID in both the original merged 'Dataframe' and the new 'Dataframe' they can be merged
i_m = i_m.merge(df_group_random, on='project_id')

# calculate the random dollar ammount per point for each entry
i_m['random_dollars_pp'] = (i_m.ran_num / i_m.grouped_random) * i_m[aid_field]

# aid field value split evenly across all project locations based on location count
i_m['split_dollars_pp'] = (i_m[aid_field] / i_m.location_count)


print("FINISHED DATA")

# --------------------------------------------------
# assign geometries

# add geom columns
i_m["agg_type"] = ["None"] * len(i_m)
i_m["agg_geom"] = ["None"] * len(i_m)

i_m.agg_type = i_m.apply(lambda x: geomType(x[is_geocoded], x[code_field]), axis=1)
i_m.agg_geom = i_m.apply(lambda x: geomVal(x.agg_type, x[code_field], x.longitude, x.latitude), axis=1)

i_mx = i_m.loc[i_m.agg_geom != "None"].copy(deep=True)


# i_mx['index'] = i_mx['project_location_id']
i_mx['unique'] = range(0, len(i_mx))
i_mx['index'] = range(0, len(i_mx))
i_mx = i_mx.set_index('index')

print("FINISHED GEOMS")

# ====================================================================================================
# master init


# if rank == 0:

results_str = "Monte Carlo Rasterization Output File\t "

results_str += "\nstart time\t" + str(Ts)
results_str += "\ncountry\t" + str(country)
results_str += "\nabbr\t" + str(abbr)
results_str += "\npixel_size\t" + str(pixel_size)
results_str += "\niterations\t" + str(iterations)
results_str += "\nnodata\t" + str(nodata)
results_str += "\nsubset\t" + str(subset)
results_str += "\naid_field\t" + str(aid_field)
results_str += "\ncode_field\t" + str(code_field)
results_str += "\ncountry bounds\t" + str((adm0_minx, adm0_miny, adm0_maxx, adm0_maxy))

results_str += "\nrows\t" + str(len(rows))
results_str += "\ncolumsn\t" + str(len(cols))
results_str += "\nlocations\t" + str(len(i_mx))

# --------------------------------------------------
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

# --------------------------------------------------
# build output directories

# creates directories 
def make_dir(path):
	try: 
		os.makedirs(path)
	except OSError as exception:
		if exception.errno != errno.EEXIST:
			raise

dir_country = base+"/outputs/"+country
dir_working = dir_country+"/"+country+"_"+subset+"_"+str(pixel_size)+"_"+str(iterations)+"_"+str(int(Ts))

make_dir(dir_working)


print("FINISHED MASTER INIT")


# ====================================================================================================
# ====================================================================================================

# comm.Barrier()
# sys.exit("! - init only")

# ====================================================================================================
# ====================================================================================================
# mpi prep


# terminate if master init fails
# 

# Define MPI message tags
tags = enum('READY', 'DONE', 'EXIT', 'START', 'ERROR')

# init for later
sum_mean_surf = 0


# ====================================================================================================
# generate mean surface raster


print("STARTING SURF...")


all_mean_surf = []
unique_ids = i_mx['unique']

# track_np = 0

for task in unique_ids:

	# print("NEW TASK")

	mean_surf = np.zeros((int(idx+1),), dtype=np.int)

	# poly grid pixel size and poly grid pixel size inverse
	# poly grid pixel size is 1 order of magnitude higher resolution than output pixel_size
	pg_pixel_size = pixel_size * 0.1
	pg_psi = 1/pg_pixel_size


	pg_data = i_mx.loc[task]
	# print("task: "+str(task)+" and type: "+str(pg_data.agg_type))
	pg_type = pg_data.agg_type


	if pg_type == "adm" or pg_type == "buffer": #(pg_type != "point" and pg_type in agg_types) or pg_type == "country" and track_np < 2:
		
		# track_np += 1
		# if track_np > 2:
			# sys.exit()

		# print(pg_type)

		# for each row generate grid based on bounding box of geometry

		pg_geom = pg_data.agg_geom	

		(pg_minx, pg_miny, pg_maxx, pg_maxy) = pg_geom.bounds
		# print( (pg_minx, pg_miny, pg_maxx, pg_maxy) )

		(pg_minx, pg_miny, pg_maxx, pg_maxy) = (math.floor(pg_minx*pg_psi)/pg_psi, math.floor(pg_miny*pg_psi)/pg_psi, math.ceil(pg_maxx*pg_psi)/pg_psi, math.ceil(pg_maxy*pg_psi)/pg_psi)
		# print( (pg_minx, pg_miny, pg_maxx, pg_maxy) )

		pg_cols = np.arange(pg_minx, pg_maxx+pg_pixel_size*0.5, pg_pixel_size)
		pg_rows = np.arange(pg_maxy, pg_miny-pg_pixel_size*0.5, -1*pg_pixel_size)

		# print(pg_cols)
		# print(pg_rows)
		# evenly split the aid for that row (i_mx['split_dollars_pp'] field) among new grid points

		# full poly grid reference object and count
		pg_gref = {}
		pg_idx = 0

		# poly grid points within actual geom and count
		# pg_in = {}
		pg_count = 0
		
		for r in pg_rows:
			pg_gref[str(r)] = {}
			for c in pg_cols:
				# build grid reference object
				# pg_gref[str(r)][str(c)] = pg_idx
				pg_idx += 1

				# check if point is within geom
				pg_point = Point(c,r)

				pg_within = pg_point.within(pg_geom)
				# print(pg_within)

				# print(" ~~~~~~~~~~~~~~ "+str(pg_point) +" "+ str(pg_within))

				# track_np += 1
				# if track_np > 70:
				# 	sys.exit()


				if pg_within:
					pg_gref[str(r)][str(c)] = pg_idx
					pg_count += 1
				else:
					pg_gref[str(r)][str(c)] = "None"

				# print(pg_gref[str(r)][str(c)])

		# npa_poly = np.zeros((int(pg_idx+1),), dtype=np.int)

		# init grid reference object
		# pg_gref = {}
		for r in pg_rows:
			# pg_gref[str(r)] = {}
			for c in pg_cols:
				# build grid reference object

				if pg_gref[str(r)][str(c)] != "None":
					# round new grid points to old grid points and update old grid
					gref_id = gref[str(round(r * psi) / psi)][str(round(c * psi) / psi)]
					mean_surf[gref_id] += pg_data['split_dollars_pp'] / pg_count
					# print( pg_data['split_dollars_pp'] / pg_count)
					# print("\n\n\t\t~~~~~~~~~~~~~~ "+str(gref_id)+" "+str(mean_surf[gref_id])+" "+str(pg_data['split_dollars_pp'] / pg_count) +"\n\n")

		# print(mean_surf)

	elif pg_type == "point":

		# round new grid points to old grid points and update old grid
		gref_id = gref[str(round(pg_data.latitude * psi) / psi)][str(round(pg_data.longitude * psi) / psi)]
		mean_surf[gref_id] += pg_data['split_dollars_pp']


	all_mean_surf.append(mean_surf)




# calc results
print("Mean Surf Master calcing")

stack_mean_surf = np.vstack(all_mean_surf)
sum_mean_surf = np.sum(stack_mean_surf, axis=0)


# write asc file

sum_mean_surf_str = ' '.join(np.char.mod('%f', sum_mean_surf))
asc_sum_mean_surf_str = asc + sum_mean_surf_str

fout_sum_mean_surf = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_"+str(iterations)+"_sum_mean_surf.asc", "w")
fout_sum_mean_surf.write(asc_sum_mean_surf_str)



print("Mean Surf Master finishing")

T_surf = time.time()
Tloc = int(T_surf - Ts)

results_str += "\nMean Surf Runtime\t" + str(Tloc//60) +'m '+ str(int(Tloc%60)) +'s'

print('\t\tMean Surf  Runtime: ' + str(Tloc//60) +'m '+ str(int(Tloc%60)) +'s')
print('\n')
