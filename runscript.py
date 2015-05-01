# 
# runscript.py
# 


# ====================================================================================================


from mpi4py import MPI

import os
import sys
import errno
from copy import deepcopy
import time
import random

import numpy as np
import pandas as pd
from shapely.geometry import Polygon, Point, shape, box
import shapefile


# ====================================================================================================
# init


Ts = time.time()

# python /path/to/runscript.py nepal NPL 0.1
arg = sys.argv

try:
	country = sys.argv[1]
	abbr = sys.argv[2]
	pixel_size = float(sys.argv[3]) # 0.025
	iterations = int(sys.argv[4]) # 2

except:
	sys.exit("invalid inputs")

# examples of valid pixel sizes: 1.0, 0.5, 0.25, 0.2, 0.1, 0.05, 0.025, ...

if (1/pixel_size) != int(1/pixel_size):
	sys.exit("invalid pixel size: "+str(pixel_size))

# pixel size inverse
psi = 1/pixel_size



comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()

# -----------------------------------------------

# nodata value for output raster
nodata = -9999

# subset / filter
subset = "all"
# sector_codes = arg[5]
# type(sector_codes)

# iterations range
i_control = range(int(iterations))

aid_field = "total_commitments"

# -----------------------------------------------

code_field = "precision_code"

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

# -----------------------------------------------
# make sure all files exist

# absolute path to script directory
base = os.path.dirname(os.path.abspath(__file__))


# -----------------------------------------------
# load project data

amp_path = base+"/countries/"+country+"/data/projects.tsv"
loc_path = base+"/countries/"+country+"/data/locations.tsv"

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


if not code_field in merged or not "longitude" in merged or not "latitude" in merged:
	sys.exit("required fields not found")


# -----------------------------------------------

# apply filters to project data
# 


# -----------------------------------------------
# load shapefiles

# must start at and inlcude ADM0
# all additional ADM shps must be included so that adm_path index corresponds to adm level
adm_paths = []
adm_paths.append(base+"/countries/"+country+"/shapefiles/ADM0/"+abbr+"_adm0.shp")
adm_paths.append(base+"/countries/"+country+"/shapefiles/ADM1/"+abbr+"_adm1.shp")
adm_paths.append(base+"/countries/"+country+"/shapefiles/ADM2/"+abbr+"_adm2.shp")


# -----------------------------------------------
# build output directories

# creates directories 
def make_dir(path):
	try: 
		os.makedirs(path)
	except OSError as exception:
		if exception.errno != errno.EEXIST:
			raise


dir_working = base+"/outputs/"+country+"/"+country+"_"+subset+"_"+str(pixel_size)+"_"+str(iterations)+"_"+str(int(Ts))

make_dir(dir_working)


# ====================================================================================================
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

# print cols
# print rows

# create grid based on output resolution (pixel size) 
op = {}

idx = 0
for r in rows:
	op[str(r)] = {}
	for c in cols:
		# build grid reference object
		op[str(r)][str(c)] = idx
		idx += 1


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


# ====================================================================================================
# master init

# if rank == 0:
print "starting iterations ("+str(iterations)+" to be run)"
total_aid = [] # [0] * iterations
total_count = [] # [0] * iterations



# ====================================================================================================
# mpi stuff

comm.Barrier()

# distribute iterations to processes
for itx in i_control:


	# ====================================================================================================
	# start individual process

	print "iter "+str(itx)+": starting"

	# initialize mean and count grids with zeros
	npa_aid = np.zeros((int(idx+1),), dtype=np.int)
	npa_count = np.zeros((int(idx+1),), dtype=np.int)

	# ====================================================================================================
	# generate random dollars

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


	print len(i_m)
	print "iter "+str(itx)+": random dollar calc complete"


	# ====================================================================================================
	# assign geometries


	agg_types = ["point","buffer","adm"]


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

	print "iter "+str(itx)+": get geom complete"


	i_mx = i_m.loc[i_m.agg_geom != "None"].copy(deep=True)

	print "iter "+str(itx)+": remove empty geoms complete"


	# ====================================================================================================
	# assign random points


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

	print "iter "+str(itx)+": get rnd pts complete"


	# round rnd_pts to match point grid
	i_mx = i_mx.merge(i_mx.rnd_pt.apply(lambda s: pd.Series({'rnd_x':(round(s.x * psi) / psi), 'rnd_y':(round(s.y * psi) / psi)})), left_index=True, right_index=True)


	# ====================================================================================================
	# add results to output arrays


	# add commitment value for each rnd pt to grid value
	for i in i_mx.iterrows():
		nx = str(i[1].rnd_x)
		ny = str(i[1].rnd_y)
		# print nx, ny, op[nx][ny]
		if int(i[1].random_dollars_pp) > 0:
			npa_aid[op[ny][nx]] += int(i[1].random_dollars_pp)
			npa_count[op[ny][nx]] += int(1)

	print "iter "+str(itx)+": mean and count array fill complete"


	# -----------------------------------------------
	# write individual asc file *** TEMP ***

	# npa_aid_str = ' '.join(np.char.mod('%f', npa_aid))
	# asc_mean_str = asc + npa_aid_str

	# fout_mean = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_cx"+str(itx)+"_mean.asc", "w")
	# fout_mean.write(asc_mean_str)

	# npa_count_str = ' '.join(np.char.mod('%f', npa_count))
	# asc_count_str = asc + npa_count_str

	# fout_count = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_cx"+str(itx)+"_count.asc", "w")
	# fout_count.write(asc_count_str)

	# print "iter "+str(itx)+": update and write grids to asc's complete"


	# -----------------------------------------------
	# misc debug

	# print op
	# print i_mx
	# print len(i_mx)

	# -----------------------------------------------
	# send np array back to master

	# master adds np array to total arrays
	total_aid.append(npa_aid)
	total_count.append(npa_count)


# ====================================================================================================
# end individual processes stuff


# wait for all processes to finish
comm.Barrier()


# ====================================================================================================
# calculate array statistics


# if rank == 0 ...

stack_aid = np.vstack(total_aid)
std_aid = np.std(stack_aid, axis=0)
mean_aid = np.mean(stack_aid, axis=0)

stack_count = np.vstack(total_count)
std_count = np.std(stack_count, axis=0)
mean_count = np.mean(stack_count, axis=0)


# write asc files

std_aid_str = ' '.join(np.char.mod('%f', std_aid))
asc_std_aid_str = asc + std_aid_str

fout_std_aid = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_cx"+str(itx)+"_std_aid.asc", "w")
fout_std_aid.write(asc_std_aid_str)


mean_aid_str = ' '.join(np.char.mod('%f', mean_aid))
asc_mean_aid_str = asc + mean_aid_str

fout_mean_aid = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_cx"+str(itx)+"_mean_aid.asc", "w")
fout_mean_aid.write(asc_mean_aid_str)


std_count_str = ' '.join(np.char.mod('%f', std_count))
asc_std_count_str = asc + std_count_str

fout_std_count = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_cx"+str(itx)+"_std_count.asc", "w")
fout_std_count.write(asc_std_count_str)


mean_count_str = ' '.join(np.char.mod('%f', mean_count))
asc_mean_count_str = asc + mean_count_str

fout_mean_count = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_cx"+str(itx)+"_mean_count.asc", "w")
fout_mean_count.write(asc_mean_count_str)


# ======================================================================================================
# clean up and close


# 

Tloc = int(time.time() - Ts)
T_iter_avg = Tloc/iterations

print '\n\tRun Results:'
print '\t\tAverage Iteration Time: ' + str(T_iter_avg//60) +'m '+ str(int(T_iter_avg%60)) +'s'
print '\t\tTotal Runtime: ' + str(Tloc//60) +'m '+ str(int(Tloc%60)) +'s'
print '\n'
