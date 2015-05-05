# 
# runscript.py
# 


# ====================================================================================================

from __future__ import print_function

from mpi4py import MPI

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
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
status = MPI.Status()

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

code_field = "precision_code"

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


# --------------------------------------------------
# load project data

# check csv delim and return if valid type
def getCSV(path):
	if path.endswith('.tsv'):
		return pd.read_csv(path, sep='\t', quotechar='\"')
	elif path.endswith('.csv'):
		return pd.read_csv(path, quotechar='\"')
	else:
		sys.exit('getCSV - file extension not recognized.\n')


def getData(path, merge_id, field_id):

	amp_path = path+"/projects.tsv"
	loc_path = path+"/locations.tsv"

	# make sure files exist
	# 

	# read input csv files into memory
	amp = getCSV(amp_path)
	loc = getCSV(loc_path)


	if not merge_id in amp or not merge_id in loc:
		sys.exit("getData - required merge_id field not found in amp or loc files")

	amp[merge_id] = amp[merge_id].astype(str)
	loc[merge_id] = loc[merge_id].astype(str)

	# create projectdata by merging amp and location files by project_id
	tmp_merged = loc.merge(amp, on=merge_id)

	if not field_id in tmp_merged or not "longitude" in tmp_merged or not "latitude" in tmp_merged:
		sys.exit("getData - required code field not found")

	return tmp_merged


merged = getData(base+"/countries/"+country+"/data", "project_id", code_field)


# --------------------------------------------------
# filters

# apply filters to project data
# 


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

(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = adm0.bounds
# print( (adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) )

(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = (math.floor(adm0_minx*psi)/psi, math.floor(adm0_miny*psi)/psi, math.ceil(adm0_maxx*psi)/psi, math.ceil(adm0_maxy*psi)/psi)
# print( (adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) )

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


# --------------------------------------------------
# defin tags enum

def enum(*sequential, **named):
    # source: http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
	enums = dict(zip(sequential, range(len(sequential))), **named)
	return type('Enum', (), enums)


# Define MPI message tags
tags = enum('READY', 'DONE', 'EXIT', 'START', 'ERROR')


# ====================================================================================================
# master init


if rank == 0:

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


	dir_working = base+"/outputs/"+country+"/"+country+"_"+subset+"_"+str(pixel_size)+"_"+str(iterations)+"_"+str(int(Ts))

	make_dir(dir_working)


# make sure workers do not proceed until master inits
comm.Barrier()

# terminate if master init fails
# 


# ====================================================================================================
# mpi stuff
# structured based on https://github.com/jbornschein/mpi4py-examples/blob/master/09-task-pull.py


if rank == 0:

	# ==================================================
	# MASTER START STUFF

	print("starting iterations - %d to be run)" % iterations)

	total_aid = [] # [0] * iterations
	total_count = [] # [0] * iterations
	
	# ==================================================
	
	task_index = 0
	num_workers = size - 1
	closed_workers = 0
	err_status = 0
	print("Master starting with %d workers" % num_workers)

	# distribute work
	while closed_workers < num_workers:
		data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
		source = status.Get_source()
		tag = status.Get_tag()

		if tag == tags.READY:
			# Worker is ready, so send it a task
			if task_index < len(i_control):
				comm.send(i_control[task_index], dest=source, tag=tags.START)
				print("Sending task %d to worker %d" % (task_index, source))
				task_index += 1
			else:
				comm.send(None, dest=source, tag=tags.EXIT)

		elif tag == tags.DONE:

			# ==================================================
			# MASTER MID STUFF
			
			total_aid.append(data[0])
			total_count.append(data[1])
			print("Got data from worker %d" % source)

			# ==================================================

		elif tag == tags.EXIT:
			print("Worker %d exited." % source)
			closed_workers += 1

		elif tag == tags.ERROR:
			print("Error reported by worker %d ." % source)
			# broadcast error to all workers
			# 
			# make sure they all get message and terminate
			# 
			err_status = 1
			break

	# ==================================================
	# MASTER END STUFF

	if err_status == 0:
		# calc results
		print("Master calcing")

		stack_aid = np.vstack(total_aid)
		std_aid = np.std(stack_aid, axis=0)
		mean_aid = np.mean(stack_aid, axis=0)

		stack_count = np.vstack(total_count)
		std_count = np.std(stack_count, axis=0)
		mean_count = np.mean(stack_count, axis=0)


		# write asc files

		std_aid_str = ' '.join(np.char.mod('%f', std_aid))
		asc_std_aid_str = asc + std_aid_str

		fout_std_aid = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_"+str(iterations)+"_std_aid.asc", "w")
		fout_std_aid.write(asc_std_aid_str)


		mean_aid_str = ' '.join(np.char.mod('%f', mean_aid))
		asc_mean_aid_str = asc + mean_aid_str

		fout_mean_aid = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_"+str(iterations)+"_mean_aid.asc", "w")
		fout_mean_aid.write(asc_mean_aid_str)


		std_count_str = ' '.join(np.char.mod('%f', std_count))
		asc_std_count_str = asc + std_count_str

		fout_std_count = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_"+str(iterations)+"_std_count.asc", "w")
		fout_std_count.write(asc_std_count_str)


		mean_count_str = ' '.join(np.char.mod('%f', mean_count))
		asc_mean_count_str = asc + mean_count_str

		fout_mean_count = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_"+str(iterations)+"_mean_count.asc", "w")
		fout_mean_count.write(asc_mean_count_str)


		print("Master finishing")

		Tloc = int(time.time() - Ts)
		# T_iter_avg = Tloc/iterations

		print('\n\tRun Results:')
		# print '\t\tAverage Iteration Time: ' + str(T_iter_avg//60) +'m '+ str(int(T_iter_avg%60)) +'s'
		print('\t\tTotal Runtime: ' + str(Tloc//60) +'m '+ str(int(Tloc%60)) +'s')
		print('\n')

	else:
		print("Master terminating due to worker error.")


	# ==================================================


else:
	# Worker processes execute code below
	name = MPI.Get_processor_name()
	print("I am a worker with rank %d on %s." % (rank, name))
	while True:
		comm.send(None, dest=0, tag=tags.READY)
		task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
		tag = status.Get_tag()

		if tag == tags.START:

			# ==================================================
			# WORKER STUFF


			# print("iter %d: starting" % itx)

			# initialize mean and count grids with zeros
			npa_aid = np.zeros((int(idx+1),), dtype=np.int)
			npa_count = np.zeros((int(idx+1),), dtype=np.int)

			# --------------------------------------------------
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


			# print("iter %d: random dollar calc complete" % itx)


			# --------------------------------------------------
			# assign geometries


			def geomType(code):
				if str(code) in lookup:
					tmp_type = lookup[str(code)]["type"]
					return tmp_type

				else:
					print("code not recognized")
					return "None"


			def getPolyWithin(item, polys):
				c = 0
				for shp in polys:
					tmp_shp = shape(shp)
					if item.within(tmp_shp):
						return tmp_shp

				return c


			# depends on adm0
			def inCountry(shp):
				return shp.within(adm0)


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


			def geomVal(agg_type, code, lon, lat):
				if agg_type in agg_types:

					tmp_geom = getGeom(str(code), lon, lat)

					if tmp_geom != 0:
						return tmp_geom

					return "None"

				else:
					print("agg_type not recognized")
					return "None"



			# add geom columns
			i_m["agg_type"] = ["None"] * len(i_m)
			i_m["agg_geom"] = ["None"] * len(i_m)

			i_m.agg_type = i_m.apply(lambda x: geomType(x[code_field]), axis=1)
			i_m.agg_geom = i_m.apply(lambda x: geomVal(x.agg_type, x[code_field], x.longitude, x.latitude), axis=1)

			i_mx = i_m.loc[i_m.agg_geom != "None"].copy(deep=True)

			# print("iter %d: get geom complete" % itx)


			# --------------------------------------------------
			# assign random points


			# add point gen function
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


			# add random points column to table
			i_mx["rnd_pt"] = [0] * len(i_mx)
			i_mx.rnd_pt = i_mx.apply(lambda x: addPt(x.agg_type, x.agg_geom), axis=1)

			# round rnd_pts to match point grid
			i_mx = i_mx.merge(i_mx.rnd_pt.apply(lambda s: pd.Series({'rnd_x':(round(s.x * psi) / psi), 'rnd_y':(round(s.y * psi) / psi)})), left_index=True, right_index=True)

			# print("iter %d: rnd pts complete" % itx)


			# --------------------------------------------------
			# add results to output arrays


			# add commitment value for each rnd pt to grid value
			for i in i_mx.iterrows():
				nx = str(i[1].rnd_x)
				ny = str(i[1].rnd_y)
				# print nx, ny, op[nx][ny]
				if int(i[1].random_dollars_pp) > 0:
					npa_aid[op[ny][nx]] += int(i[1].random_dollars_pp)
					npa_count[op[ny][nx]] += int(1)

			# print("iter %d: mean and count array fill complete" % itx)

			# --------------------------------------------------
			# send np arrays back to master

			npa_result = np.array([npa_aid,npa_count])
			comm.send(npa_result, dest=0, tag=tags.DONE)

			# ==================================================

		elif tag == tags.EXIT:
			comm.send(None, dest=0, tag=tags.EXIT)
			break

		elif tag == tags.ERROR:
			print("Error message from master. Shutting down." % source)
			# confirm error message received
			# 
			# terminate process
			# 
			break

