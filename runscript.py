#
# runscript.py
#


# ====================================================================================================
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

import json
import hashlib

import numpy as np
import pandas as pd
from shapely.geometry import Polygon, Point, shape, box
import shapefile

import pyproj
from functools import partial
from shapely.ops import transform
import geopandas as gpd

# ====================================================================================================
# ====================================================================================================
# general init


# mpi info
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
status = MPI.Status()

# absolute path to script directory
dir_base = os.path.dirname(os.path.abspath(__file__))


# ====================================================================================================
# ====================================================================================================
# inputs and variables


run_stage = "beta"
run_version_str = "007"
run_version = int(run_version_str)
run_id = run_stage[0:1] + run_version_str

Ts = int(time.time())
random_id = '{0:05d}'.format(int(random.random() * 10**5))
Rid = "msr_" + str(Ts) +"_"+ random_id

Rid = "msr_" + str(Ts) +"_"+ "56789"


# --------------------------------------------------
# input arguments

# python /path/to/runscript.py nepal NPL 0.1 10
arg = sys.argv

try:
    # country = "nepal"
    # data_version = "1.1"
    # pixel_size = 0.1
    # sector = "Agriculture"

    country = sys.argv[1]
    data_version = sys.argv[2]
    pixel_size = float(sys.argv[3])
    # raw_filter = sys.argv[4]
    sector = sys.argv[4]

except:
    sys.exit("invalid inputs")


# --------------------------------------------------
# validate country and data_version

abbrvs = {
    "drc": "COD",
    "honduras": "HND",
    "nepal": "NPL",
    "nigeria": "NGA",
    "senegal": "SEN",
    "timor-leste": "TLS",
    "uganda": "UGA"
}

if country not in abbrvs.keys() or not os.path.isdir(dir_base+"/countries/"+country):
    sys.exit("invalid country: " + country)

abbr = abbrvs[country]


version_path =  dir_base+"/countries/"+country+"/versions/"+country+"_"+data_version

if not os.path.isdir(version_path):
    sys.exit("invalid data_version: " + data_version)


# --------------------------------------------------
# validate pixel size

# check for valid pixel size
# examples of valid pixel sizes: 1.0, 0.5, 0.25, 0.2, 0.1, 0.05, 0.025, ...
if (1/pixel_size) != int(1/pixel_size):
    sys.exit("invalid pixel size: "+str(pixel_size))

# pixel size inverse
psi = 1/pixel_size


# --------------------------------------------------
# validate and build filter options

# filter_type = "all"
# filters_type = "specfic"

# filters = {
#     "ad_sector_names": {
#         "Agriculture": 0
#     }
# }


# --------------------------------------------------
# vars to potentially be added as inputs
# not used by functions

# nodata value for output raster
nodata = -9999

# field name for aid values
aid_field = "total_commitments"

# boolean field identifying if project is geocoded
is_geocoded = "is_geocoded"

# when True, only use geocoded data
only_geocoded = False


# --------------------------------------------------
# vars that may be added as some type of input
# used by functions

# agg_type definition for non geocoded projects
# either allocated at country level ("country") or ignored ("None")
not_geocoded = "country"

if only_geocoded:
    not_geocoded = "None"


# fields name associated with values in lookup dict
code_field = "precision_code"

# aggregation types used in lookup dict
agg_types = ["point", "buffer", "adm"]

# code field values
# buffer values in meters
lookup = {
    "1": {"type":"point","data":0},
    "2": {"type":"buffer","data":20000},
    "3": {"type":"adm","data":"2"},
    "4": {"type":"adm","data":"2"},
    "5": {"type":"buffer","data":20000},
    "6": {"type":"adm","data":"0"},
    "7": {"type":"adm","data":"0"},
    "8": {"type":"adm","data":"0"}
}

# --------------------------------------------------
# file paths

# dir_country = dir_base+"/outputs/"+country
# dir_working = dir_country+"/"+country+"_"+str(pixel_size)+"_"+str(iterations)+"_"+str(int(Ts))

dir_country = dir_base+"/data/"+country
dir_chain = dir_country+"/"+country+"_"+str(data_version)+"_"+run_id+"_"+str(pixel_size)
dir_outputs = dir_chain+"/outputs"
dir_working = dir_outputs+"/"+str(Rid)


# ====================================================================================================
# ====================================================================================================
# functions


def json_hash(hash_obj):
    hash_json = json.dumps(hash_obj, sort_keys = True, ensure_ascii = False)
    hash_builder = hashlib.md5()
    hash_builder.update(hash_json)
    hash_md5 = hash_builder.hexdigest()
    return hash_md5


# creates directories
def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


# --------------------------------------------------


# check csv delim and return if valid type
def getCSV(path):
    if path.endswith('.tsv'):
        return pd.read_csv(path, sep='\t', quotechar='\"', na_values='', keep_default_na=False)
    elif path.endswith('.csv'):
        return pd.read_csv(path, quotechar='\"', na_values='', keep_default_na=False)
    else:
        sys.exit('getCSV - file extension not recognized.\n')


# get project and location data in path directory
# requires a field name to merge on and list of required fields
def getData(path, merge_id, field_ids, only_geo):

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
    if only_geo:
        tmp_merged = amp.merge(loc, on=merge_id)
    else:
        tmp_merged = amp.merge(loc, on=merge_id, how="left")

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
# depends on lookup and not_geocoded
def geomType(is_geo, code):

    try:
        is_geo = int(is_geo)
        code = str(int(code))

        if is_geo == 1:
            if code in lookup:
                tmp_type = lookup[code]["type"]
                return tmp_type

            else:
                print("lookup code not recognized: " + code)
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
            # get buffer size (meters)
            tmp_int = float(lookup[code]["data"])

            # reproject point
            proj_utm = pyproj.Proj('+proj=utm +zone=45 +ellps=WGS84 +datum=WGS84 +units=m +no_defs ')
            proj_wgs = pyproj.Proj(init="epsg:4326")

            utm_pnt_raw = pyproj.transform(proj_wgs, proj_utm, tmp_pnt.x, tmp_pnt.y)
            utm_pnt_act = Point(utm_pnt_raw)

            # create buffer in meters
            utm_buffer = utm_pnt_act.buffer(tmp_int)

            # reproject back
            buffer_proj = partial(pyproj.transform, proj_utm, proj_wgs)
            tmp_buffer = transform(buffer_proj, utm_buffer)

            # clip buffer if it extends outside country
            if inCountry(tmp_buffer):
                return tmp_buffer
            else:
                return tmp_buffer.intersection(adm0)

        except:
            print("buffer value could not be converted to float")
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
# depends on agg_types and adm0
def geomVal(agg_type, code, lon, lat):
    if agg_type in agg_types:

        code = str(int(code))
        tmp_geom = getGeom(code, lon, lat)

        if tmp_geom != 0:
            return tmp_geom

        return "None"

    elif agg_type == "country":

        return adm0

    else:
        print("agg_type not recognized: " + str(agg_type))
        return "None"


# ====================================================================================================
# ====================================================================================================


# --------------------------------------------------
# load shapefiles

# must start at and inlcude ADM0
# all additional ADM shps must be included so that adm_path index corresponds to adm level
adm_paths = []
adm_paths.append(dir_base+"/countries/"+country+"/shps/"+abbr+"_adm0.shp")
adm_paths.append(dir_base+"/countries/"+country+"/shps/"+abbr+"_adm1.shp")
adm_paths.append(dir_base+"/countries/"+country+"/shps/"+abbr+"_adm2.shp")

# get adm0 bounding box
adm_shps = [shapefile.Reader(adm_path).shapes() for adm_path in adm_paths]

# define country shape
adm0 = shape(adm_shps[0][0])


# --------------------------------------------------
# create point grid for country

# country bounding box
(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = adm0.bounds
# print( (adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) )

# grid_buffer
gb = 0.5

# bounding box rounded to pixel size (always increases bounding box size, never decreases)
(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = (math.floor(adm0_minx*gb)/gb, math.floor(adm0_miny*gb)/gb, math.ceil(adm0_maxx*gb)/gb, math.ceil(adm0_maxy*gb)/gb)
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


# --------------------------------------------------
# load project data

dir_data = dir_base+"/countries/"+country+"/versions/"+country+"_"+str(data_version)+"/data"

merged = getData(dir_data, "project_id", (code_field, "project_location_id"), only_geocoded)


# --------------------------------------------------
# misc data prep

# create copy of merged project data
# i_m = deepcopy(merged)

# get location count for each project
merged['ones'] = (pd.Series(np.ones(len(merged)))).values

# get project location count
grouped_location_count = merged.groupby('project_id')['ones'].sum()


# create new empty dataframe
df_location_count = pd.DataFrame()

# add location count series to dataframe
df_location_count['location_count'] = grouped_location_count

# add project_id field
df_location_count['project_id'] = df_location_count.index

# merge location count back into data
merged = merged.merge(df_location_count, on='project_id')

# aid field value split evenly across all project locations based on location count
merged[aid_field].fillna(0, inplace=True)
merged['split_dollars_pp'] = (merged[aid_field] / merged.location_count)


# --------------------------------------------------
# filters


# filters_json = json.dumps(filters, sort_keys = True, ensure_ascii=False)
# filters_md5 = hashlib.md5()
# filters_md5.update(filters_json)
# filters_hash = filters_md5.hexdigest()

# generate filters hash
# filters_hash = json_hash(filters)


# apply filters to project data
filtered = merged.loc[merged.ad_sector_names == sector].copy(deep=True)

# !!! potential issue !!!
#
# - filters which remove only some locations from a project will skew aid splits
# - moved original project location count to before filters so that it can be used to
#   compare the count of project locations before filter to count after and generate
#   placeholder random values for the locations that were filtered out
# - method: recheck project location count and create placeholder random value if locations are missing
# - will need to rebuild how random num column is added. probaby can use apply with a new function

# filtered = deepcopy(merged)


# --------------------------------------------------
# assign geometries

# add geom columns
filtered["agg_type"] = ["None"] * len(filtered)
filtered["agg_geom"] = ["None"] * len(filtered)

filtered.agg_type = filtered.apply(lambda x: geomType(x[is_geocoded], x[code_field]), axis=1)
filtered.agg_geom = filtered.apply(lambda x: geomVal(x.agg_type, x[code_field], x.longitude, x.latitude), axis=1)

i_m = filtered.loc[filtered.agg_geom != "None"].copy(deep=True)


# i_m['index'] = i_m['project_location_id']
i_m['unique'] = range(0, len(i_m))
i_m['index'] = range(0, len(i_m))
i_m = i_m.set_index('index')


# ====================================================================================================
# ====================================================================================================
# master init


if rank == 0:

    # --------------------------------------------------
    # initialize results file output

    # results_str = "Mean Surface Rasters Output File\t "

    # results_str += "\nstart time\t" + str(Ts)
    # results_str += "\ncountry\t" + str(country)
    # results_str += "\nabbr\t" + str(abbr)
    # results_str += "\npixel_size\t" + str(pixel_size)
    # results_str += "\nnodata\t" + str(nodata)
    # results_str += "\naid_field\t" + str(aid_field)
    # results_str += "\ncode_field\t" + str(code_field)
    # results_str += "\ncountry bounds\t" + str((adm0_minx, adm0_miny, adm0_maxx, adm0_maxy))

    # results_str += "\nrows\t" + str(len(rows))
    # results_str += "\ncolumns\t" + str(len(cols))
    # results_str += "\nlocations\t" + str(len(i_m))

    # results_str += "\nfilters\t" + str(filters)


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

    make_dir(dir_working)


    # --------------------------------------------------
    # record init runtime

    time_init = time.time()
    T_init = int(time_init - Ts)

    # results_str += "\nInit Runtime\t" + str(T_init//60) +'m '+ str(int(T_init%60)) +'s'
    print('\tInit Runtime: ' + str(T_init//60) +'m '+ str(int(T_init%60)) +'s')


# ====================================================================================================
# ====================================================================================================

comm.Barrier()
# sys.exit("! - init only")

# ====================================================================================================
# ====================================================================================================
# mpi prep


# terminate if master init fails
#

# define MPI message tags
tags = enum('READY', 'DONE', 'EXIT', 'START', 'ERROR')

# init for later
sum_mean_surf = 0


# ====================================================================================================
# ====================================================================================================
# generate mean surface raster
# mpi comms structured based on https://github.com/jbornschein/mpi4py-examples/blob/master/09-task-pull.py


if rank == 0:

    # ==================================================
    # MASTER START STUFF

    all_mean_surf = []
    unique_ids = i_m['unique']

    # ==================================================

    task_index = 0
    num_workers = size - 1
    closed_workers = 0
    err_status = 0
    print("Surf Master - starting with %d workers" % num_workers)

    # distribute work
    while closed_workers < num_workers:
        data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()

        if tag == tags.READY:

            if task_index < len(unique_ids):

                #
                # !!!
                #   to do:
                #   if task if for a point (not point with small buffer, etc.)
                #   then let master do work
                #   run tests to see if this actually improves runtimes
                # !!!
                #

                comm.send(unique_ids[task_index], dest=source, tag=tags.START)
                print("Surf Master - sending task %d to worker %d" % (task_index, source))
                task_index += 1

            else:
                comm.send(None, dest=source, tag=tags.EXIT)

        elif tag == tags.DONE:

            # ==================================================
            # MASTER MID STUFF

            all_mean_surf.append(data)
            print("Surf Master - got surf data from worker %d" % source)

            # ==================================================

        elif tag == tags.EXIT:
            print("Surf Master - worker %d exited." % source)
            closed_workers += 1

        elif tag == tags.ERROR:
            print("Surf Master - error reported by surf worker %d ." % source)
            # broadcast error to all workers
            for i in range(1, size):
                comm.send(None, dest=i, tag=tags.ERROR)

            err_status = 1
            break

    # ==================================================
    # MASTER END STUFF

    if err_status == 0:
        # calc results
        print("Surf Master - processing results")

        stack_mean_surf = np.vstack(all_mean_surf)
        sum_mean_surf = np.sum(stack_mean_surf, axis=0)
        # save_mean_surf = dir_working+"/mean_surf.npy"
        # np.save(save_mean_surf, sum_mean_surf)

        # write asc file
        sum_mean_surf_str = ' '.join(np.char.mod('%f', sum_mean_surf))
        asc_sum_mean_surf_str = asc + sum_mean_surf_str
        fout_sum_mean_surf = open(dir_working+"/mean_surf.asc", "w")
        fout_sum_mean_surf.write(asc_sum_mean_surf_str)

    else:
        print("Surf Master - terminating due to worker error.")

    # ==================================================


else:
    # Worker processes execute code below
    name = MPI.Get_processor_name()
    print("Surf Worker - rank %d on %s." % (rank, name))
    while True:
        comm.send(None, dest=0, tag=tags.READY)
        task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        if tag == tags.START:

            # ==================================================
            # WORKER STUFF

            mean_surf = np.zeros((int(idx+1),), dtype=np.int)

            # poly grid pixel size and poly grid pixel size inverse
            # poly grid pixel size is 1 order of magnitude higher resolution than output pixel_size
            pg_pixel_size = pixel_size * 0.1
            pg_psi = 1/pg_pixel_size

            pg_data = i_m.loc[task]
            pg_type = pg_data.agg_type


            if (pg_type != "point" and pg_type in agg_types) or pg_type == "country":

                # for each row generate grid based on bounding box of geometry

                pg_geom = pg_data.agg_geom

                (pg_minx, pg_miny, pg_maxx, pg_maxy) = pg_geom.bounds
                # print( (pg_minx, pg_miny, pg_maxx, pg_maxy) )

                (pg_minx, pg_miny, pg_maxx, pg_maxy) = (math.floor(pg_minx*pg_psi)/pg_psi, math.floor(pg_miny*pg_psi)/pg_psi, math.ceil(pg_maxx*pg_psi)/pg_psi, math.ceil(pg_maxy*pg_psi)/pg_psi)
                # print( (pg_minx, pg_miny, pg_maxx, pg_maxy) )

                pg_cols = np.arange(pg_minx, pg_maxx+pg_pixel_size*0.5, pg_pixel_size)
                pg_rows = np.arange(pg_maxy, pg_miny-pg_pixel_size*0.5, -1*pg_pixel_size)

                # evenly split the aid for that row (i_m['split_dollars_pp'] field) among new grid points

                # full poly grid reference object and count
                pg_gref = {}
                pg_idx = 0

                # poly grid points within actual geom and count
                # pg_in = {}
                pg_count = 0

                for r in pg_rows:
                    pg_gref[str(r)] = {}

                    for c in pg_cols:
                        pg_idx += 1

                        # check if point is within geom
                        pg_point = Point(c,r)
                        pg_within = pg_point.within(pg_geom)

                        if pg_within:
                            pg_gref[str(r)][str(c)] = pg_idx
                            pg_count += 1
                        else:
                            pg_gref[str(r)][str(c)] = "None"


                # init grid reference object
                for r in pg_rows:
                    for c in pg_cols:
                        if pg_gref[str(r)][str(c)] != "None":
                            # round new grid points to old grid points and update old grid
                            gref_id = gref[str(round(r * psi) / psi)][str(round(c * psi) / psi)]
                            mean_surf[gref_id] += pg_data['split_dollars_pp'] / pg_count


            elif pg_type == "point":

                # round new grid points to old grid points and update old grid
                gref_id = gref[str(round(pg_data.latitude * psi) / psi)][str(round(pg_data.longitude * psi) / psi)]
                mean_surf[gref_id] += pg_data['split_dollars_pp']


            # --------------------------------------------------
            # send np arrays back to master

            comm.send(mean_surf, dest=0, tag=tags.DONE)

            # ==================================================

        elif tag == tags.EXIT:
            comm.send(None, dest=0, tag=tags.EXIT)
            break

        elif tag == tags.ERROR:
            print("Surf Worker - error message from Surf Master. Shutting down." % source)
            # confirm error message received and exit
            comm.send(None, dest=0, tag=tags.EXIT)
            break



if rank == 0:

    # validate sum_mean_surf
    # exit if validation fails
    if type(sum_mean_surf) == type(0):
        sys.exit("! - mean surf validation failed")

    # --------------------------------------------------

    time_surf = time.time()
    T_surf = int(time_surf - time_init)

    # results_str += "\nSurf Runtime\t" + str(T_surf//60) +'m '+ str(int(T_surf%60)) +'s'

    print('\tSurf Runtime: ' + str(T_surf//60) +'m '+ str(int(T_surf%60)) +'s')

    print('\n')


# ====================================================================================================
# ====================================================================================================

comm.Barrier()

# ====================================================================================================
# ====================================================================================================
# output unique geometries and sum of all project locations associated with that geometry

if rank == 0:

    # creating geodataframe
    geo_df = gpd.GeoDataFrame()
    # assuming even split of total project dollars is "max" dollars that project location could receive
    geo_df["dollars"] = i_m["split_dollars_pp"]
    # geometry for each project location
    geo_df["geometry"] = gpd.GeoSeries(i_m["agg_geom"])
    # string version of geometry used to determine duplicates
    geo_df["str_geo"] = geo_df["geometry"].astype(str)
    # create and set unique index
    geo_df['index'] = range(0, len(geo_df))
    geo_df = geo_df.set_index('index')

    # group project locations by geometry using str_geo field 
    # and for each unique geometry get the sum of dollars for
    # all project locations with that geometry
    sum_unique = geo_df.groupby(by ='str_geo')['dollars'].sum()

    # temporary dataframe with unique geometry and dollar sums
    # which can be used to merge with original geo_df dataframe
    tmp_geo_df = gpd.GeoDataFrame()
    tmp_geo_df['unique_dollars'] = sum_unique
    tmp_geo_df['str_geo'] = tmp_geo_df.index

    # merge geo_df with tmp_geo_df
    new_geo_df = geo_df.merge(tmp_geo_df, how = 'inner', on = "str_geo")
    # drops duplicate rows
    new_geo_df.drop_duplicates(subset = "str_geo", inplace = True)
    # gets rid of str_geo column
    new_geo_df.drop('str_geo', axis = 1, inplace = True)

    # create final output geodataframe with index, unique_dollars and unique geometry
    out_geo_df = gpd.GeoDataFrame()
    out_geo_df["geometry"] = gpd.GeoSeries(new_geo_df["geometry"])
    out_geo_df["unique_dollars"] = new_geo_df["unique_dollars"]
    out_geo_df['index'] = range(len(out_geo_df))

    # write to geojson
    geo_json = out_geo_df.to_json()
    geo_file = open(dir_working+"/unique.geojson", "w")
    json.dump(json.loads(geo_json), geo_file, indent = 4)


    # calc section runtime and total runtime
    time_end = time.time()
    T_unique = int(time_end - time_surf)
    T_total = int(time_end - Ts)

    # ====================================================================================================
    # ====================================================================================================


    # msr options
    # output as json which will be loaded into a mongo database
    mops = {}

    def add_json(field, data):
        mops[field] = data


    add_json("size",size)
    add_json("run_stage",run_stage)
    add_json("run_version_str",run_version_str)
    add_json("run_version",run_version)
    add_json("run_id",run_id)
    add_json("Ts",Ts)
    add_json("Rid",Rid)

    add_json("country",country)
    add_json("abbr",abbr)
    add_json("data_version",data_version)
    add_json("pixel_size",pixel_size)

    add_json("sector",sector)

    # add_json("filters_type",filters_type)
    # add_json("filters",filters)
    # add_json("filters_hash",filters_hash)

    add_json("nodata",nodata)
    add_json("aid_field",aid_field)
    add_json("is_geocoded",is_geocoded)
    add_json("only_geocoded",only_geocoded)
    add_json("not_geocoded",not_geocoded)
    add_json("code_field",code_field)
    add_json("agg_types",agg_types)
    add_json("lookup",lookup)

    add_json("dir_working",dir_working)
    # add_json("path of surf file used",)

    add_json("adm0_minx",adm0_minx)
    add_json("adm0_miny",adm0_miny)
    add_json("adm0_maxx",adm0_maxx)
    add_json("adm0_maxy",adm0_maxy)
    add_json("rows",len(rows))
    add_json("cols",len(cols))
    add_json("locations",len(i_m))

    add_json("T_init",T_init)
    add_json("T_surf",T_surf)
    add_json("T_unique",T_unique)
    add_json("T_total",T_total)

    # put json in msr/json/mongo/ready folder
    json_out = dir_base+'/json/mongo/ready/'+str(Rid)+'.json'
    make_dir(os.path.dirname(json_out))
    json_handle1 = open(json_out, 'w')
    json.dump(mops, json_handle1, sort_keys = True, indent = 4, ensure_ascii=False)

    # store json with outputs as meta
    json_handle2 = open(dir_working+'/'+str(Rid)+'.json',"w")
    json.dump(mops, json_handle2, sort_keys = True, indent = 4, ensure_ascii=False)
