'''
runscript.py

Summary:
Mean surface raster generation script for use on MPI configured system with mpi4py

Inputs:
- called via jobscript (shell script)shell
- request.json

Data:
- research release
- shapefiles
- dataset_geometry_lookup.json

'''

# ====================================================================================================
# ====================================================================================================

from __future__ import print_function

try:
    from mpi4py import MPI

    # mpi info
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    status = MPI.Status()

    run_mpi = True

except:
    run_mpi = False


import os
import sys
import errno
from copy import deepcopy
import time
import random
import math

import json
# import hashlib

import numpy as np
import pandas as pd
from shapely.geometry import Polygon, Point, shape, box
import shapefile

import pyproj
from functools import partial
from shapely.ops import transform
import geopandas as gpd

import itertools
from shapely.prepared import prep

# ====================================================================================================
# ====================================================================================================
# general functions


# creates directories
def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


# add information to msr log
def log(msg):
    msg = str(msg)
    # 


# quit msr job, manage error reporting, cleanup/move request files
def quit(msg):

    e_request_basename = os.path.basename(request_path)

    if e_request_basename == '':
        e_request_basename = 'unknown'

    e_request_basename_split = os.path.splitext(e_request_basename)

    error_dir = e_request_basename_split[0] +"_"+ str(Ts) 

    # make error dir
    # '/sciclone/aiddata10/REU/msr/queue/error/' + error_dir
    # make_dir()

    # if os.path.isfile(request_path):
        # move request to error_dir
        # 

    # add error file to error_dir
    # 

    sys.exit(msg)


# ====================================================================================================
# ====================================================================================================
# init, inputs and variables


if len(sys.argv) != 2:
    quit('invalid number of inputs: ' + ', '.join(sys.argv))

# request path passed via python call from jobscript
request_path = sys.argv[1]


# --------------------------------------------------


# absolute path to script directory
dir_file = os.path.dirname(os.path.abspath(__file__))

# full script start time
Ts = int(time.time())


# --------------------------------------------------
# validate request and dataset

# make sure request file exists
if os.path.isfile(request_path):
    # make sure request is valid json and can be loaded
    try:
        request = json.load(open(request_path, 'r'))

    except:
        quit("invalid json: " + request_path)

else:
    quit("json file does not exists: " + request_path)

# load dataset to iso3 crosswalk json
abbrvs = json.load(open(dir_file + '/dataset_geometry_lookup.json', 'r'))

# get dataset crosswalk id from request
dataset_id = request['dataset'].split('_')[0]

# make sure dataset crosswalk id is in crosswalk json
if dataset_id not in abbrvs.keys():
    quit("no shp crosswalk for dataset: " + dataset_id)

# make sure dataset path given in request exists
if not os.path.isdir(request['release_path']):
    quit("release path specified not found: " + request['release_path'])

abbr = abbrvs[dataset_id]


# --------------------------------------------------
# version info stuff

msr_type = request['options']['type']
msr_version = request['options']['version']

run_stage = "beta"
run_version_str = "009"
run_version = int(run_version_str)
run_id = run_stage[0:1] + run_version_str

# random_id = '{0:05d}'.format(int(random.random() * 10**5))
# Rid = str(Ts) +"_"+ random_id


# --------------------------------------------------
# validate pixel size

if not 'resolution' in request['options']:
    quit("missing pixel size input from request")

try:
    pixel_size = float(request['options']['resolution'])
except:
    quit("invalid pixel size input: "+str(request['options']['resolution']))


# check for valid pixel size
# examples of valid pixel sizes: 1.0, 0.5, 0.25, 0.2, 0.1, 0.05, 0.025, ...
if (1/pixel_size) != int(1/pixel_size):
    quit("invalid pixel size: "+str(pixel_size))

# pixel size inverse
psi = 1/pixel_size


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


# fields name associated with values in lookup dict
code_field_1 = "precision_code"
code_field_2 = "location_type_code"


# agg_type definition for non geocoded projects
# either allocated at country level ("country") or ignored ("None")
not_geocoded = "country"

if only_geocoded:
    not_geocoded = "None"


# aggregation types used in lookup dict
agg_types = ["point", "buffer", "adm"]

# # precision code field values
# # buffer values in meters
# lookup = {
#     "1": {"type":"point","data":0},
#     "2": {"type":"buffer","data":25000},
#     "3": {"type":"adm","data":"2"},
#     "4": {"type":"adm","data":"1"},
#     "5": {"type":"buffer","data":25000},
#     "6": {"type":"adm","data":"0"},
#     "7": {"type":"adm","data":"0"},
#     "8": {"type":"adm","data":"0"}
# }


# precision and feature code values (uses default if feature code not listed)
# buffer values in meters
# for adm0 / country boundary  make sure to use type "country" instead of "adm" with data "0"
lookup = {
    "1": {
        "default": {"type": "point", "data": 0}
    },
    "2": {
        "default": {"type": "buffer", "data": 25000}
    },
    "3": {
        "default": {"type": "adm", "data": "2"}
    },
    "4": {
        "default": {"type": "adm", "data": "1"}
    },
    "5": {
        "default": {"type": "buffer", "data": 25000}
    },
    "6": {
        "default": {"type": "country", "data": 0}
    },
    "7": {
        "default": {"type": "country", "data": 0}
    },
    "8": {
        "default": {"type": "country", "data": 0}
    }
}


# ====================================================================================================
# ====================================================================================================
# functions


# def json_hash(hash_obj):
#     hash_json = json.dumps(hash_obj, sort_keys = True, ensure_ascii = False, separators=(',', ':'))
#     hash_builder = hashlib.md5()
#     hash_builder.update(hash_json)
#     hash_md5 = hash_builder.hexdigest()
#     return hash_md5


# --------------------------------------------------


# check csv delim and return if valid type
def get_csv(path):
    if path.endswith('.tsv'):
        return pd.read_csv(path, sep='\t', quotechar='\"', na_values='', keep_default_na=False)
    elif path.endswith('.csv'):
        return pd.read_csv(path, quotechar='\"', na_values='', keep_default_na=False)
    else:
        sys.exit('get_csv - file extension not recognized.\n')


# get project and location data in path directory
# requires a field name to merge on and list of required fields
def get_data(path, merge_id, field_ids, only_geo):

    amp_path = path+"/projects.csv"
    loc_path = path+"/locations.csv"

    # make sure files exist
    # 

    # read input csv files into memory
    amp = get_csv(amp_path)
    loc = get_csv(loc_path)

    if not merge_id in amp or not merge_id in loc:
        sys.exit("get_data - merge field not found in amp or loc files")

    amp[merge_id] = amp[merge_id].astype(str)
    loc[merge_id] = loc[merge_id].astype(str)

    # create projectdata by merging amp and location files by project_id
    if only_geo:
        tmp_merged = amp.merge(loc, on=merge_id)
    else:
        tmp_merged = amp.merge(loc, on=merge_id, how="left")

    if not "longitude" in tmp_merged or not "latitude" in tmp_merged:
        sys.exit("get_data - latitude and longitude fields not found")

    for field_id in field_ids:
        if not field_id in tmp_merged:
            sys.exit("get_data - required code field not found")

    return tmp_merged


# --------------------------------------------------


# define tags enum
def enum(*sequential, **named):
    # source: http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)


# gets geometry type based on lookup table
# depends on lookup and not_geocoded
def get_geom_type(is_geo, code_1, code_2):

    try:
        is_geo = int(is_geo)
        code_1 = str(int(code_1))
        code_2 = str(code_2)

        if is_geo == 1:
            if code_1 in lookup:
                if code_2 in lookup[code_1]:
                    tmp_type = lookup[code_1][code_2]["type"]
                    return tmp_type
                else:
                    tmp_type = lookup[code_1]["default"]["type"]
                    return tmp_type
            else:
                print("lookup code_1 not recognized: " + code_1)
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
def get_shape_within(item, polys):
    c = 0
    for shp in polys:
        tmp_shp = shape(shp)
        if item.within(tmp_shp):
            return tmp_shp

    return c


# checks if arbitrary polygon is within country (adm0) polygon
# depends on adm0
def is_in_country(shp):
    return shp.within(adm0)


# build geometry for point based on code
# depends on lookup and adm0
def get_geom(code_1, code_2, lon, lat):
    tmp_pnt = Point(lon, lat)

    if not is_in_country(tmp_pnt):
        print("point not in country")
        return 0

    else:
        if code_2 in lookup[code_1]:
            tmp_lookup = lookup[code_1][code_2]
        else:
            tmp_lookup = lookup[code_1]["default"]

        # print(tmp_lookup["type"])

        if tmp_lookup["type"] == "point":
            return tmp_pnt

        elif tmp_lookup["type"] == "buffer":
            try:
                # get buffer size (meters)
                tmp_int = float(tmp_lookup["data"])

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
                if is_in_country(tmp_buffer):
                    return tmp_buffer
                else:
                    return tmp_buffer.intersection(adm0)

            except:
                print("buffer value could not be converted to float")
                return 0

        elif tmp_lookup["type"] == "adm":
            try:
                tmp_int = int(tmp_lookup["data"])
                return get_shape_within(tmp_pnt, adm_shps[tmp_int])

            except:
                print("adm value could not be converted to int")
                return 0

        else:
            print("geom object type not recognized")
            return 0


# returns geometry for point
# depends on agg_types and adm0
def get_geom_val(agg_type, code_1, code_2, lon, lat):
    if agg_type in agg_types:

        code_1 = str(int(code_1))
        code_2 = str(code_2)

        tmp_geom = get_geom(code_1, code_2, lon, lat)

        if tmp_geom != 0:
            return tmp_geom

        return "None"

    elif agg_type == "country":

        return adm0

    else:
        print("agg_type not recognized: " + str(agg_type))
        return "None"


# adjusts given aid value based on % of sectors/donors 
# selected via filter vs all associated with project 
def adjust_aid(raw_aid, project_sectors_string, project_donors_string, filter_sectors_list, filter_donors_list):

    project_sectors_list = project_sectors_string.split('|')
    project_donors_list = project_donors_string.split('|')

    if filter_sectors_list == ['All']:
        sectors_match = project_sectors_list
    else:
        sectors_match = [match for match in project_sectors_list if match in filter_sectors_list]

    if filter_donors_list == ['All']:
        donors_match = project_donors_list
    else:  
        donors_match = [match for match in project_donors_list if match in filter_donors_list]

    ratio = float(len(sectors_match) * len(donors_match)) / float(len(project_sectors_list) * len(project_donors_list))

    # remove duplicates? - could be duplicates from project strings
    # ratio = (len(set(sectors_match)) * len(set(donors_match))) / (len(set(project_sectors_list)) * len(set(project_donors_list)))

    adjusted_aid = ratio * float(raw_aid)

    return adjusted_aid


# convert polygon to two separate lists of 
# longitude and latitude based on geometry 
# bounds and a given increment step
def geom_to_grid_colrows(geom, step, rounded=True, no_multi=False):

    # check if geom is polygon
    if geom != Polygon:
        try:
            # make polygon if needed and possible
            geom = shape(geom)

            # if no_multi == True and geom != Polygon:
            #     return 2

        except:
            # cannot convert geom to polygon
            return 1


    # poly grid pixel size and poly grid pixel size inverse
    # poly grid pixel size is 1 order of magnitude higher resolution than output pixel_size
    tmp_pixel_size = step
    tmp_psi = 1/tmp_pixel_size

    (tmp_minx, tmp_miny, tmp_maxx, tmp_maxy) = geom.bounds

    (tmp_minx, tmp_miny, tmp_maxx, tmp_maxy) = (math.floor(tmp_minx*tmp_psi)/tmp_psi, math.floor(tmp_miny*tmp_psi)/tmp_psi, math.ceil(tmp_maxx*tmp_psi)/tmp_psi, math.ceil(tmp_maxy*tmp_psi)/tmp_psi)

    tmp_cols = np.arange(tmp_minx, tmp_maxx+tmp_pixel_size*0.5, tmp_pixel_size)
    tmp_rows = np.arange(tmp_miny, tmp_maxy+tmp_pixel_size*0.5, tmp_pixel_size)

    if rounded == True:
        tmp_sig = 10 ** len(str(tmp_pixel_size)[str(tmp_pixel_size).index('.')+1:])

        tmp_cols = [round(i * tmp_sig) / tmp_sig for i in tmp_cols]
        tmp_rows = [round(i * tmp_sig) / tmp_sig for i in tmp_rows]


    return tmp_cols, tmp_rows


# convert "negative" zero values caused by rounding
# binary floating point values that were below zero
def positive_zero(val):
    if val == 0:
        return +0.0
    else:
        return val


# ====================================================================================================
# ====================================================================================================


# --------------------------------------------------
# file paths

dir_working = os.path.dirname(request_path)


# output_base = "/sciclone/aiddata10/REU/data/rasters/internal/msr"
# output_dataset = output_base +"/"+ request['dataset']

# # if request['type'] == 'auto':
# if 'hash' in request:
#     request_hash = request['hash']
# else:
#     request_hash_object = {
#         'dataset':request['dataset'],
#         'donors':request['options']['donors'],
#         'sectors':request['options']['sectors'],
#         'years':request['options']['years']
#     }
#     request_hash = json_hash(request_hash_object)

# dir_working = output_dataset +"_"+ request_hash


# dir_country = dir_file+"/outputs/"+country
# dir_working = dir_country+"/"+country+"_"+str(pixel_size)+"_"+str(iterations)+"_"+str(int(Ts))


# dir_country = dir_file+"/data/"+country
# dir_chain = dir_country+"/"+country+"_"+str(data_version)+"_"+run_id+"_"+str(pixel_size)
# dir_outputs = dir_chain+"/outputs"
# dir_working = dir_outputs+"/"+str(Rid)


# --------------------------------------------------
# load shapefiles

# must start at and inlcude ADM0
# all additional ADM shps must be included so that adm_path index corresponds to adm level
adm_paths = []
adm_paths.append(dir_file+"/shps/"+abbr+"/"+abbr+"_adm0.shp")
adm_paths.append(dir_file+"/shps/"+abbr+"/"+abbr+"_adm1.shp")
adm_paths.append(dir_file+"/shps/"+abbr+"/"+abbr+"_adm2.shp")

# get adm0 bounding box
adm_shps = [shapefile.Reader(adm_path).shapes() for adm_path in adm_paths]

# define country shape
adm0 = shape(adm_shps[0][0])

adm0_prep = prep(adm0)


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

sig = 10 ** len(str(pixel_size)[str(pixel_size).index('.')+1:])

cols = [round(i * sig) / sig for i in cols]
rows = [round(i * sig) / sig for i in rows]


# init grid reference object

grid_product = list(itertools.product(cols, rows))

grid_gdf = gpd.GeoDataFrame()
grid_gdf['within'] = [0] * len(grid_product)
grid_gdf['geometry'] = grid_product
grid_gdf['geometry'] = grid_gdf.apply(lambda z: Point(z.geometry), axis=1)

grid_gdf['lat'] = grid_gdf.apply(lambda z: z['geometry'].y, axis=1)
grid_gdf['lon'] = grid_gdf.apply(lambda z: z['geometry'].x, axis=1)

grid_gdf['index'] = grid_gdf.apply(lambda z: str(z.lon) +'_'+ str(z.lat), axis=1)
grid_gdf.set_index('index', inplace=True)


# grid_gdf['within'] = grid_gdf['geometry'].intersects(adm0)
grid_gdf['within'] = [adm0_prep.contains(i) for i in grid_gdf['geometry']]


adm0_count = sum(grid_gdf['within'])

grid_gdf['value'] = 0

grid_gdf.sort(['lat','lon'], ascending=[False, True], inplace=True)


# gref = {}
# idx = 0

# adm0_gref = {}
# adm0_count = 0

# for r in rows:
#     gref[str(r)] = {}
#     adm0_gref[str(r)] = {}

#     for c in cols:
#         # build grid reference object
#         gref[str(r)][str(c)] = idx
#         idx += 1


#         # check if point is within geom
#         adm0_point = Point(c,r)
#         adm0_within = adm0_point.within(adm0)


#         if adm0_within:
#             adm0_gref[str(r)][str(c)] = idx
#             adm0_count += 1
#         else:
#             adm0_gref[str(r)][str(c)] = "None"





# --------------------------------------------------
# load project data

# dir_data = dir_file+"/countries/"+country+"/versions/"+country+"_"+str(data_version)+"/data"
dir_data = request['release_path'] +'/'+ os.path.basename(request['release_path']) +'/data'

merged = get_data(dir_data, "project_id", (code_field_1, code_field_2, "project_location_id"), only_geocoded)


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

# filter years
# 

# filter sectors and donors
if request['options']['donors'] == ['All'] and request['options']['sectors'] != ['All']:
    filtered = merged.loc[merged['ad_sector_names'].str.contains('|'.join(request['options']['sectors']))].copy(deep=True)

elif request['options']['donors'] != ['All'] and request['options']['sectors'] == ['All']:
    filtered = merged.loc[merged['donors'].str.contains('|'.join(request['options']['donors']))].copy(deep=True)

elif request['options']['donors'] != ['All'] and request['options']['sectors'] != ['All']:
    filtered = merged.loc[(merged['ad_sector_names'].str.contains('|'.join(request['options']['sectors']))) & (merged['donors'].str.contains('|'.join(request['options']['donors'])))].copy(deep=True)

else:
    filtered = merged.copy(deep=True)
 



# adjust aid based on ratio of sectors/donors in filter to all sectors/donors listed for project
filtered['adjusted_aid'] = filtered.apply(lambda z: adjust_aid(z.split_dollars_pp, z.ad_sector_names, z.donors, request['options']['sectors'], request['options']['donors']), axis=1)


# no filter - placeholder
# filtered = deepcopy(merged)


# !!! potential issue !!! 
# 2015-22-10 : i think this is outdated since there is no more random aid
#
# - filters which remove only some locations from a project will skew aid splits
# - * moved original project location count to before filters so that it can be used to
#   compare the count of project locations before filter to count after and generate
#   placeholder random values for the locations that were filtered out
# - method: recheck project location count and create placeholder random value if locations are missing
# - will need to rebuild how random num column is added. probaby can use apply with a new function


# --------------------------------------------------
# assign geometries
if rank == 0:
    print("geom")


# add geom columns
filtered["agg_type"] = pd.Series(["None"] * len(filtered))
filtered["agg_geom"] = pd.Series(["None"] * len(filtered))

filtered.agg_type = filtered.apply(lambda x: get_geom_type(x[is_geocoded], x[code_field_1], x[code_field_2]), axis=1)
filtered.agg_geom = filtered.apply(lambda x: get_geom_val(x.agg_type, x[code_field_1], x[code_field_2], x.longitude, x.latitude), axis=1)
i_m = filtered.loc[filtered.agg_geom != "None"].copy(deep=True)


# i_m['index'] = i_m['project_location_id']
i_m['unique'] = range(0, len(i_m))
i_m['index'] = range(0, len(i_m))
i_m = i_m.set_index('index')


# ====================================================================================================
# ====================================================================================================
# master init


if rank == 0:
    
    print("masterinit")

    # --------------------------------------------------
    # initialize results file output

    # results_str = "Mean Surface Rasters Output File\t "

    # results_str += "\nstart time\t" + str(Ts)
    # results_str += "\ncountry\t" + str(country)
    # results_str += "\nabbr\t" + str(abbr)
    # results_str += "\npixel_size\t" + str(pixel_size)
    # results_str += "\nnodata\t" + str(nodata)
    # results_str += "\naid_field\t" + str(aid_field)
    # results_str += "\ncode_field_1\t" + str(code_field_1)
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
        fout_sum_mean_surf = open(dir_working+"/raster.asc", "w")
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


            tmp_grid_gdf = grid_gdf.copy(deep=True)
            tmp_grid_gdf['value'] = 0

            def map_to_grid(geom, val):
                if val != 0:
                    tmp_grid_gdf.loc[tmp_grid_gdf['geometry'] == Point(round(geom.y * psi) / psi, round(geom.x * psi) / psi), 'value'] += val


            # mean_surf = np.zeros((int(idx+1),), dtype=np.int)



            pg_data = i_m.loc[task]
            pg_type = pg_data.agg_type

            print(str(rank) + 'running pg_type: ' + pg_type + '('+ str(pg_data['project_location_id']) +')')

            if pg_type == "country":
                
                # print("rank " + str(rank) +" " +pg_type+ " : adm0 bounds length vals within map")

                tmp_grid_gdf['value'] = tmp_grid_gdf['within'] * (pg_data['adjusted_aid'] / adm0_count)


                # for r in rows:
                #     for c in cols:
                #         if adm0_gref[str(r)][str(c)] != "None":
                #             # round new grid points to old grid points and update old grid
                #             gref_id = gref[str(r)][str(c)]
                #             mean_surf[gref_id] += pg_data['adjusted_aid'] / adm0_count



            elif pg_type != "point" and pg_type in agg_types:

                # for each row generate grid based on bounding box of geometry



                pg_geom = pg_data.agg_geom


                # factor used to determine subgrid size
                # relative to output grid size
                # sub grid res = output grid res * sub_grid_factor
                sub_grid_factor = 0.1
                pg_pixel_size = pixel_size * sub_grid_factor


                if pg_geom.geom_type == 'MultiPolygon':
                    

                    pg_cols = []
                    pg_rows = []

                    for pg_geom_part in pg_geom:

                        tmp_pg_cols, tmp_pg_rows = geom_to_grid_colrows(pg_geom_part, pg_pixel_size, rounded=True, no_multi=True)

                        pg_cols = np.append(pg_cols, tmp_pg_cols)
                        pg_rows = np.append(pg_rows, tmp_pg_rows)


                    pg_cols = set(pg_cols)
                    pg_rows = set(pg_rows)


                    # x_cols, x_rows = geom_to_grid_colrows(pg_geom, pg_pixel_size, rounded=True, no_multi=True)

                    # print('MULTIPOLYGON - ('+ str(pg_data['project_location_id']) +')  ' +str(len(pg_cols)) + ' -- ' + str(len(pg_rows)) + ' -- ' + str(len(x_cols)) + ' -- ' + str(len(x_rows)))
                    # print(pg_cols)
                    # print(pg_rows)
                    # print(x_cols)
                    # print(x_rows)


                else:
                
                    pg_cols, pg_rows = geom_to_grid_colrows(pg_geom, pg_pixel_size, rounded=True, no_multi=False)



                # -------------------------
                # old

                # sub_grid_factor = 0.1
                # pg_pixel_size = pixel_size * sub_grid_factor
                # pg_psi = 1/pg_pixel_size
                # (pg_minx, pg_miny, pg_maxx, pg_maxy) = pg_geom.bounds
                # (pg_minx, pg_miny, pg_maxx, pg_maxy) = (math.floor(pg_minx*pg_psi)/pg_psi, math.floor(pg_miny*pg_psi)/pg_psi, math.ceil(pg_maxx*pg_psi)/pg_psi, math.ceil(pg_maxy*pg_psi)/pg_psi)
                # pg_cols = np.arange(pg_minx, pg_maxx+pg_pixel_size*0.5, pg_pixel_size)
                # pg_rows = np.arange(pg_maxy, pg_miny-pg_pixel_size*0.5, -1*pg_pixel_size)
                # print("rank " + str(rank) +" bounds : minx=" +str(pg_minx) +" , maxx=" +str(pg_maxx)+" , miny=" +str(pg_miny)+" , maxy=" +str(pg_maxy))

                # pg_sig = 10 ** len(str(pg_pixel_size)[str(pg_pixel_size).index('.')+1:])

                # pg_cols = [round(i * pg_sig) / pg_sig for i in pg_cols]
                # pg_rows = [round(i * pg_sig) / pg_sig for i in pg_rows]

                # -------------------------



                # evenly split the aid for that row (i_m['adjusted_aid'] field) among new grid points

                tmp_product = list(itertools.product(pg_cols, pg_rows))
                tmp_gdf = gpd.GeoDataFrame()
                tmp_gdf['within'] = [0] * len(tmp_product)
                tmp_gdf['geometry'] = tmp_product
                tmp_gdf['geometry'] = tmp_gdf.apply(lambda z: Point(z.geometry), axis=1)
                

                # round to reference grid points and fix -0.0
                tmp_gdf['ref_lat'] = tmp_gdf.apply(lambda z: positive_zero(round(z.geometry.y * psi) / psi), axis=1)
                tmp_gdf['ref_lon'] = tmp_gdf.apply(lambda z: positive_zero(round(z.geometry.x * psi) / psi), axis=1)



                # print("rank " + str(rank) +" length :" +str(len(tmp_product)))


                # Tsx = time.time()

                pg_geom_prep = prep(pg_geom)
                tmp_gdf['within'] = [pg_geom_prep.contains(i) for i in tmp_gdf['geometry']]

                # tmp_gdf['within'] = tmp_gdf['geometry'].within(pg_geom)

                # Tsxz = time.time() - Tsx
                # print("rank " + str(rank) +" " +pg_type+ " within took " + str(Tsxz))


                # Tsx = time.time()
                pg_count = sum(tmp_gdf['within'])
                tmp_gdf['value'] = 0
                tmp_gdf['value'] = tmp_gdf['within'] * (pg_data['adjusted_aid'] / pg_count)
                # Tsxz = time.time() - Tsx
                # print("rank " + str(rank) +" " +pg_type+ " vals took " + str(Tsxz))


                # Tsx = time.time()
                # tmp_gdf.sort(['ref_lat','ref_lon'], ascending=[False, True], inplace=True)
                aggregated_total = tmp_gdf.groupby(['ref_lat','ref_lon'])['value'].sum()
                
                agg_df = aggregated_total.reset_index()

                agg_df['index'] = agg_df.apply(lambda z: str(z.ref_lon) +'_'+ str(z.ref_lat), axis=1)
                agg_df.set_index('index', inplace=True)


                try:
                    tmp_grid_gdf.loc[agg_df.index, 'value'] += agg_df['value']

                except:
                    for i in agg_df.index:
                        print('bad index iters')
                        print(i)
                        print(i in tmp_grid_gdf.index)



                # try:
                #     tmp_grid_gdf.loc[agg_df.index, 'value'] += agg_df['value']

                # except:

                #     print("!!!!!!!!!!!!!!!!!!!!!!!!!1111111")
                #     print("rank " + str(rank) +" adm0 bounds : minx=" +str(adm0_minx) +" , maxx=" +str(adm0_maxx)+" , miny=" +str(adm0_miny)+" , maxy=" +str(adm0_maxy))
                #     print("rank " + str(rank) +" bounds : minx=" +str(pg_minx) +" , maxx=" +str(pg_maxx)+" , miny=" +str(pg_miny)+" , maxy=" +str(pg_maxy))

                #     print(pg_data.project_location_id)
                #     print(list(agg_df.index))
                #     # print()
                #     # print
                #     print("!!!!!!!!!!!!!!!!!!!!!!!!!1111111")




                # print("rank " + str(rank) +" " +pg_type+ " map took " + str(Tsxz))

                # Tsx = time.time()
                # tmp_gdf.apply(lambda x: map_to_grid(x.geometry, x.value), axis=1)
                # Tsxz = time.time() - Tsx
                # print("rank " + str(rank) +" " +pg_type+ " map took " + str(Tsxz))




                # # full poly grid reference object and count
                # pg_gref = {}
                # pg_idx = 0

                # # poly grid points within actual geom and count
                # # pg_in = {}
                # pg_count = 0


                # for r in pg_rows:
                #     pg_gref[str(r)] = {}

                #     for c in pg_cols:
                #         pg_idx += 1

                #         Tsx = time.time()

                #         # check if point is within geom
                #         pg_point = Point(c,r)
                #         pg_within = pg_point.within(pg_geom)

                #         Tsxz = time.time() - Tsx
                #         print("rank " + str(rank) + " point/within took " + str(Tsxz))

                #         if pg_within:
                #             pg_gref[str(r)][str(c)] = pg_idx
                #             pg_count += 1
                #         else:
                #             pg_gref[str(r)][str(c)] = "None"


                # # init grid reference object
                # for r in pg_rows:
                #     for c in pg_cols:
                #         if pg_gref[str(r)][str(c)] != "None":
                #             # round new grid points to old grid points and update old grid
                #             gref_id = gref[str(round(r * psi) / psi)][str(round(c * psi) / psi)]
                #             mean_surf[gref_id] += pg_data['adjusted_aid'] / pg_count






            elif pg_type == "point":

                # print("rank " + str(rank) +" " +pg_type+ " : point bounds length vals within map")

                # round new grid points to old grid points and update old grid

                tmp_point = Point(round(pg_data.latitude * psi) / psi, round(pg_data.longitude * psi) / psi)
                tmp_value = pg_data['adjusted_aid']
                map_to_grid(tmp_point, tmp_value)

                # gref_id = gref[str(round(pg_data.latitude * psi) / psi)][str(round(pg_data.longitude * psi) / psi)]
                # mean_surf[gref_id] += pg_data['adjusted_aid']


            # --------------------------------------------------
            # send np arrays back to master

            mean_surf = np.array(tmp_grid_gdf['value'])

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
    geo_df["dollars"] = i_m["adjusted_aid"]
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

    def add_to_json(field, data):
        mops[field] = data


    add_to_json("size",size)
    add_to_json("run_stage",run_stage)
    add_to_json("run_version_str",run_version_str)
    add_to_json("run_version",run_version)
    add_to_json("run_id",run_id)
    add_to_json("Ts",Ts)
    # add_to_json("Rid",Rid)

    add_to_json("dataset",request['dataset'])
    add_to_json("abbr",abbr)
    add_to_json("pixel_size",pixel_size)

    # add_to_json("filters",filters)
    # add_to_json("filters_hash",filters_hash)

    add_to_json("nodata",nodata)
    add_to_json("aid_field",aid_field)
    add_to_json("is_geocoded",is_geocoded)
    add_to_json("only_geocoded",only_geocoded)
    add_to_json("not_geocoded",not_geocoded)
    add_to_json("code_field_1",code_field_1)
    add_to_json("code_field_2",code_field_2)
    add_to_json("agg_types",agg_types)
    add_to_json("lookup",lookup)

    add_to_json("dir_working",dir_working)
    # add_to_json("path of surf file used",)

    add_to_json("adm0_minx",adm0_minx)
    add_to_json("adm0_miny",adm0_miny)
    add_to_json("adm0_maxx",adm0_maxx)
    add_to_json("adm0_maxy",adm0_maxy)
    add_to_json("rows",len(rows))
    add_to_json("cols",len(cols))
    add_to_json("locations",len(i_m))

    add_to_json("T_init",T_init)
    add_to_json("T_surf",T_surf)
    add_to_json("T_unique",T_unique)
    add_to_json("T_total",T_total)

    add_to_json("status",0)


    # put json in msr/json/mongo/ready folder
    json_out = dir_working+'/output.json'
    # make_dir(os.path.dirname(json_out))
    json_handle1 = open(json_out, 'w')
    json.dump(mops, json_handle1, sort_keys = True, indent = 4, ensure_ascii=False)

    # store json with outputs as meta
    # json_handle2 = open(dir_working+'/'+str(Rid)+'.json',"w")
    # json.dump(mops, json_handle2, sort_keys = True, indent = 4, ensure_ascii=False)
