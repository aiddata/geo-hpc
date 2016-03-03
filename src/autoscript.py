"""
autoscript.py

Summary:
Mean surface raster generation script for use with mpi4py

Inputs:
- called via temporary jobscript which was automatically generated
- mongodb doc (in place of request.json)

Data:
- research release
- shapefiles
- dataset_iso3_lookup.json
- dataset_utm_lookup.json
"""

# =============================================================================
# =============================================================================


from __future__ import print_function

# from mpi4py import MPI
from mpi_utility import *

job = NewParallel()
print job.rank

# -------------------------------------

import sys
import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
sys.path.insert(0, config_dir)

from config_utility import *

config = BranchConfig(branch=branch)


# -------------------------------------


# =============================================================================
# =============================================================================


import errno
import time
import random
import math
import itertools
import json
# from copy import deepcopy

import numpy as np
import pandas as pd
import geopandas as gpd

from shapely.geometry import MultiPolygon, Polygon, Point, shape, box
from shapely.prepared import prep

import shapefile

from msr_utility import CoreMSR


# =============================================================================
# =============================================================================


def make_dir(path):
    """Make directory. 

    Args:
        path (str): absolute path for directory

    Raise error if error other than directory exists occurs.
    """
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


# def log(msg):
#     """Add message to msr log.

#     Args:
#         msg (str): message to add to log
#     """
#     msg = str(msg)
#     # 


def quit(msg):
    """Quit msr job.
    
    Args:
        msg (str): message to add to log upon exiting

    Function also manages error reporting and cleans up / moves request files.
    """
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


# =============================================================================
# =============================================================================
# init, inputs and variables

# create instance of CoreMSR class
core = CoreMSR()


# if len(sys.argv) != 2:
#     quit('invalid number of inputs: ' + ', '.join(sys.argv))

# # request path passed via python call from jobscript
# request_path = sys.argv[1]


# -------------------------------------


# absolute path to script directory
dir_file = os.path.dirname(os.path.abspath(__file__))

# full script start time
Ts = int(time.time())


# -------------------------------------
# validate request and dataset

# # make sure request file exists
# if os.path.isfile(request_path):
#     # make sure request is valid json and can be loaded
#     try:
#         request = json.load(open(request_path, 'r'))

#     except:
#         quit("invalid json: " + request_path)

# else:
#     quit("json file does not exists: " + request_path)


if job.rank == 0:

    import pymongo

    client = pymongo.MongoClient(config.server)

    msr = client[config.det_db].msr

    request_list = msr.find({'status':0}).sort([("priority", -1), ("submit_time", 1)]).limit(1)

    # make sure request was found
    if request_list.count(True) == 1:

        request = request_list[0]
        # request_id = request['_id']

    else:
        request = None 

    print(request)

else:
   request = None


request = job.comm.bcast(request, root=0)

if request == None:
    quit("no jobs found in queue")


if job.rank == 0:
    # update status of request in msr queue to 2
    pass


# =====================================
# =====================================



# load dataset to iso3 crosswalk json
iso3_lookup = json.load(open(dir_file + '/dataset_iso3_lookup.json', 'r'))
utm_lookup = json.load(open(dir_file + '/dataset_utm_lookup.json', 'r'))

# get dataset crosswalk id from request
dataset_id = request['dataset'].split('_')[0]

# make sure dataset crosswalk id is in crosswalk json
if dataset_id not in iso3_lookup.keys():
    quit("no shp crosswalk for dataset: " + dataset_id)



# lookup release path


release_path = None

if job.rank == 0:
    asdf = client[config.asdf_db].data
    release_path = asdf.find({'name': request['dataset']})[0]['base']
    print(release_path)


release_path = job.comm.bcast(release_path, root=0)



# make sure dataset path given in request exists
if not os.path.isdir(release_path):
    quit("release path specified not found: " + release_path)


# todo: make sure these exist in lookups first
abbr = iso3_lookup[dataset_id]
core.utm_zone = utm_lookup[abbr]


# -------------------------------------
# version info stuff

msr_type = request['options']['type']
msr_version = request['options']['version']

run_stage = "beta"
run_version_str = "010"
run_version = int(run_version_str)
run_id = run_stage[0:1] + run_version_str

# random_id = '{0:05d}'.format(int(random.random() * 10**5))
# Rid = str(Ts) +"_"+ random_id


# -------------------------------------
# set pixel size

if not 'resolution' in request['options']:
    quit("missing pixel size input from request")


core.set_pixel_size(request['options']['resolution'])


# =============================================================================
# =============================================================================


# -------------------------------------
# file paths

# dir_working = os.path.join(branch_dir, log, msr, jobs)
dir_working =  '/sciclone/aiddata10/REU/msr/queue/active/' + request['dataset'] +'_'+ request['hash']

if job.rank == 0:
    # build output directories
    make_dir(dir_working)


# -------------------------------------
# load shapefiles

# must start at and inlcude ADM0
# all additional ADM shps must be included so that adm_path index corresponds to adm level
adm_paths = []
adm_paths.append("/sciclone/aiddata10/REU/msr/shps/"+abbr+"/"+abbr+"_adm0.shp")
adm_paths.append("/sciclone/aiddata10/REU/msr/shps/"+abbr+"/"+abbr+"_adm1.shp")
adm_paths.append("/sciclone/aiddata10/REU/msr/shps/"+abbr+"/"+abbr+"_adm2.shp")

# build list of adm shape lists
core.adm_shps = [shapefile.Reader(adm_path).shapes() for adm_path in adm_paths]

# define country shape
tmp_adm0 = shape(core.adm_shps[0][0])
core.set_adm0(tmp_adm0)


# -------------------------------------
# create point grid for country

# country bounding box
(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = core.adm0.bounds

# grid_buffer
gb = 0.5

# bounding box rounded to pixel size (always increases bounding box size, never decreases)
(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = (math.floor(adm0_minx*gb)/gb, math.floor(adm0_miny*gb)/gb, math.ceil(adm0_maxx*gb)/gb, math.ceil(adm0_maxy*gb)/gb)

# generate arrays of new grid x and y values
cols = np.arange(adm0_minx, adm0_maxx+core.pixel_size*0.5, core.pixel_size)
rows = np.arange(adm0_maxy, adm0_miny-core.pixel_size*0.5, -1*core.pixel_size)

sig = 10 ** len(str(core.pixel_size)[str(core.pixel_size).index('.')+1:])

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
grid_gdf['within'] = [core.prep_adm0.contains(i) for i in grid_gdf['geometry']]


adm0_count = sum(grid_gdf['within'])

grid_gdf['value'] = 0

grid_gdf.sort(['lat','lon'], ascending=[False, True], inplace=True)


# -------------------------------------
# load project data

# dir_data = dir_file+"/countries/"+country+"/versions/"+country+"_"+str(data_version)+"/data"
dir_data = release_path +'/'+ os.path.basename(release_path) +'/data'

merged = core.merge_data(dir_data, "project_id", (core.code_field_1, core.code_field_2, "project_location_id"), core.only_geocoded)


# -------------------------------------
# misc data prep

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
merged[core.aid_field].fillna(0, inplace=True)
merged['split_dollars_pp'] = (merged[core.aid_field] / merged.location_count)


# -------------------------------------
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
filtered['adjusted_aid'] = filtered.apply(lambda z: core.adjust_aid(z.split_dollars_pp, z.ad_sector_names, z.donors, request['options']['sectors'], request['options']['donors']), axis=1)


# -------------------------------------
# assign geometries

# add geom columns
filtered["agg_type"] = pd.Series(["None"] * len(filtered))
filtered["agg_geom"] = pd.Series(["None"] * len(filtered))

filtered.agg_type = filtered.apply(lambda x: core.get_geom_type(x[core.is_geocoded], x[core.code_field_1], x[core.code_field_2]), axis=1)
filtered.agg_geom = filtered.apply(lambda x: core.get_geom_val(x.agg_type, x[core.code_field_1], x[core.code_field_2], x.longitude, x.latitude), axis=1)
i_m = filtered.loc[filtered.agg_geom != "None"].copy(deep=True)


# i_m['index'] = i_m['project_location_id']
i_m['unique'] = range(0, len(i_m))
i_m['index'] = range(0, len(i_m))
i_m = i_m.set_index('index')

unique_ids = i_m['unique']


# =============================================================================
# define MPI message tags

def enum(*sequential, **named):
    """Generate an enum type object."""
    # source: http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)


tags = enum('READY', 'DONE', 'EXIT', 'START', 'ERROR')


# =============================================================================
# =============================================================================


def tmp_worker_job(task_id):

    tmp_grid_gdf = grid_gdf.copy(deep=True)
    tmp_grid_gdf['value'] = 0

    task = task_list[task_id]

    pg_data = i_m.loc[task]
    pg_type = pg_data.agg_type

    print(str(self.rank) + 'running pg_type: ' + pg_type + '('+ str(pg_data['project_location_id']) +')')


    if pg_type == "country":
        
        tmp_grid_gdf['value'] = tmp_grid_gdf['within'] * (pg_data['adjusted_aid'] / adm0_count)


    elif pg_type == "point":

        # round new grid points to old grid points and update old grid

        tmp_point = Point(round(pg_data.latitude * core.psi) / core.psi, round(pg_data.longitude * core.psi) / core.psi)
        tmp_value = pg_data['adjusted_aid']

        if tmp_value != 0:
            tmp_grid_gdf.loc[tmp_grid_gdf['geometry'] == Point(round(tmp_point.y * core.psi) / core.psi, round(tmp_point.x * core.psi) / core.psi), 'value'] += tmp_value


    elif pg_type in core.agg_types:

        # for each row generate grid based on bounding box of geometry
        pg_geom = pg_data.agg_geom

        try:
            pg_geom = shape(pg_geom)
        except:
            print(type(pg_geom))
            print(pg_geom)
            sys.exit("!!!")

        # factor used to determine subgrid size
        # relative to output grid size
        # sub grid res = output grid res * sub_grid_factor
        sub_grid_factor = 0.1
        pg_pixel_size = core.pixel_size * sub_grid_factor


        if pg_geom.geom_type == 'MultiPolygon':
            
            pg_cols = []
            pg_rows = []

            for pg_geom_part in pg_geom:

                tmp_pg_cols, tmp_pg_rows = core.geom_to_grid_colrows(pg_geom_part, pg_pixel_size, rounded=True, no_multi=True)

                pg_cols = np.append(pg_cols, tmp_pg_cols)
                pg_rows = np.append(pg_rows, tmp_pg_rows)


            pg_cols = set(pg_cols)
            pg_rows = set(pg_rows)

        else:
        
            pg_cols, pg_rows = core.geom_to_grid_colrows(pg_geom, pg_pixel_size, rounded=True, no_multi=False)


        # evenly split the aid for that row (i_m['adjusted_aid'] field) among new grid points

        tmp_product = list(itertools.product(pg_cols, pg_rows))
        tmp_gdf = gpd.GeoDataFrame()
        tmp_gdf['within'] = [0] * len(tmp_product)
        tmp_gdf['geometry'] = tmp_product
        tmp_gdf['geometry'] = tmp_gdf.apply(lambda z: Point(z.geometry), axis=1)
        

        # round to reference grid points and fix -0.0
        tmp_gdf['ref_lat'] = tmp_gdf.apply(lambda z: core.positive_zero(round(z.geometry.y * core.psi) / core.psi), axis=1)
        tmp_gdf['ref_lon'] = tmp_gdf.apply(lambda z: core.positive_zero(round(z.geometry.x * core.psi) / core.psi), axis=1)


        pg_geom_prep = prep(pg_geom)
        tmp_gdf['within'] = [pg_geom_prep.contains(i) for i in tmp_gdf['geometry']]


        pg_count = sum(tmp_gdf['within'])
        tmp_gdf['value'] = 0
        tmp_gdf['value'] = tmp_gdf['within'] * (pg_data['adjusted_aid'] / pg_count)

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


    # -------------------------------------
    # send np arrays back to master

    mean_surf = np.array(tmp_grid_gdf['value'])

    return mean_surf


def tmp_master_process(worker_data):
    all_mean_surf.append(worker_data)


def complete_final_raster():
    # build and output final raster

    # initialize asc file output
    asc = ""
    asc += "NCOLS " + str(len(cols)) + "\n"
    asc += "NROWS " + str(len(rows)) + "\n"

    # asc += "XLLCORNER " + str(adm0_minx-core.pixel_size*0.5) + "\n"
    # asc += "YLLCORNER " + str(adm0_miny-core.pixel_size*0.5) + "\n"

    asc += "XLLCENTER " + str(adm0_minx) + "\n"
    asc += "YLLCENTER " + str(adm0_miny) + "\n"

    asc += "CELLSIZE " + str(core.pixel_size) + "\n"
    asc += "NODATA_VALUE " + str(core.nodata) + "\n"


    # calc results
    stack_mean_surf = np.vstack(all_mean_surf)
    sum_mean_surf = np.sum(stack_mean_surf, axis=0)

    # write asc file
    sum_mean_surf_str = ' '.join(np.char.mod('%f', sum_mean_surf))
    asc_sum_mean_surf_str = asc + sum_mean_surf_str
    fout_sum_mean_surf = open(dir_working+"/raster.asc", "w")
    fout_sum_mean_surf.write(asc_sum_mean_surf_str)


    # validate sum_mean_surf
    # exit if validation fails
    if type(sum_mean_surf) == type(0):
        sys.exit("! - mean surf validation failed")


def complete_unique_geoms():
    # output unique geometries and sum of all 
    # project locations associated with that geometry

    # creating geodataframe
    geo_df = gpd.GeoDataFrame()
    # assuming even split of total project dollars is "max" dollars 
    # that project location could receive
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


def complete_options_json():
    # output msr options as json (might be loaded into mongo?)

    mops = {}

    def add_to_json(field, data):
        mops[field] = data

    add_to_json("request",request)

    add_to_json("size",size)
    add_to_json("run_stage",run_stage)
    add_to_json("run_version_str",run_version_str)
    add_to_json("run_version",run_version)
    add_to_json("run_id",run_id)
    add_to_json("Ts",Ts)

    add_to_json("dataset",request['dataset'])
    add_to_json("abbr",abbr)
    add_to_json("pixel_size",core.pixel_size)

    add_to_json("nodata",core.nodata)
    add_to_json("aid_field",core.aid_field)
    add_to_json("is_geocoded",core.is_geocoded)
    add_to_json("only_geocoded",core.only_geocoded)
    add_to_json("not_geocoded",core.not_geocoded)
    add_to_json("code_field_1",core.code_field_1)
    add_to_json("code_field_2",core.code_field_2)
    add_to_json("agg_types",core.agg_types)
    add_to_json("lookup",core.lookup)

    add_to_json("adm0_minx",adm0_minx)
    add_to_json("adm0_miny",adm0_miny)
    add_to_json("adm0_maxx",adm0_maxx)
    add_to_json("adm0_maxy",adm0_maxy)
    add_to_json("rows",len(rows))
    add_to_json("cols",len(cols))
    add_to_json("locations",len(i_m))

    # add_to_json("T_init",T_init)
    # add_to_json("T_surf",T_surf)
    # add_to_json("T_unique",T_unique)
    # add_to_json("T_total",T_total)

    add_to_json("dir_working",dir_working)
    add_to_json("status",0)


    # write output.json
    json_out = dir_working+'/output.json'
    json_handle = open(json_out, 'w')
    json.dump(mops, json_handle, sort_keys = True, indent = 4, ensure_ascii=False)


def complete_outputs():
    # move entire dir for job from msr queue "active" dir to "done" dir  
    # and copy data files to msr data dir

    import shutil

    # move entire dir for job from msr queue "active" dir to "done" dir  
    dir_final = dir_working.replace('/active/', '/done/')

    if os.path.isdir(dir_final):
        shutil.rmtree(dir_final)

    shutil.move(dir_working, dir_final)


    # make msr data dir and move raster.asc, unique.geojson, output.json there
    msr_data_dir = '/sciclone/aiddata10/REU/data/rasters/internal/msr/' + request['dataset'] +'/'+ request['hash']
    make_dir(msr_data_dir)

    msr_data_files = ['raster.asc', 'unique.geojson', 'output.json', 'request.json']
    for f in msr_data_files:
        msr_data_file = dir_final +'/'+ f

        # if os.path.isfile(msr_data_dst_file):
            # os.remove(msr_data_dst_file)

        shutil.copy(msr_data_file, msr_data_dir)
        os.remove(msr_data_file)


def tmp_master_final():

    # record surf runtime
    time_surf = time.time()
    T_surf = int(time_surf - time_init)

    print('\tSurf Runtime: ' + str(T_surf//60) +'m '+ str(int(T_surf%60)) +'s')
    print('\n')


    # run final output gen functions
    complete_final_raster()
    complete_unique_geoms()
    complete_options_json()
    complete_outputs()


    # calc section runtime and total runtime
    time_end = time.time()
    T_unique = int(time_end - time_surf)
    T_total = int(time_end - Ts)



# =============================================================================
# =============================================================================


if job.rank == 0:


    # =====================================
    # MASTER INIT

    # -------------------------------------
    # record init runtime

    time_init = time.time()
    T_init = int(time_init - Ts)

    print('\tInit Runtime: ' + str(T_init//60) +'m '+ str(int(T_init%60)) +'s')


    # -------------------------------------
    # init for final calcs later

    sum_mean_surf = 0
    all_mean_surf = []

    # =====================================



# init / run job

job = NewParallel()
job.set_task_list(unique_ids)

# job.set_general_init(tmp_general_init)
# job.set_master_init(tmp_master_init)
job.set_master_process(tmp_master_process)
job.set_master_final(tmp_master_final)
job.set_worker_job(tmp_worker_job)

job.run()

