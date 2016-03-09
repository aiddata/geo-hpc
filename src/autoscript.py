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

import sys
import os
import errno
import time
import datetime
# import random
import math
import itertools
import json
import shutil

from copy import deepcopy
from collections import OrderedDict

import numpy as np
import pandas as pd
import geopandas as gpd

from shapely.geometry import Point, shape #, MultiPolygon, Polygon, box
from shapely.prepared import prep

import shapefile

import pymongo

import rasterio
from affine import Affine

from msr_utility import CoreMSR


# =============================================================================
# =============================================================================


# from mpi4py import MPI
import mpi_utility

job = mpi_utility.NewParallel()


# -------------------------------------

# import sys
# import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
sys.path.insert(0, config_dir)

import config_utility

config = config_utility.BranchConfig(branch=branch)


# -------------------------------------


# check mongodb connection
if config.connection_status != 0:
    sys.exit("connection status error: " + str(config.connection_error))


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


def quit(msg):
    """Quit msr job.

    Args:
        msg (str): message to add to log upon exiting

    Function also manages error reporting and cleans
    up / moves request files.
    """
    # e_request_basename = os.path.basename(request_path)

    # if e_request_basename == '':
    #     e_request_basename = 'unknown'

    # e_request_basename_split = os.path.splitext(e_request_basename)

    # error_dir = e_request_basename_split[0] +"_"+ str(Ts)

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
# GENERAL INIT

# validate request and dataset
# init, inputs and variables


# check for request
if job.rank == 0:

    # import pymongo

    client = pymongo.MongoClient(config.server)

    msr = client[config.det_db].msr

    # request_list = msr.find({
    #     'status':0
    # }).sort([("priority", -1), ("submit_time", 1)]).limit(1)

    request_list = msr.find({
        'hash':"b2076778939df0791f6aa101fcd5582a2d1a789c"
    }).sort([("priority", -1), ("submit_time", 1)]).limit(1)

    # make sure request was found
    if request_list.count(True) == 1:

        request = request_list[0]
        # request_id = request['_id']

    else:
        request = None

    print request

else:
    request = None


request = job.comm.bcast(request, root=0)

if request is None:
    quit("no jobs found in queue")


# update status of request in msr queue to 2
if job.rank == 0:
    update_msr = msr.update_one({'hash': request['hash']}, {'$set': {"status": 2,}}, upsert=False)


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

# absolute path to script directory
dir_file = os.path.dirname(os.path.abspath(__file__))

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
    print release_path


release_path = job.comm.bcast(release_path, root=0)


# make sure release path exists
if not os.path.isdir(release_path):
    quit("release path specified not found: " + release_path)


# todo: make sure these exist in lookups first
abbr = iso3_lookup[dataset_id]
utm_zone = utm_lookup[abbr]


# =============================================================================
# =============================================================================

# -------------------------------------

# create instance of CoreMSR class
core = CoreMSR()

# full script start time
core.times['start'] = int(time.time())

if job.rank == 0:
    print '\n'
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' ('+ str(int(time.time())) +')'
    print 'Starting MSR'
    print '\n'

core.utm_zone = utm_zone


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


# =============================================================================
# =============================================================================
# DATAFRAME INIT

# -------------------------------------
# load / process data and get task list

dir_data = release_path +'/'+ os.path.basename(release_path) +'/data'

active_data = core.process_data(dir_data, request)

unique_ids = list(active_data['unique'])


# =============================================================================
# =============================================================================
# GRID INIT

# -------------------------------------
# set pixel size

if not 'resolution' in request['options']:
    quit("missing pixel size input from request")


core.set_pixel_size(request['options']['resolution'])


# -------------------------------------
# create point grid for country

# country bounding box
(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = core.adm0.bounds

# grid_buffer
gb = 0.5

# bounding box rounded to pixel size (always increases bounding box size, never decreases)
(adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = (
    math.floor(adm0_minx*gb)/gb,
    math.floor(adm0_miny*gb)/gb,
    math.ceil(adm0_maxx*gb)/gb,
    math.ceil(adm0_maxy*gb)/gb)

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

grid_gdf.sort(['lat', 'lon'], ascending=[False, True], inplace=True)


# -------------------------------------
# init for later (only used by master)

sum_mean_surf = 0
all_mean_surf = []

# dir_working = os.path.join(branch_dir, log, msr, jobs)
dir_working = ('/sciclone/aiddata10/REU/msr/queue/active/'
               + request['dataset'] +'_'+ request['hash'])


# =============================================================================
# =============================================================================
# MPI FUNCTIONS

def tmp_master_init(self):

    # build output directories
    make_dir(dir_working)


    # record runtime of general init
    core.times['init'] = int(time.time())
    core.durations['init'] = core.times['init'] - core.times['start']

    print '\n'
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' ('+ str(int(time.time())) +')'
    print 'Init Runtime: ' + str(core.durations['init']//60) +'m '+ str(int(core.durations['init']%60)) +'s'
    print '\n'


def tmp_worker_job(self, task_id):

    tmp_grid_gdf = grid_gdf.copy(deep=True)
    tmp_grid_gdf['value'] = 0

    task = unique_ids[task_id]

    pg_data = active_data.loc[task]
    pg_type = pg_data.agg_type

    print str(self.rank) + 'running pg_type: ' + pg_type + '('+ str(pg_data['project_location_id']) +')'


    if pg_type == "country":

        tmp_grid_gdf['value'] = tmp_grid_gdf['within'] * (pg_data['adjusted_aid'] / adm0_count)


    elif pg_type == "point":

        # round new grid points to old grid points and update old grid

        tmp_point = Point(round(pg_data.latitude * core.psi) / core.psi,
                          round(pg_data.longitude * core.psi) / core.psi)
        tmp_value = pg_data['adjusted_aid']

        if tmp_value != 0:
            tmp_grid_gdf.loc[
                tmp_grid_gdf['geometry'] == Point(
                    round(tmp_point.y * core.psi) / core.psi,
                    round(tmp_point.x * core.psi) / core.psi),
                'value'] += tmp_value


    elif pg_type in core.agg_types:

        # for each row generate grid based on bounding box of geometry
        pg_geom = pg_data.agg_geom

        try:
            pg_geom = shape(pg_geom)
        except:
            print type(pg_geom)
            print pg_geom
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

                tmp_pg_cols, tmp_pg_rows = core.geom_to_grid_colrows(
                    pg_geom_part, pg_pixel_size, rounded=True, no_multi=True)

                pg_cols = np.append(pg_cols, tmp_pg_cols)
                pg_rows = np.append(pg_rows, tmp_pg_rows)


            pg_cols = set(pg_cols)
            pg_rows = set(pg_rows)

        else:

            pg_cols, pg_rows = core.geom_to_grid_colrows(
                pg_geom, pg_pixel_size, rounded=True, no_multi=False)


        # evenly split the aid for that row
        # ( active_data['adjusted_aid'] field ) among new grid points

        tmp_product = list(itertools.product(pg_cols, pg_rows))
        tmp_gdf = gpd.GeoDataFrame()
        tmp_gdf['within'] = [0] * len(tmp_product)
        tmp_gdf['geometry'] = tmp_product
        tmp_gdf['geometry'] = tmp_gdf.apply(lambda z: Point(z.geometry), axis=1)


        # round to reference grid points and fix -0.0
        tmp_gdf['ref_lat'] = tmp_gdf.apply(lambda z: core.positive_zero(
            round(z.geometry.y * core.psi) / core.psi), axis=1)
        tmp_gdf['ref_lon'] = tmp_gdf.apply(lambda z: core.positive_zero(
            round(z.geometry.x * core.psi) / core.psi), axis=1)


        pg_geom_prep = prep(pg_geom)
        tmp_gdf['within'] = [pg_geom_prep.contains(i) for i in tmp_gdf['geometry']]


        pg_count = sum(tmp_gdf['within'])
        tmp_gdf['value'] = 0
        tmp_gdf['value'] = tmp_gdf['within'] * (pg_data['adjusted_aid'] / pg_count)

        # tmp_gdf.sort(['ref_lat','ref_lon'], ascending=[False, True], inplace=True)
        aggregated_total = tmp_gdf.groupby(['ref_lat', 'ref_lon'])['value'].sum()

        agg_df = aggregated_total.reset_index()

        agg_df['index'] = agg_df.apply(lambda z: str(z.ref_lon) +'_'+ str(z.ref_lat), axis=1)
        agg_df.set_index('index', inplace=True)


        try:
            tmp_grid_gdf.loc[agg_df.index, 'value'] += agg_df['value']

        except:
            for i in agg_df.index:
                print 'bad index iters'
                print i
                print i in tmp_grid_gdf.index


    # -------------------------------------
    # send np arrays back to master

    mean_surf = np.array(tmp_grid_gdf['value'])

    return mean_surf


def tmp_master_process(self, worker_data):
    all_mean_surf.append(worker_data)


def complete_final_raster():
    # build and output final raster


    # # initialize asc file output
    # asc = ""
    # asc += "NCOLS " + str(len(cols)) + "\n"
    # asc += "NROWS " + str(len(rows)) + "\n"

    # # asc += "XLLCORNER " + str(adm0_minx-core.pixel_size*0.5) + "\n"
    # # asc += "YLLCORNER " + str(adm0_miny-core.pixel_size*0.5) + "\n"

    # asc += "XLLCENTER " + str(adm0_minx) + "\n"
    # asc += "YLLCENTER " + str(adm0_miny) + "\n"

    # asc += "CELLSIZE " + str(core.pixel_size) + "\n"
    # asc += "NODATA_VALUE " + str(core.nodata) + "\n"


    # # calc results
    # stack_mean_surf = np.vstack(all_mean_surf)
    # sum_mean_surf = np.sum(stack_mean_surf, axis=0)

    # # write asc file
    # sum_mean_surf_str = ' '.join(np.char.mod('%f', sum_mean_surf))
    # asc_sum_mean_surf_str = asc + sum_mean_surf_str
    # fout_sum_mean_surf = open(dir_working+"/raster.asc", "w")
    # fout_sum_mean_surf.write(asc_sum_mean_surf_str)

    # --------------------------

    # calc results
    stack_mean_surf = np.vstack(all_mean_surf)
    sum_mean_surf = np.sum(stack_mean_surf, axis=0)

    # affine takes upper left
    # (writing to asc directly used lower left)
    meta = {
        'count': 1,
        'crs': {'init': 'epsg:4326'},
        'dtype': 'float64',
        'affine': Affine(
            core.pixel_size, 0.0, (adm0_minx-core.pixel_size/2),
            0.0, -core.pixel_size, (adm0_maxy+core.pixel_size/2)),
        'driver': 'GTiff',
        'height': len(rows),
        'width': len(cols),
        'nodata': core.nodata#,
        # 'compress': 'lzw'
    }

    print sum_mean_surf.shape
    print len(sum_mean_surf)
    print len(rows)
    print len(cols)

    np.array(sum_mean_surf).shape = (len(rows), len(cols))

    # write geotif file
    with rasterio.open(dir_working+"/raster.tif", "w", **meta) as dst:
        dst.write(sum_mean_surf.astype('float64'))


    # validate sum_mean_surf
    # exit if validation fails
    if isinstance(sum_mean_surf, int):
        sys.exit("! - mean surf validation failed")



def complete_unique_geoms():
    # output unique geometries and sum of all
    # project locations associated with that geometry

    # creating geodataframe
    geo_df = gpd.GeoDataFrame()
    # location id
    geo_df["project_location_id"] = active_data["project_location_id"]
    # assuming even split of total project dollars is "max" dollars
    # that project location could receive
    geo_df["dollars"] = active_data["adjusted_aid"]
    # geometry for each project location
    geo_df["geometry"] = gpd.GeoSeries(active_data["agg_geom"])
    # string version of geometry used to determine duplicates
    geo_df["str_geo"] = geo_df["geometry"].astype(str)
    # create and set unique index
    geo_df['index'] = range(0, len(geo_df))
    geo_df = geo_df.set_index('index')

    # group project locations by geometry using str_geo field
    # and for each unique geometry get the sum of dollars for
    # all project locations with that geometry
    sum_unique = geo_df.groupby(by='str_geo')['dollars'].sum()

    # get count of locations for each unique geom
    geo_df['ones'] = (pd.Series(np.ones(len(geo_df)))).values
    sum_count = geo_df.groupby(by='str_geo')['ones'].sum()

    # create list of project location ids for unique geoms
    cat_plids = geo_df.groupby(by='str_geo')['project_location_id'].apply(
        lambda z: '|'.join(list(z)))


    # temporary dataframe with
    #   unique geometry
    #   location_count
    #   dollar sums
    # which can be used to merge with original geo_df dataframe
    tmp_geo_df = gpd.GeoDataFrame()
    tmp_geo_df['unique_dollars'] = sum_unique
    tmp_geo_df['location_count'] = sum_count
    tmp_geo_df['project_location_ids'] = cat_plids

    tmp_geo_df['str_geo'] = tmp_geo_df.index

    # merge geo_df with tmp_geo_df
    new_geo_df = geo_df.merge(tmp_geo_df, how='inner', on="str_geo")
    # drops duplicate rows
    new_geo_df.drop_duplicates(subset="str_geo", inplace=True)
    # gets rid of str_geo column
    new_geo_df.drop('str_geo', axis=1, inplace=True)

    # create final output geodataframe with index, unique_dollars and unique geometry
    out_geo_df = gpd.GeoDataFrame()
    out_geo_df["geometry"] = gpd.GeoSeries(new_geo_df["geometry"])
    out_geo_df["unique_dollars"] = new_geo_df["unique_dollars"]
    out_geo_df["location_count"] = new_geo_df["location_count"]
    out_geo_df["project_location_ids"] = new_geo_df["project_location_ids"]

    out_geo_df['index'] = range(len(out_geo_df))

    # write to geojson
    geo_json = out_geo_df.to_json()
    geo_file = open(dir_working+"/unique.geojson", "w")
    json.dump(json.loads(geo_json), geo_file, indent=4)


def complete_options_json():
    # output msr options as json (might be loaded into mongo?)

    options_obj = OrderedDict()

    def add_to_json(field, data):
        options_obj[field] = data

    # job / script info
    add_to_json("run_id", run_id)
    add_to_json("run_stage", run_stage)
    add_to_json("run_version_str", run_version_str)
    add_to_json("run_version", run_version)
    add_to_json("job_size", job.size)

    # dataset info
    add_to_json("dataset", request['dataset'])
    add_to_json("abbr", abbr)
    add_to_json("utm_zone", core.utm_zone)

    # core run options
    add_to_json("pixel_size", core.pixel_size)
    add_to_json("nodata", core.nodata)
    add_to_json("aid_field", core.aid_field)
    add_to_json("is_geocoded", core.is_geocoded)
    add_to_json("only_geocoded", core.only_geocoded)
    add_to_json("not_geocoded", core.not_geocoded)
    add_to_json("code_field_1", core.code_field_1)
    add_to_json("code_field_2", core.code_field_2)
    add_to_json("agg_types", core.agg_types)
    add_to_json("lookup", core.lookup)

    # resulting spatial / table info
    add_to_json("adm0_minx", adm0_minx)
    add_to_json("adm0_miny", adm0_miny)
    add_to_json("adm0_maxx", adm0_maxx)
    add_to_json("adm0_maxy", adm0_maxy)
    add_to_json("rows", len(rows))
    add_to_json("cols", len(cols))
    add_to_json("locations", len(active_data))

    # status
    # add_to_json("dir_working", dir_working)
    # add_to_json("status", 0)

    # times / durations
    add_to_json("times", core.times)
    add_to_json("durations", core.durations)
    add_to_json("cpu_hours",
                math.ceil(
                    100 * float(core.durations['total']) * job.size / 3600) / 100)

    # # times
    # add_to_json("time_start", core.times['start'])
    # add_to_json("time_init", core.times['init'])
    # add_to_json("time_surf", core.times['surf'])
    # add_to_json("time_output", core.times['output'])
    # add_to_json("time_total", core.times['total'])
    # add_to_json("time_end", core.times['end'])

    # # timings
    # add_to_json("dur_init", core.durations['init'])
    # add_to_json("dur_surf", core.durations['surf'])
    # add_to_json("dur_output", core.durations['output'])
    # add_to_json("dur_total", core.durations['total'])


    tmp_request = request
    if "_id" in tmp_request.keys():
        tmp_request['_id'] = str(tmp_request['_id'])

    write_options = deepcopy(options_obj)
    write_options["request"] = tmp_request

    # write output.json
    json_out = dir_working+'/output.json'
    json_handle = open(json_out, 'w')
    json.dump(write_options, json_handle, sort_keys=False, indent=4, ensure_ascii=False)

    return options_obj


def complete_outputs():
    # move entire dir for job from msr queue "active" dir to "done" dir
    # and copy data files to msr data dir


    # move entire dir for job from msr queue "active" dir to "done" dir
    dir_final = dir_working.replace('/active/', '/done/')

    if os.path.isdir(dir_final):
        shutil.rmtree(dir_final)

    shutil.move(dir_working, dir_final)


    # make msr data dir and move raster.asc, unique.geojson, output.json there
    msr_data_dir = ('/sciclone/aiddata10/REU/data/rasters/internal/msr/'
                    + request['dataset'] +'/'+ request['hash'])
    make_dir(msr_data_dir)

    msr_data_files = ['raster.asc', 'unique.geojson', 'output.json']
    for f in msr_data_files:
        msr_data_file = dir_final +'/'+ f

        # if os.path.isfile(msr_data_dst_file):
            # os.remove(msr_data_dst_file)

        shutil.copy(msr_data_file, msr_data_dir)
        os.remove(msr_data_file)


def tmp_master_final(self):


    # record surf runtime
    core.times['surf'] = int(time.time())
    core.durations['surf'] = core.times['surf'] - core.times['init']

    print '\n'
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' ('+ str(int(time.time())) +')'
    print 'Surf Runtime: ' + str(core.durations['surf']//60) +'m '+ str(int(core.durations['surf']%60)) +'s'
    print '\n'


    # run final output gen functions
    complete_final_raster()
    complete_unique_geoms()


    # calc section runtime and total runtime
    core.times['output'] = int(time.time())
    core.durations['output'] = core.times['output'] - core.times['surf']
    # core.times['total'] = int(time.time())
    core.times['end'] = int(time.time())
    core.durations['total'] = core.times['end'] - core.times['start']


    print '\n'
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' ('+ str(int(time.time())) +')'
    print 'Output Runtime: ' + str(core.durations['output']//60) +'m '+ str(int(core.durations['output']%60)) +'s'
    print 'Total Runtime: ' + str(core.durations['total']//60) +'m '+ str(int(core.durations['total']%60)) +'s'
    print '\n'

    print 'Ending MSR'


    # write output json and finalize output folders
    output_obj = complete_options_json()
    complete_outputs()

    # update status of request in msr queue
    # and add output_obj to "output" field
    update_msr = msr.update_one({'hash': request['hash']}, {'$set': {"status": 1, "info": output_obj}}, upsert=False)



# =============================================================================
# =============================================================================
# RUN MPI

# init / run job

# job = NewParallel()
job.set_task_list(unique_ids)

# job.set_general_init(tmp_general_init)
job.set_master_init(tmp_master_init)
job.set_master_process(tmp_master_process)
job.set_master_final(tmp_master_final)
job.set_worker_job(tmp_worker_job)

# try:
job.run()
# except Exception as err:
#     print err
#     # add error status to request in msr queue
#     update_msr = msr.update_one({'hash': request['hash']}, {'$set': {"status": -1,}}, upsert=False)
