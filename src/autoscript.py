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
"""

# =============================================================================
# =============================================================================

import sys
import os
import errno
import time
import datetime
import math
import itertools
# import json
import ujson as json
import shutil
import re
import hashlib

from copy import deepcopy
from collections import OrderedDict


import numpy as np
import pandas as pd
import geopandas as gpd

from shapely.geometry import Point, shape, box #, MultiPolygon, Polygon, box
from shapely.prepared import prep

import shapefile

import warnings
import rasterio
from affine import Affine

from msr_utility import CoreMSR, MasterStack


# =============================================================================
# =============================================================================
# basic init and find request

# from mpi4py import MPI
import mpi_utility

job = mpi_utility.NewParallel()

request = 0


def get_version():
    vfile = os.path.join(os.path.dirname(__file__), "_version.py")
    with open(vfile, "r") as vfh:
        vline = vfh.read()
    vregex = r"^__version__ = ['\"]([^'\"]*)['\"]"
    match = re.search(vregex, vline, re.M)
    if match:
        return match.group(1)
    else:
        raise RuntimeError("Unable to find version string in {}.".format(vfile))


import pymongo

# -------------------------------------

# import sys
# import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
sys.path.insert(0, config_dir)

from config_utility import BranchConfig

config = BranchConfig(branch=branch)


# -------------------------------------


# check mongodb connection
if config.connection_status != 0:
    sys.exit("connection status error: " + str(config.connection_error))


# -------------------------------------------------------------------------
# -------------------------------------------------------------------------
# find request

# import pymongo
client = pymongo.MongoClient(config.server)
c_asdf = client.asdf.data
c_msr = client.asdf.msr

tmp_v1 = config.versions["mean-surface-rasters"]
tmp_v2 = get_version()

if tmp_v1 == tmp_v2:
    version = tmp_v1
else:
    raise Exception("Config and src versions do not match")


general_output_base = '/sciclone/aiddata10/REU/outputs/' + branch + '/msr'


if job.rank == 0:

    print 'starting request search'
    search_limit = 5
    search_attempt = 0


    while search_attempt < search_limit:

        print 'finding request:'
        find_request = c_msr.find_one({
            'status': 0,
            'priority': {'$gte': 0}
        }, sort=[("priority", -1), ("submit_time", 1)])

        ###
        if find_request is None:
            find_request = c_msr.find_one({
                # 'hash': '980ae30d8cdeb8115ab34093cd49c499cbee4680',
                'status': 0,
                'priority': {'$lt': 0}
            }, sort=[("priority", -1), ("percentage", 1)])
            # }, sort=[("priority", -1), ("percentage", -1)])
        ###

        print find_request

        if find_request is None:
            request = None
            break

        request_accept = c_msr.update_one({
            '_id': find_request['_id'],
            'status': find_request['status']
        }, {
            '$set': {
                'status': 2,
                'update_time': int(time.time())
            }
        })

        print request_accept.raw_result

        if request_accept.acknowledged and request_accept.modified_count == 1:
            request = find_request
            break


        search_attempt += 1

        print 'looking for another request...'


    if search_attempt == search_limit:
        request = 'Error'

    print 'request found'


# ensure workers do not proceed until master successfully finds request
job.comm.Barrier()


# =============================================================================
# =============================================================================
# GENERAL FUNCTIONS

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
    #   general_output_base + '/error/' + error_dir
    # make_dir()

    # if os.path.isfile(request_path):
        # move request to error_dir
        #

    # add error file to error_dir
    #

    sys.exit(msg)


# def log(msg):
#     """Add message to msr log.

#     Args:
#         msg (str): message to add to log
#     """
#     msg = str(msg)


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


def str_sha1_hash(hash_str):

    hash_builder = hashlib.sha1()
    hash_builder.update(hash_str)
    hash_sha1 = hash_builder.hexdigest()
    return hash_sha1


# =============================================================================
# =============================================================================
# MPI FUNCTIONS

def tmp_master_init(self):

    # record runtime of general init
    core.times['init'] = int(time.time())
    core.durations['init'] = core.times['init'] - core.times['start']

    print '\n'
    print (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +
           ' ('+ str(int(time.time())) +')')
    print ('Init Runtime: ' + str(core.durations['init']//60) +'m '+
           str(int(core.durations['init']%60)) +'s')
    print '\n'


def tmp_worker_job(self, task_id):

    task = task_id_list[task_id]

    pg_data = active_data.loc[task]
    pg_type = pg_data.geom_type

    print (str(self.rank) + 'running pg_type: ' + pg_type +
           '('+ str(pg_data['project_location_id']) +')')


    if pg_type == "country":
        mean_surf = master_grid
        pg_geom = 0

    elif pg_type in core.geom_types:

        # for each row generate grid based on bounding box of geometry
        # pg_geom = pg_data.geom_val

        pg_geom = core.get_geom_val(
            pg_data.geom_type, pg_data[core.code_field_1],
            pg_data[core.code_field_2], pg_data[core.code_field_3],
            pg_data.longitude, pg_data.latitude)

        if pg_geom is None:
            sys.exit("Geom is none" + str(pg_data['project_location_id']))

        try:
            pg_geom = shape(pg_geom)
        except:
            print str(pg_data['project_location_id'])
            print type(pg_geom)
            print pg_geom
            sys.exit("Geom is invalid")

        # factor used to determine subgrid size
        # relative to output grid size
        # sub grid res = output grid res / sub_grid_factor
        subgrid_scale = 10

        # rasterized sub grid
        mean_surf = core.rasterize_geom(pg_geom, scale=subgrid_scale)


    mean_surf = mean_surf.astype('float64')
    mean_surf = pg_data['adjusted_aid'] * mean_surf / mean_surf.sum()
    return (mean_surf.flatten(), pg_geom)


def tmp_master_process(self, worker_data):
    surf, geom = worker_data
    mstack.append_stack(surf)

    if mstack.get_stack_size() > 1:
	print "reducing stack"
        mstack.reduce_stack()


def complete_final_raster():
    # build and output final raster

    # calc results
    sum_mean_surf = mstack.get_stack_sum()

    # affine takes upper left
    # (writing to asc directly used lower left)
    meta = {
        'count': 1,
        'crs': {'init': 'epsg:4326'},
        'dtype': 'float64',
        'affine': core.affine,
        'driver': 'GTiff',
        'height': core.shape[0],
        'width': core.shape[1],
        'nodata': core.nodata#,
        # 'compress': 'lzw'
    }

    sum_mean_surf.shape = core.shape

    out_mean_surf = np.array([sum_mean_surf.astype('float64')])

    # write geotif file
    with rasterio.open(dir_working + "/raster.tif", "w", **meta) as dst:
        dst.write(out_mean_surf)


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
    geo_df["project_location_id"].fillna(active_data["project_id"],
                                         inplace=True)
    geo_df["project_location_id"] = geo_df["project_location_id"].astype(str)

    # assuming even split of total project dollars is "max" dollars
    # that project location could receive
    geo_df["dollars"] = active_data["adjusted_aid"]
    # geometry for each project location
    geo_df["geometry"] = gpd.GeoSeries(active_data["geom_val"])

    # # write full to geojson
    # full_geo_json = geo_df.to_json()
    # full_geo_file = open(dir_working + "/full.geojson", "w")
    # json.dump(json.loads(full_geo_json), full_geo_file, indent=4)
    # full_geo_file.close()


    # string version of geometry used to determine duplicates
    geo_df["str_geo_hash"] = geo_df["geometry"].astype(str).apply(
        lambda z: str_sha1_hash(z))

    # create and set unique index
    geo_df['index'] = range(0, len(geo_df))
    geo_df = geo_df.set_index('index')

    # group project locations by geometry using str_geo_hash field
    # and for each unique geometry get the sum of dollars for
    # all project locations with that geometry
    sum_unique = geo_df.groupby(by='str_geo_hash')['dollars'].sum()

    # get count of locations for each unique geom
    geo_df['ones'] = (pd.Series(np.ones(len(geo_df)))).values
    sum_count = geo_df.groupby(by='str_geo_hash')['ones'].sum()

    # create list of project location ids for unique geoms
    cat_plids = geo_df.groupby(by='str_geo_hash')['project_location_id'].apply(
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

    tmp_geo_df['str_geo_hash'] = tmp_geo_df.index

    # merge geo_df with tmp_geo_df
    new_geo_df = geo_df.merge(tmp_geo_df, how='inner', on="str_geo_hash")
    # drops duplicate rows
    new_geo_df.drop_duplicates(subset="str_geo_hash", inplace=True)
    # gets rid of str_geo_hash column
    new_geo_df.drop('str_geo_hash', axis=1, inplace=True)

    # create final output geodataframe with index, unique_dollars
    # and unique geometry
    unique_geo_df = gpd.GeoDataFrame()
    unique_geo_df["geometry"] = gpd.GeoSeries(new_geo_df["geometry"])
    unique_geo_df["unique_dollars"] = new_geo_df["unique_dollars"]
    unique_geo_df["location_count"] = new_geo_df["location_count"]
    unique_geo_df["project_location_ids"] = new_geo_df["project_location_ids"]

    unique_geo_df['index'] = range(len(unique_geo_df))

    # write unique to geojson
    unique_geo_json = unique_geo_df.to_json()
    unique_geo_file = open(dir_working + "/unique.geojson", "w")
    json.dump(json.loads(unique_geo_json), unique_geo_file, indent=4)
    unique_geo_file.close()


def complete_options_json():
    # output msr options as json (might be loaded into mongo?)

    options_obj = OrderedDict()

    def add_to_json(field, data):
        options_obj[field] = data

    # job / script info
    # add_to_json("run_id", run_id)
    # add_to_json("run_stage", run_stage)
    # add_to_json("run_version_str", run_version_str)
    # add_to_json("run_version", run_version)
    add_to_json("version", version)

    add_to_json("job_size", job.size)

    # dataset info
    add_to_json("dataset", request['dataset'])
    add_to_json("iso3", iso3)

    # core run options
    add_to_json("pixel_size", core.pixel_size)
    add_to_json("nodata", core.nodata)
    add_to_json("aid_field", core.aid_field)
    add_to_json("is_geocoded", core.is_geocoded)
    add_to_json("only_geocoded", core.only_geocoded)
    add_to_json("not_geocoded", core.not_geocoded)
    add_to_json("code_field_1", core.code_field_1)
    add_to_json("code_field_2", core.code_field_2)
    add_to_json("geom_types", core.geom_types)
    add_to_json("lookup", core.lookup)

    # resulting spatial / table info
    add_to_json("adm0_minx", adm0_minx)
    add_to_json("adm0_miny", adm0_miny)
    add_to_json("adm0_maxx", adm0_maxx)
    add_to_json("adm0_maxy", adm0_maxy)
    add_to_json("rows", nrows)
    add_to_json("cols", ncols)
    add_to_json("locations", len(active_data))

    # status
    # add_to_json("dir_working", dir_working)
    # add_to_json("status", 0)

    # times / durations
    add_to_json("times", core.times)
    add_to_json("durations", core.durations)

    cpu_hours = math.ceil(
        100 * float(core.durations['total']) * job.size / 3600) / 100

    add_to_json("cpu_hours", cpu_hours)

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


    tmp_request = deepcopy(request)
    if "_id" in tmp_request.keys():
        tmp_request['_id'] = str(tmp_request['_id'])

    write_options = deepcopy(options_obj)
    write_options["request"] = tmp_request

    # write summary.json
    json_out = dir_working + '/summary.json'
    json_handle = open(json_out, 'w')
    json.dump(write_options, json_handle, sort_keys=False, indent=4,
              ensure_ascii=True)
    json_handle.close()

    return options_obj


def complete_outputs():

    dir_final = (general_output_base + '/done/' +
                    request['dataset'] + '/' + request['hash'])

    if os.path.isdir(dir_final):
        shutil.rmtree(dir_final)

    shutil.move(dir_working, dir_final)


def tmp_master_final(self):


    # record surf runtime
    core.times['surf'] = int(time.time())
    core.durations['surf'] = core.times['surf'] - core.times['init']

    print '\n'
    print (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +
           ' ('+ str(int(time.time())) +')')
    print ('Surf Runtime: ' + str(core.durations['surf']//60) +'m '+
           str(int(core.durations['surf']%60)) +'s')
    print '\n'


    # build output directories
    make_dir(dir_working)

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
    print (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +
           ' ('+ str(int(time.time())) +')')
    print ('Output Runtime: ' + str(core.durations['output']//60) +'m '+
           str(int(core.durations['output']%60)) +'s')
    print ('Total Runtime: ' + str(core.durations['total']//60) +'m '+
           str(int(core.durations['total']%60)) +'s')
    print '\n'

    print 'Ending MSR'


    # write output json and finalize output folders
    output_obj = complete_options_json()
    complete_outputs()

    # update status of request in msr queue
    # and add output_obj to "output" field
    update_msr = c_msr.update_one({
        '_id': request['_id']
    }, {
        '$set': {
            'status': 1,
            'update_time': int(time.time()),
            'info': output_obj
        }
    }, upsert=False)

    print request['_id']
    print request['hash']
    print request

    print update_msr.raw_result


# =============================================================================
# =============================================================================
# INIT

# validate request and dataset
# init, inputs and variables


request = job.comm.bcast(request, root=0)

if request is None:
    quit("no jobs found in queue")

elif request == 'Error':
    quit("error updating request status in mongodb")

elif request == 0:
    quit("error getting request from master")


# -------------------------------------
# lookup release path

release_path = None
release_preamble = None
iso3 = None

# if job.rank == 0:

dir_working = (general_output_base + '/active/' +
               request['dataset'] +'_'+ request['hash'])

release_data = c_asdf.find({'name': request['dataset']})

release_path = release_data[0]['base']
release_preamble = release_data[0]['data_set_preamble']

print release_path
print release_preamble

# make sure release path exists
if not os.path.isdir(release_path):
    quit("release path specified not found: " + release_path)

if release_preamble not in config.release_gadm:
    quit("release premable not found in config: " + release_preamble)

iso3 = config.release_gadm[release_preamble]


# release_path = job.comm.bcast(release_path, root=0)
# release_preamble = job.comm.bcast(release_preamble, root=0)
# iso3 = job.comm.bcast(iso3, root=0)


# -------------------------------------

# create instance of CoreMSR class
core = CoreMSR()

# full script start time
core.times['start'] = int(time.time())

if job.rank == 0:
    print '\n'
    print (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +
           ' ('+ str(int(time.time())) +')')
    print 'Starting MSR'
    print '\n'


# -------------------------------------
# set pixel size

if not 'resolution' in request['options']:
    quit("missing pixel size input from request")


core.set_pixel_size(request['options']['resolution'])


# =============================================================================
# =============================================================================
# SHAPES / GRID INIT


if iso3 == 'global':
    raise Exception('not ready for global yet')
    world_box = box(-190, -100, 180, 90)

else:

    # -------------------------------------
    # load shapefiles

    # must start at and inlcude ADM0
    # all additional ADM shps must be included so that adm_path index corresponds
    # to adm level
    adm_paths = []
    adm_paths.append("/sciclone/aiddata10/REU/msr/shps/"+iso3+"/"+iso3+"_adm0.shp")
    adm_paths.append("/sciclone/aiddata10/REU/msr/shps/"+iso3+"/"+iso3+"_adm1.shp")
    adm_paths.append("/sciclone/aiddata10/REU/msr/shps/"+iso3+"/"+iso3+"_adm2.shp")

    # build list of adm shape lists
    core.adm_shps = [shapefile.Reader(adm_path).shapes() for adm_path in adm_paths]

    # define country shape
    tmp_adm0 = shape(core.adm_shps[0][0])
    core.set_adm0(tmp_adm0)


    # -------------------------------------
    # create grid for country
    core.set_grid_info(core.adm0)
    master_grid = core.rasterize_geom(core.adm0)

    nrows, ncols = core.shape
    (adm0_minx, adm0_miny, adm0_maxx, adm0_maxy) = core.bounds
    adm0_count = master_grid.sum()



# =============================================================================
# =============================================================================
# DATAFRAME INIT

# -------------------------------------
# load / process data and get task list

dir_data = release_path + '/data'

active_data = core.process_data(dir_data, request)

task_id_list = None

if job.rank == 0:

    active_data["geom_val"] = pd.Series(["None"] * len(active_data))

    active_data.geom_val = active_data.apply(lambda x: core.get_geom_val(
        x.geom_type, x[core.code_field_1], x[core.code_field_2],
        x[core.code_field_3], x.longitude, x.latitude), axis=1)

    active_data = active_data.loc[
        active_data.geom_val != "None"].copy(deep=True)

    task_id_list = list(active_data['task_ids'])


task_id_list = job.comm.bcast(task_id_list, root=0)


if task_id_list is None:
    quit("task id list is missing")

if job.rank == 0:
    print str(len(task_id_list)) + " tasks to process."
    print "Preparing main grid..."


# =============================================================================
# =============================================================================
# RUN MPI (init / run job)

if job.rank == 0:
    print "Starting to process tasks..."
    sum_mean_surf = 0

    mstack = MasterStack()


job.set_task_list(task_id_list)

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
#     update_msr = c_msr.update_one({'hash': request['hash']},
#                                 {'$set': {"status": -1,}},
#                                 upsert=False)

