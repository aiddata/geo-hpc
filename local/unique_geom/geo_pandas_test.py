#
# runscript.py
#


# ====================================================================================================
# ====================================================================================================


from __future__ import print_function

#from mpi4py import MPI

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
import pyproj
from shapely.ops import transform
import shapefile
import geopandas as gpd
from functools import partial

# ====================================================================================================
# ====================================================================================================
# general init


# mpi info
# comm = MPI.COMM_WORLD
# size = comm.Get_size()
# rank = comm.Get_rank()
# status = MPI.Status()

t0 = time.time()
# absolute path to script directory
dir_base = os.path.dirname(os.path.abspath(__file__))


# ====================================================================================================
# ====================================================================================================
# inputs and variables


run_stage = "beta"
run_version_str = "005"
run_version = int(run_version_str)
run_id = run_stage[0:1] + run_version_str

Ts = int(time.time())
random_id = '{0:05d}'.format(int(random.random() * 10**5))
Rid = "mcr_" + str(Ts) +"_"+ random_id

Rid = "mcr_" + str(Ts) +"_"+ "56789"


# --------------------------------------------------
# input arguments

# python /path/to/runscript.py nepal NPL 0.1 10
arg = sys.argv

try:
    country = "nepal" #sys.argv[1]
    abbr = "NPL" #sys.argv[2]
    pixel_size = float(0.5)

    # data_version = sys.argv[10]
    data_version = 1.1

    # raw_filter = sys.argv[5]

    # force_mean_surf = int(sys.argv[x])
    force_mean_surf = 0

    # only run mean surf
    mean_surf_only = 0

    # run_mean_surf = int(sys.argv[8])
    # run_mean_surf = 3
    # path_mean_surf = "data/nepal/nepal_0.5_1432844232_12347/outputs/output_nepal_0.5_surf.npy"

    # if run_mean_surf == 3:
        # path_mean_surf = sys.argv[9]

    # log_mean_surf = int(sys.argv[12])
    # log_mean_surf = 0


except:
    sys.exit("invalid inputs")


# --------------------------------------------------
# iteration and pixel size options

# maximum number of iterations to run
iter_max = 1000

# iterations range
i_control = range(int(iter_max))

# iteration intervals at which to check error val
iter_interval = [10, 50, 100, 250, 500, 750, 1000, 5000, 10000, 50000, 100000]

# alternative to manual intervals
# generates intervals based on fixed steps
# iter_min = 10
# iter_step = 0
# iter_interval = range(10, iter_max+1, iter_step)

# difference from true mean (decimal percentage)
iter_thresh = 0.05

# minimum improvement over previous iteration interval required to continue (decimal percentage)
iter_improvement = 0.001


# check for valid pixel size
# examples of valid pixel sizes: 1.0, 0.5, 0.25, 0.2, 0.1, 0.05, 0.025, ...
if (1/pixel_size) != int(1/pixel_size):
    sys.exit("invalid pixel size: "+str(pixel_size))

# pixel size inverse
psi = 1/pixel_size


# --------------------------------------------------
# filter options

# filter_type = "all"
filters_type = "specfic"

filters = {
    "ad_sector_names": {
        "Agriculture": 0
    }
}


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
lookup = {
    "1": {"type":"point","data":0},
    "2": {"type":"buffer","data":1},
    "3": {"type":"adm","data":"2"},
    "4": {"type":"adm","data":"2"},
    "5": {"type":"buffer","data":1},
    "6": {"type":"adm","data":"0"},
    "7": {"type":"adm","data":"0"},
    "8": {"type":"adm","data":"0"}
}

# --------------------------------------------------
# file paths

# dir_country = dir_base+"/outputs/"+country
# dir_working = dir_country+"/"+country+"_"+str(pixel_size)+"_"+str(iterations)+"_"+str(int(Ts))

#dir_country = dir_base+"/data/"+country
# dir_country = dir_base + "countries"
# dir_chain = dir_country+"/"+country+"_"+str(data_version)+"_"+run_id+"_"+str(pixel_size)
# dir_outputs = dir_chain+"/outputs"
# dir_working = dir_outputs+"/"+str(Rid)


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
    # if path.endswith('.tsv'):
    #     return pd.read_csv(path, sep='\t', quotechar='\"', na_values='', keep_default_na=False)
    # elif path.endswith('.csv'):
    #     return pd.read_csv(path, quotechar='\"', na_values='', keep_default_na=False)
    # else:
    #     sys.exit('getCSV - file extension not recognized.\n')
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

from shapely.ops import transform as transform_shapely
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
            tmp_int = float(lookup[code]["data"])
            #tmp_buffer = tmp_pnt.buffer(tmp_int)

            proj_utm = pyproj.Proj('+proj=utm +zone=45 +ellps=WGS84 +datum=WGS84 +units=m +no_defs ')
            proj_wgs = pyproj.Proj(init="epsg:4326")
            tmp_pnt_2a = pyproj.transform(proj_wgs, proj_utm, tmp_pnt.x, tmp_pnt.y)
            tmp_pnt_2 = Point(tmp_pnt_2a)
            b2 = tmp_pnt_2.buffer(tmp_int)

            polyproj = partial(pyproj.transform, proj_utm, proj_wgs)
            tmp_buffer = transform(polyproj, b2)

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
# ====================================================================================================


# --------------------------------------------------
# load shapefiles

# must start at and inlcude ADM0
# all additional ADM shps must be included so that adm_path index corresponds to adm level
adm_paths = []
adm_paths.append(dir_base+"/countries/"+country+"/shapefiles/ADM0/"+abbr+"_adm0.shp")
adm_paths.append(dir_base+"/countries/"+country+"/shapefiles/ADM1/"+abbr+"_adm1.shp")
adm_paths.append(dir_base+"/countries/"+country+"/shapefiles/ADM2/"+abbr+"_adm2.shp")

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

dir_data = dir_base+"/countries/"+country+"/data/"+country+"_"+str(data_version)+"/data"

merged = getData(dir_data, "project_id", (code_field, "project_location_id"), only_geocoded)

#convert df into geoPandas df - maybe change in getData function

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
filters_hash = json_hash(filters)


# apply filters to project data
# filtered = merged.loc[merged.ad_sector_names == "Agriculture"]

# !!! potential issue !!!
#
# - filters which remove only some locations from a project will skew aid splits
# - moved original project location count to before filters so that it can be used to
#   compare the count of project locations before filter to count after and generate
#   placeholder random values for the locations that were filtered out
# - method: recheck project location count and create placeholder random value if locations are missing
# - will need to rebuild how random num column is added. probaby can use apply with a new function

filtered = deepcopy(merged)


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


# -----------------------------------------------


#creating geodataframe
geo_df = gpd.GeoDataFrame()
geo_df["dollars"] = i_m["split_dollars_pp"]
geo_df["geometry"] = gpd.GeoSeries(i_m["agg_geom"])
geo_df["str_geo"] = geo_df["geometry"].astype(str)

geo_df['index'] = range(0, len(geo_df))
geo_df = geo_df.set_index('index')
# geo_df.to_csv(dir_base+"/geo_df.csv")


sum_unique = geo_df.groupby(by ='str_geo')['dollars'].sum()
print(type(sum_unique))


tmp_geo_df = gpd.GeoDataFrame()
tmp_geo_df['unique_dollars'] = sum_unique
tmp_geo_df['str_geo'] = tmp_geo_df.index
print("TEMP_GEO_DF \n")
print(type(tmp_geo_df))
print(len(tmp_geo_df))
# tmp_geo_df.to_csv(dir_base+"/tmp_geo_df.csv")


new_geo_df = geo_df.merge(tmp_geo_df, how = 'inner', on = "str_geo")
print("NEW_GEO_DF \n")
print(type(new_geo_df))
print(len(new_geo_df))


#drops duplicate rows
new_geo_df.drop_duplicates(subset = "str_geo", inplace = True)

#gets rid of str_geo column
new_geo_df.drop('str_geo', axis = 1, inplace = True)


out_geo_df = gpd.GeoDataFrame()
out_geo_df["unique_dollars"] = new_geo_df["unique_dollars"]
out_geo_df['index'] = range(len(out_geo_df))

out_geo_df["geometry"] = gpd.GeoSeries(new_geo_df["geometry"])

#geo_df.drop("agg_geom", 1, inplace = True)
# print(new_geo_df)
# out_geo_df.to_csv(dir_base+"/out_geo_df.csv")




geo_json = out_geo_df.to_json()
geo_file = open(dir_base+"/string.geojson", "w")
json.dump(json.loads(geo_json), geo_file, indent = 4)



t1 = time.time()
print("Elapsed time is %5.3f." % (t1-t0))
#i_m.to_csv(dir_base+"/test_df.csv", index = False)

#sys.exit('!')





