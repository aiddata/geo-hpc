

# accepts request object and checks if all extracts have been processed (return boolean)

import sys
import os
import errno
import time
import json

import pymongo
from bson.objectid import ObjectId

import pandas as pd 
import geopandas as gpd

# import subprocess as sp

from rpy2.robjects.packages import importr
from rpy2 import robjects


sys.stdout = sys.stderr = open(os.path.dirname(os.path.abspath(__file__)) +'/processing.log', 'a')

print '\n------------------------------------------------'
print 'Extract Script'
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())

# connect to mongodb
client = pymongo.MongoClient()

c_asdf = client.asdf.data
c_extracts = client.det.extracts



# creates directories
def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


# get id of next job in queue
# based on priority and submit_time
# factor how many extracts need to be processed into queue order (?)
def get_next(status, limit):
    
    try:
        # find all status x jobs and sort by priority then submit_time
        sort = c_extracts.find({"status":status}).sort([("priority", -1), ("submit_time", 1)])

        if sort.count() > 0:
            return 1, (str(sort[0]["_id"]), sort[0])

        else:
            return 1, None

    except:
        return 0, None


# update status of request
def update_status(rid, status):
    
    ctime = int(time.time())

    updates = {
        "status": long(status),
        "update_time": ctime
    }
    
    # try:
        # # update request document
    c_extracts.update({"_id": ObjectId(rid)}, {"$set": updates})
        # return True, ctime

    # except:
        # return False, None


# run extract using rpy2
def rpy2_extract(raster, output, extract_type):
    print "rpy2_extract"
# try:
    r_raster = rlib_raster.raster(raster)

    # *** need to implement different kwargs based on extract type ***
    if extract_type == "mean":
        kwargs = {"fun":extract_funcs[extract_type], "sp":True, "na.rm":True, "weights":True, "small":True}
    else:
        kwargs = {"fun":extract_funcs[extract_type], "sp":True, "na.rm":True}

    robjects.r.assign('r_extract', rlib_raster.extract(r_raster, r_boundary, **kwargs))

    robjects.r.assign('r_output', output)

    robjects.r('colnames(r_extract@data)[length(colnames(r_extract@data))] <- "ad_extract"')
    make_dir(os.path.dirname(output))

    robjects.r('write.table(r_extract@data, r_output, quote=T, row.names=F, sep=",")')
    
    return True, None

# except:
#     return False, "R extract failed"



# use subprocess to run Rscript
# def script_extract(boundary, raster, output, extract_type):

#     try:
#         cmd = "Rscript " + os.path.dirname(__file__) + "/extract.R " + boundary +" "+ raster +" "+ output +" "+ extract_type
#         print cmd

#         sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
#         print sts

#         return True

#     except:
#         return False



# reliability calcs for extract
# intersect for every single boundary that intersects with unique ones from geojson
# sum for each intersect
def run_reliability(boundary, reliability_geojson, output):
    print "run_reliability"
# try: 

    # extract boundary geo dataframe
    bnd_df = gpd.GeoDataFrame.from_file(boundary)

    # mean surface unique polygon dataframe
    rel_df = gpd.GeoDataFrame.from_file(reliability_geojson)


    # result of mean surface extracted to boundary
    df = pd.DataFrame.from_csv(output)

    # index to merge with bnd_df
    # df['ad_id'] = bnd_df['ad_id']

    # init max column
    df['max'] = 0 *len(df)

    # iterate over shapes in boundary dataframe
    for row_raw in bnd_df.iterrows():

        row = row_raw[1]
        geom = row['geometry']
        # id field common to both dfs
        unique_id = row['ad_id']
        rel_df['intersect'] = rel_df['geometry'].intersects(geom)
        tmp_series = rel_df.groupby(by = 'intersect')['unique_dollars'].sum()
        
        df.loc[df['ad_id'] == unique_id, 'max'] = tmp_series[True]
        

    # calculate reliability statistic
    df["ad_extract"] = df['ad_extract']/df['max']

    # output to reliability csv
    df.to_csv(output[:-5]+"r.csv")

    return True

# except:
#     return False







rlib_rgdal = importr("rgdal")
rlib_raster = importr("raster")

# list of valid extract types with r functions
extract_funcs = {
    "mean": robjects.r.mean,
    "max": robjects.r.max,
    "sum": robjects.r.sum
}

extract_options_path = os.path.dirname(os.path.abspath(__file__)) + '/extract_options.json'
extract_options = json.load(open(extract_options_path, 'r'))




gn_status, gn_item = get_next(0, 1)

if not gn_status:
    sys.exit("Error while searching for next request in extract queue")
elif gn_item == None:
    sys.exit("Extract queue is empty")


rid = gn_item[0]
request = gn_item[1]


print request

us_proc = update_status(rid, 2)


# --------------------------------------------------
# boundary

# lookup boundary path in asdf
boundary_data = c_asdf.find({'name': request['boundary']})[0]


# build boundary file path
boundary_path = boundary_data['base'] +'/'+ boundary_data['resources'][0]['path']


# check boundary exists
# 

# initialize boundary using rpy2
try:
    boundary_dirname = os.path.dirname(boundary_path)
    boundary_filename, boundary_extension = os.path.splitext(os.path.basename(boundary_path))

    # break boundary down into path and layer
    # different for shapefiles and geojsons
    if boundary_extension == ".geojson":
        boundary_info = (boundary_path, "OGRGeoJSON")

    elif boundary_extension == ".shp":
        boundary_info = (boundary_dirname, boundary_filename)

    r_boundary = rlib_rgdal.readOGR(boundary_info[0], boundary_info[1])
    
except:
    us_error = update_status(rid, -1)



# --------------------------------------------------
# raster

extract_type = request['extract_type']

# lookup raster path

if not 'classification' in request.keys() or request['classification'] == 'external':

    print request['raster'].split('_')
    raster_data = c_asdf.find({'options.mini_name': request['raster'].split('_')[0]})[0]
    print raster_data


    raster_path = ''
    for i in raster_data['resources']:
        if request['raster'] == i['name']:
            raster_path = raster_data['base'] +'/'+ i['path']

    # build output file path
    extract_output = '/sciclone/aiddata10/REU/extracts/'+ request['boundary'] +'/cache/'+ raster_data['name'] +'/'+ extract_type +'/'+ request['raster'] 

    if request['raster'] == raster_data['options']['mini_name']:
        extract_output += '_'

    extract_output += extract_options[extract_type] + '.csv'


elif request['classification'] == 'msr':

    split_index = request['raster'].rindex('_')
    raster_data = (request['raster'][:split_index], request['raster'][split_index+1:])
    raster_path = '/sciclone/aiddata10/REU/data/rasters/internal/msr/' + raster_data[0] +'/'+ raster_data[1] + '/raster.asc'

    # build output file path
    extract_output = '/sciclone/aiddata10/REU/extracts/'+ request['boundary'] +'/cache/'+ raster_data[0] +'/'+ extract_type +'/'+ raster_data[1] + '_s.csv'


else:
    us_error = update_status(rid, -1)



# check raster exists
# 




# --------------------------------------------------
# run

print raster_path
print boundary_path
print extract_output
# sys.exit("!")


# run extract
# re_status = script_extract(request["boundary"]["path"], raster_path, extract_output, extract_type)
re_status = rpy2_extract(raster_path, extract_output, extract_type)

# return False if extract fails
if not re_status:
    us_error = update_status(rid, -1)

# run reliability calcs if needed
elif request['reliability'] ==  True:
    raster_parent = os.path.dirname(raster_path)
    rr_status = run_reliability(boundary_path, raster_parent+"/unique.geojson", extract_output)

    # return False if reliability calc fails
    if not rr_status:
        us_error = update_status(rid, -1)



us_done = update_status(rid, 1)

print us_done
print "extract done"



