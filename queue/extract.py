

# accepts request object and checks if all extracts have been processed (return boolean)

import os
import errno

import pymongo
import pandas as pd 
import geopandas as gpd

# import subprocess as sp

from rpy2.robjects.packages import importr
from rpy2 import robjects





# connect to mongodb
client = pymongo.MongoClient()
db = client.det

c_extracts = db.extracts
c_msr = db.msr          


rlib_rgdal = importr("rgdal")
rlib_raster = importr("raster")

# list of valid extract types with r functions
extract_funcs = {
    "mean": robjects.r.mean,
    "max": robjects.r.max,
    "sum": robjects.r.sum
}


boundary = request["boundary"]["path"])

# initialize boundary using rpy2

try:
    boundary_dirname = os.path.dirname(boundary)
    boundary_filename, boundary_extension = os.path.splitext(os.path.basename(boundary))

    # break boundary down into path and layer
    # different for shapefiles and geojsons
    if boundary_extension == ".geojson":
        boundary_info = (boundary, "OGRGeoJSON")

    elif boundary_extension == ".shp":
        boundary_info = (boundary_dirname, boundary_filename)

    r_boundary = rlib_rgdal.readOGR(boundary_info[0], boundary_info[1])
    
    return True

except:
    return False



print "running extracts"
# run extract

# re_status = self.script_extract(request["boundary"]["path"], raster_path, extract_output, extract_type)
re_status = self.rpy2_extract(raster_path, extract_output, extract_type)

# return False if extract fails
if not re_status:
    return False, 0

# run reliability calcs if needed
elif is_reliability_raster:
    raster_parent = os.path.dirname(raster_path)
    rr_status = self.run_reliability(request["boundary"]["path"], raster_parent+"/unique.geojson", extract_output)

    # return False if reliability calc fails
    if not rr_status:
        return False, 0

# update cache db
cache_data = {
    "boundary": request["boundary"]["name"], 
    "raster": df_name, 
    "extract_type": extract_type, 
    "reliability": is_reliability_raster
}

self.c_extracts.replace_one(cache_data, cache_data, upsert=True)



# creates directories
def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

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
    make_dir(os.path.dirname(base_output))

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

