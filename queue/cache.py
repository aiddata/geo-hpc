# accepts request object and checks if all extracts have been processed (return boolean)

import sys
import os

import pymongo
import pandas as pd 
import geopandas as gpd

import subprocess as sp

from rpy2.robjects.packages import importr
from rpy2 import robjects


class cache():
    
    def __init__(self):
        # connect to mongodb
        self.client = pymongo.MongoClient()
        self.db = self.client.det
        self.cache = self.db.cache
        
        self.rlib_rgdal = importr("rgdal")
        self.rlib_raster = importr("raster")

        # list of valid extract types with r functions
        self.extract_funcs = {
            "mean":robjects.r.mean
        }


    # check for cache given boundary, dataset and extract type
    def check_individual(self, boundary, dataset, extract_type, reliability):

        print "check_individual"

      # # check db
      # db_exists = 

      # if db_exists:
      #     # check file
      #     file_exists = 
            
      #     if file_exists:
      #         return True
      #     else:
      #         # remove from db
      #         # 
      #         return False

      #   else:
      #     return False
    

    # initialize boundary vector using rpy2
    def init_boundary(self, vector):

        try:
            vector_dirname = os.path.dirname(vector)
            vector_filename, vector_extension = os.path.splitext(os.path.basename(vector))

            # break vector down into path and layer
            # different for shapefiles and geojsons
            if vector_extension == ".geojson":
                vector_info = (vector, "OGRGeoJSON")

            elif vector_extension == ".shp":
                vector_info = (vector_dirname, vector_filename)

            self.r_vector = self.rlib_rgdal.readOGR(vector_info[0], vector_info[1])
            
            return True

        except:
            return False


    # run extract using rpy2
    def rpy2_extract(self, raster, output, extract_type):

        try:
            r_raster = self.rlib_raster.raster(raster)

            # *** need to implement different kwargs based on extract type ***
            kwargs = {"fun":self.extract_funcs[extract_type], "sp":True, "weights":True, "small":True, "na.rm":True}

            robjects.r.assign('r_extract', self.rlib_raster.extract(r_raster, self.r_vector, **kwargs))

            robjects.r.assign('r_output', output)

            robjects.r('colnames(r_extract@data)[length(colnames(r_extract@data))] <- "ad_extract"')
            robjects.r('write.table(r_extract@data, r_output, quote=T, row.names=F, sep=",")')
            
            return True, None

        except:
            return False, "R extract failed"


    # use subprocess to run Rscript
    # def script_extract(self, vector, raster, output, extract_type):

    #     try:
    #         cmd = "Rscript " + os.path.dirname(__file__) + "/extract.R " + vector +" "+ raster +" "+ output +" "+ extract_type
    #         print cmd

    #         sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
    #         print sts

    #         return True

    #     except:
    #         return False



    # reliability calcs for extract
    # intersect for every single boundary that intersects with unique ones from geojson
    # sum for each intersect
    def run_reliability(self, boundary, reliability_geojson):
        
        extract_bound = sys.argv[1] # .../NPL_adm2.shp
        mean_surf_geom = sys.argv[2] # .../string.geojson
        extract_result = sys.argv[3] # .../extract_ADM2.csv
        output = sys.argv[4] # .../reliability_output_ADM2.csv

        # extract boundary geo dataframe
        gdf_a = gpd.GeoDataFrame.from_file(extract_bound)
        gdf_a['idx'] = range(len(gdf_a)) #creates unique index to merge on

        # mean surface unique polygon dataframe
        mean_surf_df = gpd.GeoDataFrame.from_file(mean_surf_geom)

        # result of mean surface extracted to boundary
        df = pd.DataFrame.from_csv(extract_result)
        df['idx'] = range(len(df)) #index to merge with gdf_a
        df['max'] = 0 *len(df)


        # iterate over shapes
        for row_raw in gdf_a.iterrows():

            row = row_raw[1]
            geom = row['geometry']
            unique_id = row['idx'] # ID field common to both dfs
            mean_surf_df['intersect'] = mean_surf_df['geometry'].intersects(geom)
            tmp_series = mean_surf_df.groupby(by = 'intersect')['unique_dollars'].sum()
            
            df.loc[df['idx'] == unique_id, 'max'] = tmp_series[True]
            

        # reliability statistic
        df["reliability"] = df['mean_aid']/df['max']

        # output
        df.to_csv(output)

        return x 


    # check entire request object for cache
    def check_request(self, request, extract=False):

        print "check_request"

        self.init_boundary(request["boundary"]["path"])

        count = 0

        for name, data in request["data"].iteritems():

            for i in data["files"]:
                df_name = data["files"][i]["name"]
                raster_path = data["base"] +"/"+ data["files"][i]["path"]

                for extract_type in request["data"]["options"]["extract_types"]:

                    # check if cache exists
                    exists = self.check_individual(request["boundary"]["name"], df_name, extract_type, data["files"][i]["reliability"])

                    if not exists:
                        if extract:
                            # run extract
                            output = "/sciclone/aiddata10/REU/extracts/" + request["boundary"]["name"] +"/cache/"+ data["name"] +"/"+ extract_type +"/extract_"+df_name[df_name.rindex("_")+1:df_name.rindex(".")] +".csv"
                            
                            # re_status = self.script_extract(request["boundary"]["path"], raster_path, output, extract_type)
                            re_status = self.rpy2_extract(request["boundary"]["path"], raster_path, output, extract_type)

                            # return False if extract fails
                            if not re_status:
                                return False, 0

                            # run reliability calcs if needed
                            elif data["files"][i]["reliability"]:
                                raster_parent = os.path.dirname(raster_path)
                                rr_status = self.run_reliability(vector, raster_parent+"/string.geojson")

                                # return False if reliability calc fails
                                if not rr_status:
                                    return False, 0

                            # update cache db
                            # 
                            
                        else:
                            count += 1

        return True, count


    # merge extracts when all are completed
    def merge_extracts(self, request):

        print "merge_extracts"

        # something
        # 

        return something

