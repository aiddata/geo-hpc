# accepts request object and checks if all extracts have been processed (return boolean)

import sys
import os

import pymongo
import pandas as pd 
import geopandas as gpd

# import subprocess as sp

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

        self.merge_list = []


    # check for cache given boundary, raster and extract type
    def check_individual(self, boundary, raster, extract_type, reliability, output):

        print "check_individual"

        check_data = {"boundary": boundary, "raster": raster, "extract_type": extract_type, "reliability": reliability}

        # check db
        db_exists = self.cache.find(check_data).count() > 0

        if db_exists:

            # check file
            extract_path = output + "e.csv"
            extract_exists = os.path.isfile(extract_path)

            if reliability:
                reliability_path = output + "r.csv"
                reliability_exists = os.path.isfile(reliability_path)

            if (reliability and extract_exists and reliability_exists) or (not reliability and extract_exists):
                return True

            else:
                # remove from db
                self.cache.delete_one(check_data)
                return False

        else:
            return False
    

    # initialize boundary using rpy2
    def init_boundary(self, boundary):

        try:
            boundary_dirname = os.path.dirname(boundary)
            boundary_filename, boundary_extension = os.path.splitext(os.path.basename(boundary))

            # break boundary down into path and layer
            # different for shapefiles and geojsons
            if boundary_extension == ".geojson":
                boundary_info = (boundary, "OGRGeoJSON")

            elif boundary_extension == ".shp":
                boundary_info = (boundary_dirname, boundary_filename)

            self.r_boundary = self.rlib_rgdal.readOGR(boundary_info[0], boundary_info[1])
            
            return True

        except:
            return False


    # run extract using rpy2
    def rpy2_extract(self, raster, output, extract_type):

        try:
            r_raster = self.rlib_raster.raster(raster)

            # *** need to implement different kwargs based on extract type ***
            kwargs = {"fun":self.extract_funcs[extract_type], "sp":True, "weights":True, "small":True, "na.rm":True}

            robjects.r.assign('r_extract', self.rlib_raster.extract(r_raster, self.r_boundary, **kwargs))

            robjects.r.assign('r_output', output+"e.csv")

            robjects.r('colnames(r_extract@data)[length(colnames(r_extract@data))] <- "ad_extract"')
            robjects.r('write.table(r_extract@data, r_output, quote=T, row.names=F, sep=",")')
            
            return True, None

        except:
            return False, "R extract failed"


    # use subprocess to run Rscript
    # def script_extract(self, boundary, raster, output, extract_type):

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
    def run_reliability(self, boundary, reliability_geojson, output):
       
       try: 

            # extract boundary geo dataframe
            bnd_df = gpd.GeoDataFrame.from_file(boundary)

            # mean surface unique polygon dataframe
            rel_df = gpd.GeoDataFrame.from_file(reliability_geojson)


            # result of mean surface extracted to boundary
            df = pd.DataFrame.from_csv(output+"e.csv")

            # index to merge with bnd_df
            df['ad_id'] = bnd_df['ad_id']

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
            df["reliability"] = df['mean_aid']/df['max']

            # output to reliability csv
            df.to_csv(output+"r.csv")

            return True

        except:
            return False


    # check entire request object for cache
    def check_request(self, request, extract=False):

        print "check_request"

        self.init_boundary(request["boundary"]["path"])

        count = 0

        for name, data in request["data"].iteritems():

            for i in data["files"]:
                df_name = data["files"][i]["name"]
                raster_path = data["base"] +"/"+ data["files"][i]["path"]
                is_reliability_raster = data["files"][i]["reliability"]

                for extract_type in request["data"]["options"]["extract_types"]:

                    # core basename for output file 
                    # does not include file type identifier (...e.ext for extracts and ...r.ext for reliability) or file extension
                    if data["temporal_type"] == "None":
                        output_name = df_name + "_"
                    else:
                        output_name = df_name

                    # output file string without file type identifier or file extension
                    output = "/sciclone/aiddata10/REU/extracts/" + request["boundary"]["name"] +"/cache/"+ data["name"] +"/"+ extract_type +"/"+output_name


                    self.merge_list.append(output +"e.csv")
                    if is_reliability_raster:
                        self.merge_list.append(output +"r.csv")


                    # check if cache exists
                    exists = self.check_individual(request["boundary"]["name"], df_name, extract_type, is_reliability_raster, output)

                    if not exists:
                        if extract:
                            # run extract
                            
                            # re_status = self.script_extract(request["boundary"]["path"], raster_path, output, extract_type)
                            re_status = self.rpy2_extract(request["boundary"]["path"], raster_path, output, extract_type)

                            # return False if extract fails
                            if not re_status:
                                return False, 0

                            # run reliability calcs if needed
                            elif is_reliability_raster:
                                raster_parent = os.path.dirname(raster_path)
                                rr_status = self.run_reliability(request["boundary"]["path"], raster_parent+"/unique.geojson", output)

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

                            self.cache.replace_one(cache_data, cache_data, upsert=True)

                        else:
                            count += 1

        return True, count


    # merge extracts when all are completed
    def merge_extracts(self, request):

        print "merge_extracts"

        merged = 0

        try:
            # for each extract there should be from request (dataset, extract_types)
                # get extract
                # 

                # check if merged df exists
                # if merged.istype(pd.DataFrame):
                    # if merge df exists add data to it
                    # add only extract column to merged df
                    # with column name = new extract file name
                    # 
                # else:
                    # if merged df does not exists initialize it 
                    # init merged df using full csv
                    # 
                    # change extract column name to file name
                    # 

                # check if extract has reliability associated with it
                # if ...["reliability"]:
                    # get reliability csv
                    # 
                    # add reliability csv to merged df
                    # with column name = reliability file name
                    # 

        except:
            return False, "error building merged dataframe"

        try:
            # generate output folder for merged df using request id
            # 

            # write merged df to csv
            # merge_df.to_csv(...)
            
            return True, None
        
        except:
            return False, "error writing merged dataframe"           

