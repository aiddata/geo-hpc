# accepts request object and checks if all extracts have been processed (return boolean)

import os
import errno

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
            "mean": robjects.r.mean,
            "max": robjects.r.max
        }

        self.merge_list = []


    # creates directories
    def make_dir(self, path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise


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


    # check for cache given boundary, raster and extract type
    def check_individual(self, boundary, raster, extract_type, reliability, output):

        print "check individual"

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
    


    # run extract using rpy2
    def rpy2_extract(self, raster, output, extract_type):
        print "rpy2_extract"
    # try:
        r_raster = self.rlib_raster.raster(raster)

        # *** need to implement different kwargs based on extract type ***
        if extract_type == "mean":
            kwargs = {"fun":self.extract_funcs[extract_type], "sp":True, "weights":True, "small":True, "na.rm":True}
        else:
            kwargs = {"fun":self.extract_funcs[extract_type], "sp":True, "na.rm":True}

        robjects.r.assign('r_extract', self.rlib_raster.extract(r_raster, self.r_boundary, **kwargs))

        robjects.r.assign('r_output', output+"e.csv")

        robjects.r('colnames(r_extract@data)[length(colnames(r_extract@data))] <- "ad_extract"')
        robjects.r('write.table(r_extract@data, r_output, quote=T, row.names=F, sep=",")')
        
        return True, None

    # except:
    #     return False, "R extract failed"


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
        print "run_reliability"
    # try: 

        # extract boundary geo dataframe
        bnd_df = gpd.GeoDataFrame.from_file(boundary)

        # mean surface unique polygon dataframe
        rel_df = gpd.GeoDataFrame.from_file(reliability_geojson)


        # result of mean surface extracted to boundary
        df = pd.DataFrame.from_csv(output+"e.csv")

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
        df["ad_extract"] = df['mean_aid']/df['max']

        # output to reliability csv
        df.to_csv(output+"r.csv")

        return True

    # except:
    #     return False


    # check entire request object for cache
    def check_request(self, request, extract=False):
        print "check_request"

        self.init_boundary(request["boundary"]["path"])

        count = 0

        for name, data in request["data"].iteritems():

            for i in data["files"]:

                df_name = i["name"]
                raster_path = data["base"] +"/"+ i["path"]
                is_reliability_raster = i["reliability"]

                for extract_type in data["options"]["extract_types"]:

                    # core basename for output file 
                    # does not include file type identifier (...e.ext for extracts and ...r.ext for reliability) or file extension
                    if data["temporal_type"] == "None":
                        output_name = df_name + "_"
                    else:
                        output_name = df_name

                    # output file string without file type identifier or file extension
                    output = "/sciclone/aiddata10/REU/extracts/" + request["boundary"]["name"] +"/cache/"+ data["name"] +"/"+ extract_type +"/"+ output_name
                    self.make_dir(os.path.dirname(output))

                    self.merge_list.append(output + "e.csv")

                    if is_reliability_raster:
                        self.merge_list.append(output + "r.csv")


                    # check if cache exists
                    exists = self.check_individual(request["boundary"]["name"], df_name, extract_type, is_reliability_raster, output)

                    if not exists and extract:
                        print "running extracts"
                        # run extract
                        
                        # re_status = self.script_extract(request["boundary"]["path"], raster_path, output, extract_type)
                        re_status = self.rpy2_extract(raster_path, output, extract_type)

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

                    elif not exists:
                        count += 1


        return True, count


    # merge extracts when all are completed
    def merge(self, rid, request):
        print "merge"

        merged_df = 0

        # created merged dataframe from results
    # try:

        # for each result file that should exist for request (extracts and reliability)
        for result_csv in self.merge_list:
            
            # make sure file exists
            if os.path.isfile(result_csv):

                # get field name from file
                result_field =  os.path.splitext(os.path.basename(result_csv))[0]

                # load csv into dataframe
                result_df = pd.read_csv(result_csv, quotechar='\"', na_values='', keep_default_na=False)

                # check if merged df exists
                if not isinstance(merged_df, pd.DataFrame):
                    # if merged df does not exists initialize it 
                    # init merged df using full csv                    
                    merged_df = result_df.copy(deep=True)
                    # change extract column name to file name
                    merged_df.rename(columns={"ad_extract": result_field}, inplace=True)

                else:
                    # if merge df exists add data to it
                    # add only extract column to merged df
                    # with column name = new extract file name
                    merged_df[result_field] = result_df["ad_extract"]

    # except:
        # return False, "error building merged dataframe"


        # output merged dataframe to csv
    # try:
        merged_output = "/sciclone/aiddata10/REU/det/results/"+rid+"/results.csv"

        # generate output folder for merged df using request id
        self.make_dir(os.path.dirname(merged_output))

        # write merged df to csv
        merged_df.to_csv(merged_output, index=False)
        
        return True, None
    
    # except:
    #     return False, "error writing merged dataframe"           

