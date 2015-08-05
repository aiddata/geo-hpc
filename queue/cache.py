# accepts request object and checks if all extracts have been processed (return boolean)

import os
import pymongo
import pandas as pd 
import geopandas as gpd


class cache():
    
    def __init__(self):
        # connect to mongodb
        self.client = pymongo.MongoClient()
        self.db = self.client.det
        self.cache = self.db.cache
        

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
    

    # merge extracts when all are completed
    def merge_extracts(self, request):

        print "merge_extracts"

        # something
        # 

        return something


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


    # use subprocess to run Rscript
    # run reliability calcs if needed
    def run_extract(self, vector, raster, output, extract_type, reliability):

        try:

            cmd = "Rscript " + os.path.dirname(__file__) + "/extract.R " + vector +" "+ raster +" "+ output +" "+ extract_type
            print cmd

            sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
            print sts

            if reliability:
                r = self.run_reliability(vector, raster_parent+"/string.geojson")
                return r
            else:
                return True

        except:
            return False


    # check entire request object for cache
    def check_request(self, request, extract=False):

        print "check_request"
        count = 0
        for name, data in request["data"].iteritems():

            for i in data["files"]:

                file_path = data["base"] +"/"+ data["files"][i]["path"]

                for extract_type in request["data"]["options"]["extract_types"]:

                    # check if cache exists
                    exists = self.check_individual(request["boundary"]["name"], data["files"][i]["name"], extract_type, data["files"][i]["reliability"])

                    if not exists:
                        if extract:
                            # run extract
                            output = 
                            re = self.run_extract(request["boundary"]["path"], file_path, output, extract_type, data["files"][i]["reliability"])
                            
                            if not re:
                                return False, 0

                        else:
                            count += 1

        return True, count

