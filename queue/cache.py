# accepts request object and checks if all extracts have been processed (return boolean)

import os
import errno

import pymongo
import pandas as pd 
import geopandas as gpd



class cache():
    
    def __init__(self):
        # connect to mongodb
        self.client = pymongo.MongoClient()
        self.db = self.client.det

        self.c_extracts = self.db.extracts
        self.c_msr = self.db.msr        

        self.extract_options = {
            "mean": "e",
            "max": "x",
            "sum": "s"

        }

        self.merge_list = []


    # creates directories
    def make_dir(self, path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise


    # check if cache for extract exists
    def check_cache(self, boundary, raster, extract_type, reliability, output):
        print "check_cache"

        check_data = {"boundary": boundary, "raster": raster, "extract_type": extract_type, "reliability": reliability, "status": 1}

        # check db
        db_exists = self.c_extracts.find(check_data).count() > 0

        if db_exists:

            # check file
            extract_path = output
            extract_exists = os.path.isfile(extract_path)

            if reliability:
                reliability_path = output[:-5] + "r.csv"
                reliability_exists = os.path.isfile(reliability_path)

            if (reliability and extract_exists and reliability_exists) or (not reliability and extract_exists):
                return True

            else:
                # remove from db
                self.c_extracts.delete_one(check_data)
                return False

        else:
            return False
    



    # check entire request object for cache
    def check_request(self, request, extract=False):
        print "check_request"


        count = 0


        for name, data in request['d1_data'].iteritems():
                   
            print name

            # check if msr for request exists
            # 

            # if it does not exists
                # msr_request = True
                # generate msr job file
                # add msr job to msr queue folder on sciclone

            # if it does exist
                # generate new item for d2 data to treat it like normal extract using sum with reliability calcs



        for name, data in request["d2_data"].iteritems():

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
                    base_output = "/sciclone/aiddata10/REU/extracts/" + request["boundary"]["name"] +"/cache/"+ data["name"] +"/"+ extract_type +"/"+ output_name
                    extract_output = base_output + self.extract_options[extract_type] + ".csv"
                    self.make_dir(os.path.dirname(base_output))

                    self.merge_list.append(extract_output)

                    if is_reliability_raster:
                        self.merge_list.append(base_output + "r.csv")


                    # check if cache exists
                    exists = self.check_cache(request["boundary"]["name"], df_name, extract_type, is_reliability_raster, extract_output)

                    if not exists and extract:
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

                    elif not exists:
                        count += 1


                    return True, cou
        nt, msr_request


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

