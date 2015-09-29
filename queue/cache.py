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






    # generate merge list for request
    def generate_merge_list(self, request):
        print "generate_merge_list"

        tmp_merge_list =  []

        boundary_path = request['boundary']['path']


        for name, data in request['d1_data'].iteritems():                   
            print name

            msr_raster_path = ''
            msr_extract_output = ''
            msr_field = ''

            tmp_merge_item = {
                'boundary': boundary_path,
                'raster': msr_raster_path,
                'extract': 'sum',
                'reliability': True,
                'field': msr_field,
                'output': msr_extract_output,
                'type': 'raster',
                'source': 'd1_data'
            }
            
            tmp_merge_list.append(tmp_merge_item)


        for name, data in request["d2_data"].iteritems():
            print name

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
                    

                    tmp_merge_item = {
                        'boundary': boundary_path,
                        'raster': raster_path,
                        'extract': extract_type,
                        'reliability': is_reliability_raster,
                        'field': os.path.basename(extract_output),
                        'output': extract_output,
                        'type': 'raster',
                        'source': 'd2_data'
                    }

                    tmp_merge_list.append(tmp_merge_item)


        self.merge_list = tmp_merge_list

        return len(merge_list)





    # check entire request object for cache
    def check_request(self, request, extract=False):
        print "check_request"

        extract_count = 0
        msr_count = 0


        for name, data in request['d1_data'].iteritems():                   
            print name

            # check if msr exists
            # msr_exists, msr_completed = self.msr_exists()

            # if msr_exists:
                # check if extract for msr exists    
                # extract_exists, extract_completed = self.extract_exists()
                # 

                # if not extract_exists:
                    # add to extract queue
                    # 

                    # extract_count += 1

            # else:
                # add to msr tracker
                # 

                # extract_count += 1
                # msr_count +=1

            # add to merge list
            # 


        for name, data in request["d2_data"].iteritems():
            print name

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
                    

                    # check if extract exists
                    # extract_exists, extract_completed = self.extract_exists(request["boundary"]["name"], df_name, extract_type, is_reliability_raster, extract_output)

                    # if not extract_exists:
                        # add to extract queue
                        # 

                        # extract_count += 1


                    # add to merge list
                    # 

        return extract_count, msr_count



# ---------------------------------------------------------------------------



    # merge extracts when all are completed
    def merge(self, rid, request):
        print "merge"

        # generate list of csv files to merge (including relability calcs)
        csv_merge_list = []
        for item in self.merge_list:
            csv_merge_list.append(item['output'])
            if item['reliability']:
                csv_merge_list.append(item['output'][:-5]+"r.csv")


        merged_df = 0

        # created merged dataframe from results
    # try:

        # for each result file that should exist for request (extracts and reliability)
        for result_csv in csv_merge_list:

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



# ---------------------------------------------------------------------------



    # 1) check if extract exists in extract queue
    #    run redundancy check on actual extract file and delete extract queue entry if file is missing
    #    also check for reliability calc if field is specified
    # 2) check if extract is completed, waiting to be run, or encountered an error
    def extract_exists(self, boundary, raster, extract_type, reliability, csv_path):
        print "exists_in_extract_queue"
        
        check_data = {"boundary": boundary, "raster": raster, "extract_type": extract_type, "reliability": reliability}

        # check db
        search = self.c_extracts.find(check_data)

        db_exists = search.count() > 0

        valid_db_exists = False
        valid_completed_exists = False

        if db_exists:

            if search[0]['status'] == 0:
                valid_db_exists = True

            elif search[0]['status'] == 1:
                # check file
                extract_exists = os.path.isfile(csv_path)

                reliability_path = csv_path[:-5] + "r.csv"

                if extract_exists and (not reliability or (reliability and os.path.isfile(reliability_path))):
                    valid_db_exists = True
                    valid_completed_exists = True

                else:
                    # remove from db
                    self.c_extracts.delete_one(check_data)

            else:
                valid_db_exists = True
                valid_completed_exists = "Error"


        return valid_db_exists, valid_completed_exists


    # 1) check if msr exists in msr tracker
    #    run redundancy check on actual msr raster file and delete msr tracker entry if file is missing
    # 2) check if msr is completed, waiting to be run, or encountered an error
    def msr_exists(self, dataset_name, filter_hash, raster_path):
        print "exists_in_msr_tracker"
        
        check_data = {"dataset": dataset_name, "hash": filter_hash}

        # check db
        search = self.c_msr.find(check_data)

        db_exists = search.count() > 0

        valid_db_exists = False
        valid_completed_exists = False

        if db_exists:

            if search[0]['status'] == 0:
                valid_db_exists = True

            elif search[0]['status'] == 1:
                # check file
                msr_exists = os.path.isfile(raster_path)

                if msr_exists:
                    valid_db_exists = True
                    valid_completed_exists = True

                else:
                    # remove from db
                    self.c_msr.delete_one(check_data)

            else:
                valid_db_exists = True
                valid_completed_exists = "Error"


        return valid_db_exists, valid_completed_exists
