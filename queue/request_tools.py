
import os
import sys

import errno
import time
import json
import shutil
from requests import post

import hashlib

import pandas as pd
import geopandas as gpd


from documentation_tool import DocBuilder


def make_dir(path):
    """creates directories

    does not raise error if directory exists
    """
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def json_sha1_hash(hash_obj):
    hash_json = json.dumps(hash_obj, sort_keys = True, ensure_ascii=False,
                           separators=(',', ':'))
    hash_builder = hashlib.sha1()
    hash_builder.update(hash_json)
    hash_sha1 = hash_builder.hexdigest()
    return hash_sha1



class QueueCheck():
    """utilty functions for processing requests in queue
    """
    def __init__(self):
        self.cache = CacheTools()
        self.doc = DocBuilder()

        self.request_objects = {}


    # exit function used for errors
    def quit(self, rid, status, message):
        self.update_status(rid, int(status))
        sys.exit(">> det processing error ("+str(status)+"): \n\t\t" + str(message))



    def get_requests(self, search_type, search_val, limit=0):
        """get requests from queue

        Args
            search_type (str):
            search_val (str, int):
            limit (int):

        Returns
            tuple (function status, list of request objects)
        """
        post_data = {
            'call': 'get_requests',
            'search_type': search_type,
            'search_val': search_val,
            'limit': limit
        }
        try:
            r = post("http://devlabs.aiddata.org/DET/search.php", data=post_data).json()
            return 1, r
        except Exception as e:
            print e
            return 0, None

        # try:
        #     # find all status 1 jobs and sort by priority then submit_time
        #     sort = self.c_queue.find({
        #         "status":status
        #     }).sort([("priority", -1), ("submit_time", 1)])

        #     if sort.count() > 0:
        #         if limit == 0 or limit > sort.count():
        #             limit = sort.count()

        #         for i in range(limit):
        #             rid = str(sort[i]["_id"])
        #             self.request_objects[rid] = sort[i]

        #         return 1, self.request_objects
        #     else:
        #         return 1, None
        # except:
        #     return 0, None


    def check_id(self, rid):
        """verify request with given id exists

        Args
            rid (str): request id
        Returns
            tuple: (function status, request exists, request object)
        """
        request = self.get_requests('id', rid, 1)

        if request[0]:
            exists = len(request[1])
            return 1, exists, request[1][0]
        else:
            return 0, None, None


    def get_status(self, rid):
        """get status of request.

        Args
            rid (str): request id
        Returns
            tuple (function status, request status)
        """
        request = self.get_requests('id', rid, 1)

        if request[0]:
            return 1, = r[0]['status']
        else:
            return 0, None


    def update_status(self, rid, status, send_email=False):
        """update status of request
        """
        ctime = int(time.time())

        stage_options = {
            1: "complete_time",
            2: "prep_time",
            3: "process_time"
        }

        try:
            stage = stage_options[stage]

            post_data = {
                'call': 'update_request_status',
                'rid': rid
                'status': status,
                'stage': stage,
                'timestamp': ctime,
                'send_email': send_email
            }

            r = post("http://devlabs.aiddata.org/DET/search.php", data=post_data).json()

            if r['status'] == 'success':
                return True, ctime
            else:
                return False, ctime

        except:
            return False, None


###

    def build_output(self, request_id, run_extract):
        """build output

        merge extracts, generate documentation, update status,
            cleanup working directory, send final email
        """
        # merge cached results if all are available
        merge_status = self.cache.merge(request_id,
                                        self.request_objects[request_id])

        # handle merge error
        if not merge_status[0]:
            self.quit(request_id, -2, merge_status[1])


        # add processed time
        if not run_extract:
            us = self.update_status(request_id, 3)

        # update status 1 (done)
        us = self.update_status(request_id, 1)


        # generate documentation
        self.doc.request = self.request_objects[request_id]
        print self.doc.request

        bd_status = self.doc.build_doc(request_id)
        print bd_status


        # zip files and delete originals
        request_id_dir = "/sciclone/aiddata10/REU/det/results/" + request_id
        shutil.make_archive(request_id_dir, "zip",
                            "/sciclone/aiddata10/REU/det/results/", request_id)
        shutil.rmtree(request_id_dir)

        # send final email
        c_message = ("Your data extraction request (" + request_id +
                     ") has completed. The results are available via " +
                     "devlabs.aiddata.wm.edu/DET/status/#" + request_id)
        self.send_email(
            "aiddatatest2@gmail.com",
            self.request_objects[request_id]["email"],
            "AidData Data Extraction Tool Request Completed ("+request_id+")",
            c_message)

###


class CacheTools():
    """Accepts request object and checks if all extracts have been processed

    Returns:
        boolean
    """
    def __init__(self):
        self.extract_options = json.load(open(
            os.path.dirname(
                os.path.abspath(__file__)) + '/extract_options.json', 'r'))

        self.merge_lists = {}

        self.msr_resolution = 0.05
        self.msr_version = 0.1


    def check_request(self, rid, request, extract=False):
        """check entire request object for cache
        """
        print "check_request"

        self.merge_lists[rid] = []
        extract_count = 0
        msr_count = 0

        msr_field_id = 1

        for name in sorted(request['d1_data'].keys()):
            data = request['d1_data'][name]

        # for name, data in request['d1_data'].iteritems():

            print name

            data['resolution'] = self.msr_resolution
            data['version'] = self.msr_version

            # get hash
            data_hash = json_sha1_hash(data)

            msr_extract_type = "sum"
            msr_extract_output = ("/sciclone/aiddata10/REU/extracts/" +
                                  request["boundary"]["name"] + "/cache/" +
                                  data['dataset'] +"/" + msr_extract_type +
                                  "/" + data_hash + "_" +
                                  self.extract_options[msr_extract_type] +
                                  ".csv")

            # check if msr exists in tracker and is completed
            msr_exists, msr_completed = self.msr_exists(data['dataset'],
                                                        data_hash)

            print "MSR STATE:" + str(msr_completed)

            if msr_completed == True:

                # check if extract for msr exists in queue and is completed
                extract_exists, extract_completed = self.extract_exists(
                    request["boundary"]["name"], data['dataset']+"_"+data_hash,
                    msr_extract_type, True, msr_extract_output)

                if not extract_completed:
                    extract_count += 1

                    if not extract_exists:
                        # add to extract queue
                        self.add_to_extract_queue(
                            request["boundary"]["name"],
                            data['dataset']+"_"+data_hash,
                            True, msr_extract_type, "msr")

            else:

                msr_count += 1
                extract_count += 1

                if not msr_exists:
                    # add to msr tracker
                    self.add_to_msr_tracker(data, data_hash)


            # add to merge list
            self.merge_lists[rid].append(
                ('d1_data', msr_extract_output, msr_field_id))
            self.merge_lists[rid].append(
                ('d1_data', msr_extract_output[:-5]+"r.csv", msr_field_id))

            msr_field_id += 1


        for name, data in request["d2_data"].iteritems():
            print name

            for i in data["files"]:

                df_name = i["name"]
                raster_path = data["base"] +"/"+ i["path"]
                is_reliability_raster = i["reliability"]

                for extract_type in data["options"]["extract_types"]:

                    # core basename for output file
                    # does not include file type identifier
                    #   (...e.ext for extracts and ...r.ext for reliability)
                    #   or file extension
                    if data["temporal_type"] == "None":
                        output_name = df_name + "_"
                    else:
                        output_name = df_name

                    # output file string without file type identifier
                    # or file extension
                    base_output = ("/sciclone/aiddata10/REU/extracts/" +
                                   request["boundary"]["name"] + "/cache/" +
                                   data["name"] + "/" + extract_type + "/" +
                                   output_name)

                    extract_output = (base_output +
                                      self.extract_options[extract_type] +
                                      ".csv")

                    # check if extract exists in queue and is completed
                    extract_exists, extract_completed = self.extract_exists(
                        request["boundary"]["name"], df_name, extract_type,
                        is_reliability_raster, extract_output)

                    # incremenet count if extract is not completed
                    # (whether it exists in queue or not)
                    if extract_completed != True:
                        extract_count += 1

                        # add to extract queue if it does not already
                        # exist in queue
                        if not extract_exists:
                            self.add_to_extract_queue(
                                request['boundary']['name'], i['name'],
                                is_reliability_raster, extract_type,
                                "external")


                    # add to merge list
                    self.merge_lists[rid].append(('d2_data', extract_output, None))
                    if is_reliability_raster:
                        self.merge_lists[rid].append(
                            ('d2_data', extract_output[:-5]+"r.csv", None))


        return 1, extract_count, msr_count


    def add_to_extract_queue(self, boundary, raster, reliability,
                             extract_type, classification):
        """
        add extract item to det->extracts mongodb collection
        """
        print "add_to_extract_queue"

        ctime = int(time.time())

        insert = {
            'raster': raster,
            'boundary': boundary,
            'reliability': reliability,
            'extract_type': extract_type,
            'classification': classification,

            'status': 0,
            'priority': 0,
            'submit_time': ctime,
            'update_time': ctime
        }

        self.c_extracts.insert(insert)


    def add_to_msr_tracker(self, selection, msr_hash):
        """add msr item to det->msr mongodb collection
        """
        print "add_to_msr_tracker"

        ctime = int(time.time())

        insert = {
            'hash': msr_hash,
            'dataset': selection['dataset'],
            'options': selection,

            'classification': 'det-release',
            'status': 0,
            'priority': 0,
            'submit_time': ctime,
            'update_time': ctime
        }

        self.c_msr.insert(insert)



    def extract_exists(self, boundary, raster, extract_type, reliability,
                       csv_path):
        """
        1) check if extract exists in extract queue
           run redundancy check on actual extract file and delete extract
           queue entry if file is missing
           also check for reliability calc if field is specified
        2) check if extract is completed, waiting to be run, or
           encountered an error
        """
        print "exists_in_extract_queue"

        check_data = {
            "boundary": boundary,
            "raster": raster,
            "extract_type": extract_type,
            "reliability": reliability
        }

        # check db
        search = self.c_extracts.find(check_data)

        db_exists = search.count() > 0

        valid_exists = False
        valid_completed = False

        if db_exists:
            print search[0]

            if search[0]['status'] in [0,2,3]:
                valid_exists = True

            elif search[0]['status'] == 1:
                # check file
                extract_exists = os.path.isfile(csv_path)

                reliability_path = csv_path[:-5] + "r.csv"

                if (extract_exists and (not reliability or
                        (reliability and os.path.isfile(reliability_path)))):
                    valid_exists = True
                    valid_completed = True

                else:
                    # remove from db
                    self.c_extracts.delete_one(check_data)

            else:
                valid_exists = True
                valid_completed = "Error"


        return valid_exists, valid_completed



    def msr_exists(self, dataset_name, msr_hash):
        """
        1) check if msr exists in msr tracker
           run redundancy check on actual msr raster file and delete msr
           tracker entry if file is missing
        2) check if msr is completed, waiting to be run, or encountered an error
        """
        print "exists_in_msr_tracker"

        check_data = {"dataset": dataset_name, "hash": msr_hash}

        # check db
        search = self.c_msr.find(check_data)

        db_exists = search.count() > 0

        valid_exists = False
        valid_completed = False

        if db_exists:

            if search[0]['status'] in [0,2]:
                valid_exists = True

            elif search[0]['status'] == 1:
                # check file
                raster_path = ('/sciclone/aiddata10/REU/data/rasters/' +
                               'internal/msr/' + dataset_name +'/'+ msr_hash +
                               '/raster.asc')

                msr_exists = os.path.isfile(raster_path)

                if msr_exists:
                    valid_exists = True
                    valid_completed = True

                else:
                    # remove from db
                    self.c_msr.delete_one(check_data)

            else:
                valid_exists = True
                valid_completed = "Error"


        return valid_exists, valid_completed


    def merge(self, rid, request):
        """
        merge extracts when all are completed
        """
        print "merge"

        # # generate list of csv files to merge (including relability calcs)
        # csv_merge_list = []
        # for item in self.merge_lists[rid]:
        #     csv_merge_list.append(item['output'])
        #     if item['reliability']:
        #         csv_merge_list.append(item['output'][:-5]+"r.csv")


        merged_df = 0

        # used to track dynamically generated field names
        # so corresponding extract and reliability have consistent names
        merge_log = {}

        # created merged dataframe from results
    # try:

        # for each result file that should exist for request
        # (extracts and reliability)
        for merge_item in self.merge_lists[rid]:
            merge_class, result_csv, dynamic_merge_count = merge_item

            # make sure file exists
            if os.path.isfile(result_csv):

                if merge_class == 'd2_data':
                    # get field name from file
                    result_field =  os.path.splitext(os.path.basename(result_csv))[0]

                elif merge_class == 'd1_data':

                    csv_basename = os.path.splitext(os.path.basename(result_csv))[0]

                    merge_log_name = csv_basename[:-2]

                    if not merge_log_name in merge_log.keys():

                        dynamic_merge_string = '{0:03d}'.format(dynamic_merge_count)

                        merge_log[merge_log_name] = 'ad_msr' + dynamic_merge_string


                    result_field = merge_log[merge_log_name] + csv_basename[-1:]


                # load csv into dataframe
                result_df = pd.read_csv(result_csv, quotechar='\"',
                                        na_values='', keep_default_na=False)

                # check if merged df exists
                if not isinstance(merged_df, pd.DataFrame):
                    # if merged df does not exists initialize it
                    # init merged df using full csv
                    merged_df = result_df.copy(deep=True)
                    # change extract column name to file name
                    merged_df.rename(columns={"ad_extract": result_field},
                                     inplace=True)

                else:
                    # if merge df exists add data to it
                    # add only extract column to merged df
                    # with column name = new extract file name
                    merged_df[result_field] = result_df["ad_extract"]

    # except:
        # return False, "error building merged dataframe"


        # output merged dataframe to csv
    # try:
        merged_output = ("/sciclone/aiddata10/REU/det/results/" + rid +
                         "/results.csv")

        # generate output folder for merged df using request id
        make_dir(os.path.dirname(merged_output))

        # write merged df to csv
        merged_df.to_csv(merged_output, index=False)

        return True, None

    # except:
    #     return False, "error writing merged dataframe"


