
import os
import sys
import errno
import time
import json
import warnings
import shutil
import hashlib
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

import pymongo
from bson.objectid import ObjectId

import pandas as pd

# from documentation_tool import DocBuilder


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


class QueueToolBox():
    """utilty functions for processing requests in queue

    old cachetools descrip:
    Accepts request object and checks if all extracts have been processed
    """
    def __init__(self):
        self.client = pymongo.MongoClient()

        self.c_queue = self.client.det.queue
        self.c_email = self.client.det.email
        self.c_extracts = self.client.asdf.extracts
        self.c_msr = self.client.asdf.msr

        # self.doc = DocBuilder()

        self.request_objects = {}

        ###
        # self.extract_options = json.load(open(
        #     os.path.dirname(
        #         os.path.abspath(__file__)) + '/extract_options.json', 'r'))

        self.merge_lists = {}

        self.msr_resolution = 0.05
        self.msr_version = 0.1
        ###


    # exit function used for errors
    def quit(self, rid, status, message):
        self.update_status(rid, int(status))
        sys.exit(">> det processing error ("+str(status)+"): \n\t\t" + 
                  str(message))


    def get_requests(self, status, limit=0):
        """get requests from queue

        Args
            status (int):
            limit (int):

        Returns
            tuple (function status, list of request objects)
        """
        try:
            search = self.c_queue.find({
                "status": status
            }).sort([("priority", -1), ("submit_time", 1)]).limit(limit)

            count = search.count(True)

            if count > 0:

                # for i in range(count):
                #     rid = str(search[i]["_id"])
                #     self.request_objects[rid] = search[i]

                # return 1, self.request_objects

                return 1, list(search)

            else:
                return 1, []

        except:
            return 0, None



    def check_id(self, rid):
        """verify request with given id exists

        Args
            rid (str): request id
        Returns
            tuple: (function status, request object)
        """
        try:
            # check document with request id exists
            search = self.c_queue.find_one({"_id": ObjectId(rid)})

            # self.request_objects[rid] = search

            return 1, search

        except:
            return 0, None



    def get_status(self, rid):
        """get status of request.

        Args
            rid (str): request id
        Returns
            tuple (function status, request status)
        """
        try:
            # check document with request id exists
            search = self.c_queue.find_one({"_id": ObjectId(rid)})
            status = search['status']

            return 1, status

        except:
            return 0, None


    def update_status(self, rid, status):
        """ update status of request
        """
        valid_stages = {
            "-2": None,
            "-1": None,
            "0": "prep_time",
            "1": "complete_time",
            "2": "process_time"
        }

        ctime = int(time.time())

        updates = {
            "status": long(status)
        }

        if not str(status) in valid_stages:
            return 0, None

        stage = valid_stages[str(status)]
        if stage is not None:
            updates[stage] = ctime
            self.request_objects[rid][stage] = ctime


        try:
            # update request document
            self.c_queue.update({"_id": ObjectId(rid)},
                                {"$set": updates})

        except:
            return 0, None

        if str(status) == "0":
            pass

        elif str(status) == "1":
            pass

        return 1, ctime


    # sends an email
    def send_email(self, sender, receiver, subject, message):

        try:
            pw_search = self.c_email.find({"address": sender},
                                                   {"password":1})

            if pw_search.count() > 0:
                passwd = str(pw_search[0]["password"])
            else:
                return 0, "Specified email does not exist"

        except:
            return 0, "Error looking up email"


        try:
            # source:
            # http://stackoverflow.com/questions/64505/
            #   sending-mail-from-python-using-smtp

            msg = MIMEMultipart()

            msg.add_header('reply-to', sender)
            msg['From'] = sender
            msg['To'] = receiver
            msg['Subject'] = subject
            msg.attach(MIMEText(message))

            mailserver = smtplib.SMTP('smtp.gmail.com', 587)
            # identify ourselves to smtp gmail client
            mailserver.ehlo()
            # secure our email with tls encryption
            mailserver.starttls()
            # re-identify ourselves as an encrypted connection
            mailserver.ehlo()

            mailserver.login(sender, passwd)
            mailserver.sendmail(sender, receiver, msg.as_string())
            mailserver.quit()

            return 1, None

        except:
            return 0, "Error generating or sending email"

    # $mail_headers = "";
    # $mail_headers .= 'Reply-To: AidData <data@aiddata.org>' . "\r\n";
    # $mail_headers .= 'From: AidData <data@aiddata.org>' . "\r\n";
    # $mail_headers .= 'MIME-Version: 1.0' . "\r\n";
    # $mail_headers .= 'Content-type: text/html; charset=utf-8' . "\r\n";

    # // send email based on status
    # if ($status == "0") {

    #     $mail_subject = "AidData Data Extract Tool - Request 123456.. Received";

    #     $mail_message = "Your request has been received. ";
    #     $mail_message .= "You will receive an additional email when the request has been completed. ";
    #     $mail_message .= "The status of your request can be viewed using the following link: ";
    #     // $mail_message .= "http://not_a_real_link.org/DET/results/" . $rid;
    #     $mail_message .= "http://google.com";

    #     $mail = mail($mail_to, $mail_subject, $mail_message, $mail_headers);


    # } else if ($status == "1") {

    #     $mail_subject = "AidData Data Extract Tool - Request 123456.. Completed";

    #     $mail_message = "Your request has been completed. ";
    #     $mail_message .= "The results can be accessed using the following link: ";
    #     // $mail_message .= "http://not_a_real_link.org/DET/results/" . $rid;
    #     $mail_message .= "http://google.com";

    #     $mail = mail($mail_to, $mail_subject, $mail_message, $mail_headers);

    }


# =============================================================================
# =============================================================================
# =============================================================================


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
                        self.update_extract(
                            request["boundary"]["name"],
                            data['dataset']+"_"+data_hash,
                            msr_extract_type, True, "msr")

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


                    exi = ExtractItem(
                        boundary=request["boundary"]["name"],
                        raster=df_name,
                        extract_type=extract_type,
                        reliability=is_reliability_raster
                    )



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
                            self.update_extract(
                                request['boundary']['name'], i['name'],
                                extract_type, is_reliability_raster,
                                "external")


                    # add to merge list
                    self.merge_lists[rid].append(('d2_data', extract_output, 
                                                  None))
                    if is_reliability_raster:
                        self.merge_lists[rid].append(
                            ('d2_data', extract_output[:-5]+"r.csv", None))


        return 1, extract_count, msr_count



    def build_output(self, request_id, run_extract):
        """build output

        merge extracts, generate documentation, update status,
            cleanup working directory, send final email
        """
        # merge cached results if all are available
        merge_status = self.merge(request_id,
                                        self.request_objects[request_id])

        # handle merge error
        if not merge_status[0]:
            self.quit(request_id, -2, merge_status[1])


        # add processed time
        if not run_extract:
            us = self.update_status(request_id, 3)

        # update status 1 (done)
        us = self.update_status(request_id, 1)


        # # generate documentation
        # self.doc.request = self.request_objects[request_id]
        # print self.doc.request

        # bd_status = self.doc.build_doc(request_id)
        # print bd_status


        # zip files and delete originals
        request_id_dir = "/sciclone/aiddata10/REU/det/results/" + request_id
        shutil.make_archive(request_id_dir, "zip",
                            "/sciclone/aiddata10/REU/det/results/", request_id)
        shutil.rmtree(request_id_dir)



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


