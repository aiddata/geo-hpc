
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

from documentation_tool import DocBuilder

from extract_check import ExtractItem
from msr_check import MSRItem


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
    hash_json = json.dumps(hash_obj,
                           sort_keys = True,
                           ensure_ascii = True,
                           separators=(', ', ': '))
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
        self.c_config = self.client.info.config

        self.branch_info = None
        self.branch = None
        self.msr_version = None
        self.extract_version = None

        self.msr_resolution = 0.05


    # exit function used for errors
    def quit(self, rid, status, message):
        self.update_status(rid, int(status))
        sys.exit(">> det processing error ("+str(status)+"): \n\t\t" +
                  str(message))


    def get_branch_info(self):
        branch_config = self.c_config.find_one()
        if branch_config is None:
            raise Exception('no branch config found')
        self.branch_info = branch_config
        self.branch = branch_config['name']
        self.msr_version = branch_config['versions']['mean-surface-rasters']
        self.extract_version = branch_config['versions']['extract-scripts']
        return branch_config


    def check_id(self, rid):
        """verify request with given id exists

        Args
            rid (str): request id
        Returns
            (dict) request object or None
        """
        # check document with request id exists
        search = self.c_queue.find_one({"_id": ObjectId(rid)})
        return search


    def get_requests(self, status, limit=0):
        """get requests from queue

        Args
            status (int):
            limit (int):

        Returns
            tuple (function status, list of request objects)
        """
        search = self.c_queue.find({
            "status": status
        }).sort([("priority", -1), ("submit_time", 1)]).limit(limit)

        count = search.count(True)

        if count > 0:
            return list(search)
        else:
            return []


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
            return status
        except Exception as e:
            print "error retrieving status of request (id: {0})".format(rid)
            raise e



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

        stage = valid_stages[str(status)]
        if stage is not None:
            updates[stage] = ctime

        try:
            # update request document
            self.c_queue.update({"_id": ObjectId(rid)},
                                {"$set": updates})
            return ctime

        except Exception as e:
            print ('error updating status of request '
                   '(id: {0}, status: {1}').format(rid, status)
            raise e



    # sends an email
    def send_email(self, receiver, subject, message):

        reply_to = 'AidData W&M <data@aiddata.org>'
        sender = 'noreply@aiddata.wm.edu'

        try:
            pw_search = self.c_email.find({"address": sender},
                                          {"password":1})

            if pw_search.count() > 0:
                passwd = str(pw_search[0]["password"])
            else:
                msg = "Error - specified email does not exist"
                return 0, msg, Exception(msg)

        except Exception as e:
            return 0, "Error looking up email", e


        try:
            # source:
            # http://stackoverflow.com/questions/64505/
            #   sending-mail-from-python-using-smtp

            msg = MIMEMultipart()

            msg.add_header('reply-to', reply_to)
            msg['From'] = reply_to
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

            return 1, None, None

        except Exception as e:
            return 0, "Error sending email", e


    def notify_received(self, request_id, email):
        """send email that request was received
        """
        mail_to = email

        mail_subject = ("AidData Data Extract Tool - "
                        "Request {0}.. Received").format(request_id[:7])

        mail_message = ("Your request has been received. \n"
                        "You will receive an additional email when the"
                        " request has been completed. \n\n"
                        "The status of your request can be viewed using"
                        " the following link: \n"
                        "http://{0}/DET/status/#{1}\n\n"
                        "You can also view all your current and previous "
                        "requests using: \n"
                        "http://{0}/DET/status/#{2}\n\n").format(
                            self.branch_info['server'], request_id, mail_to)

        mail_status = self.send_email(mail_to, mail_subject, mail_message)

        if not mail_status[0]:
            print mail_status[1]
            update_status = self.update_status(request_id, -2)
            raise mail_status[2]


    def notify_completed(self, request_id, email):
        """send email that request was completed
        """

        mail_to = email

        mail_subject = ("AidData Data Extract Tool - "
                        "Request {0}.. Completed").format(request_id[:7])

        mail_message = ("Your request has been completed. \n"
                        "The results can be accessed using the following "
                        "link: \n"
                        "http://{0}/DET/status/#{1}\n\n"
                        "You can also view all your current and previous "
                        "requests using: \n"
                        "http://{0}/DET/status/#{2}\n\n").format(
                            self.branch_info['server'], request_id, mail_to)

        mail_status = self.send_email(mail_to, mail_subject, mail_message)

        if not mail_status[0]:
            print mail_status[1]
            update_status = self.update_status(request_id, -2)
            raise mail_status[2]


# =============================================================================
# =============================================================================
# =============================================================================


    def check_request(self, request, dry_run=False):
        """check entire request object for cache
        """
        outputs_base = "/sciclone/aiddata10/REU/outputs"
        extract_base = os.path.join(outputs_base, self.branch, 'extracts',
                                    self.extract_version.replace('.', '_'))
        msr_base = os.path.join(outputs_base, self.branch, 'msr', 'done')

        merge_list = []
        extract_count = 0
        msr_count = 0

        # id used for field names in results
        msr_id = 1

        print "\nchecking aid data..."
        for raw_data in request['release_data']:
            print ''

            tmp_filters = {
                fk: fv
                for fk, fv in raw_data['filters'].iteritems()
                if not any([fvx in ['All', 'None', None] for fvx in fv])
            }


            data = {
                'dataset': raw_data['dataset'],
                'type': 'release',
                'resolution': self.msr_resolution,
                'version': self.msr_version,
                'filters': tmp_filters
            }


            # get hash
            data_hash = json_sha1_hash(data)

            print '\t' + data_hash
            print '\t %s' % data
            print '\t----------'

            msr_item = MSRItem(self.client,
                               data["dataset"],
                               data_hash,
                               data,
                               msr_base)

            # check if extract exists in queue and is completed
            msr_exists, msr_completed = msr_item.exists()

            print '\tmsr exists: %s' % msr_exists
            print '\tmsr completed: %s' % msr_completed

            if msr_completed == True:

                msr_ex_item = ExtractItem(self.client,
                                          request["boundary"]["name"],
                                          data["dataset"],
                                          data["dataset"] + '_' + data_hash,
                                          "sum",
                                          "None",
                                          extract_base,
                                          self.extract_version)

                msr_ex_exists, msr_ex_completed = msr_ex_item.exists()

                print '\tmsr extract exists: %s' % msr_ex_exists
                print '\tmsr extract completed: %s' % msr_ex_completed

                if not msr_ex_completed:
                    extract_count += 1
                    if not dry_run:
                        # add to extract queue
                        msr_ex_item.add_to_queue("msr")

                else:
                    # add to merge list
                    merge_list.append(
                        ('release_data', msr_ex_item.extract_path, msr_id))
                    merge_list.append(
                        ('release_data', msr_ex_item.reliability_path, msr_id))

            else:

                msr_count += 1
                extract_count += 1
                if not dry_run:
                    # add to msr tracker
                    msr_item.add_to_queue()


            msr_id += 1


        print "\nchecking external data..."
        for data in request["raster_data"]:
            name = data['name']

            for i in data["files"]:

                for extract_type in data["options"]["extract_types"]:
                    print ''
                    print '\tdataset: %s' % name
                    print '\tfile: %s' % i['name']
                    print '\textract type: %s' % extract_type
                    print '\t----------'

                    extract_item = ExtractItem(self.client,
                                               request["boundary"]["name"],
                                               data["name"],
                                               i["name"],
                                               extract_type,
                                               data["temporal_type"],
                                               extract_base,
                                               self.extract_version)

                    # check if extract exists in queue and is completed
                    extract_exists, extract_completed = extract_item.exists()

                    print '\textract exists: %s' % extract_exists
                    print '\textract completed: %s' % extract_completed

                    # incremenet count if extract is not completed
                    # (whether it exists in queue or not)
                    if not extract_completed:
                        extract_count += 1

                        # add to extract queue if it does not already
                        # exist in queue
                        if not dry_run:
                            extract_item.add_to_queue("raster")

                    else:

                        # add to merge list
                        merge_list.append(
                            ('raster_data', extract_item.extract_path, None))

                        if i["reliability"]:
                            merge_list.append(
                                ('raster_data', extract_item.reliability_path,
                                 None))


        print ''
        print 'missing msr count: %s' % msr_count
        print 'missing extract count: %s' % extract_count
        print ''

        missing_items = extract_count + msr_count

        return missing_items, merge_list


# =============================================================================
# =============================================================================
# =============================================================================


    def build_output(self, request, merge_list):
        """build output

        merge extracts, generate documentation, update status,
            cleanup working directory, send final email
        """
        request_id = str(request['_id'])

        results_dir = ("/sciclone/aiddata10/REU/outputs/" +
                       self.branch + "/det/results")

        request_dir = os.path.join(results_dir, request_id)

        merge_output = os.path.join(request_dir, "results.csv")

        # merge cached results if all are available
        merge_status = self.merge(merge_list, merge_output)


        # generate documentation
        doc_output =  os.path.join(request_dir, "documentation.pdf")
        doc = DocBuilder(request, doc_output)
        bd_status = doc.build_doc()
        print bd_status


        # zip files and delete originals
        shutil.make_archive(request_dir, "zip", results_dir, request_id)
        shutil.rmtree(request_dir)

        return True


    def merge(self, merge_list, output):
        """
        merge extracts when all are completed
        """
        merged_df = 0

        # used to track dynamically generated field names
        # so corresponding extract and reliability have consistent names
        merge_log = {}

        # created merged dataframe from results
    # try:

        # for each result file that should exist for request
        # (extracts and reliability)
        for merge_item in merge_list:
            merge_class, result_csv, dynamic_merge_count = merge_item

            # make sure file exists
            if os.path.isfile(result_csv):

                if merge_class == 'raster_data':
                    # get field name from file
                    result_field =  os.path.splitext(os.path.basename(
                        result_csv))[0]

                elif merge_class == 'release_data':

                    csv_basename = os.path.splitext(os.path.basename(
                        result_csv))[0]

                    merge_log_name = csv_basename[:-2]

                    if not merge_log_name in merge_log.keys():

                        # dynamic merge string
                        tmp_str = '{0:03d}'.format(dynamic_merge_count)

                        merge_log[merge_log_name] = 'ad_msr' + tmp_str


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

    # except Exception as e:
        # warnings.warn(e)
        # return False, "error building merged dataframe"


        # output merged dataframe to csv
    # try:
        # generate output folder for merged df using request id
        make_dir(os.path.dirname(output))

        # write merged df to csv
        merged_df.to_csv(output, index=False)

        return True

    # except Exception as e:
        # warnings.warn(e)
    #     return False, "error writing merged dataframe"

