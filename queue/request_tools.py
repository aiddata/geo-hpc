
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
        self.client = None

        self.c_queue = None
        self.c_email = None

        self.branch_info = None
        self.branch = None
        self.msr_version = None
        self.extract_version = None

        self.msr_resolution = 0.05


    # def quit(self, rid, status, message):
    #     """exit function used for errors
    #     """
    #     self.update_status(rid, int(status))
    #     sys.exit(">> det processing error ("+str(status)+"): \n\t\t" +
    #               str(message))


    def set_branch_info(self, branch_config=None):
        """get branch info from config collection
        """
        if branch_config is None:
            raise Exception('no branch config found')

        self.client = branch_config.client

        self.c_queue = self.client.asdf.det
        self.c_email = self.client.asdf.email

        self.branch_info = branch_config
        self.branch = branch_config.name
        self.msr_version = branch_config.versions['mean-surface-rasters']
        self.extract_version = branch_config.versions['extract-scripts']
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
        }).sort([("priority", -1), ("stage.0.time", 1)]).limit(limit)

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


    def update_status(self, rid, status, is_prep=False):
        """ update status of request
        """
        valid_stages = {
            "-2": None,
            "-1": None,
            "0": None,
            "2": "stage.2.time",
            "1": "stage.3.time"
        }

        ctime = int(time.time())

        updates = {
            "status": long(status)
        }

        stage = valid_stages[str(status)]
        if stage is not None:
            updates[stage] = ctime


        if is_prep:
            updates['stage.1.time'] = ctime

        try:
            # update request document
            self.c_queue.update({"_id": ObjectId(rid)},
                                {"$set": updates})

        except Exception as e:
            print ('error updating status of request '
                   '(id: {0}, status: {1}').format(rid, status)
            raise e


    def send_email(self, receiver, subject, message):
        """send an email

        Args:
            receiver (str): email address to send to
            subject (str): subject of email
            message (str): body of email

        Returns:
            (tuple): status, error message, exception
            status is bool
            error message and exception are None on success
        """
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

        mail_subject = ("AidData geo(query) - "
                        "Request {0}.. Received").format(request_id[:7])

        mail_message = ("Your request has been received. \n"
                        "You will receive an additional email when the"
                        " request has been completed. \n\n"
                        "The status of your request can be viewed using"
                        " the following link: \n"
                        "http://{0}/query/#/status/{1}\n\n"
                        "You can also view all your current and previous "
                        "requests using: \n"
                        "http://{0}/query/#/requests/{2}\n\n").format(
                            self.branch_info.det['download_server'], request_id, mail_to)

        mail_status = self.send_email(mail_to, mail_subject, mail_message)

        if not mail_status[0]:
            print mail_status[1]
            update_status = self.update_status(request_id, -2)
            raise mail_status[2]


    def notify_completed(self, request_id, email):
        """send email that request was completed
        """
        mail_to = email

        mail_subject = ("AidData geo(query) - "
                        "Request {0}.. Completed").format(request_id[:7])

        mail_message = (
            """
            Your request has been completed.

            You can review your request and download the results using the following page:
            \thttp://{0}/query/#/status/{1}

            or download the results directly (this link will always be available):
            \thttp://{0}/data/geoquery_results/{1}/{1}.zip


            You can also view all your current and previous requests using:
            \thttp://{0}/query/#/requests/{2}


            If you have not done so before or if you would like to provide additional feedback,
            please fill out this brief survey regarding your experience with geo(query).
            \thttps://goo.gl/4WZ46M


            Citations enable continued support and development of this tool.
            As a part of your use of this tool, you have agreed to cite geo(query)
            in any derived products (including academic publications, reports, or other works).
                \tGoodman, S., BenYishay, A., Runfola, D., 2016. Overview of the geo Framework.
                \tAidData. Available online at http://geo.aiddata.org. DOI: 10.13140/RG.2.2.28363.59686


            Thank you,
            \tThe AidData Team
            """).format(
                self.branch_info.det['download_server'], request_id, mail_to)

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
        for ix, raw_data in enumerate(request['release_data']):

            # # mongo was defaulting to 32bit vals for numbers on insert
            # # making float should prevent that
            # if 'total_commitments' in raw_data['filters']:
            #     raw_data['filters']['total_commitments'] = [
            #         float(i) for i in raw_data['filters']['total_commitments']]
            # if 'total_disbursements' in raw_data['filters']:
            #     raw_data['filters']['total_disbursements'] = [
            #         float(i) for i in raw_data['filters']['total_disbursements']]


            # remove filters without actual fields
            tmp_filters = {
                fk: fv
                for fk, fv in raw_data['filters'].iteritems()
                if not any([fvx in ['All', 'None', None] for fvx in fv])
            }


            # msr request object format
            data = {
                'dataset': raw_data['dataset'],
                'type': 'release',
                'resolution': self.msr_resolution,
                'version': self.msr_version,
                'filters': tmp_filters
            }

            # get hash of msr request object
            data_hash = json_sha1_hash(data)

            # add hash to request
            if not 'hash' in raw_data or raw_data['hash'] == data_hash:
                self.c_queue.update(
                    { "_id": ObjectId(request['_id']) },
                    { "$set": { 'release_data.'+str(ix)+'.hash' : data_hash}}
                )


            print ''
            print '\t{0}'.format(data_hash)
            print '\t{0}'.format(data)
            print '\t----------'

            msr_item = MSRItem(self.client,
                               msr_base,
                               data_hash,
                               data)

            # check if msr exists in queue and is completed
            msr_exists, msr_completed = msr_item.exists()

            print '\tmsr exists: {0}'.format(msr_exists)
            print '\tmsr completed: {0}'.format(msr_completed)

            if msr_completed == True:

                ###
                tmp_extract_type = 'reliability'
                if data["dataset"].startswith('worldbank'):
                    tmp_extract_type = 'sum'
                ###

                msr_ex_item = ExtractItem(self.client,
                                          extract_base,
                                          request["boundary"]["name"],
                                          data["dataset"],
                                          data["dataset"] + '_' + data_hash,
                                          tmp_extract_type,
                                          data_hash,
                                          self.extract_version)

                msr_ex_exists, msr_ex_completed = msr_ex_item.exists()

                print '\tmsr extract exists: {0}'.format(msr_ex_exists)
                print '\tmsr extract completed: {0}'.format(msr_ex_completed)

                if not msr_ex_completed:
                    extract_count += 1
                    if not dry_run:
                        # add to extract queue
                        msr_ex_item.add_to_queue("msr")

                else:
                    # add to merge list
                    merge_list.append(
                        (msr_ex_item.extract_path, 'release_data', msr_id))

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
                    print '\tdataset: {0}'.format(name)
                    print '\tfile: {0}'.format(i['name'])
                    print '\textract type: {0}'.format(extract_type)
                    print ''

                    temporal = i["name"][len(data["name"])+1:]

                    extract_item = ExtractItem(self.client,
                                               extract_base,
                                               request["boundary"]["name"],
                                               data["name"],
                                               i["name"],
                                               extract_type,
                                               temporal,
                                               self.extract_version)

                    # check if extract exists in queue and is completed
                    extract_exists, extract_completed = extract_item.exists()

                    print '\textract exists: {0}'.format(extract_exists)
                    print '\textract completed: {0}'.format(extract_completed)
                    print '\t--------------------'

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
                            (extract_item.extract_path, 'raster_data', None))


        print ''
        print 'missing msr count: {0}'.format(msr_count)
        print 'missing extract count: {0}'.format(extract_count)
        print ''

        missing_items = extract_count + msr_count

        return missing_items, merge_list


# =============================================================================
# =============================================================================
# =============================================================================


    def build_output(self, request, merge_list, branch):
        """build output

        merge extracts, generate documentation, update status,
            cleanup working directory, send final email
        """
        request_id = str(request['_id'])
        request['_id'] = request_id

        results_dir = ("/sciclone/aiddata10/REU/outputs/" +
                       self.branch + "/det/results")

        request_dir = os.path.join(results_dir, request_id)

        shutil.rmtree(request_dir, ignore_errors=True)

        merge_output = os.path.join(request_dir,
                                    "{0}_results.csv".format(request_id))

        # merge cached results if all are available
        merge_status = self.merge_file_list(merge_list, merge_output)

        if not merge_status:
            raise Exception('\tWarning: no extracts merged for '
                            'request_id = {0}').format(request_id)
        else:
            print '\tMerge completed for {0}'.format(request_id)


        # generate documentation
        doc_output =  os.path.join(request_dir,
                                   "{0}_documentation.pdf".format(request_id))
        doc = DocBuilder(self.client, request, doc_output, self.branch_info.det['download_server'])
        bd_status = doc.build_doc()
        # print bd_status


        # output request doc as json
        print "creating request json"
        rdoc_path = os.path.join(request_dir, "request_details.json")
        rdoc_file = open(rdoc_path, "w")
        json.dump(request, rdoc_file, indent=4)
        rdoc_file.close()


        dir_base = os.path.dirname(os.path.abspath(__file__))
        geo_pdf_src = dir_base + "/other/IntroducingtheAidDataGeoFramework.pdf"
        geo_pdf_dst = os.path.join(request_dir, "IntroducingtheAidDataGeoFramework.pdf")
        shutil.copyfile(geo_pdf_src, geo_pdf_dst)


        # # make msr json folder in request_dir
        # msr_jsons_dir = os.path.join(request_dir, 'msr_jsons')
        # make_dir(msr_jsons_dir)

        # # copy all msr jsons into msr json folder
        # for i in request['release_data']:
        #     tmp_dataset = i['dataset']
        #     tmp_hash = i['hash']

        #     src = "/sciclone/aiddata10/REU/outputs/{0}/msr/done/{1}/{2}/summary.json".format(
        #         branch, tmp_dataset, tmp_hash)
        #     dst = os.path.join(msr_jsons_dir, "{0}_{1}.json".format(
        #         tmp_dataset, tmp_hash))

        #     shutil.copyfile(src, dst)


        # make zip of request dir
        shutil.make_archive(request_dir, "zip", request_dir)

        # move zip of request dir into request dir
        shutil.move(request_dir + ".zip", request_dir)


        # remove unzipped files which do not need direct access
        os.remove(geo_pdf_dst)
        # shutil.rmtree(msr_jsons_dir)





    def merge_file_list(self, file_list, merge_output):
        """merge extracts for given file list

        outputs to given csv path

        Args:
        file_list (list): contains file paths of extract csv files
                          to merge. may be a tuple, to pass additional
                          info, but extract file path must be first item

        merge_output (str): absolute path for merged output file
        """

        field_list = ['asdf_id']

        merged_df = 0
        for file_info in file_list:

            if isinstance(file_info, tuple):
                result_csv = file_info[0]
            else:
                result_csv = file_info


            if not os.path.isfile(result_csv):
                raise Exception("missing file ({0})".format(result_csv))


            result_df = pd.read_csv(result_csv, quotechar='\"',
                                    na_values='', keep_default_na=False)


            exfields = [
                cname for cname in list(result_df.columns)
                if cname.startswith("exfield_")
            ]

            if not isinstance(merged_df, pd.DataFrame):
                merged_df = result_df.copy(deep=True)
                merged_df.drop(exfields, axis=1, inplace=True)


            result_field = result_csv[result_csv.rindex('/')+1:-4]

            # could add something here that attempt to cap field name at 10 chars
            #
            # rasters... lookup mini name, extract method abbrv,
            #            attempt to include temporal infl
            # msr... use dynamic_merge_count and something else?


            for c in exfields:

                if result_field.endswith('categorical'):
                    tmp_field = "{0}_{1}".format(
                        result_field,
                        c[len("exfield_"):])

                elif result_field.endswith('reliability'):
                    tmp_field = "{0}{1}".format(
                        result_field[:-len('reliability')],
                        c[len("exfield_"):])

                else:
                    tmp_field = result_field


                merged_df[tmp_field] = result_df[c]
                field_list.append(tmp_field)


        if isinstance(merged_df, pd.DataFrame):

            # reorder columns to put id and extract columns first
            field_list += [i for i in list(merged_df.columns)
                           if i not in field_list]
            merged_df = merged_df[field_list]

            # output merged dataframe to csv
            # generate output folder for merged df using request id
            make_dir(os.path.dirname(merge_output))

            # write merged df to csv
            merged_df.to_csv(merge_output, index=False)
            print '\tResults output to {0}'.format(merge_output)
            return True

        else:
            print '\tWarning: no extracts merged'
            return False


