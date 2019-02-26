
import os
import sys
import errno
import time
import json
import warnings
import shutil
import hashlib
import textwrap

import pymongo
from bson.objectid import ObjectId

import pandas as pd

from email_utility import GeoEmail
from extract_utility import merge_file_list
from extract_check import ExtractItem
from msr_check import MSRItem
from geoquery_documentation import DocBuilder


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
                           sort_keys=True,
                           ensure_ascii=True,
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
        self.email = None

        self.config = None
        self.branch = None
        self.msr_version = None
        self.extract_version = None

        self.msr_resolution = 0.05

        self.dev = " (dev) " if self.branch == "develop" else " "


    # def quit(self, rid, status, message):
    #     """exit function used for errors
    #     """
    #     self.update_status(rid, int(status))
    #     sys.exit(">> det processing error ("+str(status)+"): \n\t\t" +
    #               str(message))


    def set_branch_info(self, config=None):
        """get branch info from config collection
        """
        if config is None:
            raise Exception('no branch config found')

        self.assets_dir = os.path.join(config.source_dir, 'geo-hpc/assets')

        self.config = config

        self.client = config.client

        self.c_queue = self.client.asdf.det

        self.email = GeoEmail(config)

        self.branch = config.name
        self.msr_version = config.versions['msr']
        self.extract_version = config.versions['extracts']
        return config


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

            column_info_html = ''.join(
                open(self.assets_dir + '/templates/column_info.txt')
            ).replace('\n', '<br/>')

            updates['info'] = [column_info_html]

            # Example push/prepend for info field if needed for manual updates
            # https://docs.mongodb.com/manual/reference/operator/update/push/
            # https://stackoverflow.com/questions/10131957/can-you-have-mongo-push-prepend-instead-of-append

        try:
            # update request document
            self.c_queue.update({"_id": ObjectId(rid)},
                                {"$set": updates})

        except Exception as e:
            print ('error updating status of request '
                   '(id: {0}, status: {1}').format(rid, status)
            raise e


    def notify_received(self, request_id, email):
        """send email that request was received
        """
        mail_to = email

        mail_subject = ("AidData GeoQuery{0}- "
                        "Request {1}.. Received").format(self.dev, request_id[:7])

        mail_message = (
            """
            Thanks for using GeoQuery. This is an automated email to let you
            know that we received your request and will process your data as
            soon as we can. We will send another email when your data is ready.

            You can view the status of this data request here:
            http://{0}/query/#!/status/{1}

            You can also keep track of all of the data requests that you've
            submitted with this email address here:
            http://{0}/query/#!/requests/{2}

            Thank you,
            \tAidData's GeoQuery Team
            """).format(self.config.det['download_server'], request_id, mail_to)

        mail_message = textwrap.dedent(mail_message)

        mail_status = self.email.send_email(mail_to, mail_subject, mail_message)

        if not mail_status[0]:
            print mail_status[1]
            update_status = self.update_status(request_id, -2)
            raise mail_status[2]


    def notify_completed(self, request_id, email):
        """send email that request was completed
        """
        mail_to = email

        mail_subject = ("AidData GeoQuery{0}- "
                        "Request {1}.. Completed").format(self.dev, request_id[:7])

        mail_message = (
            """
            Thanks again for using GeoQuery. This is another automated email to let you know that
            your data is ready.

            You can review your request, and download the results and documentation here:
            \thttp://{0}/query/#!/status/{1}

            Or download the results directly (this link will always be available):
            \thttp://{0}/data/geoquery_results/{1}/{1}.zip

            You can also view all your current and previous requests using:
            \thttp://{0}/query/#!/requests/{2}

            Also, one quick reminder about citations. Don't forget to cite both AidData's GeoQuery
            tool as well as each dataset you selected within GeoQuery. All citations can be found
            in the Documentation PDF at the link above. Here's the correct citation for GeoQuery:

                Goodman, S., BenYishay, A., Lv, Z., & Runfola, D. (2019).
                GeoQuery: Integrating HPC systems and public web-based
                geospatial data tools. Computers & Geosciences, 122, 103-112.

            Thank you in advance for citing us when you publish your research.
            This helps us to demonstrate how GeoQuery is making a difference
            as a freely available public good.

            Thank you,
            \tAidData's GeoQuery Team
            """).format(self.config.det['download_server'], request_id, mail_to)

        mail_message = textwrap.dedent(mail_message)

        mail_status = self.email.send_email(mail_to, mail_subject, mail_message)

        if not mail_status[0]:
            print mail_status[1]
            update_status = self.update_status(request_id, -2)
            raise mail_status[2]


    def notify_comments(self, email):
        """send email asking for information about their work using geoquery
        """
        mail_to = email

        mail_subject = ("Was GeoQuery{0} helpful?").format(self.dev)

        mail_message = (
            """
            Hello there!

            We would like to hear about your experience using AidData's GeoQuery tool. Would you
            please respond to this email with a couple sentences about how GeoQuery has helped you?

            We are able to make GeoQuery freely available thanks to the generosity of donors and
            open source data providers. These people love to hear about new research enabled by
            GeoQuery, and what kind of difference this research is making in the world.

            Also, we love feedback of all kinds. If something did not go the way you expected, we
            want to hear about that too.

            Thanks!
            \tAidData's GeoQuery Team
            """)

        mail_message = textwrap.dedent(mail_message)

        mail_status = self.email.send_email(mail_to, mail_subject, mail_message)

        if not mail_status[0]:
            print mail_status[1]
            raise mail_status[2]


# =============================================================================
# =============================================================================
# =============================================================================


    def check_request(self, request, dry_run=False):
        """check entire request object for cache
        """
        merge_list = []
        extract_count = 0
        msr_count = 0

        # id used for field names in results
        msr_id = 1

        print "\nchecking aid data (dry=run = {}...".format(dry_run)
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
            tmp_filters = {}
            if len(raw_data['filters']) > 0:
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

            msr_item = MSRItem(self.config,
                               data_hash,
                               data)

            # check if msr exists in queue and is completed
            msr_exists, msr_completed = msr_item.exists()

            print '\tmsr exists: {0}'.format(msr_exists)
            print '\tmsr completed: {0}'.format(msr_completed)

            if msr_completed == True:

                tmp_extract_type = 'reliability'
                # if data["dataset"].startswith(('worldbank', 'globalenvironmentfacility')):
                #     tmp_extract_type = 'sum'

                msr_ex_item = ExtractItem(self.config,
                                          request["boundary"]["name"],
                                          data["dataset"],
                                          data["dataset"] + '_' + data_hash,
                                          tmp_extract_type,
                                          data_hash,
                                          self.extract_version)

                msr_ex_exists, msr_ex_completed = msr_ex_item.exists()

                print '\tmsr extract exists: {0}'.format(msr_ex_exists)
                print '\tmsr extract completed: {0}'.format(msr_ex_completed)

                if msr_ex_completed == "Error":
                    extract_count += 1

                elif not msr_ex_completed:
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


        print "\nchecking raster data (dry=run = {}...".format(dry_run)
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

                    extract_item = ExtractItem(self.config,
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
                    if extract_completed == "Error" or not extract_completed:
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

        results_dir = os.path.join(self.config.branch_dir, "outputs/det/results")

        request_dir = os.path.join(results_dir, request_id)

        shutil.rmtree(request_dir, ignore_errors=True)

        merge_output = os.path.join(request_dir,
                                    "{0}_results.csv".format(request_id))

        # merge cached results if all are available
        merge_status = merge_file_list(merge_list, merge_output)

        if not merge_status:
            raise Exception('\tWarning: no extracts merged for '
                            'request_id = {0}').format(request_id)
        else:
            print '\tMerge completed for {0}'.format(request_id)


        # generate documentation
        doc_output = os.path.join(request_dir,
                                  "{0}_documentation.pdf".format(request_id))
        doc = DocBuilder(self.config, request, doc_output, self.config.det['download_server'])
        bd_status = doc.build_doc()
        # print bd_status


        # output request doc as json
        print "creating request json"
        rdoc_path = os.path.join(request_dir, "request_details.json")
        rdoc_file = open(rdoc_path, "w")
        json.dump(request, rdoc_file, indent=4)
        rdoc_file.close()


        geo_pdf_src = self.assets_dir + "/other/GeoQuery_Goodman2019.pdf"
        geo_pdf_dst = os.path.join(request_dir, "GeoQuery_Goodman2019.pdf")
        shutil.copyfile(geo_pdf_src, geo_pdf_dst)

        if not 'boundaries/gadm' in request['boundary']['path']:
            bnd_src = request['boundary']['path']
            bnd_dst = os.path.join(request_dir, os.path.basename(request['boundary']['path']))
            shutil.copyfile(bnd_src, bnd_dst)


        # # make msr json folder in request_dir
        # msr_jsons_dir = os.path.join(request_dir, 'msr_jsons')
        # make_dir(msr_jsons_dir)

        # # copy all msr jsons into msr json folder
        # for i in request['release_data']:
        #     tmp_dataset = i['dataset']
        #     tmp_hash = i['hash']

        #     src = "{0}/outputs/msr/done/{1}/{2}/summary.json".format(
        #         self.config.branch_dir, tmp_dataset, tmp_hash)
        #     dst = os.path.join(msr_jsons_dir, "{0}_{1}.json".format(
        #         tmp_dataset, tmp_hash))

        #     shutil.copyfile(src, dst)


        # make msr aid folder in request_dir
        msr_aid_dir = os.path.join(request_dir, 'raw_aid_data')
        make_dir(msr_aid_dir)

        # copy all aid csv into msr aid folder
        for i in request['release_data']:
            tmp_dataset = i['dataset']
            tmp_hash = i['hash']

            src = "{0}/outputs/msr/done/{1}/{2}/project_locations.csv".format(
                self.config.branch_dir, tmp_dataset, tmp_hash)
            dst = os.path.join(msr_aid_dir, "{0}_{1}.csv".format(
                tmp_dataset, tmp_hash))
            try:
                shutil.copyfile(src, dst)
            except:
                pass


        # make zip of request dir
        shutil.make_archive(request_dir, "zip", request_dir)

        # move zip of request dir into request dir
        shutil.move(request_dir + ".zip", request_dir)


        # remove unzipped files which do not need direct access
        os.remove(geo_pdf_dst)
        # shutil.rmtree(msr_jsons_dir)

        # set permissions
        os.chmod(request_dir, 0775)
        for ro, di, fi in os.walk(request_dir):
            for d in di:
                os.chmod(os.path.join(ro, d), 0775)
            for f in fi:
                os.chmod(os.path.join(ro, f), 0664)
