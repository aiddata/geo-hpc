# utilty functions for processing requests in queue

import sys
import time
import shutil

import pymongo
from bson.objectid import ObjectId

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText


class queue():


    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.det
        self.c_queue = self.db.queue

        self.c_extracts = self.db.extracts
        self.c_msr = self.db.msr

        self.cache = 0
        self.doc = 0

        # self.request_id = 0
        # self.request_obj = 0

        self.request_objects = {}


    # exit function used for errors
    def quit(self, rid, status, message):
        self.update_status(rid, int(status))
        sys.exit(">> det processing error: \n\t\t" + str(message))


    # verify request with id exists
    def check_id(self, rid):

        try:
            # check document with request id exists
            search = self.c_queue.find({"_id":ObjectId(rid)})
            exists = search.count()

            # self.request_id = rid
            # self.request_obj = search[0]

            self.request_objects[rid] = search[0]

            return 1, exists, search[0]

        except:
            return 0, None, None


    # get id of next job in queue
    # based on priority and submit_time
    # factor how many extracts need to be processed into queue order (?)
    def get_next(self, status, limit):
        
        try:
            # find all status 1 jobs and sort by priority then submit_time
            sort = self.c_queue.find({"status":status}).sort([("priority", -1), ("submit_time", 1)])

            if sort.count() > 0:

                if limit == 0 or limit > sort.count():
                    limit = sort.count()

                for i in range(limit):
                    rid = str(sort[i]["_id"])
                    self.request_objects[rid] = sort[i]

                return 1, self.request_objects

            else:
                return 1, None

        except:
            return 0, None


    # update status of request
    def update_status(self, rid, status):
        
        ctime = int(time.time())

        updates = {
            "status": long(status)
        }

        if status == 2:
            updates["prep_time"] = ctime
            self.request_obj["prep_time"] = ctime
        elif status == 3:
            updates["process_time"] = ctime
            self.request_obj["process_time"] = ctime

        elif status == 1:
            updates["complete_time"] = ctime
            self.request_obj["complete_time"] = ctime


        try:
            # update request document
            self.c_queue.update({"_id": ObjectId(rid)}, {"$set": updates})
            return True, ctime

        except:
            return False, None


    # sends an email 
    def send_email(self, sender, receiver, subject, message):

        try:
            pw_search = self.db.email.find({"address": sender}, {"password":1})

            if pw_search.count() > 0:
                passwd = str(pw_search[0]["password"])
            else:
                return 0, "Specified email does not exist"

        except:
            return 0, "Error looking up email"


        try:
            # source: 
            # http://stackoverflow.com/questions/64505/sending-mail-from-python-using-smtp

            msg = MIMEMultipart()

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
            

    # build output
    # merge extracts, generate documentation, update status, cleanup working directory, send final email
    def build_output(self, request_id, run_extract):
        
        # merge cached results if all are available
        merge_status = self.cache.merge(request_id, self.request_obj)

        # handle merge error
        if not merge_status[0]:
            self.quit(merge_status[1])


        # add processed time
        if not run_extract:
            us = self.update_status(request_id, 3)

        # update status 0 (done)
        us = self.update_status(request_id, 1)


        # generate documentation
        self.doc.request = self.request_obj
        print self.doc.request

        bd_status = self.doc.build_doc(request_id)
        print bd_status


        # zip files and delete originals
        shutil.make_archive("/sciclone/aiddata10/REU/det/results/"+ request_id, "zip", "/sciclone/aiddata10/REU/det/results/", request_id)
        shutil.rmtree("/sciclone/aiddata10/REU/det/results/"+ request_id)

        # send final email
        c_message = "Your data extraction request (" + request_id + ") has completed. The results are available via devlabs.aiddata.wm.edu/DET/status/#"+request_id
        self.send_email("aiddatatest2@gmail.com", self.request_obj["email"], "AidData Data Extraction Tool Request Completed ("+request_id+")", c_message)


# ---------------------------------------------------------------------------




    # 1) check if extract exists in extract queue
    #    run redundancy check on actual extract file and delete extract queue entry if file is missing
    #    also check for reliability calc if field is specified
    # 2) check if extract is completed, waiting to be run, or encountered an error
    def exists_in_extract_queue(self, boundary, raster, extract_type, reliability, csv_path):
        print "exists_in_extract_queue"
        
        check_data = {"boundary": boundary, "raster": raster, "extract_type": extract_type, "reliability": reliability}

        # check db
        search = self.c_cache.find(check_data)

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
                    self.c_cache.delete_one(check_data)

            else:
                valid_db_exists = True
                valid_completed_exists = "Error"


        return valid_db_exists, valid_completed_exists


    # 1) check if msr exists in msr tracker
    #    run redundancy check on actual msr raster file and delete msr tracker entry if file is missing
    # 2) check if msr is completed, waiting to be run, or encountered an error
    def exists_in_msr_tracker(self, dataset_name, filter_hash, raster_path):
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
