# utilty functions for processing requests in queue

import time
import pymongo
from bson.objectid import ObjectId

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText


class queue():


    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.det
        self.queue = self.db.queue


    # verify request with id exists
    def check_id(self, rid):

        try:
            # check document with request id exists
            search = self.queue.find({"_id":ObjectId(rid)})
            exists = search.count()
            
            return 1, exists, search[0]

        except:
            return 0, None, None


    # get id of next job in queue
    # based on priority and submit_time
    # factor how many extracts need to be processed into queue order (?)
    def get_next(self):
        
        try:
            # find all status 1 jobs and sort by priority then submit_time
            sort = self.queue.find({"status":1}).sort([("priority", -1), ("submit_time", 1)])
            if sort.count() > 0:
                rid = str(sort[0]["_id"])
                return 1, rid, sort[0]
            else:
                return 1, None, None

        except:
            return 0, None, None


    # update status of request
    def update_status(self, rid, status):
        
        ctime = int(time.time())

        updates = {
            "status": long(status)
        }

        if status == 2:
            updates["prep_time"] = ctime
        elif status == 3:
            updates["process_time"] = ctime
        elif status == 0:
            updates["complete_time"] = ctime

        try:
            # update request document
            self.queue.update({"_id": ObjectId(rid)}, {"$set": updates})
            return True

        except:
            return False


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
            





