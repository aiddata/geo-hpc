# utilty functions for processing requests in queue


import pymongo


class utility():


    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.det
        self.cache = self.db.queue


    # verify request with id exists
    def check_id(id):

        # check document with request id exists
        # 

        return boolean


    # get id of next job in queue
    # based on priority and submit_time
    def get_next():
        
        # find all status 1 jobs and sort by priority then submit_time
        # 

        return _id


    # get request object based on request id
    def get_request(id):

        # mongo search
        # 

        return request


    # update status of request
    def update_status(id, status):
        
        # update request document
        #

        return something

