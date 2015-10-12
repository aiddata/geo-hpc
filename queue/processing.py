# process queue requests

# need better error handling /notification for
# what are currently "fatal" errors
# at minimum: update request status so 
# we are aware and can check it or restart it

import os
import sys
import time

sys.stdout = sys.stderr = open(os.path.dirname(os.path.abspath(__file__)) +'/processing.log', 'a')

from queue import queue
from cache import cache
from documentation import doc

queue = queue()
cache = cache()
doc = doc()

queue.cache = cache
queue.doc = doc

request_id = 0

print '\n------------------------------------------------'
print 'Processing Script'
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())

# run given a request_id via input
if len(sys.argv) == 2:
    request_id = sys.argv[1]

    # check if request with id exists
    # return status of check, boolean of exists and request data if exists 
    ci_status, ci_return, request_obj = queue.check_id(sys.argv[1])

    if not ci_status:
        sys.exit("Error while checking request id")
    elif not ci_return:
        sys.exit("Request with id does not exist")

    request_objects = {}
    request_objects[request_id] = request_obj

else:
    # get list of requests in queue (status: 0) based on priority and submit time
    # returns status of search and request data objecft

    gn_status, request_objects = queue.get_next(0, 0)

    if not gn_status:
       sys.exit("Error while searching for next request in queue")
    elif request_id == None:
       sys.exit("Processing queue is empty")



for i in request_objects.keys():
    request_id = request_objects.keys()[i]
    request_obj = request_objects[request_id]

    print 'Request id: ' + request_id


    # update status to being processed 
    # (without running extracts: 2, with runnning extracts: 3)
    us = queue.update_status(request_id, 3)


    # check results for cached data
    # run missing extracts if run_extract is True
    cr_status, cr_extract_count, cr_msr_count = cache.check_request(request_id, request_obj, True)


    if not cr_status:
        queue.quit(request_id, -2, "Error while checking request cache")

    # if extracts are cached then build output
    if cr_extract_count == 0:
        print "finishing request"
        # merge results and generate documentation
        queue.build_output(request_id, True)

    else:
        print "request not ready"

        # update status 0 (ready for processing)
        us = queue.update_status(request_id, 0)

