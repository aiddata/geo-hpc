# process queue requests

import os
import sys
import time
import warnings

# sys.stdout = sys.stderr = open(
#     os.path.dirname(os.path.abspath(__file__)) + '/processing.log', 'a')

from request_tools import QueueToolBox

# =============================================================================

queue = QueueToolBox()

request_id = 0

print '\n------------------------------------------------'
print 'Processing Script'
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())

# run given a request_id via input
if len(sys.argv) == 2:
    request_id = str(sys.argv[1])

    # check if request with id exists
    # return status of check, boolean of exists and request data if exists
    ci_status, ci_exists, request_objects = queue.check_id(request_id)

    if not ci_status:
        sys.exit("Error while checking request id (" + request_id + ")")
    elif not ci_exists:
        sys.exit("Request with id does not exist (" + request_id + ")")

else:
    # get list of requests in queue (status: 0) based on priority and
    #   submit time
    # returns status of search and request data objecft

    gr_status_1, request_objects_1 = queue.get_requests('status', -2, 0)

    if not gr_status_1:
        warnings.warn('could not get check requests for status "-1"')

    gr_status_2, request_objects_2 = queue.get_requests('status', 0, 0)

    if not gr_status_2:
        warnings.warn('could not get check requests for status "0"')

    if gr_status_1 and gr_status_2:
        request_objects = request_objects_1 + request_objects_2
    elif gr_status_1 and not gr_status_2:
        request_objects = request_objects_1
    elif not gr_status_1 and gr_status_2:
        request_objects = request_objects_2
    elif not (gr_status_1 or gr_status_2):
       sys.exit("Error while searching for requests in queue")

    if len(request_objects) == 0:
       sys.exit("Request queue is empty")


for request_obj in request_objects:

    request_id = str(request_obj['_id']['$id'])

    print '\nRequest id: ' + request_id
    print request_obj



    status = queue.get_status(request_id)

    if not status[0]:
        warnings.warn("error retrieving status of request")
        continue

    status = status[1]
    print "Current status: " + str(status)



    # # set status 2 (no email)
    # update_status = queue.update_status(request_id, 2)
    # if not update_status[0]:
    #     warnings.warn("unable to update status of request (2)")
    #     continue
    # print "processing request"

    # new_items = None
    # if status == -1:
    #     # check request and add extract/msr items to queue, return # added
    #     # new_items = queue.add_items(123)

    #     ### status, extract_count, msr_count = queue.cache.check_request(
    #     ###     request_id, request_obj, True)

    # if new_items in [None, 0]:
    #     # check if extracts/msr are all ready, return # not ready
    #     # unprocessed_items = queue.check_items(123)

    #     if unprocessed_items == 0:
    #         # build request
    #         # result = queue.build_output(123)

    #         ### queue.build_output(request_id, True)

    #         if not result[0]:
    #             warnings.warn("error building request output")
    #             continue

    #         # set status 1 (email request is ready)
    #         update_status = queue.update_status(request_id, 1)
    #         if not update_status[0]:
    #             warnings.warn("unable to update status of request (1)")
    #             continue
    #         print "request completed"

    #     else:
    #         # set status 0 (no email)
    #         update_status = queue.update_status(request_id, 0)
    #         if not update_status[0]:
    #             warnings.warn("unable to update status of request (0)")
    #             continue
    #         print "request not ready"



print "\nFinished checking requests"
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())
