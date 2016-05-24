# process queue requests

# need better error handling /notification for
# what are currently "fatal" errors
# at minimum: update request status so
# we are aware and can check it or restart it

import os
import sys
import time
import warnings

# sys.stdout = sys.stderr = open(
#     os.path.dirname(os.path.abspath(__file__)) + '/processing.log', 'a')

from request_tools import QueueCheck

# =============================================================================

queue = QueueCheck()

request_id = 0

print '\n------------------------------------------------'
print 'Processing Script'
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())

# run given a request_id via input
if len(sys.argv) == 2:
    request_id = sys.argv[1]

    # check if request with id exists
    # return status of check, boolean of exists and request data if exists
    ci_status, ci_exists, request_objects = queue.check_id(sys.argv[1])

    if not ci_status:
        sys.exit("Error while checking request id")
    elif not ci_exists:
        sys.exit("Request with id does not exist")

else:
    # get list of requests in queue (status: 0) based on priority and
    #   submit time
    # returns status of search and request data objecft

    gr_status_1, request_objects_1 = queue.get_requests('status', -1, 0)

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
    print request_obj

    request_id = str(request_obj['_id']['$id'])

    print '\nRequest id: ' + request_id



    ###
    """

    status = queue.get_status(request_id)

    if status == "error":
        continue
    # set status 2, no email
    #

    new_items = None
    if status == -1:
        # check request and add extract/msr items to queue, return # added
        new_items = add items

    if new_items in [None, 0]:
        # check if extracts/msr are all ready, return # not ready
        unprocessed_items = check items

        if unprocessed_items == 0:
            # build request
            #

            # set status 1, email request is ready
            #

        else:
            # set status 0, no email
            #



    """
    ###



    # # update status to being processed
    # # (without running extracts: 2, with runnning extracts: 3)
    # us = queue.update_status(request_id, 3)

    # if request_obj['status'] == -1:
    #     # send initial email
    #     p_message = ("Your data extraction request (" + request_id +
    #                  ") has been received. You can check on the status " +
    #                  "of the request via devlabs.aiddata.wm.edu/DET/status/#" +
    #                  request_id +". Results can be downloaded from the same " +
    #                  "page when they are ready.")
    #     queue.send_email("aiddatatest2@gmail.com", request_obj["email"],
    #                      "AidData Data Extraction Tool Request Received (" +
    #                      request_id + ")", p_message)


    # # check results for cached data
    # # run missing extracts if run_extract is True
    # cr_status, cr_extract_count, cr_msr_count = queue.cache.check_request(
    #     request_id, request_obj, True)


    # if not cr_status:
    #     queue.quit(request_id, -2, "Error while checking request cache")

    # # if extracts are cached then build output
    # if cr_extract_count == 0:
    #     print "finishing request"
    #     # merge results and generate documentation
    #     queue.build_output(request_id, True)

    # else:
    #     print "request not ready"

    #     # update status 0 (ready for processing)
    #     us = queue.update_status(request_id, 0)


