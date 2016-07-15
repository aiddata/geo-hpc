"""process det queue requests

called by cronjob on server for branch


to do (maybe)

    when unable to perform task on request, should we:
    - just continue to next request
    - set request to error (or original) status, if possible
    - track number of requests with errors and stop processing if multiple
        requests get errors (indicating something may be wrong with this script)
    - should we add a "retry" status to requests so the script can attempt
        to reprocess them one or more times before assigning a true error status

"""
import os
import sys
import time
import warnings

# # used for logging
# sys.stdout = sys.stderr = open(
#     os.path.dirname(os.path.abspath(__file__)) + '/processing.log', 'a')

from request_tools import QueueToolBox

# =============================================================================

print '\nProcessing Script'
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())


queue = QueueToolBox()

# load config setting for branch script is running on
branch_info = queue.get_branch_info()
print "`{0}` branch on {1}".format(branch_info['name'], branch_info['server'])

request_id = 0

# run if given a request_id via input arg
if len(sys.argv) == 2:
    request_id = str(sys.argv[1])

    # check for request with given id
    # return status of check and request data object if request exists
    try:
        request_objects = queue.check_id(request_id)
    except Exception as e:
        print "Error while checking request id (" + request_id + ")"
        raise e

    if request_objects is None:
        sys.exit("Request with id does not exist (" + request_id + ")")

else:
    # get list of requests in queue based on status, priority and
    # submit time

    # check for new unprocessed requests (status -1) first, before
    # checking status of requests with items already in queue (status 0)
    # (new requests may have items that need to be added to queue,
    # or might already be done)
    try:
        request_objects = []
        request_objects += queue.get_requests(-1, 0)
        request_objects += queue.get_requests(0, 0)
    except Exception as e:
            print "Error while searching for requests in queue"
            raise e

    # verify that we have some requests
    if len(request_objects) == 0:
       sys.exit("Request queue is empty")


print '\n---------------------------------------'

# go through found requests, checking status of items in
# in extract/msr queue, building final output when ready
# and emailing user who requested data
for request_obj in request_objects:

    request_id = str(request_obj['_id'])

    print 'Request (id: {0})\n{1}\n'.format(request_id, request_obj)
    print 'Boundary: {0}'.format(request_obj['boundary']['name'])

    original_status = queue.get_status(request_id)

    # set status 2 (no email)
    update_status = queue.update_status(request_id, 2)


    # try:
    #     missing_items, merge_list = queue.check_request(request_obj,
    #                                                     dry_run=False)
    # except Exception as e:
    #     print "unable to run check_request"
    #     update_status = queue.update_status(request_id, -2)
    #     raise e


    # if original_status == -1:
    #     # send email that request was received
    #     queue.notify_received(request_id, request_obj['email'])


    # if missing_items == 0:

    #     try:
    #         # build request
    #         result = queue.build_output(request_obj, merge_list)
    #     except Exception as e:
    #         print "error building request output"
    #         update_status = queue.update_status(request_id, -2)
    #         raise e

    #     # send email that request was completed
    #     queue.notify_completed(request_id, request_obj['email'])

    #     # set status 1 (email request is ready)
    #     update_status = queue.update_status(request_id, 1)

    #     print "request completed"

    # else:
    #     # set status 0 (no email)
    #     update_status = queue.update_status(request_id, 0)

    #     print "request not ready"


    print '---------------------------------------'

    ###
    # for testing
    queue.update_status(request_id, -1)
    ###

print "\nFinished checking requests"
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())
print '\n======================================='

