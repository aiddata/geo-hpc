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


# -----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'utils')
sys.path.insert(0, config_dir)

from config_utility import BranchConfig

config_attempts = 0
while True:
    config = BranchConfig(branch=branch)
    config_attempts += 1
    if config.connection_status == 0 or config_attempts > 5:
        break


# -------------------------------------------------------------------------


import mpi_utility
job = mpi_utility.NewParallel()


connect_status = job.comm.gather((job.rank, config.connection_status, config.connection_error), root=0)

if job.rank == 0:
    connection_error = False
    for i in connect_status:
        if i[1] != 0:
            print "mongodb connection error ({0} - {1}) on processor rank {2})".format(i[1], i[2], [3])
            connection_error = True

    if connection_error:
        sys.exit()


job.comm.Barrier()


import os
import sys
import time
import warnings

# # used for logging
# sys.stdout = sys.stderr = open(
#     os.path.dirname(os.path.abspath(__file__)) + '/processing.log', 'a')

from request_tools import QueueToolBox

# =============================================================================

print '\n======================================='
print '\nProcessing Script'
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())

dry_run = False

queue = QueueToolBox()

# load config setting for branch script is running on
branch_info = queue.set_branch_info(config)
print "`{0}` branch on {1}".format(branch_info.name, branch_info.database)

request_id = 0
request_objects = []

# run if given a request_id via input arg
if len(sys.argv) == 3:
    request_id = str(sys.argv[2])

    # check for request with given id
    # return request data object if request exists else None
    try:
        request_check = queue.check_id(request_id)
    except Exception as e:
        print "Error while checking request id (" + request_id + ")"
        raise

    if request_check is None:
        sys.exit("Request with id does not exist (" + request_id + ")")

    request_objects += [request_check]

else:
    # get list of requests in queue based on status, priority and
    # submit time

    # check for new unprocessed requests (status -1) first, before
    # checking status of requests with items already in queue (status 0)
    # (new requests may have items that need to be added to queue,
    # or might already be done)
    try:
        request_objects += queue.get_requests(-1, 0)
        request_objects += queue.get_requests(0, 0)
    except Exception as e:
            print "Error while searching for requests in queue"
            raise

    # verify that we have some requests
    if len(request_objects) == 0:
       sys.exit("Request queue is empty")



# go through found requests, checking status of items in
# in extract/msr queue, building final output when ready
# and emailing user who requested data
for request_obj in request_objects:

    request_id = str(request_obj['_id'])

    print '\n---------------------------------------'
    print 'Request (id: {0})\n{1}\n'.format(request_id, request_obj)
    print 'Boundary: {0}'.format(request_obj['boundary']['name'])

    original_status = queue.get_status(request_id)

    is_prep = original_status == -1

    # set status 2 (no email)
    queue.update_status(request_id, 2, is_prep)


    try:
        missing_items, merge_list = queue.check_request(request_obj,
                                                        dry_run=dry_run)
    except Exception as e:
        print "unable to run check_request"
        queue.update_status(request_id, -2)
        raise


    if is_prep:
        # send email that request was received
        queue.notify_received(request_id, request_obj['email'])


    if missing_items == 0:

        # pull request again, since check_request may have updated fields
        try:
            updated_request_obj = queue.check_id(request_id)
        except Exception as e:
            print "Error while checking for updated request (id: " + request_id + ")"
            raise

        if updated_request_obj is None:
            sys.exit("Error getting updated request: Request with id does not exist (" + request_id + ")")


        try:
            # build request
            queue.build_output(updated_request_obj, merge_list, branch)
        except Exception as e:
            print "error building request output"
            queue.update_status(request_id, -2)
            raise


        # send email that request was completed
        queue.notify_completed(request_id, request_obj['email'])

        # set status 1 (email request is ready)
        queue.update_status(request_id, 1)

        print "request completed"

    else:
        # set status 0 (no email)
        queue.update_status(request_id, 0)

        print "request not ready"


    ###
    # for testing
    if dry_run:
        queue.update_status(request_id, original_status)
    ###


print '\n---------------------------------------'
print "\nFinished checking requests"
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())

