# process queue requests

# need better error handling /notification for
# what are currently "fatal" errors
# at minimum: update request status so 
# we are aware and can check it or restart it

import sys
import shutil

from queue import queue
from cache import cache
from documentation import doc


queue = queue()
cache = cache()
doc = doc()

request_id = 0
run_extract = False

# run given a request_id via input
# does not run extract
if len(sys.argv) == 2:
    request_id = sys.argv[1]

    # check if request with id exists
    # return status of check, boolean of exists and request data if exists 
    ci_status, ci_return, request_obj = queue.check_id(sys.argv[1])

    if not ci_status:
        sys.exit("Error while checking request id")
    elif not ci_return:
        sys.exit("Request with id does not exist")


# automatically determine next request to process
# runs extract if needed
else:
    run_extract = True

    # get next request in queue based on priority and submit time
    # returns status of search, request id if search succeeds, and request data
    gn_status, request_id, request_obj = queue.get_next()
    
    if not gn_status:
       sys.exit("Error while searching for next request in queue")
    elif request_id == None:
       sys.exit("Queue is empty")


# exit function used for errors
def quit(rid, status, message):
    queue.update_status(rid, int(status))
    sys.exit(">> det processing error: \n\t\t" + str(message))



# ---
# doc.request = request_obj
# doc.build_doc(request_obj)
# sys.exit("@!")
# ---


# update status to being processed 
# (without running extracts: 2, with runnning extracts: 3)
us = queue.update_status(request_id, 2+run_extract)


# send initial email
if not run_extract:
    p_message = "Your data extraction request (" + request_id + ") has been received. You can check on the status of the request via devlabs.aiddata.wm.edu/DET/status/#"+request_id +". Results can be downloaded from the same page when they are ready."
    queue.send_email("aiddatatest2@gmail.com", request_obj["email"], "AidData Data Extraction Tool Request Received ("+request_id+")", p_message)

# check results for cached data
# run missing extracts if run_extract is True
cr_status, cr_count = cache.check_request(request_obj, run_extract)


if not cr_status:
    quit("Error while checking request cache")


# if extracts are cached then merge and generate documentation
if (not run_extract and cr_count == 0) or run_extract:
    print "finishing request"
    # merge cached results if all are available
    merge_status = cache.merge(request_id, request_obj)

    # handle merge error
    if not merge_status[0]:
        quit(merge_status[1])


    # add processed time
    if not run_extract:
        us = queue.update_status(request_id, 3)

    # update status 0 (done)
    us = queue.update_status(request_id, 0)


    # generate documentation
    doc.request = request_obj
    print doc.request
    bd_status = doc.build_doc(request_id)
    print bd_status


    # zip files and delete originals
    shutil.make_archive("/sciclone/aiddata10/REU/det/results/"+ request_id, "zip", "/sciclone/aiddata10/REU/det/results/", request_id)
    shutil.rmtree("/sciclone/aiddata10/REU/det/results/"+ request_id)

    # send final email
    c_message = "Your data extraction request (" + request_id + ") has completed. The results are available via devlabs.aiddata.wm.edu/DET/status/#"+request_id
    queue.send_email("aiddatatest2@gmail.com", request_obj["email"], "AidData Data Extraction Tool Request Completed ("+request_id+")", c_message)

else:
    print "finishing prep"
    # add cr_count to request so number of needed extracts 
    # can be factored into queue order (?)
    # 

    # update status 1 (ready for processing)
    us = queue.update_status(request_id, 1)

