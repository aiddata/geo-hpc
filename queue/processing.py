# process queue requests


import sys

from utility import utility
from cache import cache
# from documentation import doc


util = utility()
cache = cache()
# doc = doc()

request_id = 0
run_extract = False

# run given a request_id via input
# does not run extract
if len(sys.argv) == 2:
    request_id = sys.argv[1]

    # check if request with id exists
    # return status of check, boolean of exists and request data if exists 
    ci_status, ci_return, request_obj = util.check_id(sys.argv[1])

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
    gn_status, request_id, request_obj = util.get_next()
    
    if not gn_status:
       sys.exit("Error while searching for next request in queue")
    elif request_id == None:
       sys.exit("Queue is empty")



# update status to being processed 
# (without running extracts: 2, with runnning extracts: 3)
us = util.update_status(request_id, 2+run_extract)


# check results for cached data
# run missing extracts if run_extract is True
cr_status, cr_count = cache.check_request(request_obj, run_extract)


if not cr_status:
    sys.exit("Error while checking request cache")


# if extracts are cached then merge and generate documentation
if (not run_extract and cr_count == 0) or run_extract:

    # merge cached results if all are availed
    merge_status = cache.merge(request_obj)

    # generate documentation
    # doc.documentation()

    # add processed time
    if not run_extract:
        us = util.update_status(request_id, 3)

    # update status 0 (done)
    us = util.update_status(request_id, 0)

else:
    # add cr_count to request so number of needed extracts 
    # can be factored into queue order (?)
    # 

    # update status 1 (ready for processing)
    us = util.update_status(request_id, 1)

