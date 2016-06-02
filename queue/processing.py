# process queue requests

import os
import sys
import time
import warnings

# sys.stdout = sys.stderr = open(
#     os.path.dirname(os.path.abspath(__file__)) + '/processing.log', 'a')

from request_tools import QueueToolBox

# =============================================================================

print '\n------------------------------------------------'
print 'Processing Script'
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())


queue = QueueToolBox()

branch_info = queue.get_branch_info()
print '`' + branch_info['name'] + '` branch on ' + branch_info['server']

request_id = 0

# run given a request_id via input
if len(sys.argv) == 2:
    request_id = str(sys.argv[1])

    # check if request with id exists
    # return status of check, boolean of exists and request data if exists
    ci_status, request_objects = queue.check_id(request_id)

    if not ci_status:
        raise Exception("Error while checking request id (" + request_id + ")")
    elif request_objects is None:
        sys.exit("Request with id does not exist (" + request_id + ")")

else:
    # get list of requests in queue (status: 0) based on priority and
    #   submit time
    # returns status of search and request data objecft

    gr_status_1, request_objects_1 = queue.get_requests(-2, 0)

    if not gr_status_1:
        warnings.warn('could not get check requests for status "-2"')

    gr_status_2, request_objects_2 = False, None #queue.get_requests(0, 0)

    if not gr_status_2:
        warnings.warn('could not get check requests for status "0"')

    if gr_status_1 and gr_status_2:
        request_objects = request_objects_1 + request_objects_2
    elif gr_status_1 and not gr_status_2:
        request_objects = request_objects_1
    elif not gr_status_1 and gr_status_2:
        request_objects = request_objects_2
    elif not (gr_status_1 or gr_status_2):
       raise Exception("Error while searching for requests in queue")

    if len(request_objects) == 0:
       sys.exit("Request queue is empty")


for request_obj in request_objects:

    request_id = str(request_obj['_id'])

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


    status, extract_count, msr_count, merge_list = queue.check_request(request_obj, dry_run=False)
    print extract_count, msr_count

    new_items = extract_count + msr_count


    if status == -2:

        # send email
        mail_to = request_obj['email']

        mail_subject = "AidData Data Extract Tool - Request "+request_id[:7]+".. Received"

        mail_message = ("Your request has been received. \n"
                        "You will receive an additional email when the"
                        " request has been completed. \n\n"
                        "The status of your request can be viewed using"
                        " the following link: \n"
                        "http://" + branch_info['server'] + "/DET/status/#" + request_id + "\n\n"
                        "You can also view all your current and previous requests using: \n"
                        "http://" + branch_info['server'] + "/DET/status/#" + mail_to + "\n\n")

        mail_status = queue.send_email(mail_to, mail_subject, mail_message)


    if new_items == 0:

        # build request
        result = queue.build_output(request_id, request_obj, merge_list)

        if not result:
            warnings.warn("error building request output")

            update_status = queue.update_status(request_id, -2)
            if not update_status[0]:
                warnings.warn("unable to update status of request (-2)")

            continue


        # set status 1 (email request is ready)
        update_status = queue.update_status(request_id, 1)
        if not update_status[0]:
            warnings.warn("unable to update status of request (1)")
            continue


        # send email
        mail_to = request_obj['email']

        mail_subject = "AidData Data Extract Tool - Request "+request_id[:7]+".. Completed"

        mail_message = ("Your request has been completed. \n"
                        "The results can be accessed using the following link: \n"
                        "http://" + branch_info['server'] + "/DET/status/#" + request_id + "\n\n"
                        "You can also view all your current and previous requests using: \n"
                        "http://" + branch_info['server'] + "/DET/status/#" + mail_to + "\n\n")
        mail_status = queue.send_email(mail_to, mail_subject, mail_message)

        print "request completed"

    else:
        # set status 0 (no email)
        update_status = queue.update_status(request_id, 0)
        if not update_status[0]:
            warnings.warn("unable to update status of request (0)")
            continue
        print "request not ready"



    queue.update_status(request_id, -2)


print "\nFinished checking requests"
print time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())


