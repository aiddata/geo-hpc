
import sys
import os
import errno
import time
import json

import pymongo
from bson.objectid import ObjectId

import subprocess as sp


# connect to mongodb
client = pymongo.MongoClient()

c_asdf = client.asdf.data
c_msr = client.det.msr


# input flag to ignore whether an existing job has finished
force = 0
if len(sys.argv) > 1:
	force = sys.argv[1]



# creates directories
def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def get_next(status, limit):
    
    request_objects = {}

    try:
        # find all status 1 jobs and sort by priority then submit_time
        sort = self.c_msr.find({"status":status}).sort([("priority", -1), ("submit_time", 1)])

        if sort.count() > 0:

            if limit == 0 or limit > sort.count():
                limit = sort.count()

            for i in range(limit):
                rid = str(sort[i]["_id"])
                request_objects[rid] = sort[i]

            return 1, request_objects

        else:
            return 1, None

    except:
        return 0, None


# update status of request
def update_status(rid, status):
    
    ctime = int(time.time())

    updates = {
        "status": long(status),
        "update_time": ctime
    }
    
    # try:
        # # update request document
    c_msr.update({"_id": ObjectId(rid)}, {"$set": updates})
        # return True, ctime

    # except:
        # return False, None


# prepare options for request
def build_json(active_base, request):
	print "build_json"
	
	try:
		# find path to release root
		release_path = c_asdf.find('something')

		# request path
		json_path = active_base +'/request.json'
		 
		# add release and request path to request object
		request['release_path'] = release_path 
		request['request_path'] = json_path
		json_output = json.dumps(request)

		# write json
		json_file = open(json_path, 'w')
		json_file.write(json_output)

		# return json path
		return 0, request

	except:
		return 1, None


# build jobscript text based on options
def build_jobscript(active_base, request):
	print "build_jobscript"

	try:
		jobscript_output = ''

		jobscript_output += '#!/bin/tcsh' + '\n'
		jobscript_output += '#PBS -N ad:det-msr:' + request['hash'][0:7] + '\n'
		jobscript_output += '#PBS -l nodes=6:vortex:compute:ppn=12' + '\n'
		jobscript_output += '#PBS -l walltime=180:00:00' + '\n'
		jobscript_output += '#PBS -j oe' + '\n'

		jobscript_output += 'set request_path = ' + request['request_path'] + '\n'
		jobscript_output += 'set resolution = ' + request['resolution'] + '\n'

		jobscript_output += 'cd $PBS_O_WORKDIR' + '\n'
		jobscript_output += 'mvp2run -m cyclic python-mpi /sciclone/aiddata10/REU/msr/scripts/runscript.py "$request_path" "$resolution" ' + '\n\n'

		jobscript_path = active_base +'/jobscript'
		jobscript_file = open(jobscript_path, 'w')
		jobscript_file.write(jobscript_output)

		return 0, jobscript_path

	except:
		return 1, None


# submit jobscript with qsub
def submit_job(something):
	print "submit_job"

	try: 
		# set/get working directory
		# ???
		
		cmd = "ssh sgoodman@hurricane.sciclone.wm.edu 'cd " + something + "; qsub " + something + "'"
		print cmd

        # run command
        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

        return 0, 'job id number???'

    except sp.CalledProcessError as sts_err:                                                                                                   
        print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output

		return 1, None






def run_core():
	print "run_core"

	gn_status, gn_item = get_next(0, 1)

	if not gn_status:
	    sys.exit("Error while searching for next request in extract queue")
	elif gn_item == None:
	    sys.exit("MSR tracker is empty")

	rid = gn_item[0]
	request = gn_item[1]

	# make active dir
	working_dir =  '/sciclone/aiddata10/REU/msr/active/' + request['dataset'] +'_'+ request['hash']
	make_dir(working_dir)


	# prepare options for json
	json_status, tmp_request = build_json(working_dir, request)

	if json_status == 0:
		# build request jobscript
		jobscript_status, jobscript_path = build_jobscript(working_dir, tmp_request)

		if jobscript_status == 0:
			# submit job
			submit_job(jobscript_path)
		else:
			# add specific error status to job request in msr tracker
			# 

	else:
		# add specific error status to job request in msr tracker
		# 




# search for active job(s) in msr_tracker (should only be 1 at a time normally, but could potentially be multiple due to force flag)
# active_count, active_jobs = 
active_check_status, active_jobs = get_next(2, 0)
active_count = 0
if not active_check_status:
    sys.exit("Error while searching for next request in extract queue")
elif active_jobs != None:
	active_count = len(active_jobs.keys())


if force or active_count == 0:
	run_core()

else:
	for job in active_jobs.keys():
		
		# check if there is an output for active job (location based on some deterministic path using hash or something)
		oe_status, oe_output = 

		if oe_status != 0:
		    sys.exit("Error while searching for oe_output")


		if oe_output == None: 
				# redundancy check - see if there is any output from qstat command
				qstat_status, qstat_text = 
				if qstat_status != 0:
				    sys.exit("Error while searching for qstat text")


				if qstat_text != '':
					# job is running so exit script
					#

				else
					# set request in tracker to specific error status
					#

					# run next
					run_core()

		else:
			# check status in output file (done vs error vs other?)
			oe_output_status = oe_output['status']
			
			if oe_output_status < 0:
				# move jobscript and output to error folder 
				#
				
				# (maybe send me some notification or update log?)
				#

			elif oe_output_status == 1:
				# move jobscript and output to completed folder (different from folder for actual job outputs) 
				#
			
			# get next
			run_core()

		


