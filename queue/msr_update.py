
import sys
import os
import json

import pymongo
import subprocess as sp



# input flag to ignore whether an existing job has finished
force = 0
if len(sys.argv) > 1:
	force = sys.argv[1]


# search for active job(s) in msr_tracker (should only be 1 at a time normally, but could potentially be multiple due to force flag)
# active_count, active_jobs = 

# if force or active_count == 0:
	# core()

# else:
	# for job in active_jobs:
		
		# check if there is an output for active job (location based on some deterministic path using hash or something)
		# 

		# if no output 
				# redundancy check - see if there is any output from qstat command
				# 
				# if qstat has text
					# job is running so exit script
					#
				# else
					# set request in tracker to specific error status
					#
					# run next
					# core()

		# else:
			# check status in output file (done vs error vs other?)
			#
			
			# if error status
				# move jobscript and output to error folder 
				#
				# (maybe send me some notification or update log?)
				#

			# elif if done status
				# move jobscript and output to completed folder (different from folder for actual job outputs) 
				#
			
			# get next
			# core()



# core:
	# get_next()
	# prepare options for json
		# add path to root of release for specified dataset to request json
	# build request jobscript
	# submit job


def core():
	print "core"
	job = get_next()
	json_status, json_path = build_json(job)
	if json_status == 0:
		jobscript_status = build_jobscript(json_path)

	else:
		# add specific error status to job request in msr tracker
		# 


# get next in queue from msr tracker (sort by priority and submit time)
def get_next():
	print "get_next"

	# request_object = mongo stuff

	# return request_object


# prepare options for request
def build_json(request_object):
	print "build_json"

	# find path to release root
	# 

	# finalize object
	# 

	# json output path = 

	# write json
	# 

	# return json output path


# build jobscript text based on options
def build_jobscript(path):
	print "build_jobscript"

	try:
		text = '#!/bin/tcsh'
		text = '#PBS -N ad:det-msr'
		text = '#PBS -l nodes=6:vortex:compute:ppn=12'
		text = '#PBS -l walltime=180:00:00'
		text = '#PBS -j oe'

		text = 'set path = ' + ''
		text = 'set resolution = ' + ''

		text = 'cd $PBS_O_WORKDIR'
		text = 'mvp2run -m cyclic python-mpi ./runscript.py "$path" "$resolution" '

		output = open(path to jobscript, 'w')
		output.write(text)

		return 0

	except:
		return 1


# submit jobscript with qsub
def submit_job():
	print "submit_job"

	try: 
		# set working directory
		# 
		
		cmd = "ssh sgoodman@hurricane.sciclone.wm.edu 'qsub'"
		print cmd

        # run command
        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

        return 0

    except sp.CalledProcessError as sts_err:                                                                                                   
        print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output

		return 1


