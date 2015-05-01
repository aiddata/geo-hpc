# 
# runscript.py
# 


# ====================================================================================================

from __future__ import print_function

from mpi4py import MPI

import os
import sys
import errno
from copy import deepcopy
import time
import random
import numpy as np


# ====================================================================================================
# init


# Ts = time.time()


iterations = 12
i_control = range(int(iterations))


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
status = MPI.Status()


# if rank == 0:
# 	print("starting iterations (%d) to be run)" % iterations)
# 	total_aid = [] # [0] * iterations
# 	total_count = [] # [0] * iterations


# comm.Barrier()

def enum(*sequential, **named):
    """Handy way to fake an enumerated type in Python
    http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

# Define MPI message tags
tags = enum('READY', 'DONE', 'EXIT', 'START')

if rank == 0:
	# Master process executes code below

	print("starting iterations (%d) to be run)" % iterations)
	total_aid = [] # [0] * iterations
	total_count = [] # [0] * iterations

	task_index = 0
	num_workers = size - 1
	closed_workers = 0
	print("Master starting with %d workers" % num_workers)
	while closed_workers < num_workers:
		data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
		source = status.Get_source()
		tag = status.Get_tag()

		if tag == tags.READY:
			# Worker is ready, so send it a task
			if task_index < len(i_control):
				comm.send(i_control[task_index], dest=source, tag=tags.START)
				print("Sending task %d to worker %d" % (task_index, source))
				task_index += 1
			else:
				comm.send(None, dest=source, tag=tags.EXIT)

		elif tag == tags.DONE:
			total_aid.append(data[0])
			total_count.append(data[1])
			print("Got data from worker %d" % source)

		elif tag == tags.EXIT:
			print("Worker %d exited." % source)
			closed_workers += 1

	print("Master calcing")
	stack_aid = np.vstack(total_aid)
	mean_aid = np.mean(stack_aid, axis=0)
	print(mean_aid)

	# write asc files
	# std_aid_str = ' '.join(np.char.mod('%f', std_aid))
	# print(std_aid_str)
	# fout_std_aid = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_cx"+str(itx)+"_std_aid.asc", "w")
	# fout_std_aid.write(asc_std_aid_str)

	print("Master finishing")

else:
	# Worker processes execute code below
	name = MPI.Get_processor_name()
	print("I am a worker with rank %d on %s." % (rank, name))
	while True:
		comm.send(None, dest=0, tag=tags.READY)
		task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
		tag = status.Get_tag()

		if tag == tags.START:
			# Do the work here
			b1 = np.array([500,600,700,800,900])
			b2 = np.array([1,2,3,4,5])
			result = np.array([b1,b2])
			comm.send(result, dest=0, tag=tags.DONE)

		elif tag == tags.EXIT:
			break

	comm.send(None, dest=0, tag=tags.EXIT)






# # distribute iterations to processes
# # for itx in i_control:

# if rank != 0:
# 	# start individual process
# 	itx = rank
# 	idx = 20
# 	print "iter "+str(itx)+": starting"

# 	# initialize mean and count grids with zeros
# 	npa_aid = np.zeros((int(idx+1),), dtype=np.int)
# 	npa_count = np.zeros((int(idx+1),), dtype=np.int)


# 	# send np array back to master

# 	# master adds np array to total arrays
# 	total_aid.append(npa_aid)
# 	total_count.append(npa_count)



# comm.Barrier()



# if rank == 0:
# 	stack_aid = np.vstack(total_aid)
# 	std_aid = np.std(stack_aid, axis=0)

# 	# write asc files
# 	std_aid_str = ' '.join(np.char.mod('%f', std_aid))
# 	asc_std_aid_str = asc + std_aid_str
# 	fout_std_aid = open(dir_working+"/"+country+"_output_"+str(pixel_size)+"_cx"+str(itx)+"_std_aid.asc", "w")
# 	fout_std_aid.write(asc_std_aid_str)


# ======================================================================================================
# clean up and close


# Tloc = int(time.time() - Ts)
# T_iter_avg = Tloc/iterations

# print '\n\tRun Results:'
# print '\t\tAverage Iteration Time: ' + str(T_iter_avg//60) +'m '+ str(int(T_iter_avg%60)) +'s'
# print '\t\tTotal Runtime: ' + str(Tloc//60) +'m '+ str(int(Tloc%60)) +'s'
# print '\n'
