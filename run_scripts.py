from mpi4py import MPI
import subprocess as sp
import sys
import os

runscript = sys.argv[1]


comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()


# get years
path_base = "/sciclone/data20/aiddata/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"
# years = [x[x.rindex('/')+1:] for x,y,z in os.walk(base) if x[x.rindex("/")+1:] != self]


# for year in years:

year = '2010'


# get days for year
path_year = path_base + year

days =[ name for name in os.listdir(path_year) if os.path.isdir(os.path.join(path_year, name)) and name != year ]

# use limited days for testing 
# days = ['001','009','017','025','033','041','049','057']


if len(days) > size:

	jobs = len(days) # num of jobs to run
	even = jobs // size # min jobs each processor will run
	left = jobs % size # num of jobs to be split between some size

	# for each node assign jobs

	if rank < left:
		r = range( rank*even+rank, (rank+1)*even+rank+1 )
	else:
		r = range( rank*even+left, (rank+1)*even+left )


	for i in r:

		# print "("+str(rank)+") " + days[i] + "\n"

		day = days[i]
		cmd = runscript+" "+year+" "+days[i]
		sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)

		print sts

elif len(days) > rank:

	# print "("+str(rank)+") " + days[rank] + "\n"

	cmd = runscript+" "+year+" "+days[rank]
	sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)

	print sts


comm.Barrier()
