from mpi4py import MPI
import subprocess as sp
import sys
import os

runscript = sys.argv[1]


comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()

# base path where year/day directories for processed data are located
path_base = "/sciclone/data20/aiddata/REU/data/historic_gimms_ndvi/"

# list of years to ignore
ignore = []


# list of all [year, day] combos
qlist = []


# get years
years = [name for name in os.listdir(path_base) if os.path.isdir(os.path.join(path_base, name)) and name not in ignore]

# use limited years for testing 
# years = ['1990']


for year in years:

	# get days for year
	path_year = path_base + year
	days = [name for name in os.listdir(path_year) if os.path.isdir(os.path.join(path_year, name))]

	# use limited days for testing 
	# days = ['001','009']
	# days = ['001','009','017','025']

	# days = ['001','009','017','025','033','041']
	# days = ['001','009','017','025','033','041','049','057']
	# days = ['065','073','081','089','097','105','113','121']
	# days = ['001','009','017','025','033','041','049','057','065','073','081','089','097','105','113','121']

	# days = ['01','02','03','04','05','06','07','08']

	for day in days:

		path_day = path_year + "/" + day

		flist = [name for name in os.listdir(path_day) if not os.path.isdir(os.path.join(path_day, name)) and name.endswith('.tif')]

		qlist += [[year,day,name] for name in flist if len(flist) == 1]


c = rank
while c < len(qlist):

	try:
		cmd = "Rscript "+runscript+" "+qlist[c][0]+" "+qlist[c][1]+" "+qlist[c][2]
		sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
		print sts

	except sp.CalledProcessError as sts_err:                                                                                                   
	    print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output

	c += size


comm.Barrier()
