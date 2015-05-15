
from mpi4py import MPI
import subprocess as sp
import sys
import os


runscript = sys.argv[1]

comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()



# base path where year/day directories for processed data are located
path_base = "/sciclone/data20/aiddata/REU/data/ltdr.nascom.nasa.gov/allData/Ver4/ndvi"

# list of years to ignore
ignore = []

# list of all [year, day] combos
qlist = []

# get years
years = [name for name in os.listdir(path_base) if os.path.isdir(os.path.join(path_base, name)) and name not in ignore]

# use limited years for testing 
# years = ['2002']


for year in years:

	# get days for year
	path_year = path_base + year
	days = [name for name in os.listdir(path_year) if not os.path.isdir(os.path.join(path_year, name)) and name.endswith('.tif')]
			
	qlist += [[year, name.split(".")[1][5:], name[:-4]] for name in days]


c = rank
while c < len(qlist):

	try:
		cmd = "Rscript "+runscript+" "+qlist[c][0]+" "+qlist[c][1]
		sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
		print sts

	except sp.CalledProcessError as sts_err:                                                                                                   
	    print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output

	c += size


comm.Barrier()
