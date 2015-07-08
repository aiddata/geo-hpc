from mpi4py import MPI
import subprocess as sp
import sys
import os


comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()


runscript = sys.argv[1]

# project name which corresponds to folder in /sciclone/aiddata10/REU/projects
project_name = sys.argv[2]

# name of shapefile without extension
shape_name = sys.argv[3]

# data_path relative to /sciclone/aiddata10/REU/data
# eg: v4avg_lihgts_x_pct or ltdr_yearly/ndvi_mean
data_path = sys.argv[4]

# output path relative to /sciclone/aiddata10/REU/projects/<project_name>/extracts
extract_name = sys.argv[5]

# column name to be used for extract data
# will be truncated in shapefile if over 10 characters
extract_field = sys.argv[6]

# year must be 4 digits and specified in file mask with "YYYY"
# other chars in mask do not matter as long as they are not "Y"
# file mask must be same length as file names
# eg: (for v4avg_lights_x_pct)  F1xYYYY.v4x.avg_lights_x_pct.tif
file_mask = sys.argv[7]

# validate file mask
if file_mask.count("Y") != 4 or not "YYYY" in file_mask:
    sys.exit("invalid file mask")


# base path where year/day directories for processed data are located
path_base = "/sciclone/aiddata10/REU/data/" + data_path

# validate path_base
if not os.path.isdir(path_base):
    sys.exit("path_base is not valid ("+ path_base +")")


# list of years to ignore
ignore = []


# list of all [year, file] combos
qlist = [["".join([x for x,y in zip(name, file_mask) if y == 'Y' and x.isdigit()]), name] for name in os.listdir(path_base) if not os.path.isdir(os.path.join(path_base, name)) and name.endswith(".tif") and "".join([x for x,y in zip(name, file_mask) if y == 'Y' and x.isdigit()]) not in ignore]
qlist = sorted(qlist)


c = rank
while c < len(qlist):

	try:
		cmd = "Rscript "+runscript+" "+qlist[c][0]+" "+qlist[c][1]+" "+project_name+" "+shape_name+" "+data_path+" "+extract_name+" "+extract_field
		sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
		print sts

	except sp.CalledProcessError as sts_err:                                                                                                   
	    print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output

	c += size


comm.Barrier()
