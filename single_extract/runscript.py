
# from mpi4py import MPI
import subprocess as sp
import sys
import os


# comm = MPI.COMM_WORLD
# size = comm.Get_size()
# rank = comm.Get_rank()


runscript = sys.argv[1]

# project name which corresponds to folder in /sciclone/aiddata10/REU/projects
project_name = sys.argv[2]

# path of vector file relative to projects/<project_name>/shps folder 
# includes file with extension
shape_name = sys.argv[3]

# data_path relative to /sciclone/aiddata10/REU/data
# eg: v4avg_lihgts_x_pct or ltdr_yearly/ndvi_mean
data_path = sys.argv[4]

# output path relative to /sciclone/aiddata10/REU/projects/<project_name>/extracts
extract_name = sys.argv[5]

# year must be 4 digits and specified in file mask with "YYYY"
# other chars in mask do not matter as long as they are not "Y"
# file mask must be same length as file names
# eg: (for v4avg_lights_x_pct)  F1xYYYY.v4x.avg_lights_x_pct.tif
file_mask = sys.argv[6]

# path to data folder parent
data_base = sys.argv[7]

# path to project folder parent
project_base = sys.argv[8]


# base path where year/day directories for processed data are located
path_base = data_base + "/data/" + data_path 

# validate path_base
if not os.path.isfile(path_base):
    sys.exit("path_base is not valid ("+ path_base +")")

# ==================================================

# validate raster exists
raster = data_base + "/data/" + data_path
if not os.path.isfile(raster):
    sys.exit("raster does not exist (" + raster + ")")


# validate vector (and thus project) exists
vector = project_base + "/projects/" + project_name + "/shps/" + shape_name
if not os.path.isfile(vector):
    sys.exit("vector does not exist (" + vector + ")")


output = project_base + "/projects/" + project_name + "/extracts/" + extract_name + "/extract"

try:  
    # core = ["Rscript", runscript ]
    # args = [project_name, shape_name, data_path, extract_name, data_base, project_base]
    # cmd = " ".join(str(e) for e in core + args)

    cmd = "Rscript extract.R " + vector +" "+ raster +" "+ output
    print cmd

    sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
    print sts

except sp.CalledProcessError as sts_err:                                                                                                   
    print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output

