
from mpi4py import MPI
import subprocess as sp
import sys
import os


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


runscript = sys.argv[1]

# boundary name which will be used to identify/generate folder in output path
# if boundary file exists in asdf, bnd_name should match asdf name
bnd_name = sys.argv[2]

# absolute path of boundary file
# includes file with extension
bnd_absolute = sys.argv[3]

# path to data folder parent
data_base = sys.argv[4]

# data_path relative to /sciclone/aiddata10/REU/data
# eg: v4avg_lihgts_x_pct or ltdr_yearly/ndvi_mean
data_path = sys.argv[5]

# dataset name
# if dataset exists in asdf, data_name should match asdf name
data_name = sys.argv[6]

# year must be 4 digits and specified in file mask with "YYYY"
# other chars in mask do not matter as long as they are not "Y"
# file mask must be same length as file names
# eg: (for v4avg_lights_x_pct)  F1xYYYY.v4x.avg_lights_x_pct.tif
file_mask = sys.argv[7]

# extract type (mean, max, etc.)
extract_type = sys.argv[8]

# parent folder for outputs
# bnd_name folder should exist or will be created in this folder
output_base = sys.argv[9]


# base path where year/day directories for processed data are located
path_base = data_base + "/" + data_path 

# validate path_base
if not os.path.isfile(path_base):
    sys.exit("path_base is not valid ("+ path_base +")")

# validate vector exists
vector = bnd_absolute
if not os.path.isfile(vector):
    sys.exit("vector does not exist (" + vector + ")")
    
# ==================================================


# validate raster exists
raster = data_base + "/" + data_path
if not os.path.isfile(raster):
    sys.exit("raster does not exist (" + raster + ")")


# full path to output file (without file extension)
# output = output_base + "/projects/" + bnd_name + "/extracts/" + data_name + "/extract"
output = output_base + "/extracts/" + bnd_name + "/cache/" + data_name +"/"+ extract_type + "/extract"

try:  

    cmd = "Rscript extract.R " + vector +" "+ raster +" "+ output +" "+ extract_type
    print cmd

    sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
    print sts

except sp.CalledProcessError as sts_err:                                                                                                   
    print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output

