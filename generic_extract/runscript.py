# generic runscript for sciclone extract jobs

from mpi4py import MPI
import subprocess as sp
import sys
import os


run_option = sys.argv[1]

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


# ==================================================


# base path where year/day directories for processed data are located
path_base = data_base + "/" + data_path 

# validate path_base
if not os.path.isfile(path_base):
    sys.exit("path_base is not valid ("+ path_base +")")

# validate vector exists
vector = bnd_absolute
if not os.path.isfile(vector):
    sys.exit("vector does not exist (" + vector + ")")

# accepted raster file extensions
extensions = [".tif", ".asc"]


# run R extract script using subprocess call
def run_extract(vector, raster, output, extract_type):
    try:  

        cmd = "Rscript extract.R " + vector +" "+ raster +" "+ output +" "+ extract_type
        print cmd

        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

    except sp.CalledProcessError as sts_err:                                                                                                   
        print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output

    
# ==================================================


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

c = rank


# different method for listing years to ignore/accept
# comment / uncomment as needed

# specify ignore
ignore = []

# ignore range
# ignore = [str(e) for e in range(1900, 1982)]

# specify accept by using exceptions in ignore range
# accept = []
# ignore = [str(e) for e in range(1800, 2100) if str(e) not in accept]


# temporally invariant dataset
if run_option == 1:

    # validate raster exists
    raster = data_base + "/" + data_path
    if not os.path.isfile(raster):
        sys.exit("raster does not exist (" + raster + ")")

    # check extension
    if not raster.endswith(tuple(extensions)):
        sys.exit("invalid extension (" + raster + ")")

    # full path to output file (without file extension)
    # output = output_base + "/projects/" + bnd_name + "/extracts/" + data_name + "/extract"
    output = output_base + "/extracts/" + bnd_name + "/cache/" + data_name +"/"+ extract_type + "/extract"

   run_extract(vector, raster, output, extract_type)


# temporal dataset
else:


    # year
    if run_option == 2:

        qlist = [[["".join([x for x,y in zip(name, file_mask) if y == 'Y' and x.isdigit()])], name] for name in os.listdir(path_base) if not os.path.isdir(os.path.join(path_base, name)) and name.endswith(tuple(extensions)) and "".join([x for x,y in zip(name, file_mask) if y == 'Y' and x.isdigit()]) not in ignore]


    # year month
    elif run_option == 3:

        years = [name for name in os.listdir(path_base) if os.path.isdir(os.path.join(path_base, name)) and name not in ignore]

        # list of all [year, month, name] combos
        qlist = []

        for year in years:
            path_year = path_base +"/"+ year
            qlist += [[[year, "".join([x for x,y in zip(name, file_mask) if y == 'M' and x.isdigit()])], name] for name in os.listdir(path_year) if not os.path.isdir(os.path.join(path_year, name)) and name.endswith(tuple(extensions))]


    # year day
    elif run_option == 4:

        year = str(sys.argv[10])

        # special ignore for year_day datasets
        if year in ignore:
            sys.exit("Ignoring year "+year)

        path_year = path_base +"/"+ year
        files = [name for name in os.listdir(path_year) if not os.path.isdir(os.path.join(path_year, name)) and name.endswith(tuple(extensions))]

        # list of all [year, day, name] combos for year 
        qlist = [[[year, "".join([x for x,y in zip(name, file_mask) if y == 'D' and x.isdigit()])], name] for name in files]
        

    else:
        sys.exit("Invalid run_option value: " + str(run_option))


    # sort qlist
    qlist = sorted(qlist)

    # iterate over qlist
    # generate raster and output 
    # run extract
    while c < len(qlist):

        item = qlist[c]

        raster = data_base +"/"+ data_path +"/"+ item[1]
        output = output_base + "/extracts/" + bnd_name + "/cache/" + data_name +"/"+ extract_type + "/extract_" + '_'.join([str(e) for e in item[0]])

        run_extract(vector, raster, output, extract_type)

        c += size

