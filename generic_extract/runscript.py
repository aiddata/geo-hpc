# generic runscript for sciclone extract jobs

from mpi4py import MPI
import subprocess as sp
import sys
import os

from rpy2.robjects.packages import importr
from rpy2 import robjects


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

# accepted vector and raster file extensions
vector_extensions = [".geojson", ".shp"]
raster_extensions = [".tif", ".asc"]


# base path where year/day directories for processed data are located
path_base = data_base + "/" + data_path 

# validate path_base
if not os.path.isfile(path_base):
    sys.exit("path_base is not valid ("+ path_base +")")


# validate vector exists
vector = bnd_absolute

if not os.path.isfile(vector):
    sys.exit("vector does not exist (" + vector + ")")

# check extension
if not vector.endswith(tuple(vector_extensions)):
    sys.exit("invalid vector extension (" + vector + ")")

vector_dirname = os.path.dirname(vector)
vector_filename, vector_extension = os.path.splitext(os.path.basename(vector))

# break vector down into path and layer
# different for shapefiles and geojsons
if vector_extension == ".geojson":
    vector_info = (vector, "OGRGeoJSON")

elif vector_extension == ".shp":
    vector_info = (vector_dirname, vector_filename)

else:
    sys.exit("invalid vector extension (" + vector_extension + ")")

# try loading r packages and vector file
try:
    rlib_rgdal = importr("rgdal")
    rlib_raster = importr("raster")

    r_vector = rlib_rgdal.readOGR(vector_info[0], vector_info[1])

except:
    sys.exit("rpy2 initialization failed")


# list of valid extract types with r functions
extract_funcs = {
    "mean":robjects.r.mean
}

# validate input extract type
if extract_type not in extract_funcs.keys():
    sys.exit("invalid extract type")


# run R extract script using subprocess call
def script_extract(vector, raster, output, extract_type):
    try:  

        cmd = "Rscript extract.R " + vector +" "+ raster +" "+ output +" "+ extract_type
        print cmd

        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

    except sp.CalledProcessError as sts_err:                                                                                                   
        print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output


# run extract using rpy2
def rpy2_extract(r_vector, raster, output, extract_type):

    try:
        r_raster = rlib_raster.raster(raster)

        # *** need to implement different kwargs based on extract type ***
        kwargs = {"fun":extract_funcs[extract_type], "sp":True, "weights":True, "small":True, "na.rm":True}

        robjects.r.assign('r_extract', rlib_raster.extract(r_raster, r_vector, **kwargs))

        robjects.r.assign('r_output', output)

        robjects.r('colnames(r_extract@data)[length(colnames(r_extract@data))] <- "ad_extract"')
        robjects.r('write.table(r_extract@data, r_output, quote=T, row.names=F, sep=",")')
        
        return True, None

    except:
        return False, "R extract failed"


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
    if not raster.endswith(tuple(raster_extensions)):
        sys.exit("invalid raster extension (" + raster + ")")

    # full path to output file (without file extension)
    # output = output_base + "/projects/" + bnd_name + "/extracts/" + data_name + "/extract"
    output = output_base + "/extracts/" + bnd_name + "/cache/" + data_name +"/"+ extract_type + "/extract"

   # run_extract(vector, raster, output, extract_type)
   run_extract(r_vector, raster, output, extract_type)


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

        # run_extract(vector, raster, output, extract_type)
        run_extract(r_vector, raster, output, extract_type)

        c += size

