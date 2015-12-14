# generic runscript for sciclone extract jobs


from mpi4py import MPI
import sys
import os
import errno
import time

from numpy import isnan


# inputs from jobscript
# see jobscript comments for detailed descriptions of inputs

# temporal type option 
run_option = sys.argv[1]

# boundary name 
bnd_name = sys.argv[2]

# absolute path of boundary file
bnd_absolute = sys.argv[3]

# folder which contains data
data_base = sys.argv[4]

# path relative to data_base
data_path = sys.argv[5]

# dataset name
data_name = sys.argv[6]

# dataset mini_name
data_mini = sys.argv[7]

# file mask for dataset files
file_mask = sys.argv[8]

# extract type
extract_type = sys.argv[9]

# output folder
output_base = sys.argv[10]

# extract method
extract_method = sys.argv[11]


# ==================================================


# accepted vector and raster file extensions
vector_extensions = [".geojson", ".shp"]
raster_extensions = [".tif", ".asc"]


# base path where year/day directories for processed data are located
path_base = data_base + "/" + data_path 

# validate path_base
if not os.path.exists(path_base):
    sys.exit("path_base is not valid ("+ path_base +")")


# available extract types and associated identifiers
extract_options = {
    # "var": "v",
    # "std": "d",
    "sum": "s",
    "max": "x",
    # "min": "m",
    "mean": "e"
}

# validate input extract type
if extract_type not in extract_options.keys():
    sys.exit("invalid extract type ("+ extract_type +")")


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



# load packages and init for specified extract method
if extract_method == "rscript":
    
    import subprocess as sp

    e_vector = vector_info

elif extract_method == "rpy2":

    from rpy2.robjects.packages import importr
    from rpy2 import robjects

    # try loading rpy2 packages, open vector file and other init
    try:
        # load packages
        rlib_rgdal = importr("rgdal")
        rlib_raster = importr("raster")

        # open vector files
        r_vector = rlib_rgdal.readOGR(vector_info[0], vector_info[1])

        # list of valid extract types with r functions
        extract_funcs = {
            "sum": robjects.r.sum,
            "max": robjects.r.max,
            "mean": robjects.r.mean
        }

        e_vector = r_vector

    except:
        sys.exit("rpy2 initialization failed")

elif extract_method == "python":

    import rasterstats as rs

    e_vector = vector

else:
    sys.exit("invalid extract method (" + extract_method + ")")



# creates directories
def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


# run R extract script using subprocess call
def rscript_extract(vector, raster, output, extract_type):
    try:  

        # buildt command for Rscript
        cmd = "Rscript extract.R " + vector[0] +" "+ vector[1] +" "+ raster +" "+ output +" "+ extract_type
        print cmd

        # spawn new process for Rscript
        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

    except sp.CalledProcessError as sts_err:                                                                                                   
        print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output


# run extract using rpy2
def rpy2_extract(r_vector, raster, output, extract_type):

    try:
        # open raster
        r_raster = rlib_raster.raster(raster)

        # *** need to implement different kwargs based on extract type ***
        if extract_type == "mean":
            kwargs = {"fun":extract_funcs[extract_type], "sp":True, "na.rm":True, "weights":True, "small":True}
        else:
            kwargs = {"fun":extract_funcs[extract_type], "sp":True, "na.rm":True}

        Te_start = int(time.time())

        robjects.r.assign('r_extract', rlib_raster.extract(r_raster, r_vector, **kwargs))

        Te_run = int(time.time() - Te_start)
        print 'extract ('+ output +') completed in '+ str(Te_run) +' seconds'


        robjects.r.assign('r_output', output+".csv")

        robjects.r('colnames(r_extract@data)[length(colnames(r_extract@data))] <- "ad_extract"')
        robjects.r('write.table(r_extract@data, r_output, quote=T, row.names=F, sep=",")')
        
        return True, None

    except:
        return False, "R extract failed"


# run extract user rasterstats
def python_extract(vector, raster, output, extract_type):

    try:
        Te_start = int(time.time())

        stats = rs.zonal_stats(vector, raster, stats=extract_type, copy_properties=True, all_touched=True)

    except:
        print "error with python_extract: " + output

        if os.path.isfile(output+".csv"):
            os.remove(output+".csv")

        return False


    # try:
    for i in stats:
        i["ad_extract"] = i.pop(extract_type)
        try:
            if isnan(i["ad_extract"]):
                i["ad_extract"] = "NA"
        except:
            i["ad_extract"] = "NA"

    
    out = open(output+".csv", "w")
    out.write(rs.utils.stats_to_csv(stats))

    # except:
    #     if os.path.isfile(output+".csv"):
    #         os.remove(output+".csv")

    #     return False


    Te_run = int(time.time() - Te_start)
    print 'extract ('+ output +') completed in '+ str(Te_run) +' seconds'
    return True


# run proper extract based on extract method
def run_extract(in_vector, in_raster, in_output, in_extract_type, in_extract_method):
    
    print "running extract: " + in_output

    if in_extract_method == "rscript":
        rscript_extract(in_vector, in_raster, in_output, in_extract_type)

    elif in_extract_method == "rpy2":
        rpy2_extract(in_vector, in_raster, in_output, in_extract_type)

    elif in_extract_method == "python":
        python_extract(in_vector, in_raster, in_output, in_extract_type)


# ==================================================


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

c = rank


# different method for listing years to ignore/accept
# comment / uncomment "ignore = ..." lines as needed
# always use 4 digit integers to specify years

# specify ignore
ignore = []

# ignore range
ignore = range(1900, 1982)

# specify accept by using exceptions in ignore range 
# (manually adjust range if years fall outside of 1800-2100)
accept = []
# ignore = [i for i in range(1800, 2100) if i not in accept]


# convert years to strings
ignore = [str(e) for e in ignore]



output_dir =  output_base + "/" + bnd_name + "/cache/" + data_name +"/"+ extract_type 

if rank == 0:
    Ts = int(time.time())
    T_start = time.localtime()
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', T_start)
    make_dir(output_dir)


# temporally invariant dataset
if run_option == "1":

    # validate raster exists
    raster = path_base
    if not os.path.isfile(raster):
        sys.exit("raster does not exist (" + raster + ")")

    # check extension
    if not raster.endswith(tuple(raster_extensions)):
        sys.exit("invalid raster extension (" + raster + ")")

    # full path to output file (without file extension)
    # output = output_base + "/projects/" + bnd_name + "/extracts/" + data_name + "/extract"
    output = output_dir +"/"+ data_mini +"_"+ extract_options[extract_type]

    run_extract(e_vector, raster, output, extract_type, extract_method)

    # rscript_extract(vector, raster, output, extract_type)
    # rpy2_extract(r_vector, raster, output, extract_type)
    # python_extract(vector, raster, output, extract_type)


# temporal dataset
else:

    # year
    if run_option == "2":

        qlist = [[["".join([x for x,y in zip(name, file_mask) if y == 'Y' and x.isdigit()])], name] for name in os.listdir(path_base) if not os.path.isdir(os.path.join(path_base, name)) and name.endswith(tuple(raster_extensions)) and "".join([x for x,y in zip(name, file_mask) if y == 'Y' and x.isdigit()]) not in ignore]


    # year month
    elif run_option == "3":

        years = [name for name in os.listdir(path_base) if os.path.isdir(os.path.join(path_base, name)) and name not in ignore]

        # list of all [year, month, name] combos
        qlist = []

        for year in years:
            path_year = path_base +"/"+ year
            qlist += [[[year, "".join([x for x,y in zip(year+"/"+name, file_mask) if y == 'M' and x.isdigit()])], year+"/"+name] for name in os.listdir(path_year) if not os.path.isdir(os.path.join(path_year, name)) and name.endswith(tuple(raster_extensions))]


    # year day
    elif run_option == "4":

        year = str(sys.argv[11])

        # special ignore for year_day datasets
        if year in ignore:
            sys.exit("Ignoring year "+year)

        path_year = path_base +"/"+ year
        files = [name for name in os.listdir(path_year) if not os.path.isdir(os.path.join(path_year, name)) and name.endswith(tuple(raster_extensions))]

        # list of all [year, day, name] combos for year 
        qlist = [[[year, "".join([x for x,y in zip(year+"/"+name, file_mask) if y == 'D' and x.isdigit()])], year+"/"+name] for name in files]
        

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
        # output = output_base + "/extracts/" + bnd_name + "/cache/" + data_name +"/"+ extract_type + "/extract_" + '_'.join([str(e) for e in item[0]])
        output = output_dir + "/" + data_mini +"_"+ ''.join([str(e) for e in item[0]]) + extract_options[extract_type]

        run_extract(e_vector, raster, output, extract_type, extract_method)
        
        # rscript_extract(vector, raster, output, extract_type)
        # rpy2_extract(r_vector, raster, output, extract_type)
        # python_extract(vector, raster, output, extract_type)

        c += size


    comm.Barrier()


if rank == 0:
    T_run = int(time.time() - Ts)
    T_end = time.localtime()
    print '\n\n'
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', T_start)
    print 'End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end)
    print 'Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s'

