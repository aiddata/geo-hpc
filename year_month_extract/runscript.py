
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


# base path where year directories (or actual data) for processed data are located
path_base = data_base + "/data/" + data_path

# validate path_base
if not os.path.isdir(path_base):
    sys.exit("path_base is not valid ("+ path_base +")")

# ==================================================

# validate file mask
if (file_mask.count("Y") != 4 or not "YYYY" in file_mask) or (file_mask.count("M") != 2 or not "MM" in file_mask):
    sys.exit("invalid file mask")


vector = project_base + "/projects/" + project_name + "/shps/" + shape_name

# check that vector (and thus project) exist
if not os.path.isfile(vector):
    sys.exit("vector does not exist (" + vector + ")")


# list of years to ignore/accept
# list of all years

ignore = [str(e) for e in range(1900, 1982)]
years = [name for name in os.listdir(path_base) if os.path.isdir(os.path.join(path_base, name)) and name not in ignore]

# accept = ["1990"]
# years = [name for name in os.listdir(path_base) if os.path.isdir(os.path.join(path_base, name)) and name in accept]



# list of all [year, month, name] combos
qlist = []

for year in years:

    path_year = path_base +"/"+ year
    qlist += [[year, "".join([x for x,y in zip(name, file_mask) if y == 'M' and x.isdigit()]), name] for name in os.listdir(path_year) if not os.path.isdir(os.path.join(path_year, name)) and (name.endswith(".tif") or name.endswith(".asc"))]

qlist = sorted(qlist)


c = rank
while c < len(qlist):

    raster = data_base + "/data/" + data_path + "/" + qlist[c][2]
    output= project_base + "/projects/" + project_name + "/extracts/" + extract_name + "/output/" + qlist[c][0] +"/"+ qlist[c][1] + "/extract_" + qlist[c][0] +"_"+ qlist[c][1] 
       
    try:
        cmd = "Rscript extract.R " + vector +" "+ raster +" "+ output
        print cmd

        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

    except sp.CalledProcessError as sts_err:                                                                                                   
        print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output

    c += size


comm.Barrier()


import pandas as pd
from copy import deepcopy

merge = 0
if rank == 0:

    
    # if len(qlist) > 0:

    #     for item in qlist:
    #         year = item[0]
    #         month = item[1]


    output_base = project_base + "/projects/" + project_name + "/extracts/" + extract_name +"/output"
    rlist = [[year, month] for year in os.listdir(output_base) for month in os.listdir(output_base +"/"+ year)]

    if len(rlist) > 0:

        for item in rlist:

            year = item[0]
            month = item[1]

            result_csv = output_base +"/"+ year +"/"+ month + "/extract_" + year +"_"+ month + ".csv"

            if os.path.isfile(result_csv):

                result_df = pd.read_csv(result_csv, quotechar='\"', na_values='', keep_default_na=False)

                if not isinstance(merge, pd.DataFrame):
                    merge = deepcopy(result_df)
                    merge.rename(columns={"ad_extract": "ad_"+year+"_"+month}, inplace=True)

                else:
                    merge["ad_"+year+"_"+month] = result_df["ad_extract"]


        merge_output = project_base + "/projects/" + project_name + "/extracts/" + extract_name + "/extract_merge.csv"
        merge.to_csv(merge_output, index=False)

