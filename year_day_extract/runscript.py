
from mpi4py import MPI
import subprocess as sp
import sys
import os


comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()

# =========================

runscript = sys.argv[1]
project_name = sys.argv[2]
shape_name = sys.argv[3]
data_path = sys.argv[4]
extract_name = sys.argv[5]
file_mask = sys.argv[6]
data_base = sys.argv[7]
project_base = sys.argv[8]
year = str(sys.argv[9])

# =========================

# ignore = ["1982"]

# if year in ignore:
    # sys.exit("Ignoring year "+year)


# base path where year/day directories for processed data are located
path_base = data_base + "/data/" + data_path

path_year = path_base +"/"+ year
files = [name for name in os.listdir(path_year) if not os.path.isdir(os.path.join(path_year, name)) and (name.endswith('.tif') or name.endswith('.asc'))]

# list of all [year, day, name] combos for year 
qlist = [[year, "".join([x for x,y in zip(name, file_mask) if y == 'D' and x.isdigit()]), name] for name in files]
qlist = sorted(qlist)


c = rank
while c < len(qlist):

    try:
        core = ["Rscript", runscript, qlist[c][0], qlist[c][1], qlist[c][2]]
        args = [project_name, shape_name, data_path, extract_name, data_base, project_base]
        cmd = " ".join(str(e) for e in core + args)

        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

    except sp.CalledProcessError as sts_err:                                                                                                   
        print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output

    c += size


comm.Barrier()
