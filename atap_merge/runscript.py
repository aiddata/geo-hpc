from mpi4py import MPI
import subprocess as sp
import sys
import os

runscript = sys.argv[1]

# --------------------------------------------------

# project name (must match folder in /sciclone/aiddata10/REU/projects)
project_name = sys.argv[2] # "kfw"

extract_type = sys.argv[3] # "terrestrial_air_temperature"
# extract_type = "terrestrial_precipitation"

# --------------------------------------------------

comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()

if rank == 0:

    try:
        cmd = "Rscript "+runscript+" "+extract_type+" "+project_name
        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

    except sp.CalledProcessError as sts_err:                                                                                                   
        print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output


comm.Barrier()
