from mpi4py import MPI
import subprocess as sp
import sys
import os

runscript = sys.argv[1]

extract_type = "ltdr"
# extract_type = "contemporary"
# extract_type = "historic"

comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()

if rank == 0:

	try:
		cmd = "Rscript "+runscript+" "+extract_type
		sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
		print sts

	except sp.CalledProcessError as sts_err:                                                                                                   
	    print ">> subprocess error code:", sts_err.returncode, '\n', sts_err.output


comm.Barrier()
