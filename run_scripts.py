from mpi4py import MPI
import subprocess as sp
import sys

runscript = sys.argv[1]

years = {
	"start":2000,
	"end":2003
}

comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()


year = '2002'
days = ['001','009','017','025','033','041','049','057']

cmd = runscript+" "+year+" "+days[rank]
sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)

print sts

comm.Barrier()

