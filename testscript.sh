#!/bin/tcsh
#PBS -N ad:sg-etest
#PBS -l nodes=1:c11:ppn=8
#PBS -l walltime=00:10:00
#PBS -j oe

cd $PBS_O_WORKDIR
mvp2run -m cyclic python-mpi ./runscript_test.py
