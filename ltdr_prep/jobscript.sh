#!/bin/tcsh
#PBS -N ad:sg-ltdr
#PBS -l nodes=1:c11:ppn=8
#PBS -l walltime=01:00:00
#PBS -j oe

cd $PBS_O_WORKDIR
mvp2run -m cyclic python-mpi ./runscript.py  ./ltdr_ndvi_prep.sh
