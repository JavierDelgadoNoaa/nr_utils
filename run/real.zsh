#!/usr/bin/env zsh
#PBS -N g5nr_nemsInterp_5k
#PBS -A aoml-osse
#PBS -l nodes=32:ppn=2
###PBS -q debug
###PBS -l walltime=600
#PBS -d .
#PBS -j oe
##
#PBS -l walltime=08:00:00
#PBS -q batch

set -aeux

#NPS=/home/Javier.Delgado/scratch/apps_tmp/nps/201611/dbg_test
NPS=/home/Javier.Delgado/scratch/apps_tmp/nps/201611/semi_opt_planA_bin8

METGRID=$NPS/metgrid.exe
NEMSINTERP=$NPS/nemsinterp.exe

mpirun -np $PBS_NP $NEMSINTERP

