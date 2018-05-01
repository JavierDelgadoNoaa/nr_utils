#!/usr/bin/env zsh
#PBS -N g5nr_geo_800
#PBS -A aoml-osse
###PBS -l nodes=24:ppn=2
#PBS -l nodes=12:ppn=2
#PBS -l walltime=800
#PBS -q debug
#PBS -d .
#PBS -j oe

set -aeux

NPS=/home/Javier.Delgado/scratch/apps_tmp/nps/201611/dbg_test
#NPS=/home/Javier.Delgado/scratch/apps_tmp/nps/201611/dbg
#NPS=/home/Javier.Delgado/scratch/apps_tmp/nps/201611/semi_opt
NPS=/home/Javier.Delgado/scratch/apps_tmp/nps/201611/semi_opt_planA

GEOGRID=$NPS/geogrid.exe
#echo "jza running metgrid"
#mpirun -np $PBS_NP ./metgrid.exe
mpirun -np $PBS_NP $GEOGRID
#echo "jza running nemsinterp"
#mpirun -np $PBS_NP ./nemsinterp.exe

