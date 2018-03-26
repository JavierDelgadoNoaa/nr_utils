#!/usr/bin/env zsh
##
#PBS -N nr_data_gen
#PBS -A aoml-osse
#PBS -d .
#PBS -j oe
#
# Note: createHGT uses lots of memory, so 1ppn max or "insufficient virtual mem"
#PBS -l nodes=20:ppn=3
###PBS -l nodes=16:ppn=1
###PBS -l nodes=4:ppn=1
#
#PBS -q  batch
##PBS -q  debug
#
#PBS -l walltime=08:00:00
##PBS -l walltime=1800
##

limit stacksize unlimited
source env.sh
source /home/Javier.Delgado/apps/pycane_dist/trunk/scripts/env.sh

#DEFAULT_CONFIG_FILE="parallel.conf"
DEFAULT_CONFIG_FILE="beta_pl_storm10.conf"
#DEFAULT_CONFIG_FILE="test.conf"
LOG_LEVEL="DEBUG"

if [[ ! -z $1 ]] ; then
    config_file=$1
else
    config_file=$DEFAULT_CONFIG_FILE
fi

mpirun -np $PBS_NP -l ./nr_input_generator.py -c $config_file -l $LOG_LEVEL 
