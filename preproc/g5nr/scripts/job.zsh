#!/usr/bin/env zsh
##
#PBS -N regional_nr_data_gen
#PBS -A aoml-osse
#PBS -q service
#PBS -l procs=2
###PBS -l procs=1
###PBS -l vmem=30GB
###PBS -q service
##PBS -q  debug
#PBS -q  batch
#PBS -l walltime=03:00:00
###PBS -l walltime=1800
#PBS -d .
#PBS -j oe
##

limit stacksize unlimited
source env.sh
source /home/Javier.Delgado/apps/pycane_dist/trunk/scripts/env.sh

DEFAULT_CONFIG_FILE="beta_pl.conf"
#DEFAULT_CONFIG_FILE="era.conf"
#DEFAULT_CONFIG_FILE="test.conf"
LOG_LEVEL="DEBUG"

if [[ ! -z $1 ]] ; then
    config_file=$1
else
    config_file=$DEFAULT_CONFIG_FILE
fi

./nr_input_generator.py -c $config_file -l $LOG_LEVEL 
