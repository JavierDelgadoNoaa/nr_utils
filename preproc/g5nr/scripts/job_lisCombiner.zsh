#!/usr/bin/env zsh
##
#PBS -N lisCombiner
#PBS -A aoml-osse
#PBS -d .
#PBS -j oe
#
#PBS -l nodes=2:ppn=4
#
#PBS -q  bigmem
##PBS -q  debug
#
#PBS -l walltime=08:00:00
##PBS -l walltime=1800
##

limit stacksize unlimited
source env.sh

DEFAULT_CONFIG_FILE="parallel.conf"
#DEFAULT_CONFIG_FILE="era.conf"
#DEFAULT_CONFIG_FILE="test.conf"
LOG_LEVEL="DEBUG"

if [[ ! -z $1 ]] ; then
    config_file=$1
else
    config_file=$DEFAULT_CONFIG_FILE
fi

mpirun -np $PBS_NP -l ./lis_input_combiner.py -c $config_file -l $LOG_LEVEL 
