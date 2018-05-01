#!/usr/bin/env zsh
#PBS -N g5nr_met_5k
#PBS -A aoml-osse
#PBS -l nodes=12:ppn=2
#PBS -l walltime=08:00:00
###PBS -l walltime=00:30:00
###PBS -q debug
#PBS -d .
#PBS -j oe

set -aeux

#NPS=/home/Javier.Delgado/scratch/apps_tmp/nps/201611/dbg_test
NPS=/home/Javier.Delgado/scratch/apps_tmp/nps/201611/semi_opt_planA

METGRID=$NPS/metgrid.exe

# Get timestamp of last met file to be (partially) written
# and update start_date so Metgrid begins where it left off
last_met_file=`ls -tr met_nmb* | tail -n 1`
if [[ ! -z $last_met_file ]] ; then
    #met_nmb.d01.2006-09-12_00:00:00.dio
    yyyy=$last_met_file[13,16]
    mm=$last_met_file[18,19]
    dd=$last_met_file[21,22]
    HH=$last_met_file[24,25]
    MM=$last_met_file[27,28]
    # Change start_date
    sed -i  "s#start_date.*#start_date\ =\ ${yyyy}-${mm}-${dd}_${HH}:${MM}:00#"  namelist.nps
    # assume job was killed before it could finish writing the last file completely
    rm $last_met_file 
fi

mpirun -np $PBS_NP $METGRID

