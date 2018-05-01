#!/usr/bin/env zsh 
#PBS -N bsnr_fcst_5k
#PBS -A aoml-osse
#PBS -l partition=theia
#PBS -l nodes=150:ppn=24+120:ppn=1 # 60*60
#PBS -q novel
#PBS -d .
#PBS -j oe
#PBS -l walltime=05:40:00

source ~Javier.Delgado/.zsh.d/theia.zsh
#module load intel/15.1.133 #mvapich2/2.1rc1 #netcdf #jdelgado
#module load impi
#module load netcdf/3.6.3
#module load esmf

module purge
source ~Javier.Delgado/scratch/apps_tmp/nems/leg/201611/dbg/NEMS/src/conf/modules.nems 
module load impi/5.1.2.150
module list


##
# TAU OPTIONS
##
#https://www.alcf.anl.gov/user-guides/tuning-and-analysis-utilities-tau
# Turn on tracing - currently failing - maybe because not all src files were compiled w pdt
##export TAU_TRACE=1
# Callpath
export TAU_CALLPATH=1
export TAU_CALLPATH_DEPTH=7
#export TAU_TRACK_HEAP=1 # measure heap on func entry and exit
#export TAU_TRACK_MESSAGE=1 # track message sizes
#export TAU_COMM_MATRIX=1 # collect details about point2point comms
#export TAU_THROTTLE=1 # turn off instrumentation for routines called very often
#export TAU_VERBOSE=1

set -aexu

ls -l NEMS.x*

CONFIG_FILE=configure_file_01 # will check inside FCST 
FCST_DIR=`pwd`

# If we've already started running and have a restart file, just restart
# and exit
if [[ ! -z `ls -1 $FCST_DIR | grep nmmb_rst` ]] ; then
    echo "Restarting run"
    max_size=0
    ll_rst_files=`ls -1 -t $FCST_DIR | grep nmmb_rst | grep -v ctl`
    # look for the latest file that is not smaller than the one that came before it
    for fileName in `echo $ll_rst_files` ; do
        line=`ls -l $FCST_DIR/$fileName`
        echo $line
        size=`echo $line | awk '{print $5}'`
        if [ $size -gt $max_size ] ; then
            max_size=$size
            newest_file=$fileName
        fi
        relPath=`echo $line | awk '{print $9}'`
    done
    echo "Will restart from file $newest_file"
    
    cd $FCST_DIR
    # change configure file
    sed -i "s#^[\s]*restart:.*#restart:  true#" $CONFIG_FILE 
    # rename restart file
    #[[ -e restart_file_01_nemsio ]] && mv restart_file_01_nemsio restart_file_01_nemsio.prev
    [[ -e restart_file_01 ]] && mv restart_file_01 restart_file_01.prev
    #mv $newest_file restart_file_01_nemsio
    mv $newest_file restart_file_01
    # ready to run it
    #module load impi
    #module load esmf
    #mpirun -np $PBS_NP ./NEMS.x
    #exit

else
    echo "Starting from scratch"
    sed -i "s#^[\s]*restart:.*#restart:  false#" $CONFIG_FILE
fi

TAU_PATH=/home/Javier.Delgado/scratch/apps_tmp/tau/tau-2.26.3/install/x86_64/bin/
export PATH=$PATH:/home/Javier.Delgado/scratch/apps_tmp/tau/tau-2.26.3/install/x86_64/bin/

# TAU memory debugging settings
export TAU_MEMDBG_PROTECT_ABOVE=1
export TAU_TRACK_MEMORY_LEAKS=1
export TAU_MEMDBG_PROTECT_BELOW=1
export TAU_MEMDBG_ZERO_MALLOC=1
#export TAU_MEMDBG_PROTECT_FREE=1

mpirun -np $PBS_NP -prepend-rank ./NEMS.x
#mpirun -np $PBS_NP -prepend-rank $TAU_PATH/tau_exec -memory ./NEMS.x

