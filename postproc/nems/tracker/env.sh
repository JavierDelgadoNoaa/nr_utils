module load wgrib2

# This will pull pycane for the tracker utils and nwpy for specdata
source /home/Javier.Delgado/apps/pycane_dist/master/etc/env.sh

# need produtil from HWRF for file locking
export PYTHONPATH=$PYTHONPATH:/home/Javier.Delgado/scratch/apps_tmp/pyhwrf/emc/dist/trunk/ush/
