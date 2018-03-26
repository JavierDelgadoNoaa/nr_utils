import sys
import os
import shutil
from datetime import datetime as dtime
from datetime import timedelta as tdelta
import numpy as np

from PyNIO import Nio

from nps import nps_int_utils
from params import LIS_Params as lis

#start_date = dtime(year=2006, month=9, day=10)
start_date = dtime(year=2006, month=9, day=6)
duration = tdelta(days=2)
frequency = tdelta(hours=3)

"""
# Map extents
print "Using HARDCODED resolution and data extents !!"
START_LAT = -21.640
END_LAT = 39.636
START_LON = -135.738
END_LON = 5.988+.05 # add padding since arange is exclusive
RESOLUTION = 0.0625
"""

freq_s = int(frequency.total_seconds())
dur_s = int(duration.total_seconds())
dateRange = range(0, dur_s+1, freq_s)
all_dates = [start_date + tdelta(seconds=curr) for curr in dateRange]

for currDate in all_dates:
    
    curr_lis_fileName = lis.OUTPUT_FILE_PATTERN.format(currDate, domNum=1)
    subdir = '{:%Y%m}'.format(currDate)
    topdir = "/home/Javier.Delgado/scratch/nems/g5nr/lsm_experiments/beta/OUTPUT/SURFACEMODEL/"
    curr_lis_file = os.path.join(topdir, subdir, curr_lis_fileName)
    print curr_lis_file
    lis_nc = Nio.open_file(curr_lis_file, "a")
    
    # Create landsea variable
    vtype = lis_nc.variables["lat"].typecode()
    dims = lis_nc.variables["lat"].dimensions
    if not "LAND_LIS" in lis_nc.variables:
        lis_nc.create_variable("LAND_LIS", vtype, dims)
    # set landsea data = 1 where SM field is not masked and 0 elsewhere
    data = lis_nc.variables['SoilMoist_tavg'][1,:] # Only need one slab 
    #print data.typecode()
    print data.shape
    print lis_nc.variables["LAND_LIS"][:].shape
    #import pdb ; pdb.set_trace()
    landsea = np.zeros(data.shape, dtype=data.dtype)
    landsea[~data.mask] = 1.
    print landsea.shape
    lis_nc.variables["LAND_LIS"][:] = landsea
    lis_nc.close()

    xfcst = 0
    fields  = [ ["LAND_LIS", "LAND_LIS", "proprtn", "Land (1) / Sea (0) mask in LIS"] ]
    outfile = nps_int_utils.get_int_file_name("LAND_LIS", currDate)
    nps_int_utils.nc_to_nps_int(curr_lis_file, outfile, currDate, xfcst, fields, "lis")

