"""
Merge variables in separate collections onto a single data file
This was only used for testing. It's for the coarse datasets, though
modifying for Native/hires shouldn't be hard
"""

import os
import copy
from datetime import datetime as dtime
from datetime import timedelta as tdelta
import shutil

import numpy as np
from PyNIO import Nio

input_variables = {"inst01hr_3d_T_Cp" : ["T"], 
                   "inst01hr_3d_PL_Cp": ["PL"],
                  }

outfile_prefix = "g5nr_coarse_merged"
output_dir = "/scratch4/NAGAPE/aoml-osse/Javier.Delgado/nems/g5nr/data/coarse_collections/merged"
input_dir = "/scratch4/NAGAPE/aoml-osse/Javier.Delgado/nems/g5nr/data/coarse_collections/"

start_date = dtime(year=2006, month=9, day=10, hour=0)
duration = tdelta(days=5)
interval = tdelta(hours=6)
    
    
levs = np.array([0.02,  0.03, 0.04,   0.05,  0.07,  0.10,  0.20,  0.30,  0.40,  0.50,  0.70,   1.0,   2.0,   3.0,   4.0    ,  5.0,   7.0,  10.0,  20.0,  30.0,  40.0,   50.0,   70.0, 100.0, 150.0, 200.0, 250.0, 300.0, 350.0, 400.0, 450.0, 500.0,     550.0, 600.0, 650.0, 700.0, 725.0, 750.0, 775.0, 800.0, 825.0, 850.0, 875.0, 900.0, 925.0, 950.0, 975.0, 1000])
# This is avialble via variable "lev" in the coarse dataset
    
curr_date = copy.copy(start_date)
in_datasets = input_variables.keys()

#dates = range(0, int(duration.total_seconds()+1), int(interval.total_seconds()))
dateRange = range(0, int(duration.total_seconds())+1, int(interval.total_seconds()))
all_dates = [start_date + tdelta(seconds=curr) for curr in dateRange]

#while (curr_date + tdelta(seconds=0)).total_seconds() <= (start_date + duration).total_seconds():
for curr_date in all_dates:
    infiles = []
    for ds in in_datasets:
        # TODO : account for different availabliity/pfx/sfx
        fil = "c1440_NR.{ds}.{currDate:%Y%m%d_%H%M}z.nc4"\
              .format(ds=ds, currDate=curr_date)
        infiles.append(os.path.join(input_dir, ds, fil))
    outfile = "{pfx}_{currDate:%Y%m%d_%H%M}z.nc4"\
              .format(pfx=outfile_prefix, currDate=curr_date)
    out_path = os.path.join(output_dir, outfile)
    shutil.copy(infiles[0], out_path)
    out_dataset = Nio.open_file(out_path, "w")
    
    #import pdb ; pdb.set_trace()
    for idx,ds in enumerate(in_datasets): 
        if idx == 0: continue# skip first since it was copied
        in_dataset = Nio.open_file(infiles[idx])
        for varName in input_variables[ds]:
            vtype = in_dataset.variables[varName].typecode()
            dims = in_dataset.variables[varName].dimensions
            out_dataset.create_variable(varName, vtype, dims)
            out_dataset.variables[varName][:] = in_dataset.variables[varName][:]
            # Try setting variable attributes
            for attrName in ["units", "long_name"]:
                try:
                    attrValue = getattr(in_dataset.variables[varName], attrName)
                    setattr(out_dataset.variables[varName], attrName, attrValue)
                except:
                    print "Unable to set attribute:", attrName
        in_dataset.close()
    create_pressure_variable(out_dataset)
    out_dataset.close()

    curr_date += interval

