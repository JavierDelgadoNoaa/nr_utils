"""
Verify that a set of GriB files have the number of levels expected.

HOW TO USE
1. Adjust parameters under "SETTINGS"
 a. Set date/time related params and paths
 b. Set `fields' according to field_lev_type:[fields] expected
 c. Ensure the other dictionaries are ok
2. Run it
"""

import os
import subprocess
from datetime import datetime as dtime
from datetime import timedelta as tdelta
import numpy as np

#
# SETTINGS
#
duration = tdelta(hours=300)
interval = tdelta(hours=0.5)
first_fhr = 82
init_date = dtime(year=2006, month=9, day=4, hour=0)
files_topdir  = "/home/Javier.Delgado/scratch/nems/g5nr/data/gamma/2j_5500x4000/2006090400/postprd"
files_topdir  = "/home/Javier.Delgado/scratch/nems/g5nr/data/gamma/2j_5500x4000/2006090400/postprd_test_wrapper_sfc"

num_isobaric_levs = 46
num_hybrid_levs = 5

# Set fields expected for each level type. Main logic will loop over all of 
# these for each forecast offset
fields = dict(
              prs=["HGT", "TMP", "UGRD", "VGRD", "RH", "CICE", "VVEL", "ABSV"],
              sfc=["APCP", "PRATE"],
              sfc1=[ "PRES", "HGT", "POT", "SPFH", "TMP", "WEASD", "CAPE", "CIN", "VEG", "LAND"],
              hgt10=["UGRD", "VGRD", "SPFH", "POT"],
              hgt2 =["TMP", "SPFH", "DPT", "RH", "PRES"],
              msl = ["PRMSL", "MSLET"] 
              hyb=["DPT", "HGT", "POT", "PRES", "RH", "SPFH", "TMP", "VVEL"]
             )
# How many level values are expected for each level type?
expected_num_entries = dict(prs=num_isobaric_levs, sfc=1, sfc1=1, hgt2=1, 
                            hgt10=1, msl=1, hyb=num_hybrid_levs)
#nmbprs_ABSV_2006090400.f165.00.grb2
suffix_pattern = "{pname}_{init_date:%Y%m%d%H}.f{fhr:03d}.{fmin:02d}.grb2"
file_patterns = dict(prs="nmbprs_" + suffix_pattern,
                     sfc1="nmbsfc_" + suffix_pattern,
                     sfc="nmbsfc_" + suffix_pattern,
                     hgt10="nmbhgt_" + suffix_pattern,
                     hgt2 ="nmbhgt_" + suffix_pattern,
                     msl="nmbprs_" + suffix_pattern,
                     hyb="nmbhyb_" + suffix_pattern, 
                    )


##
# LOGIC
##
for levType,fieldList in fields.iteritems():
    for field in fieldList: 
        numTimesteps =  ( (duration.total_seconds()-first_fhr*3600) / interval.total_seconds()) + 1
        for fcstOffset in np.linspace(first_fhr*3600, duration.total_seconds(), 
                                      numTimesteps):
        #for fhr in range(first_fhr, duration_hrs+1, interval_hrs):
                fhr = int(fcstOffset / 3600)
                #fhr = (fcstOffset / 3600).astype(np.int32)
                fmin = int(( fcstOffset / 60 ) % 60)
            #for fmin in minute_intervals:
                args = dict(pname=field, init_date=init_date, fhr=fhr, fmin=fmin)
                filename = file_patterns[levType].format(**args)
                filename = os.path.join(files_topdir, filename)
                wgrib_cmd = ["wgrib2", "-s", filename]
                try:
                    p = subprocess.check_call(wgrib_cmd) # ensure no errors first
                    p = subprocess.Popen(wgrib_cmd, stdout=subprocess.PIPE)
                    output = p.communicate()[0].strip()
                    num_gribs = output.count("\n") + 1
                    if num_gribs > expected_num_entries[levType]:
                        print filename, ": expected:", expected_num_entries[levType], " ; found:", num_gribs
                    elif num_gribs < expected_num_entries[levType]:
                        print "PROBLEM: Less gribs than expected in", filename, "expected:", expected_num_entries[levType], " ; found:", num_gribs
                except:
                    print "Exception querying file: ", filename
