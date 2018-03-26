#!/usr/bin/env python

"""
Create links as expected by Ungrib.exe using inputs from GriB analyses and 
(as necessary) forecast outputs.
The script will try to get as much from different analyses as possible and fill
the gaps with the forecast outputs according to the ``src_cycle_interval''.
e.g. if the source (GFS) data is cycled daily and we want 6-hourly data, 
     it will get the analysis, 6z, 12z, and 18z data from each forecast
"""

import os
from datetime import datetime as dtime
from datetime import timedelta as tdelta
#import copy

##
# PARAMETERS - SET THESE
##
start_date = dtime(year=2006, month=9, day=4, hour=0)
# how frequently is the src data cycled
src_cycle_interval = tdelta(days=1) 
# how frequently do we want input data (i.e. Metgrid interval)
# Note : This can be no greater than the GriB output interval
# of the source data
dest_input_interval = tdelta(hours=6) 
# how long do we want NPS to go out
duration = tdelta(days=1.8)
# Pattern of input (grib) files
fil_pattern = "pgbf{fhr:02d}.gfs.{init_date:%Y%m%d%H}"


##
# LOGIC - LOOK NO FURTHER
##
# Get list of available analyses in source data, for the necessary
# duration
all_dates = [start_date + tdelta(seconds=s) for s in \
             range(0, int(duration.total_seconds())+1,
                   int(src_cycle_interval.total_seconds()))]

# Since the cycle interval is less than the NPS frequency, we
# need to get more than just the analysis from each cycle
fhrs_per_src_cycle = int(src_cycle_interval.total_seconds() / dest_input_interval.total_seconds())

suffix = "AAA" # note this doesn't change, currSuffix does
linkCtr = 0
for currAnalysis in all_dates:
    for i in range(fhrs_per_src_cycle):
        #currDate += (dest_input_interval * i)
        currFhr = int((dest_input_interval * i).total_seconds() / 3600)
        currGribFile = fil_pattern.format(init_date=currAnalysis, fhr=currFhr)
        if not os.path.exists(currGribFile):
            raise Exception("Does not exist: {0}".format(currGribFile))
        # Determine the current letter for each of the 3 chars in the suffix
        last = linkCtr % 26
        middle = linkCtr / 26
        first = linkCtr / (26*26)
        # Increment each index of the suffix
        one = chr(ord(suffix[0]) + first)
        two  = chr(ord(suffix[1]) + middle)
        three = chr(ord(suffix[2]) + last)
        currSuffix = one + two + three
        # Create links
        currLink = "GRIBFILE.{0}".format(currSuffix)
        #print currGribFile, currLink
        os.symlink(currGribFile, currLink)    
        linkCtr += 1
