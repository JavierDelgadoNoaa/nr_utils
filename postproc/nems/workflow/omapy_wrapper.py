"""
Wrapper to run Omap.py to generate a set of plots for a given case, with a 
given start time, frequency, and duration.
The figures that are generated are controlled by the variables defined in params.py.
The set of dates and forecast offsets to process are specified via command
line arguments. There are two options for command line arguments:
  1. Specify first forecast hour, interval and duration OR 
  2. Specify forecast initialization date, first date to process, interval, 
     and duration. This is useful if working with Rocoto workflows since
     there is no way to pass forecast offset.
     Doing this will also override the initialization date in the Omap.py 
     configuration.
LIMITATIONS
 - plots_per_figure (in OmapPy plot settings) must be 1 due to file naming convention
 - does not work with less than hourly intervals
"""

import os
import sys
from datetime import datetime as dtime
import subprocess
import copy

from params import plotsets, OMAPY_DIST
import params

# Parse args
init_date_given = False
if len(sys.argv) == 6:
    first_fhr = int(sys.argv[1])
    last_fhr = int(sys.argv[2])
    interval = int(sys.argv[3])  # TODO : Make float and iterate thru minutes if necessary
    main_conf = sys.argv[4]
    case_conf_file = sys.argv[5]
elif len(sys.argv) == 15:
    init_date_given = True
    init_year, init_month, init_day, init_hour, init_minute = \
        [int(x) for x in sys.argv[1:6]]
    first_year, first_month, first_day, first_hour, first_minute = \
        [int(x) for x in sys.argv[6:11]]
    interval = int(sys.argv[11])
    duration = float(sys.argv[12])
    main_conf = sys.argv[13]
    case_conf_file = sys.argv[14]
    # Calculate fhr
    init_date = dtime(year=init_year, month=init_month, day=init_day, 
                      hour=init_hour, minute=init_minute)
    first_date = dtime(year=first_year, month=first_month, day=first_day,
                       hour=first_hour, minute=first_minute)
    #first_fhr = (first_date.total_seconds() - init_date.total_seconds() ) / 3600.
    first_fhr = (first_date - init_date).total_seconds() / 3600.
    last_fhr = first_fhr + duration
    # TODO minutes if duratoin is a float. 
    first_fhr = int(first_fhr)
    last_fhr = int(last_fhr)

else:
    sys.stderr.write("USAGE: {exe} <firstFhr> <lastFhr> <interval> "
                     "<main config file> <case config file>\n OR\n"
                     "{exe} <initYear> <initMonth> <initDay> <initHour> "
                     "<initMinute> <firstYear> <firstMonth> <firstDay> "
                     "<firstHour> <firstMinute> <frequency(hours)> "
                     "<duration (hours)> <mainConfig> <caseConfig>"
                     .format(exe=sys.argv[0]))
    sys.exit(1)

# Do it
#subprocess.check_call(["source",os.path.join(OMAPY_DIST,"env.sh")], shell=True)
#os.system("source " + os.path.join(OMAPY_DIST,"env.sh"))
for plotset in plotsets:
    omapy_script = os.path.join(OMAPY_DIST, "omap.py")
    args = ["python", omapy_script, main_conf, case_conf_file]
    # Tell omapy which plots to generate either by overriding BASIC.plots in 
    # the command line ((NOPE)) ==>or by passing in an aux config file that overrides
    # the [BASIC].plots parameter
    if "auxconf" in plotset:
        args.append(plotset["auxconf"])
    
    # Tell omapy what forecast offsets to process
    l = ["-o", "BASIC.first_fhr="+str(first_fhr)]
    l.extend(["-o", "BASIC.duration="+str(last_fhr)])
    l.extend(["-o", "BASIC.interval="+str(interval)])
    args.extend(l)

    # Set storm ID
    args.extend(["-o", "BASIC.storm_id="+params.storm_id])
    # Set path
    args.extend(["-o", "paths.output_path="+params.omapy_outdir])

    # To use debug mode
    #args.extend(["-l", "DEBUG"])

    # If init date given in cmdline, pass it on to omapy
    if init_date_given:
        d = "{0:%m-%d-%Y %H:%M}".format(init_date)
        #args.extend(["-o", "'BASIC.start_date="+d+"'"])
        args.extend(["-o", "BASIC.start_date="+d])

    if "levs" in plotset:
        levs = plotset["levs"]
    else:
        levs = [""] # hack: since we iterate thru levs

    for lev in levs:
        args_lev = copy.copy(args)
        plotconf = plotset["plotConfName"].format(levValue=lev)
        l = ["-o", "BASIC.plots="+plotconf] # what about multilev
        args_lev.extend(l)
            
        l.extend(["-o", plotconf+".plots_per_figure=1"])
        # Due to naming convention, this only works with 1 plot/file
        # NOTE: This won't help if the file naming convention does
        # not account for fhr

        # Map types
        #args_lev.append("-o", "default_plot_settings.map_extents_setting="+mapType)
        args_lev.extend(["-o", plotconf+".map_extents_setting="+plotset["mapType"]])
        
        # run it
        subprocess.check_call(args_lev)

        # Verify files were created and move if necessary
        for fhr in range(first_fhr, last_fhr+1, interval):
            curr_file = plotset["fname"].format(levValue=lev, fhr=fhr)
            curr_file = os.path.join(params.omapy_outdir, curr_file)
            if "outpath" in plotset:
                outpath = plotset["outpath"].format(fname=curr_file)
                print outpath
                print curr_file
                os.renames(curr_file, outpath)
            else:
                assert os.path.exists(curr_file)
                
