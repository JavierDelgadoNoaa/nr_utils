"""
Performs necessary set up for running the tracker - creating files, making
 links, etc.
PreCondition: track_subsetter.py was run to generate the GriB files used by
              the tracker
"""              
import os
import copy
from datetime import datetime as dtime
from datetime import timedelta as tdelta
import subprocess

from pycane.postproc.tracker import utils as trkutils

start_date = dtime(year=2006, month=9, day=4, hour=0, minute=0)
#input_frequency = tdelta(minutes=180)
input_frequency = tdelta(minutes=60)
#duration = tdelta(hours=180)
duration = tdelta(hours=222)
FIRST_FHR = 90

storm_number = 8
basin = "L"
storm_id = "{0:02d}{1}".format(storm_number, basin)
TC_VITALS_PATH = "/home/Javier.Delgado/scratch/nems/g5nr/tc_stuff/tc_vitals/geos5trk/{fdate:%Y}_{stormId}.txt"
GMODNAME = "nmb"
RUNDESCR  = "trk"
ATCF_NAME = "bsnr_gamma"
TRACKER_INPUTS_PATH = "."
#TRACKER_INPUT_FILE_PATTERN = "{gmodname}.{rundescr}.{atcfname}.{start_date:%Y%m%d%H}.f{fhr:03d}.{fmin:02d}"
#TRACKER_INPUT_FILE_PATTERN = "nmbtrk.{start_date:%Y%m%d%H}.f{fhr:03d}.{fmin:02d}.grb2"
TRACKER_INPUT_FILE_PATTERN = "nmbtrk.{start_date:%Y%m%d%H}.f{fhr:03d}.{fmin:02d}.grb1"
TRACKER_LINK_PATTERN = "{gmodname}.{rundescr}.{atcfname}.{start_date:%Y%m%d%H}.f{fmin:05d}"

# paths
TRACKER_UTIL_PATH = "/home/Javier.Delgado/apps/gfdl_vortex_tracker/dtc/3.5b/tracker_util"
GRBINDEX = os.path.join(TRACKER_UTIL_PATH, "exec", "grbindex.exe")
TRACKER = "/home/Javier.Delgado/apps/gfdl_vortex_tracker/dtc/3.5b/gfdl-vortextracker/trk_exec"


def create_links_and_index_files():
    start_ymdh = "{0:%Y%m%d%H}".format(start_date)
    pfx = GMODNAME + "." + RUNDESCR
    """
    for currfile in os.listdir(TRACKER_INPUTS_PATH):
        if not currfile.startswith(pfx): continue
        if currfile.endswith("ix"): continue
        if not "fmin" in TRACKER_INPUT_FILE_PATTERN:
            log.info("Assuming minutes not in tracker input files, setting to 0")
            fmin = 0
    """
    duration_hours = int(duration.total_seconds() / 3600)
    duration_minutes = (duration.total_seconds() / 3600.) % 60  
    freq_hours = int(input_frequency.total_seconds() / 3600)
    inp_args = dict(gmodname=GMODNAME, rundescr=RUNDESCR, atcfname=ATCF_NAME,
                    start_date=start_date )
    for fhr in range(FIRST_FHR, duration_hours+1, freq_hours):
        # TODO : minutes
        fmin = 0
        #import pdb ; pdb.set_trace()
        inp_args.update({"fhr":fhr, "fmin":fmin})
        infile = TRACKER_INPUT_FILE_PATTERN.format(**inp_args)
        fmin_5dig = (fhr * 60) + fmin
        lnk_args = copy.copy(inp_args)
        lnk_args.update({"fmin":fmin_5dig})
        link = TRACKER_LINK_PATTERN.format(**lnk_args)
        if os.path.islink(link): os.unlink(link)
        os.symlink(infile, link)
        subprocess.check_call([GRBINDEX, link, link+".ix"])

def create_fort15():
    #import pdb ; pdb.set_trace()
    duration_minutes = int(duration.total_seconds() / 60)
    interval_mins = int(input_frequency.total_seconds() / 60)
    with open("fort.15", "w") as fil:
        #for ctr,fmin in enumerate(range(FIRST_FHR, duration_minutes + 1, interval_mins)):
        ctr = FIRST_FHR
        for fmin in range(FIRST_FHR*60, duration_minutes + 1, interval_mins):
            #fil.write("{0:04d} {1:05d}\n".format(ctr+1, fmin))
            fil.write("{0:04d} {1:05d}\n".format(ctr, fmin)) # without enumerate
            ctr += int(input_frequency.total_seconds() / 3600)

def run_tracker():
    open("fort.14", "w").close() # TODO : need this for cyclogenesis
    
if __name__ == "__main__":
    # Build ForecastTrack object and create fort.12 (tcvitals first guess) file
    args = dict(fdate=start_date, stormId=storm_id)
    path = TC_VITALS_PATH.format(**args)
    trk = trkutils.get_track_data(path)
    trk.originating_center = 'AOML'
    trk.storm_number = storm_number
    trk.basin = basin
    trk.dump_gfdltrk_fort12(outfile="fort.12") # TODO : GENERALIZE this function @ pycane - it's just atcf i think
    # TODO ? pass in fhr offset? (first_fhr)
    # Create NL
    # TODO - just use premade one for now
    create_links_and_index_files()
    create_fort15()
    run_tracker()

