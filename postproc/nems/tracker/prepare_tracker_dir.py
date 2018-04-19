"""
Sets up the environment from running the GFDL vortex tracker. 
"""

import os
import copy
from datetime import datetime as dtime
from datetime import timedelta as tdelta
import subprocess
from ConfigParser import ConfigParser

from pycane.postproc.tracker import utils as trkutils

def confget(item, section="DEFAULT"):
    return conf.get(section, item)

def datestr_to_datetime(startDate):
    """  
    Convert a string in `MM-DD-YYYY hh:mm' format to a datetime
    @param startDate The String
    @return the datetime object
    """
    try: 
        mdy = startDate.split(" ")[0]
        hm = startDate.split(" ")[1]
        (month, day, year) = [int(x) for x in mdy.split("-")]
        (hour, minute) = [int(x) for x in hm.split(":")]
        #startDate = startDate.replace("'", "").replace('"', '')
        #os.environ['TZ'] = 'UTC'
        #tzset()
        #return time.mktime(time.strptime(startDate, '%m-%d-%Y %H:%M'))
        return dtime(year=year, month=month, day=day,
                                 hour=hour, minute=minute)
    except ValueError:
        print 'Given start date', startDate, 'does not match expected format MM-DD-YYYY hh:mm'
        sys.exit(1)

# CONFIG 
conf = ConfigParser()
conf.read(["trk.conf"])
start_date = datestr_to_datetime(confget("start_date"))
duration = tdelta(hours=float(confget("duration_hours")))
input_frequency = tdelta(hours=float(confget("interval_hours")))
first_fhr = confget("first_forecast_hour")
if int(first_fhr) != float(first_fhr): 
    raise Exception("first_fhr must be non-decimal")
first_fhr = int(first_fhr)
inspec = [confget("inspec")]
domain = confget("domain")
tc_vitals_path = confget("tc_vitals_path")
storm_number = int(confget("storm_number"))
storm_basin = confget("storm_basin")
storm_id = "{0:02d}{1}".format(storm_number, storm_basin)
gmodname = confget("gmodname")
rundescr = confget("rundescr")
atcf_name = confget("atcf_name")
tracker_inputs_path = confget("tracker_inputs_path")
tracker_input_file_pattern = confget("tracker_input_file_pattern")
tracker_link_pattern = confget("tracker_link_pattern")
# paths
tracker_util_path = confget("tracker_util_path")
grbindex_exe = confget("grbindex_exe")
tracker_exe = confget("tracker_exe")


def create_links_and_index_files():
    start_ymdh = "{0:%Y%m%d%H}".format(start_date)
    pfx = gmodname + "." + gmodname
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
    inp_args = dict(gmodname=gmodname, rundescr=rundescr, atcfname=atcf_name,
                    start_date=start_date )
    for fhr in range(first_fhr, duration_hours+1, freq_hours):
        # TODO : minutes
        fmin = 0
        #import pdb ; pdb.set_trace()
        inp_args.update({"fhr":fhr, "fmin":fmin})
        infile = tracker_input_file_pattern.format(**inp_args)
        fmin_5dig = (fhr * 60) + fmin
        lnk_args = copy.copy(inp_args)
        lnk_args.update({"fmin":fmin_5dig})
        link = tracker_link_pattern.format(**lnk_args)
        if os.path.islink(link): os.unlink(link)
        os.symlink(infile, link)
        subprocess.check_call([grbindex_exe, link, link+".ix"])

def create_fort15():
    #import pdb ; pdb.set_trace()
    duration_minutes = int(duration.total_seconds() / 60)
    interval_mins = int(input_frequency.total_seconds() / 60)
    with open("fort.15", "w") as fil:
        #for ctr,fmin in enumerate(range(FIRST_FHR, duration_minutes + 1, interval_mins)):
        ctr = first_fhr
        for fmin in range(first_fhr*60, duration_minutes + 1, interval_mins):
            #fil.write("{0:04d} {1:05d}\n".format(ctr+1, fmin))
            fil.write("{0:04d} {1:05d}\n".format(ctr, fmin)) # without enumerate
            ctr += int(input_frequency.total_seconds() / 3600)

def run_tracker():
    open("fort.14", "w").close() # TODO : need this for cyclogenesis
    
if __name__ == "__main__":
    # Build ForecastTrack object and create fort.12 (tcvitals first guess) file
    args = dict(fdate=start_date, stormId=storm_id)
    path = tc_vitals_path.format(**args)
    trk = trkutils.get_track_data(path)
    trk.originating_center = 'AOML'
    trk.storm_number = storm_number
    trk.basin = storm_basin
    trk.dump_gfdltrk_fort12(outfile="fort.12") # TODO : GENERALIZE this function @ pycane - it's just atcf i think
    # TODO ? pass in fhr offset? (first_fhr)
    # Create NL
    # TODO - just use premade one for now
    create_links_and_index_files()
    create_fort15()
    run_tracker()

