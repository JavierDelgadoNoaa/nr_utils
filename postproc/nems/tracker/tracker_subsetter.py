"""
Use the GriB outputs from UPP to create the tracker input files.
Uses a similar input style to the (current) operational HWRF in terms
of the fields it produces and their size (these can be tweaked via
variables TRACKER_SUBSET and NLATS,NLONS,DX,DY).
The order of the fields is similar to HWRF; not the same due to the
regridding library processing UGRD and VGRD together.

ASSUMPTIONS
- Model UPP files are in GriB 2 format
"""

import os
import subprocess
from datetime import datetime as dtime
from datetime import timedelta as tdelta
import logging
import shutil
import numpy as np
from distutils.util import strtobool
from ConfigParser import ConfigParser

from  nwpy.dataproc.specdata import objects as specdata_objects
from nwpy.dataproc.specdata.objects import SpecifiedForecastDataset as SpecData
from pycane.postproc.tracker import utils as trkutils
import produtil
from produtil.cd import TempDir
from produtil.run import mpirun, mpi, openmp, checkrun, bigexe

# The VGRD entries are automatically added to the UGRD during
# processing since they must be regridded together
# NOTE That this will put fields in a different order than HWRF
TRACKER_SUBSET=[ 
                 'HGT:925', 'HGT:850', 'HGT:700', 'UGRD:850', 'UGRD:700',
                 'UGRD:500', 'UGRD:10 m ', 'ABSV:850', 'ABSV:700',
                 'PRMSL', 'HGT:900', 'HGT:800', 'HGT:750', 'HGT:650',
                 'HGT:600', 'HGT:550', 'HGT:500', 'HGT:450', 'HGT:400',
                 'HGT:350', 'HGT:300', 'HGT:250', 'TMP:500', 'TMP:450',
                 'TMP:400', 'TMP:350', 'TMP:300', 'TMP:250' ]                 
"""
TRACKER_SUBSET=[ 'HGT:925', 'HGT:850', 'HGT:700', 'UGRD:850', 'UGRD:700',
                 'UGRD:500', 'VGRD:850', 'VGRD:700', 'VGRD:500',
                 'UGRD:10 m ', 'VGRD:10 m ', 'ABSV:850', 'ABSV:700',
                 'PRMSL', 'HGT:900', 'HGT:800', 'HGT:750', 'HGT:650',
                 'HGT:600', 'HGT:550', 'HGT:500', 'HGT:450', 'HGT:400',
                 'HGT:350', 'HGT:300', 'HGT:250', 'TMP:500', 'TMP:450',
                 'TMP:400', 'TMP:350', 'TMP:300', 'TMP:250' ]
"""

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

##
# CONFIG 
##
#INSPECS_TOPDIR = "/home/Javier.Delgado/libs/nwpy/conf/inspec"
#DATA_TOPDIR ="/home/Javier.Delgado/scratch/nems/g5nr/data/gamma/plan_b_1.5km/alternative/{init_date:%Y%m%d%H}/postprd"
conf = ConfigParser()
conf.read(["trk.conf"])
data_topdir_pattern = confget("data_topdir")
start_date = datestr_to_datetime(confget("start_date"))
duration = tdelta(hours=float(confget("duration_hours")))
interval = tdelta(hours=float(confget("interval_hours")))
first_fhr = float(confget("first_forecast_hour"))
inspec = [confget("inspec")]
domain = confget("domain")
tc_vitals_path = confget("tc_vitals_path")
storm_number = int(confget("storm_number"))
storm_basin = confget("storm_basin")
storm_id = "{0:02d}{1}".format(storm_number, storm_basin)
find_nearest_fdate = strtobool(confget("find_nearest_fdate"))

## The following 4 parameters determine the size and resolution of the output 
nlats = int(confget("num_latitude_grid_points"))
nlons = int(confget("num_longitude_grid_points"))
dx = float(confget("dx"))
dy = float(confget("dy"))
make_grib1 = strtobool(confget("make_grib1"))
grib2_to_grib1_exe = confget("grib2_to_grib1_exe")
tmpdir = confget("temp_directory")

def _default_log(log2stdout=logging.INFO, log2file=None, name='el_default'):
    global _logger
    if _logger is None:
        _logger = logging.getLogger(name)
        _logger.setLevel(log2stdout)
        msg_str = '%(asctime)s::%(name)s::%(lineno)s::%(levelname)s - %(message)s'
        msg_str = '%(asctime)s::%(funcName)s::%(filename)s:%(lineno)s::%(levelname)s - %(message)s'
        date_format = "%H:%M:%S"
        formatter = logging.Formatter(msg_str, datefmt=date_format)
        if log2file is not None:
            fh = logging.FileHandler('log.txt')
            fh.setLevel(log2file)
            fh.setFormatter(formatter)
            _logger.addHandler(fh)
        if log2stdout is not None:
            ch = logging.StreamHandler()
            ch.setLevel(log2stdout)
            ch.setFormatter(formatter)
            _logger.addHandler(ch)
    return _logger
_logger=None

def cat_file(src, dest):
    """
    Move a file from src path to dest path. If dest path exists, concatenate
    it. This is a thread-safe operation, although it uses a rudimentary 
    locking mechanism.
    """
    lock_file = os.path.join(os.path.dirname(dest), 
                             ".lock_" + os.path.basename(dest))
    with produtil.locking.LockFile(filename=lock_file, logger=log, max_tries=10) as lock:    
        if os.path.exists(dest):
            log.info("Destination file '{0}' exists. Will concatenate to it"
                     .format(dest))
            srcObj = open(src, "rb")
            destObj = open(dest, 'ab')
            shutil.copyfileobj(srcObj, destObj, 65536) # last arg=buff size
            srcObj.close()
            destObj.close()       
        else:
            log.debug("Moving file {0}->{1}".format(src, dest))
            shutil.move(src, dest)
    if not os.path.exists(lock_file):
        log.warn("jza33 lock file {0} does not exist!".format(lock_file))
    else:
        os.unlink(lock_file)

def get_cen_latlon(curr_fdate, find_nearest_fdate=False):
    """
    Using the TC vitals path for the storm, determine the center lat and lon
    If find_nearest_fdate is True and the `curr_fdate' is not in the vitals,
    find the nearest date
    :returns: 2-tuple consisting of center lat and lon
    """
    args = dict(fdate=curr_fdate, stormId=storm_id)
    path = tc_vitals_path.format(**args)
    trk = trkutils.get_track_data(path)
    # map forecast dates to pycane.postproc.tracker.objects.TrackerEntry objects
    trk_fdates = trk.fcst_date_dict 

    if curr_fdate in trk_fdates:
        nearest = curr_fdate
    elif find_nearest_fdate:
        nearest = min(trk_fdates.keys(), key=lambda d: abs(d - curr_fdate) )
        log.info("Current forecast date {0} is not in tc vitals. Using "
                 " nearest date: {1}".format(curr_fdate, nearest))
    else:
        raise Exception("Current forecast date {0} is not in tc vitals"
                        .format(curr_fdate))
    cenlat = trk_fdates[nearest].lat
    cenlon = trk_fdates[nearest].lon

    return (cenlat,cenlon)


def subset_and_regrid(infile, outfile, fieldstr, latstr, lonstr):
    """
    Extract a field from infile according to `fieldstr' which can be
    just the field pname or "pname:levValue" or "pname:levValue levUnit"
    """
    # winds must be subset/regridded together
    #import pdb ; pdb.set_trace()
    if fieldstr[0:4] == "UGRD":
        #import pdb ; pdb.set_trace()
        lev = fieldstr.split(":")[1]
        fieldstr = fieldstr + "|" + fieldstr.replace("UGRD","VGRD")
    # hack : combine input files for U/V
    # assume file name for VGRD is same as UGRD with VGRD in name instead
    infile2 = infile.replace("UGRD", "VGRD")
    if infile != infile2:
        infile_tmp = os.path.join(tmpdir, "tmp_winds.grb2")
        if os.path.exists(infile_tmp): os.unlink(infile_tmp)
        #import pdb ; pdb.set_trace()
        for fld,fil in zip(["UGRD","VGRD"], [infile, infile2]): 
            # NOTE : U must be adjacent to V and come first in the grib file 
            # for the new_grid option of wgrib2 to work
            fld = fld + ":" + lev
            tmp_tmpfile = infile_tmp + ".tmp"
            if os.path.exists(tmp_tmpfile): os.unlink(tmp_tmpfile)
            inv = subprocess.Popen(["wgrib2", "-s", fil], stdout=subprocess.PIPE)
            subset = subprocess.Popen(["grep", "-E", fld], stdout=subprocess.PIPE, stdin=inv.stdout)
            # ensure there is only one grib entry per field/lev since U and V must
            # be adjacent
            subset = subprocess.Popen(["tail", "-n", "1"], stdout=subprocess.PIPE, stdin=subset.stdout)
            new = subprocess.check_output(["wgrib2", "-i", fil, "-GRIB", tmp_tmpfile ], stdin=subset.stdout)
            cat_file(tmp_tmpfile, infile_tmp)
        #import pdb ; pdb.set_trace()
        infile = infile_tmp
    inv = subprocess.Popen(["wgrib2", "-s", infile], stdout=subprocess.PIPE)
    subset = subprocess.Popen(["grep", "-E", fieldstr], stdout=subprocess.PIPE, stdin=inv.stdout)
    # take out duplicates
    if "|" in fieldstr:
        subset = subprocess.Popen(["tail","-n", "2"], stdout=subprocess.PIPE, stdin=subset.stdout)
    else:
        subset = subprocess.Popen(["tail","-n", "1"], stdout=subprocess.PIPE, stdin=subset.stdout)
    #import pdb ; pdb.set_trace()
    new = subprocess.check_output(["wgrib2", "-i", infile, "-new_grid_winds","grid",
                                   "-new_grid", "latlon", lonstr, latstr,
                                    outfile], stdin=subset.stdout)
    
def pname_to_standard_name(name):
    d = dict(HGT="geopotential_height", VGRD="y_wind",
             UGRD="x_wind", ABSV="atmosphere_absolute_vorticity", PRMSL="mslp",
             TMP="air_temperature")
    return d[name]

##
# MAIN
##
numTimesteps =  ((duration.total_seconds()-first_fhr*3600) / interval.total_seconds()) + 1
offsets = np.linspace(first_fhr*3600, duration.total_seconds(), numTimesteps)
for fcstOffset in offsets:
    fhr = int(fcstOffset / 3600)
    fmin = int( round(int( fcstOffset / 60 ) % 60, 0) )
    #import pdb ; pdb.set_trace()
    log = _default_log()
    data_topdir = data_topdir_pattern.format(init_date=start_date)
    fcst_offset = tdelta(hours=fhr, minutes=fmin)
    specdata = SpecData(inspec, data_topdir, start_date,
                        fcst_offset=fcst_offset, domain=domain,)
                       # inspecsTopdir=INSPECS_TOPDIR)
    outfile = "nmbtrk.{init_date:%Y%m%d%H}.f{fhr:03d}.{fmin:02d}.grb2"
    outfile = outfile.format(init_date=start_date, fhr=fhr, fmin=fmin)
    for fieldstr in TRACKER_SUBSET:
        #print fieldstr
        if not ":" in fieldstr:
            fieldName = pname_to_standard_name(fieldstr)
            filename = specdata.get_filename(fieldName, "2d")
        else:
            fieldName,lev = fieldstr.split(":")
            fieldName = pname_to_standard_name(fieldName)
            if " m " in lev: 
                levType = "sfcDelta"
                levVal = int(lev.split(" ")[0])
            else:
                levType = "isobaric"
                levVal = int(lev)
            filename = specdata.get_filename(fieldName, levType)
        print 'filename = ', filename
        
        curr_fdate = start_date + fcst_offset
        (cenlat,cenlon) = get_cen_latlon(curr_fdate, find_nearest_fdate)
        south_lat = cenlat - ( (nlats/2) * dy ) 
        west_lon = cenlon - ( (nlons/2) * dx )
        latstr = str(south_lat) + ":" + str(nlats) + ":" + str(dy)
        lonstr = str(west_lon) + ":" + str(nlons) + ":" + str(dx)
        outfile_tmp = outfile + ".tmp"
        subset_and_regrid(filename, outfile_tmp, fieldstr, latstr, lonstr)
        cat_file(outfile_tmp, outfile)
        #output to merged outfile
    if make_grib1:
        subprocess.check_call([grib2_to_grib1_exe, outfile, outfile.replace("grb2","grb1")])
