"""
Creates GriB files with the fields necessary by the tracker. The 
extents of the generated files will depend on the values given below
under 'SETTINGS'
"""

import os
import subprocess
from datetime import datetime as dtime
from datetime import timedelta as tdelta
import logging
import shutil
import numpy as np

from  nwpy.postproc.io.specdata import objects as specdata_objects
from nwpy.postproc.io.specdata.objects import SpecifiedForecastDataset as SpecData
from pycane.postproc.tracker import utils as trkutils
import produtil
from produtil.cd import TempDir
from produtil.run import mpirun, mpi, openmp, checkrun, bigexe

## 
# SETTINGS
##

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
INSPECS_TOPDIR = "/home/Javier.Delgado/libs/nwpy/conf/inspec"
DATA_TOPDIR = "/home/Javier.Delgado/scratch/nems/g5nr/data/gamma/2j_5500x4000/{init_date:%Y%m%d%H}/postprd_test_wrapper"
START_DATE = dtime(year=2006, month=9, day=4, hour=0)

# ** If first fhr is not on the hour, duration should be adjusted accordingly
#    or rounding will be off and it will look for weird forecast offsets
# duration should be relative to 0, not first_fhr. The program will process
# all fhrs from first_fhr to duration, inclusive
duration = tdelta(hours=300.0) # storm10 starts @ 206hr ; storm8 ends @ 208hr
interval = tdelta(hours=1.0)
first_fhr = 223.0

inspec = [os.path.join(INSPECS_TOPDIR, "grib/upp/upp_nmb_grib2_multifile.conf")]
domain = 1 
TC_VITALS_PATH = "/home/Javier.Delgado/scratch/nems/g5nr/tc_stuff/tc_vitals/geos5trk/{fdate:%Y}_{stormId}.txt"
storm_id = "08L"
find_nearest_fdate = True
NLATS = 830
NLONS = 830
DX = 0.018
DY = 0.018
MAKE_GRIB1 = True # tracker needs grib1

GRIB2_TO_GRIB1 = "/home/Javier.Delgado/local/bin/grib2_to_grib1"

##
# LOGIC
##
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
    path = TC_VITALS_PATH.format(**args)
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
#field = "UGRD|VGRD" # TODO : Need to do these together?
#lonstr = '224:5000:0.027' # northern-point
#latstr = '-21:2500:0.027' # eastern/western point
    #if lev is None: fieldstr = pname
    #else: fieldstr = pname + ":" + str(lev)
    
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
        infile_tmp = "/home/Javier.Delgado/scratch/tmp_winds.grb2"
        if os.path.exists(infile_tmp): os.unlink(infile_tmp)
        #cat_file(infile, infile_tmp)
        #cat_file(infile2, infile_tmp)
        #import pdb ; pdb.set_trace()
        for fld,fil in zip(["UGRD","VGRD"], [infile, infile2]): 
            # NOTE : U must be adjacent to V and come first in the grib file 
            # for the new_grid to work
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
if MAKE_GRIB1: assert os.path.exists(GRIB2_TO_GRIB1)
numTimesteps =  ((duration.total_seconds()-first_fhr*3600) / interval.total_seconds()) + 1
offsets = np.linspace(first_fhr*3600, duration.total_seconds(), numTimesteps)
for fcstOffset in offsets:
    fhr = int(fcstOffset / 3600)
    fmin = int( round(int( fcstOffset / 60 ) % 60, 0) )
    #import pdb ; pdb.set_trace()
    log = _default_log()
    data_topdir = DATA_TOPDIR.format(init_date=START_DATE)
    fcst_offset = tdelta(hours=fhr, minutes=fmin)
    specdata = SpecData(inspec, data_topdir, START_DATE,
                        fcst_offset=fcst_offset, domain=domain,
                        inspecsTopdir=INSPECS_TOPDIR)
    outfile = "nmbtrk.{init_date:%Y%m%d%H}.f{fhr:03d}.{fmin:02d}.grb2"
    outfile = outfile.format(init_date=START_DATE, fhr=fhr, fmin=fmin)
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
        
        curr_fdate = START_DATE + fcst_offset
        (cenlat,cenlon) = get_cen_latlon(curr_fdate, find_nearest_fdate)
        #import pdb ; pdb.set_trace()
        #north_lat = ( (NLATS/2) * DY ) + cenlat
        #east_lon = ( (NLONS/2) * DX ) + cenlon
        south_lat = cenlat - ( (NLATS/2) * DY ) 
        west_lon = cenlon - ( (NLONS/2) * DX )
        latstr = str(south_lat) + ":" + str(NLATS) + ":" + str(DY)
        lonstr = str(west_lon) + ":" + str(NLONS) + ":" + str(DX)
        outfile_tmp = outfile + ".tmp"
        subset_and_regrid(filename, outfile_tmp, fieldstr, latstr, lonstr)
        cat_file(outfile_tmp, outfile)
        #output to merged outfile
    if MAKE_GRIB1:
        subprocess.check_call([GRIB2_TO_GRIB1, outfile, outfile.replace("grb2","grb1")])
