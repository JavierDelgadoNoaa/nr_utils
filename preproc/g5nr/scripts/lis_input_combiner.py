#!/usr/bin/env python

'''
This program reads meteorological variables needed by LIS, which may be
scattered across different geos5-generated output files (i.e. "collections").
The program will combine specified variables onto a single output file. In addition
to combining them, it does the following:
 - Converts U and V componenet wind speeds (ULML and VLML) into a 
   magnitude (SPEEDLML) as needed for LIS
 - Since some of the sources provide instantaneous values and others
   provide time-averaged values, and the time-average values are 
   at 15 and 45 minutes after the hour, we take the average of the
   time-averaged values and use as the :00 and :30 values

This script combines a given set of fields from different netCDF input files
into an output netCDF file. Input fields and their sources, and the output file
name are controlled via global variables. Currently, the output file naming convention
matches the pattern I hardcoded into my modified LIS distribution to support
forcing from G5NR data.

USAGE: lis_input_combiner.py -c <config file> [-l <log level>]

The program can be run in parallel using MPI, in which case the list of dates
to process will be split among workers.

Since LIS must be spun up and the dates specified in the config file correspond 
to the model start/stop date, the start/stop is separately specified in the 
program. (i.e. You MUST specify START_DATE, DURATION, and FREQUENCY) in the 
program. The config file will be used to read the lsm_merged_files_outdir 
(where merged outputs go) and src_met_output_directory (the top level
directory containing geos-5 collections). The params::G5NRParams class
will be used for determining what collections contain what variables, so 
you must ensure that the mappings for all input_variables exist there.
'''

import sys
import os
from ConfigParser import ConfigParser
from optparse import OptionParser
from datetime import datetime as dtime
from datetime import timedelta as tdelta
import copy
import logging
import re

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import netCDF4 as nc4

from field_types import MetField, SoilField
from field_types import get_met_field, get_soil_field


#
# Set global options
#

#START_TIME = dtime(year=2006, month=9, day=10, hour=1, minute=0)
START_TIME = dtime(year=2006, month=5, day=26, hour=6, minute=0)
DURATION = tdelta(days=120)
INPUT_FREQUENCY = tdelta(hours=3)
INPUT_FREQUENCY = tdelta(hours=1)


# specify the number of grid points in the input data
NUM_LATS = 2881
NUM_LONS = 5760

# Prefix of input file to use (c1440_NR. to use full res input file)
G5NR_FILE_PREFIX = "c1440_NR."
# path where input data reside
G5NR_DATA_TOPDIR = os.getcwd()

#
# Globals
#
_logger=None


#
# Functions
#

def _default_log(log2stdout=logging.INFO, log2file=None, logFile=None, 
                 name='el_default'):
    global _logger
    if _logger is None:
        _logger = logging.getLogger(name)
        _logger.setLevel(log2stdout)
        msg_str = '%(asctime)s::%(name)s::%(lineno)s::%(levelname)s - %(message)s'
        msg_str = '%(asctime)s::%(funcName)s::%(filename)s:%(lineno)s::%(levelname)s - %(message)s'
        date_format = "%H:%M:%S"
        formatter = logging.Formatter(msg_str, datefmt=date_format)
        if log2file is not None:
            if logFile is None:
                raise Exception("You must specify a logFile")
            fh = logging.FileHandler(logFile)
            fh.setLevel(log2file)
            fh.setFormatter(formatter)
            _logger.addHandler(fh)
        if log2stdout is not None:
            ch = logging.StreamHandler()
            ch.setLevel(log2stdout)
            ch.setFormatter(formatter)
            _logger.addHandler(ch)
    return _logger


def _create_dim_vars(dest_dataset, src_dataset, in_levs=None):
    '''
    Copy the 4 dimension variables (lat,lon,levs,time) from Dataset src_dataset
    to Dataset dest_dataset.
    '''
    #vars = ['lat', 'lon', 'lev', 'time']
    vars = ['lat', 'lon', 'time']
    for var in vars:
        srcVariable = src_dataset.variables[var]
        _copy_variable_attr(dest_dataset, srcVariable)
        # TODO : UNHACK
        if var == 'lev' and in_levs is not None:
            dest_dataset.variables[var][:] = in_levs
        else:
            dest_dataset.variables[var][:] = srcVariable[:]


def _copy_variable_attr(dest_dataset, src_variable, outVarName=None, dims=None):
    '''
    Create a variable in Dataset `dest_dataset' using the attributes
    of src_variable. The name of the variable will be obtained 
    `outVarName'. 
    NOTE : Only attributes are copied, not the data
    @param dest_dataset nc4.Dataset where the variable will be created
    @param src_variable nc4.Variable containing the attributes to be copied
    @param outVarName Name of the variable in dest_dataset. If not passed in,
           use the same name as src_varable
    @param dims The dimensions to use in the copied variable. By default, 
           use the same ones as src_varable
    '''
    if outVarName is None:
        outVarName = src_variable.name
    if dims is None:
        dims = src_variable.dimensions
    outVar = dest_dataset.createVariable(
        outVarName, src_variable.datatype, dims, 
        zlib=True
        #zlib=v.zlib, 
        #complevel=v.complevel, shuffle=v.shuffle,
        #fletcher32=v.fletcher32
                                   )
    inAttrKeys = src_variable.ncattrs()
    inAttrValues = [ src_variable.getncattr(k) for k in inAttrKeys]
    #d = {}
    for k in inAttrKeys: 
        #d[k] = v.getncattr(k)
        outVar.setncattr(k, src_variable.getncattr(k))

def _create_speedlml_field(output_dataset, date, metInputTopdir):
    '''
    Add the SPEEDLML field to `output_dataset` using the ULML and VLML fields
    '''
    vlmlFld = get_met_field("VLML", topdir=metInputTopdir, log=logger)
    ulmlFld = get_met_field("ULML", topdir=metInputTopdir, log=logger)
    #import pdb ; pdb.set_trace()
    vlml = nc4.Dataset(vlmlFld.get_input_file_path(date)).variables['VLML']
    ulml = nc4.Dataset(ulmlFld.get_input_file_path(date)).variables['ULML']
    v_magn = np.sqrt( ulml[:]**2 + vlml[:]**2 )
    # TODO ? Must the sign of v_magn be changed according to the direction?
    #v_dir = np.arctan2(vlml[:], ulml[:]) * 180/np.pi $ not used
    
    # Create SPEEDLML var using attributes from ULML var
    _copy_variable_attr(output_dataset, ulml, outVarName='SPEEDLML')
    outSpeedLml = output_dataset.variables['SPEEDLML']
    
    # ** Gotta delete the attr first or it will SEGfault when calling close() **
    ##outSpeedLml.setncattr('standard_name', "surface_wind")
    ##outSpeedLml.setncattr('long_name', "surface_wind")
    outSpeedLml.delncattr('standard_name')#, "surface_eastward_wind")
    outSpeedLml.setncattr('standard_name', "surface_wind")
    outSpeedLml.delncattr('long_name')#, "surface_eastward_wind")
    outSpeedLml.setncattr('long_name', "surface_wind")
    
    outSpeedLml[:] = v_magn[:]
    output_dataset.variables['SPEEDLML'][:] = v_magn[:]
    


def _get_file_name(curr_date, log=None):
    '''
    Get the output file name corresponding to the current date
    @param currDate datetime object representing current date being processed
    '''
    global _logger
    if log is None:
        log = _logger
    #aug29.geosgcm_surfh.20060909_2330z.nc4"
    # first, get the prefix. Copying the code in get_geos5fcst.F90::geos5fcstfile()
    mo = curr_date.month
    if mo == 1:
        month = 'jan'
        ftime4 = '01'
    elif mo == 2:
        month = 'jan'
        ftime4 = '31'
    elif mo == 3:
        month = 'mar'
        ftime4 = '02'
    elif mo == 4:
        month = 'apr'
        ftime4 = '01'
    elif mo == 5:
        month = 'may'
        ftime4 = '01'
    elif mo == 6:
        month = 'may'
        ftime4 = '31'
    elif mo == 7:
        month = 'jun'
        ftime4 = '30'
    elif mo == 8:
        month = 'jul'
        ftime4 = '30'
    elif mo == 9:
        month = 'aug'
        ftime4 = '29'
    elif mo == 10:
        month = 'oct'
        ftime4 = '03'
    elif mo == 11:
        month = 'nov'
        ftime4 = '02'
    elif mo == 12:
        month = 'dec'
        ftime4 = '02'  
    else:
		raise Exception("Invalid Month")
    #aug29.geosgcm_surfh.20060909_2330z.nc4"
    #prefix = "{}{}geosgcm_surfh".format(month, ftime4)
    prefix = 'c1440_NR.combined'
    datestr = curr_date.strftime("%Y%m%d_%H%Mz")
    name = "{}.{}.nc4".format(prefix, datestr)
    log.debug("Output file name: {}".format(name))
    return name

def _parse_args():
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="config_file")
    parser.add_option("-l", "--log-level", dest="log_level", default=logging.INFO,
                      help="(0-100 or constant defined in the logging module")
    (options, args) = parser.parse_args()
    try:
        log_level = int(options.log_level)
    except ValueError:
        try:
            log_level = getattr(logging, options.log_level)
        except:
            print 'Unrecognized log level:', options.log_level, '. Not setting.'
    return (options.config_file, log_level)

##
# MAIN
##
if __name__ == '__main__':

    input_fields = ["SWLAND", 'TLML', 'QLML', 'SWGDN', "LWGAB", 'PS', 
                    "PRECTOT", "PRECSNO", "PRECCON", "HLML", "PARDR", 
                    "PARDF"]

    confbasic = lambda param: conf.get("BASIC", param)
    confbasicbool = lambda param: conf.getboolean("BASIC", param)

    # read args
    (config_file, log_level) = _parse_args()
    # read config
    conf = ConfigParser()
    conf.readfp(open(config_file))

    #import pdb ; pdb.set_trace()

    metInputTopdir = confbasic('src_met_output_directory')
    
    # Set up parallelization
    run_parallel = True
    rank = 0
    #try:
    if True:
        from mpi4py import MPI as mpi
        from asaptools import simplecomm,partition,timekeeper
        global_communicator = mpi.COMM_WORLD
        rank = global_communicator.Get_rank()
        # move old log file if it exists
        log_file = "log_rank{}.txt".format(rank)
        if os.path.exists(log_file):
            i = 1
            while os.path.exists(log_file+'.'+str(i)):
                i += 1
                continue
            os.rename(log_file, log_file+'.'+str(i))
        log2file_lev = log_level
    #except:
    if global_communicator.Get_size() == 1:
        run_parallel = False
        log_file = None # process-specific logger not needed
        log2file_lev = None

    logger = _default_log(name='lis_input_combiner', log2stdout=log_level,
                          log2file=log2file_lev, 
                          logFile=log_file)
    if not run_parallel:
        logger.debug("Exception while obtaining rank. Will run serial")
    else:
        logger.debug("My rank == {}".format(rank))



    # determine subset of dates to process by this rank
    comm = simplecomm.create_comm(serial=False)
    frequency = int(INPUT_FREQUENCY.total_seconds())
    duration = int(DURATION.total_seconds())
    dateRange = range(0, duration+1, frequency)
    all_dates = [START_TIME + tdelta(seconds=curr) for curr in dateRange]
    if rank == 0:     
        logger.debug("Global list of dates to be processed: {}".format(all_dates))
    local_date_range = comm.partition(all_dates, func=partition.EqualLength(), involved=True)
    logger.info("List of dates to be processed by this process: {}".format(local_date_range))
    #
    # get it done
    #
#    currDate = copy.copy(START_TIME)
#    while currDate <= START_TIME + DURATION:
    for currDate in local_date_range:
        # create output file ; e.g. "aug29.geosgcm_surfh.20060909_2330z.nc4"
        outFileName = _get_file_name(currDate)
        outdir = confbasic("lsm_merged_files_outdir")
        outfile_path = os.path.join(outdir, outFileName)
        if os.path.exists(outfile_path):
            logger.info("Skipping existing file '{}'".format(outfile_path))
            continue
        temp_outfile_path = outfile_path + '.tmp'
        logger.info("Populating output file {}".format(temp_outfile_path))
        rootgrp = nc4.Dataset(temp_outfile_path, 'w', format="NETCDF4")
        # Create dimensions 
        time = rootgrp.createDimension('time', 1)
        lat = rootgrp.createDimension('lat', NUM_LATS)
        lon = rootgrp.createDimension('lon', NUM_LONS)

        # create the dimension variables from the first input_field
        fld = get_met_field(input_fields[0], topdir=metInputTopdir, log=logger)
        inFileName = fld.get_input_file_path(currDate)
        logger.debug("Reading input file {}".format(inFileName))
        inDataset = nc4.Dataset(inFileName)
        _create_dim_vars(rootgrp, inDataset) #, in_levs=range(1,len(GFS_LEVELS)+1))
        _create_speedlml_field(rootgrp, currDate, metInputTopdir)
        # TODO ? : add units and any other metadata
        # TODO (maybe) : ensure the missingValue here corresponds to that used in LIS

        #rootgrp.close()

        # Loop through fields, outputting each one's values
        for fieldName in input_fields:
            fld = get_met_field(fieldName, topdir=metInputTopdir, log=logger)
            logger.debug("Reading field '{}' from file '{}'"
                         .format(fieldName, fld.get_input_file_path(currDate)))
            inDataset = nc4.Dataset(fld.get_input_file_path(currDate), 'r')
            srcVar = inDataset.variables[fld.g5nr_name]
            outVarName = fld.g5nr_name
            _copy_variable_attr(rootgrp, srcVar, outVarName) #, dims=dest_dimensions)
            rootgrp.variables[outVarName][:] = srcVar[:]
            srcVar = None
            inDataset.close()
        rootgrp.close()
        os.rename(temp_outfile_path, outfile_path)
