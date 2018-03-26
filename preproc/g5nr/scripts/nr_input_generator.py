#!/usr/bin/env python

"""
This script does all necessary preprocessing of G5NR- and LIS- generated data
files for use by the NEMS Preprocessing System (NPS). 
This includes:
 - Combining netCDF variables from separate collections of G5NR data into a 
   single file
 - Interpolating 3-D variables to isobaric levels (if option is set)
 - Generating the NPS intermediate format (nps_int) files from the combined 
   files
  -> For derived fields, it can call the appropriate geos2wps utility to
     generate the field.

ASSUMPTIONS
 - While I tried to be as general as possbile, there are a some hacks 
   that assume that:
     * Meterological fields come from G5NR
     *Soil fields come from LIS

USAGE:
 * ./nr_input_generatory.py -c <config> -l <log_level>
 - This program is driven by two config files: (1) The passed-in config file 
   which specifies the paths to find input data, where to write output, 
   date range, etc. (2) Is actually a Python script that specifies which fields
   to use and which fields in g5nr/lis map to which fields in NPS. This is
   ../lib/params.py
 - This script will NOT create the LIS Landmask (which is separate from the
   G5NR fields' Landmask and should be specified as such in the Metgrid.TBL.
   It must be created after using the "create_lis_landsea.py". Note that the
   path is hardcoded in that source file.

ADDITIONAL NOTES:
 - This program can be run in parallel using MPI. If it is, it will distribute
   the dates to process evenly among all workers (including rank 0)
 - Since LIS netCDF files are available separately and have a different
   structure for the lat and lon dimensions, they are not merged with the
   rest of the fields. They are used directly when generating the nps_int files.
 - Theia time for 3 files with old code (g5nr_input_xform.py):  4409.21s user 438.99s 
                                                       system 98% cpu 1:22:01.94 total

TODO:
 - Make more general, while addressing hacks
 - It is not necessary to first create the combined netCDF file (except for debugging
   purposes), so make it possible to just go directly from creating the (possibly
   interpolated) netCDF variable and passing it to the nc2nps (cum ncVar2nps) routine, 
 - Create the LAND_LIS field containing the Landmask for LIS data. Currently it is 
   necessary to do this with "create_lis_landsea.py"

"""
import sys
import os
import re
import logging
from datetime import datetime as dtime
from datetime import timedelta as tdelta
import copy
from ConfigParser import ConfigParser
from optparse import OptionParser
import importlib

import numpy as np
import netCDF4 as nc4
#import matplotlib.pyplot as plt
#from mpl_toolkits.basemap import Basemap

#from interp_routines import linear_interp_to_z
#import pdb ; pdb.set_trace()
from nwpy.interp.vinterp.hwrf_vertical_interp import interp_press2press_lin_wrapper
from params import G5NR_Params as g5nr
from params import GFS_Params as gfs
from params import LIS_Params as lis
from params import NPS_Params as nps_params
from nps import nps_utils
from nps import nps_int_utils

#
# Globals
#
_logger=None

#
# Module functions
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


#
# Classes
#
class DerivedMetField(object):
    """ 
    A DerivedMetField, unlike a MetField, does not have a corresponding
    source file from which to obtain the data directly. The data (and other
    attributes) are dynamically generated during the execution of the program.
    """
    def __init__(self, npsName, deps, units, description):
        # NPS name of the field
        self.nps_name = npsName
        # Dependencies of the field. Should be Field objects
        self.deps = deps
        self._units = units
        self._description = description
    
    def units(self, date=None):
        """hack to make the interface the same as Field object """
        return self._units

    def description(self, date=None):
        """hack to make the interface the same as Field object """
        return self._description

class DirectlyDerivedMetField(DerivedMetField):
    """ Class to identify Derived field as directly-derived. i.e. derived
         from readily-available netCDF variables in the source dataset """
    pass

class IndirectlyDerivedMetField(DerivedMetField):
    """ Class to identify derived field as indirectly-derived. i.e. derived
        after having created the NPS file, using some external library """
    pass

class Field(object):
    def __init__(self, log=None):
        if log is not None:
            self._log = log
        else:
            self._log=_default_log()

    def set_attr_from_infile(self, date, ncAttr=['units','description']):
        """
        Sets class fields based on values of corresponding input file 
        attributes. Will use the netCDF file obtained using get_input_file_path,
        so see comments for that function
        """
        ncFile = self.get_input_file_path(date)
        self._log.debug("Using input file '{}' to set attributes".format(ncFile))
        rootgrp = nc4.Dataset(ncFile, 'r')
        var = self.native_model_name
        for attr in ncAttr:
            self._log.debug("Setting attribute '{}'".format(attr))
            try:
                value = rootgrp.variables[var].getncattr(attr)
            except:
                self._log.debug("Attribute does not exist. Leaving blank")
                value = ""
            setattr(self, "_"+attr, value)

    def units(self, date):
        """
        NOTE : This will only read from the file once, so in the unlikely 
        event that the units are different for different dates, it will
        not return the correct valaue
        """
        if not '_units' in self.__dict__:
            self.set_attr_from_infile(date, ncAttr=['units'])
        return self._units

    def description(self, date):
        """
        Note: See comments for units()
        """
        if not '_description' in self.__dict__:
            self.set_attr_from_infile(date, ncAttr=['description'])
        return self._description


class SoilField(Field):
    def __init__(self, lisName, npsName=None, topdir='.', log=None):
        '''
        Instantiate a SoilField.
        @param lisName name of the variable in LIS
        @param npsName name of the variable in NPS. If not passed in, 
                       attempt will be made to guess using 
                       LIS_Params.LIS_TO_NPS_MAPPINGS.
        @param topdir Top-level directory where LIS output files are
        '''
        super(SoilField, self).__init__(log=log)
        self.lis_name = lisName
        if npsName is not None:
            self._nps_name = npsName
        self.data_topdir = topdir

    @property
    def native_model_name(self):
        return self.lis_name

    @property
    def nps_name(self):
        '''
        This is basically a hack that returns 'ST' or 'SM'. It
        was created since _get_nc2nps_fields_tupple needs an 
        nps_name and we do not know the depths apriori since nc2nps iterates
        through all the levels
        TODO: Find a less hacky solution
        '''
        if '_nps_name' in self.__dict__ and self._nps_name is not None:
            return self._nps_name
        assert self.lis_name in ('SoilMoist_tavg', 'SoilTemp_tavg')
        if self.lis_name == 'SoilMoist_tavg': ret = 'SM'
        elif self.lis_name == 'SoilTemp_tavg': ret = 'ST'
        else: raise Exception("Agony agony agony. Fix this hack")
        self._log.warn("Returning '{}' for 'nps_name' based on lisname {}. "
                       "This is a hack".format(ret, self.lis_name))
        return ret

    def get_nps_name(self, startDepth, endDepth):
        if self._nps_name is not None:
            return self._nps_name
        else:
            if self.lis_name in lis.LIS_TO_NPS_MAPPINGS.keys():
                return lis.LIS_TO_NPS_MAPPINGS[self.lis_name]
            else:
                prefix = nps_utils.lis_name_to_nps_prefix(self.lis_name)
                return nps_utils.get_nps_soil_field_name(
                            prefix, startDepth, endDepth)

    def get_input_file_path(self, date, domNum=1):
        '''
        Get the file path for a given date. The path returned is:
          <self.data_topdir>/YYYYmm/<lis.OUTPUT_FILE_PATTERN>
        @param data Datetime object representing desired date
        @param domNum Domain number - only tested with 1 domain
        '''
        fileName = lis.OUTPUT_FILE_PATTERN.format(date, domNum=domNum)
        subdir = '{:%Y%m}'.format(date)
        return os.path.join(self.data_topdir, subdir, fileName)

class MetField(Field):
    def __init__(self, g5nrName, srcDataset, wpsName=None, num_levels=None,
                 topdir='.', derived=False, log=None):
        '''
        @param g5nrName Name of the field in the g5nr data
        @param srcDataset Name of the g5nr dataset containing the field
        @param wpsName name of the field in WPS/NPS. IF not passed in, it
               can be determined using the get_nps_name()
        @param levels Which levels should be used, if applicable
        @param topdir Top-level directory where input files are located
        @param derived Is this a field that is derived from 2+ others? If you
                       do not know ahead of time, you can change it directly 
                       later
        '''
        super(MetField, self).__init__(log=log)
        self.g5nr_name = g5nrName
        self.src_dataset = srcDataset 
        if wpsName is not None:
            self.nps_name = wpsName
        if num_levels is not None:
            self.num_levels = num_levels
        self.input_data_topdir = topdir
        #self.derived = derived

    @property
    def native_model_name(self):
        return self.g5nr_name

    def get_nps_name(self):
        ''' Get wps field name for given g5nrName. 
            TODO :: Broken: Use g5nr.NPS_2_G5NR 
            and set the `derived' attribute'''
        #return NPS_2_G5NR[
        if g5nr.NR_TO_WPS_MAPPINGS.has_key(self.g5nr_name):
            return NR_TO_WPS_MAPPINGS[self.g5nr_name]
        else:
            raise Exception("Unable to find out NPS name for given G5NR var")
    
    def get_input_file_path(self, fcstDate):
        ''' 
        Get netCDF file name for the given `fcstDate'. For time-averaged fields
        (i.e.  self.src_dataset.startsWith("tavg")), look for the previous
        output, which is 15 minutes earlier, since that represents the average.
        @param fcstDate Forecast date as datetime object
        '''
        prefix = g5nr.FILE_PREFIX
        if self.src_dataset.startswith("const"):
            suffix = ".%Y%m%d.nc4"
        else: 
            suffix = ".%Y%m%d_%H%Mz.nc4"
        if self.src_dataset.startswith("tavg"):
            fcstDate -= tdelta(minutes=15)
        fileName = fcstDate.strftime("{pfx}{ds}{sfx}".format(pfx=prefix, 
                                                             sfx=suffix, 
                                                             ds=self.src_dataset))
        return os.path.join(self.input_data_topdir, self.src_dataset, fileName)

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

def _create_dim_vars(dest_dataset, src_dataset, in_levs=None, log=None):
    """
    Copy the 4 dimension variables (lat,lon,levs,time) from Dataset src_dataset
    to Dataset dest_dataset. If in_levs is passed in, use it instead of the 
    src_dataset's lev values. Since NPS just uses indices for the level
    dimension, you'll want to pass this in (I think)
    @param dest_dataset netCDF4.Dataset that will get the dimension variables
    @param src_dataset netCDF4.Dataset that will provide the dimension variables.
    @param in_levs List of levels to override src_dataset values with
    """
    log.debug("Copying attributes")
    for var in ['lat', 'lon', 'lev', 'time']:
        srcVariable = src_dataset.variables[var]
        _copy_variable_attr(dest_dataset, srcVariable, log=log)
        if var == 'lev' and in_levs is not None:
            log.debug("Overriding input levels with passed in values")
            dest_dataset.variables[var][:] = in_levs
        else:
            dest_dataset.variables[var][:] = srcVariable[:]


def _copy_variable_attr(destDataset, srcVariable, outVarName=None, 
                        dims=None, useZlib=True, log=None):
    '''
    Create a variable in Dataset `dest_dataset' using the attributes
    of src_variable. The name of the variable will be obtained 
    `outVarName'. 
    NOTE : Only attributes are copied, not the data
    @param destDataset nc4.Dataset where the variable will be created
    @param srcVariable nc4.Variable containing the attributes to be copied
    @param outVarName Name of the variable in dest_dataset. If not passed in,
           use the same name as src_varable
    @param dims The dimensions to use in the copied variable. By default, 
           use the same ones as src_varable
    @param useZlib True if zlib compression should be used
    '''
    if outVarName is None:
        outVarName = srcVariable.name
    if dims is None:
        dims = srcVariable.dimensions
    log.debug("Adding variable '{}' to `destDataset'...must be unique. Dims={}"
              .format(outVarName, dims))
    outVar = destDataset.createVariable(
        outVarName, srcVariable.datatype, dims, 
        zlib=useZlib
        #zlib=v.zlib, 
        #complevel=v.complevel, shuffle=v.shuffle,
        #fletcher32=v.fletcher32
                                   )
    inAttrKeys = srcVariable.ncattrs()
    inAttrValues = [ srcVariable.getncattr(k) for k in inAttrKeys]
    #d = {}
    # TODO : Make it possible to override attributes. Specifically, this 
    # is needed because the PRESSURE variable currently copies the 
    # DELP attribute, so it says "pressure_thickness" even though it
    # is not (search for  _copy_variable_attr.*DELP
    for k in inAttrKeys: 
        #d[k] = v.getncattr(k)
        outVar.setncattr(k, srcVariable.getncattr(k))

def get_g5nr_pressure_array(date, rootgrp, topdir, log=None):
    """
    Get the pressure array at the given time from the given dataset.
    The dataset should have a field named 'DELP' that contains the
    pressure thickness (i.e. delta), with the highest level being
    at index 0 and the lowest level at the final index. 
    This method just sums the DELP from the top, since that is what
    the documentation says to do.
    @param rootgrp NC4 Dataset object containing th needed fields
    @param date datetime object representing the date of interest
    @return the 3-d pressure array containing values at model levels,
            in Pascals since that is what NPS uses.
    @param topdir Directory where file containing DELP data can be
           found
    """
    
    '''
    Previous version: I thought we had to add surface pressure (PS):
    m = rootgrp.variables['DELP']
    assert m.getncattr('units') == 'Pa' # this is what NPS uses
    # delp is a 4-d array. We want to cummulatively sum along the
    # level (i.e. second) dimension, starting from the bottom.
    sfc_pres = rootgrp.variables['PS'][0,:,:]
    m = m[0, ::-1, :, :] # note now it's 3d and in descending altitude
    # we need to do a cummulative difference to the top of the atmosphere, 
    # which is the bottom of the array
    m = np.negative(m) 
    m = np.insert(m, 0, sfc_pres, axis=0) # now has sfc pres. at (array) top
    cumm_pres = np.cumsum(m, axis=0) # since m is now 3-d, lev axis=0
    #import pdb ; pdb.set_trace()
    return cumm_pres # i think
    '''

    '''
    #
    # This is to use pressure at the edges for PRESSURE
    #
    fld =  MetField(g5nrName='DELP', srcDataset='inst30mn_3d_DELP_Nv', 
                    topdir=topdir)
    fileName = fld.get_input_file_path(date)
    log.debug("Reading DELP from input file {}".format(fileName))
    inDataset = nc4.Dataset(fileName)
    # round off differences that can affect the interpolation, so round values 
    assert inDataset.variables['DELP'].getncattr('units') == 'Pa' 
    hyb_pres_array = np.cumsum(inDataset.variables['DELP'],axis=1)[0]
    hyb_pres_array = np.round(hyb_pres_array, decimals=5)
    # TODO ? : add units and any other metadata
    # TODO (maybe) : ensure the missingValue here corresponds to that used in NPS
    # TODO ? : DOES NPS need the pressure descending?
    '''
    #
    # This is to use pressure at mid-layer (PL) for PRESSURE
    #
    fld = MetField(g5nrName="PL", srcDataset="inst30mn_3d_PL_Nv", 
                   topdir=topdir)
    fileName = fld.get_input_file_path(date)
    log.debug("Reading PL (mid-layer Pres) from input file {0}".format(fileName))
    inDataset = nc4.Dataset(fileName)
    assert inDataset.variables["PL"].getncattr('units') == 'Pa'
    hyb_pres_array = inDataset.variables["PL"][:]
    
    return hyb_pres_array

def populate_pressure_var(destDataset, hybPresArray=None, interpolate=False,
                        targetLevels=None, datatype=np.dtype('float32')):
        """
        Populate the 'PRESSURE' variable with either the hybrid
        levels specified by input variable `hyb_pres_array' or the target 
        levels specified by input argument `targetLevels' (if `interpolate' 
        is True). Currently, only 1-d arrays are supported for `targetLevels' 
        (i.e. isobaric conversion). Although it will still create a 3-D field
        since that is what NPS expects.
        PRECONDITION: The 'PRESSURE' variable must have already been created
        @param destDataset netCDF4.Dataset containing the unpopulated
                'PRESSURE' variable
        @param hybPresArray 3-D array containing the hybrid pressure levels to
               populate with if `interpolate' is False. Ignored otherwise
        @param interpolate Do we want to interpolate to a different set of 
                levels. Note that there is no interpolation done in this
                function, we simply need to know whether to use the hybPresArray 
                or the targetLevels.
        @param targetLevels The target pressure levels to use, if interpolating
        @param datatype NO LONGER USED ## The datatype to use when filling the Variable
        """
        #print dest_dataset.dimensions
        #print inDataset.variables['DELP'].dimensions
        # TODO : Get levels dynamically instead of hardcoding 
        #         (hint: inDataset.variables['DELP'].dimensions)
        if interpolate:
            for idx,gfsLev in enumerate(targetLevels):
                destDataset.variables['PRESSURE'][0,idx] = gfsLev
        else:
            #print hyb_pres_array[:,1000,1000]
            destDataset.variables['PRESSURE'][0] = hybPresArray
    
def interp_modelLev_to_isobaric(input_array, in_levs, out_levs=None, fill_value=None, 
                        increases_up=True, dataset=None, varName=None, log=None):
    '''
    Interpolate an input array from model to isobaric pressure levels
    TODO : Clean up function signature now that we're using the new interpolation
    function
    @param arr array to interpolate
    @param in_levs the levels that the data is available at
    @param out_levs the levels to interpolate to
    @param fill_value The value to give missing values
    @param increases_up True if the variable increases with height (e.g. temp).
                        False otherwise (e.g. Pressure)
    @return the interpolated array
    '''
    if log is None: log = _default_log()
    log.debug("Interpolating model levels to isobaric levels; var={v}"
              .format(v=varName))
    if out_levs is None:
        out_levs = GFS_LEVELS
    if fill_value is None:
        fill_value = 1.e15
    # ensure variables are of the type expected in interp_to_z
    # TODO ? ensure integer types are same length?
    assert input_array.dtype == np.dtype('float32')
    assert in_levs.dtype == np.dtype('float32')
    assert out_levs.dtype == np.dtype('float32')
    #assert fill_value == np.dtype('float32')
    #inArr = input_array[0]
    #ret = linear_interp_to_z(
    #                    data_in=input_array, #inArr,
    #                    z_in=in_levs, 
    #                    z_out=out_levs, 
    #                    undef=fill_value, 
    #                    increases_up=increases_up)
                        #input_array.shape(1),
                        #input_array.shape(2), input_array.shape(0), 
                        #len(out_levs), 
    extrapolate = True
    #import pdb ; pdb.set_trace()
    # TODO : plevs_in @ wrapper (dataset.variables[plev_var_name][:]) is
    # just a sequence of ints (i.e. range(1,73) for g5nr. It needs
    # the actual pressure array
    # Also, the PRESSURE variable is not present in `dataset', since
    # it is the inDataset
    # Need to have PRESSURE Array AND need dimVars=("lat", "lon", "PRESSURE") and
    # dimDims=("lat","lon", "lev")
    ret = interp_press2press_lin_wrapper(dataset, out_levs, varName,
                                         ("lat","lon", "lev"), 
                                         extrapolate, ignore_lowest=False, 
                                         log=log)
                                         #dimDims)
    return ret

def get_met_field(nps_name, topdir, log=None):
    '''
    @return a MetField or DerivedMetField object for the variable with the given `nps_name`. Use
            the params.G5NR_Params.NPS_2_G5NR mappings to determine the 
            corresponding  NPS parameter and params.G5NR_Params.PARAM_2_COLLECTION
            to determine what collection it belongs to.
            If it is a derived parameter (i.e. multiple G5NR Met fields are used
            to create the NPS field, it will be created as a subclass of 
            DerivedMetField. Namely, a DirectlyDerivedMetField if the 
            DERIVED_VAR_GENERATOR mapping for the field comes from the 
            "nps.conversions" module, and an IndirectlyDerivedMetField otherwise.
    '''
    if log is None: log = _default_log()
    if not nps_name in g5nr.NPS_2_G5NR.keys():
        raise Exception("{} has no mapping in params.G5NR_Params.NPS_2_G5NR"
                        .format(nps_name))
    g5nr_name = g5nr.NPS_2_G5NR[nps_name]
    if g5nr_name is None:
        # derived field
        func = g5nr.DERIVED_VAR_GENERATOR[nps_name]
        deps = g5nr.DERIVED_VAR_DEPENDENCIES[nps_name]
        dep_fields = []
        for fldName in deps:
            depField = get_met_field(fldName, topdir, log)
            dep_fields.append(depField)
        if func.startswith("nps.conversions"): 
            dfClass = DirectlyDerivedMetField
        else:
            dfClass = IndirectlyDerivedMetField
        fld = dfClass(nps_name, dep_fields, units="still unknown", 
                              description="still unknown")
        log.debug("Will use function {func} with input fields {d} to "
                  "generate field {fn}".format(func=func, d=deps, fn=nps_name))
    else:
        # not a derived field
        log.debug("Will use g5nr variable '{}' for NPS variable {}"
                  .format(g5nr_name, nps_name))
        if not g5nr_name in g5nr.VAR_2_COLLECTION.keys():
            raise Exception("{} has no mapping in params.G5NR_Params.VAR_2_COLLECTION"
                            .format(g5nr_name))
        collection = g5nr.VAR_2_COLLECTION[g5nr_name]
        fld = MetField(g5nrName=g5nr_name, srcDataset=collection, wpsName=nps_name,
                       topdir=topdir)
        #if isinstance(g5nr_name, list):
        #    fld.derived = True
        #else:
        #    fld.derived = False
    return fld

def get_soil_field(nps_prefix, topdir, log=None):
    """
    @param nps_prefix Prefix of parameter in NPS (e.g. SM, ST)
    @return a SoilField object for the variable with the given `nps_prefix'. 
            
    """
    try:
        lis_name = lis.NPS_2_LIS[nps_prefix]
    except:
        raise Exception("{} has no mapping in params.LIS_Params.NPS_2_LIS"
                        .format(nps_prefix))
    log.debug("Using {} for NPS variable {}".format(lis_name, nps_prefix))
    fld = SoilField(lisName=lis_name, topdir=topdir)
    return fld

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

def merge_met_field(outVarName, g5nrField, dest_dataset, currDate,
                    interpolate=False, inLevs=None, outLevs=None, log=None):
    """
    Merge an NPS field onto a target dataset, interpolating 3-D variables
    to a different set of levels if necessary
    @param outVarName The name to give the variable in the netCDF file. 
                      Typically this is either the name expected by NPS
                      or the same as 'g5nrField'
    @param g5nrField  A MetField object representing field to be merged.
                      It's important that mappings exist for this field
                      in the G5NR_Params class to know where to retrieve
                      the corresponding netCDF file and what variable name.
    @param dest_dataset netCDF4.Dataset onto which the variable will be merged
    @param currDate datetime.datetime object encapsulating the date of interest
    @param interpolate True if we are interpolating to a different set of
           levels.
    @param inLevs 3-D array containing hybrid pressure levels of source 
                  variable
    @param outLevs    Array specifying the levels to interpolate to. Currently
                      only isobaric levels are tested (so use a 1-d array)
    """
    if log is None: log = _default_log()
    if interpolate and (outLevs is None or inLevs is None):
        raise Exception("If interpolate=True, specify outLevs and inLevs")
    log.debug("Processing NPS field: {}".format(outVarName))
    inPath = g5nrField.get_input_file_path(currDate)
    #outVarName = g5nrField.get_nps_name()
    log.debug("Will retrieve meterological field {} from file {}"
              .format(g5nrField.g5nr_name, inPath))
    inDataset = nc4.Dataset(inPath, 'r')
    srcVar = inDataset.variables[g5nrField.g5nr_name]
    log.debug("Copying attributes for NPS variable {}".format(outVarName))
    _copy_variable_attr(dest_dataset, srcVar, outVarName, log=log) #, dims=dest_dimensions)
    # TODO : Ensure all levels are being copied
    if 'lev' in srcVar.dimensions:
        log.debug("Copying 3-d variable {} to output dataset"
                 .format(g5nrField.nps_name))
        if interpolate:
            log.debug("Interpolating variable {} to isobaric levs"
                     .format(g5nrField.nps_name))
            targetLevs = (1, len(outLevs), srcVar.shape[2],  
                          srcVar.shape[3])
            v_isobaric = np.empty(targetLevs,
                                  order='F', # fortran
                                  dtype=np.dtype('float32'))
            # NOTE: Takes almost 1 minute to get here from the previous debug statment
            v_isobaric[0] = \
                interp_modelLev_to_isobaric(srcVar[0], in_levs=inLevs,
                                            out_levs=outLevs, dataset=inDataset,
                                            varName=g5nrField.g5nr_name, log=log)
            dest_dataset.variables[outVarName][:] = v_isobaric
        else:
            log.debug("Merging 3-d variable '{}' to output dataset"
                     .format(g5nrField.nps_name))
            dest_dataset.variables[outVarName][:] = srcVar[:]
        # test
        print 'verification'
        print 'non_interpolated values: ', srcVar[0,1:6,100,100]
        if interpolate:
            print 'after: ', v_isobaric[0,1:6,100,100]
    
    else: # 2-d var
        if len(dest_dataset.variables[outVarName].dimensions) > 3:
            log.warn("Variable {} has more than 3 dimensions, but "
                     "processing as 2-D".format(outVarName))
        dest_dataset.variables[outVarName][:] = srcVar[:]
    
    inDataset.close()

def create_dims(destDataset, srcDatasetMet, srcDatasetSoil, log, numLevs=None):
    '''
    Create the netCDF dimension variables for destDataset, which
    will have the same levels as the `srcDatasetSoil' and `srcDatasetMet`.
    Only the 'num_soil_layers' will be obtained from the former, unless
    there is not `srcDatasetMet'
    '''
    assert srcDatasetMet is not None or srcDatasetSoil is not None
    log.debug("Creating dimensions for `destDataset'")
    if srcDatasetMet is not None:
        num_lats_src = len(srcDatasetMet.dimensions['lat'])
        num_lons_src = len(srcDatasetMet.dimensions['lon'])
        num_levs_src = len(srcDatasetMet.dimensions['lev'])
  
    num_soil_levs_src = None
    if srcDatasetSoil is not None:
        if srcDatasetMet is None:
            log.debug("Since no met fields, getting lat and lon dimensions "
                      "from soil fields".format())
            num_lats_src = len(srcDatasetSoil.dimensions['lat'])
            num_lons_src = len(srcDatasetSoil.dimensions['lon'])
        # TODO : Generalize this for different soil layer names
        #        Also, I'm working on the assumption that the ST and SM 
        #        layer depths are the same
        num_soil_levs_src = len(srcDatasetSoil.dimensions['SoilMoist_profiles'])
            
    # Create dimensions - hmmm levs are different for different vars
    if numLevs is None:
        numLevs = num_levs_src

    log.debug("Will create the dimensions time, lev, lat, lon, num_soil_layers")
    time = destDataset.createDimension('time', 1)
    lev = destDataset.createDimension('lev', numLevs)
    lat = destDataset.createDimension('lat', num_lats_src)
    lon = destDataset.createDimension('lon', num_lons_src)
    if num_soil_levs_src:
        soil_levs = destDataset.createDimension('num_soil_layers', num_soil_levs_src)
    else:
        soil_levs = None

    return (lat, lon, lev, soil_levs)

def _get_nc2nps_fields_tupple(fieldList, date, metDataTopdir, onlyUnique=True):
    """
    @param fieldList List of Field objects
    @param metDataTopdir Path to find meterological data for the MetFields
    @param onlyUnique If True, only return unique entries. Since <DerivedMetField>s
           may require MetFields that are already in fieldList, we should ignore thm
    @return A list/set of 4-tupples, as expected by the nps_utils.nc2nps routine.
            The 4-tupple consists of (inName, outName, units, description)
            e.g. ('TT','TT','K','air temperature')
            The returned list have one entry for each MetField in fieldList. 
            This list may be larger than ``fieldList'' if there are 
            <DerivedMetField>s in ``fieldList''.
    """
    ret_list = []
    #import pdb ; pdb.set_trace()
    for field in fieldList:
        #mf = get_met_field(fieldName, metDataTopdir)
        # TODO : units and description are not in the metfield, they are in the nc file
        # TODO : See hack in SoilField.nps_name definition
        # TODO : If going direct from g5nr collection to nps_int, need to use the
        #        mf.g5nr_name/lis_name. Since we're merging first, use nps_name
        #ret_list.append( (mf.g5nr_name, mf.nps_name, mf.units(date), 
        #if isinstance(field, DirectlyDerivedMetField):
        #    import pdb ; pdb.set_trace()
        if not isinstance(field, IndirectlyDerivedMetField):
            fields = [field]
        else:
            fields = field.deps
        for fld in fields:
            ret_list.append( (fld.nps_name, fld.nps_name, fld.units(date), 
                              fld.description(date)) )
    if onlyUnique:
        ret_list = set(ret_list)
    return ret_list

def generate_input(startDate, duration, frequency, outFilePattern, npsIntOutDir,
                   makeIsobaric=True, createNpsInt=True, pLevs=None, 
                   metInputDir=None, lsmInputDir=None,
                   metVars=[], lsmVars=[], log=None, extraNpsInt=False,
                   geos2wrf_utils_path=None):
    """
    This is the main method for generating input files compatible with Metgrid
    and NemsInterp, given a given set of meteorological and land surface 
    variables, for a given period of time.
    Use simplecomm to determine subset of dates to process, if applicable
    @param startDate DateTime object representing first date to process
    @param duration TimeDelta object representing duration to prrocess
    @param frequency TimeDelta object representing how frequently to process
    @param outFilePattern String representing the output file path. It will
                          be interpolated with strftime
    @param npsIntOutDir Path to put the intermediate format ("nps_int") files
    @param makeIsobaric True if 3D fields should be interpolated
                        to isobaric pressure levels
    @param createNpsInt True if we should create the NPS intermediate format
                        files
    @param pLevs Pressure levels to interpolate to. Ignored if makeIsobaric is 
                 False
    @param metInputDir Top-level directory in which model data to be used as
                       input resides
    @param lsmInputDir Top-level directory in which land surface  data to be 
                        used as input resides
    @param metVars (NPS names of) meterological fields to process 
    @param lsmVars (NPS names of) soil fields to process 
    @param extraNpsInt If True, ask the nc_to_nps_int routine to create extra, 
           separate nps_int files for each field/level (e.g. for debugging)
    @param geos2wrf_utils_path Path to the geos2wrf utilities. This is needed
           if the utilities will be used to create derived fields
    """

    # determine subset of dates to process by this rank
    comm = simplecomm.create_comm(serial=False)
    frequency = int(frequency.total_seconds())
    duration = int(duration.total_seconds())
    dateRange = range(0, duration+1, frequency)
    all_dates = [startDate + tdelta(seconds=curr) for curr in dateRange]
    if rank == 0:     
        log.debug("Global list of dates to be processed: {}".format(all_dates))
    local_date_range = comm.partition(all_dates, func=partition.EqualLength(), involved=True)
    log.info("List of dates to be processed by this process: {}".format(local_date_range))
    
    for currDate in local_date_range:

        # create list of MetField and SoilFields that need to be processed
        met_fields = []
        lsm_fields = []
        for npsFieldName in metVars:
            fld = get_met_field(npsFieldName, topdir=metInputDir, log=log)
            met_fields.append(fld)
        for npsFieldName in lsmVars:
            lisField = get_soil_field(npsFieldName, topdir=lsmInputDir, log=log)
            lsm_fields.append(lisField)


        # create output file
        outfile = currDate.strftime(outFilePattern)
        tmp_outfile = outfile + '.tmp'
        if os.path.exists(outfile):
            # TODO : Also make sure all variables are present, since config may have
            # changed
            log.info("Skipping existing output file '{}'".format(outfile))
        else:
            # read first MET field to get dimensions
            src_dataset_met = None
            if len(metVars) > 0:
                # This will fail if metVars[0] is a DerivedMetField
                i = 0
                while not isinstance(met_fields[i], MetField): 
                    i+=1
                src_dataset_metfield = met_fields[i]
                #src_dataset_metfield = get_met_field(metVars[i], 
                #                                     topdir=metInputDir, log=log)
                inMetPath = src_dataset_metfield.get_input_file_path(currDate)
                log.debug("Reading met data input file {}".format(inMetPath))
                src_dataset_met = nc4.Dataset(inMetPath, 'r')

            # read first soil field to get num_soil_layers
            src_dataset_soil = None
            if len(lsmVars) > 0:
                src_dataset_soilfield = get_soil_field(lsmVars[0], 
                                                       topdir=lsmInputDir, log=log)
                inSoilPath = src_dataset_soilfield.get_input_file_path(currDate)
                log.debug("Reading soil data input file {}".format(inSoilPath))
                try:
                    src_dataset_soil = nc4.Dataset(inSoilPath)
                except:
                    log.critical("unable to open file `{0}'".format(inSoilPath))
                    sys.exit(3)
            
            # Start populating output
            log.debug('Creating output file {}'.format(tmp_outfile))
            dest_dataset = nc4.Dataset(tmp_outfile, 'w', format="NETCDF4")
            # TODO : The folllowing 4 lines only work if metInputDir passed in
            # -> it's probably not necessary if only processing soil fields
            #    since there is no interpolation
            delp_metfield = MetField(g5nrName='DELP', 
                                     srcDataset=g5nr.VAR_2_COLLECTION['DELP'], 
                                     topdir=metInputDir, log=log)
            delp_dataset = nc4.Dataset(delp_metfield.get_input_file_path(currDate), 'r')

            numOutLevs = delp_dataset.variables['lev'].shape[0]
            out_lev_idc = delp_dataset.variables['lev'][:]

            if makeIsobaric:
                numOutLevs = len(gfs.GFS_LEVELS)
                out_lev_idc = range(1,len(gfs.GFS_LEVELS)+1)

            (lat,lon,lev,soilLevs) = create_dims(dest_dataset, delp_dataset, 
                                                 src_dataset_soil, log=log,
                                                 numLevs=numOutLevs)
            
            _create_dim_vars(dest_dataset, delp_dataset, 
                             in_levs=out_lev_idc, log=log)

            # Create PRESSURE variable ; use DELP attributes 
            # TODO figure out why I can't overwrite the attributes
            hyb_pres_array = get_g5nr_pressure_array(currDate, delp_dataset, 
                                                     topdir=metInputDir, log=log)
            _copy_variable_attr(dest_dataset, delp_dataset.variables['DELP'], 
                                'PRESSURE', log=log)
            #dest_dataset.variables['PRESSURE'].setncattr("long_name", "pressure")
            #dest_dataset.variables['PRESSURE'].setncattr("short_name", "pressure")
            #print dest_dataset.variables['PRESSURE'].ncattrs()
            #dest_dataset.variables['PRESSURE'].setncatts({'long_name':'pressure', 
            #                                              "standard_name":"pressure"})
            # note : targetLevels ignored if interpolate is false
            populate_pressure_var(dest_dataset, hybPresArray=hyb_pres_array, 
                                interpolate=makeIsobaric, targetLevels=gfs.GFS_LEVELS,
                                datatype=delp_dataset.variables['DELP'].datatype)

            # Loop through meterological fields, merging each one's values into 
            # dest_dataset
            # hack: The non directly-derived fields are not added since the
            #       source data is not directly available. The directly-derived
            #       fields are not added either since their values are directly
            #       added to the output dataset
            mergeable_met_fields = []
            directly_derived_met_fields = []
            for metField in met_fields:
                if isinstance(metField, IndirectlyDerivedMetField):
                    # Don't add field (since it doesn't exist), but add dependencies
                    for fld in metField.deps:
                        assert isinstance(fld, Field)
                        if not fld.nps_name in [x.nps_name for x in mergeable_met_fields]:
                            mergeable_met_fields.append(fld) 
                elif isinstance(metField, DirectlyDerivedMetField):
                    # Don't add anything since it won't be merged, it will be 
                    # directly added to the output dataset
                    directly_derived_met_fields.append(metField)
                else:
                    # Non-derived field, so it's mergeable
                    if not metField.nps_name in [x.nps_name for x in mergeable_met_fields]:
                        mergeable_met_fields.append(metField)
            
            # Add variables of directly-derived fields to the dest_dataset
            for metField in directly_derived_met_fields:
                lib_and_func = g5nr.DERIVED_VAR_GENERATOR[metField.nps_name]
                lib_name = lib_and_func[0:lib_and_func.rindex(".")]
                func_name = lib_and_func[lib_and_func.rindex(".")+1:]
                lib = importlib.import_module(lib_name)
                func = getattr(lib, func_name)
                varMaps = {} # map variable name to dataset path, to pass to func
                for dep in metField.deps:
                    #depField = get_met_field(dep, topdir=metInputDir, log=log)
                    #varMaps[dep.g5nr_name] = depField.get_input_file_path(currDate)
                    varMaps[dep.g5nr_name] = dep.get_input_file_path(currDate)
                log.debug("Creating directly-derived variable {name} using "
                          "function {func}"
                          .format(name=metField.nps_name, func=func))
                (data, dims, units, long_name) = func(varMaps=varMaps)
                # Create Variable
                derived_var = dest_dataset.createVariable(metField.nps_name, 
                                                   np.float32, dims, zlib=True)
                derived_var[:] = data
                #setattr(derived_var, "units", units)
                #setattr(derived_var, "long_name", long_name)
                # TODO? Copy other attributes (descr, missing_value, etc.)
                derived_var.setncattr("units", units)
                derived_var.setncattr("long_name", long_name)
                #derived_var.close()

            # Merge fields that are used as-is from source dataset
            for metField in mergeable_met_fields:
                # TODO : if it is a derived field, there will be multiple g5nr fields to process
                g5nrField = metField
                # TODO ? It seems that the memory used to add each variable is not 
                # being freed. Maybe we should close and reopen the dataset 
                # on each iteration.
                dest_dataset.close()
                dest_dataset = nc4.Dataset(tmp_outfile, 'a', format="NETCDF4")
                # note : outLevs ignored if `interpolate' is False
                merge_met_field(g5nrField.nps_name, g5nrField, dest_dataset, 
                                currDate, interpolate=makeIsobaric, 
                                inLevs=hyb_pres_array, outLevs=gfs.GFS_LEVELS, 
                                log=log)
            # LSM fields will be kept separate
            #for npsFieldName in lsmVars:
            #    log.debug("Processing LSM field w/ NPS name={}".format(npsFieldName))

            #print 'before closing', dest_dataset.variables['TT'][0,:,100,100]
            dest_dataset.close()
            log.info("Finished creating merged netCDF4 file.")
            log.debug("Renaming '{}' => '{}'".format(tmp_outfile, outfile))
            os.rename(tmp_outfile, outfile)
        
        # Add pressure field to met_fields; cannot do this earlier since 
        # it we don't want to merge it (it was already copied earlier). 
        presField = MetField('PRESSURE', "None I am derived", wpsName='PRESSURE')
        # Hack: put values for units and descripion; otherwise it will try to 
        # get them from the srcDataset, but there is no srcDataset
        presField._description = "pressure"
        presField._units = "Pa"
        met_fields.append(presField)
        
        # Now convert merged nc4 data to nps_int format for Met fields and soil fields
        #derived = [ f for f in met_fields if f.derived ]
        #non_derived = [ f for f in met_fields if not f.derived ]
        # TODO : Have hardcoded source names here
        #sources = [ ('G5NR',non_derived,outfile), ('G5NR',derived,outfile), ('LIS',lsm_fields,lsm_outfile) ]
        
        # Don't need nps_int files for the diagnostic met fields
        # NOTE: To generate nps_int files for them, they need to have mappings
        # in expected_units in the nps_int_utils module
        #import pdb ; pdb.set_trace()
        nps_met_fields = [f for f in met_fields if not isinstance(f, DirectlyDerivedMetField) \
                          if not f.g5nr_name in nps_params.NPS_DIAGNOSTIC_MET_PARAMS]
        nps_met_fields.extend([f for f in met_fields if isinstance(f, DirectlyDerivedMetField)])

        #sources = [ ('G5NR', met_fields,outfile), ('LIS',lsm_fields,lsm_outfile)]
        sources = [ ('G5NR', nps_met_fields, outfile) ]
        if lsm_fields:
            lsm_outfile = lsm_fields[0].get_input_file_path(currDate) 
            sources.append( ('LIS',lsm_fields,lsm_outfile) )
        
        #import pdb ; pdb.set_trace()
        for srcName,fieldList,filename in sources:
            # PROBLEMS: (1) passing in derived and non_derived, which are lists of MetField
            #               and lsmVars, whicih is a list of string (e.g. ['SM']
            #                 -> i think i just need to make sure _get_nc2nps_fields_tupple works with MetField and SoilField types
            # TODO move this entire block to a separate function
            if len(fieldList) == 0: 
                continue
            log.info("Preparing to convert fields from {}".format(srcName))
            int_file_name = nps_int_utils.get_int_file_name(srcName, currDate)
            int_path = os.path.join(npsIntOutDir, int_file_name)
            # TODO ? Can't remove here because there may be multiple sources with the same
            # target (e.g. non_derived and derived both output to 'G5NR'
            if os.path.exists(int_path):
                log.warn("Appending to existing nps_int file {}".format(int_path))
                #log.info("Removing existing nps_int file {}".format(int_path))
                #os.unlink(int_path)
            #tField = ('TT','TT','K','air temperature')
            fields = _get_nc2nps_fields_tupple(fieldList, currDate, metInputDir)
            xfcst = 0.0 # TODO - figure out if this needs to be actual forecast hour or what
            #if 'derived' in fieldList[0].__dict__ and fieldList[0].derived:
            #    geos2wrf = True
            #else:
            #    geos2wrf = False
            geos2wrf = False # TODO? Set 'derived' for fields being used for geos2wrf utils - this will pose a problem for duplicate fields
            nps_int_utils.nc_to_nps_int(filename, int_path, currDate, xfcst, 
                                        fields, source=srcName.lower(), 
                                        geos2wrf=geos2wrf, 
                                        createIndividualFiles=extraNpsInt,
                                        log=log)
        
        # Now convert derived fields
        # Since we do not know what function we'll be using, we don't know the
        # exact args, so create a dict with all possible args
        inPrefix = "G5NR" # TODO : make this dynamic
        kwargs_gen = { "inPrefix":inPrefix, "outPath":npsIntOutDir, 
                       "currDate": currDate, "geos2wrf_utils":geos2wrf_utils_path,
                       "inDir":npsIntOutDir, "catOutput": True, "log":log }
        derived = [ f for f in met_fields if isinstance(f, IndirectlyDerivedMetField)]
        for field in derived:
            npsFieldName = field.nps_name
            log.debug("Creating derived field {d}".format(d=npsFieldName))
            # get callback corresponding to the NPS field and call it
            #g5nrFields = get_met_field(npsFieldName, topdir=metInputDir, log=log)
            func = g5nr.DERIVED_VAR_GENERATOR[npsFieldName]
            #int_file_name = nps_int_utils.get_int_file_name(npsFieldName, currDate)
            #int_path = os.path.join(npsIntOutDir, int_file_name)
            # -> just use combined int_path generatred above
            log.info("Calling function {} to process derived variable {}. "
                     "Output will be written to {}"
                    .format(func.func_name, npsFieldName, int_path))
            out = func(kwargs_gen)
            if isinstance(out, dict):
                try:
                    field.units = out["units"]
                    field.description = out["description"]
                except KeyError:
                    log.info("Units/description not returned from function {f}"
                             .format(f=func))
            #create_ght_geos2wrf(inPrefix, outPath, currDate, createHGTexePath, inDir=".", modelTop=1.0):
        #dest_dataset = nc4.Dataset(outFileName, 'r')
        #print 'after re-opening', dest_dataset.variables['TT'][0,:,100,100]
        #print 'v_isobaric, ', v_isobaric[0,:,100,100]

##
# MAIN
##
if __name__ == '__main__':
    
    #import pdb ; pdb.set_trace()
    global rank

    confbasic = lambda param: conf.get("BASIC", param)
    confbasicbool = lambda param: conf.getboolean("BASIC", param)

    # read args
    (config_file, log_level) = _parse_args()
    # read config
    conf = ConfigParser()
    conf.readfp(open(config_file))

    start_date = datestr_to_datetime(confbasic("start_date"))
    duration = tdelta(hours = float(confbasic("duration")))
    frequency = tdelta(hours = float(confbasic("frequency")))

    #g5nr = G5NRDataSource()
    #input_fields = g5nr.get_input_fields()
    
    # Create MetField objects to map needed fields to the G5NR datasets they
    # are in - NOT USED, ONLY FOR QUICK LOOK. Using params.py
    '''
    input_fields = [ 
        MetField(g5nrName='H', srcDataset='inst30mn_3d_H_Nv'), 
        MetField(g5nrName='T', srcDataset='inst30mn_3d_T_Nv'), 
        MetField(g5nrName='U', srcDataset='inst30mn_3d_U_Nv'), 
        MetField(g5nrName='V', srcDataset='inst30mn_3d_V_Nv'), 
        MetField(g5nrName='SLP', srcDataset='inst30mn_2d_met1_Nx'), 
        MetField(g5nrName='PS', srcDataset='inst30mn_3d_DELP_Nv'), 
        MetField(g5nrName='PL', srcDataset='inst30mn_3d_PL_Nv'), 
        MetField(g5nrName='QL', srcDataset='inst30mn_3d_QL_Nv'), 
        MetField(g5nrName="QV", srcDataset="inst30mn_3d_QV_Nv"), 
        MetField(g5nrName="RH", srcDataset="inst30mn_3d_RH_Nv"), 
        MetField(g5nrName="TS", srcDataset="inst30mn_2d_met1_Nx"),
        MetField(g5nrName="SNOMAS", srcDataset="tavg30mn_2d_met2_Nx"),
        MetField(g5nrName="HLML", srcDataset="inst30mn_2d_met1_Nx"), # Need be processed w createSurfaceGeo
        MetField(g5nrName="FRSEAICE", srcDataset="tavg30mn_2d_met2_Nx"),
        MetField(g5nrName="FRLAND", srcDataset="const_2d_asm_Nx"), # need to be processed w createLANDSEA
                   ]
   '''
   # TODO : verify fields:
   # *Using PL for PINT, but I don't know if this is correct - 
   #   PINT is 'pressure field on model layer interface'
   #   PL is 'pressure at middle of each layer'
   # * using "surface layer height (HLML) as SOILHGT ("Terrain field of source analysis")
   # * FRSEAICE in g5nr is "fraction of each grid box that is covered by sea ice". I don't
   #   know if this is the same as for HWRF/WPS. 
   #   I checked one of the wrfinput files and it was all 0s.
   #     -> accoridng  to http://www2.mmm.ucar.edu/wrf/users/wrfv3.1/polar-fractional_seaice.html,
   #        for fractional sea ice, the "XICE" field would be used, so I'm guessing SEAICE in 
   #        NPS expects 1/0
   # * FRLAND is the fraction of land in GEOS5, but I think the corresponding
   #   LANDSEA in WPS is discrete (1 for land and 0 for ocean, even though it
   #   is classified as  "proprtn"). I'm using the FRLAND value directly for
   #   now, but it may be necessary to discretize.
   # * SNOMAS in g5nr is defined as "total snow storage land". SNOW in WPS correspdonds
   #   to grib field 65, which is defined as "Water equiv. of accum. snow depth ".
   #   Since both are measures of the water weight (kg/m**2), I think we can just use
   #   SNOMAS directly
   # Also:
   #    the fields processed w create* must still be interpolated here _if_ we are to use
   #    isobaric levels

#    test
#    input_fields = [
#        MetField(g5nrName='T', srcDataset='inst30mn_3d_T_Nv', 
#        MetField(g5nrName='SLP', srcDataset='inst30mn_2d_met1_Nx', levels=[1]),
#                    ]

    
    #_logger = logging.getLogger('nr_input_generator')
    #_logger.setLevel(log_level)
    #formatter = logging.Formatter('%(asctime)s::%(name)s::%(levelname)s::%(message)s')
    #ch = logging.StreamHandler()
    #ch.setLevel(log_level)
    #ch.setFormatter(formatter)
    #_logger.addHandler(ch)a

    # Set config parameters
    outdir = confbasic('output_directory')
    metInputTopdir = confbasic('src_met_output_directory')
    lsmInputTopdir = confbasic('src_lsm_output_directory')
    outfile = confbasic('output_file_pattern')
    expt_id = confbasic('experiment_id')
    make_isobaric = confbasicbool('make_isobaric')
    extra_nps_int = confbasicbool('create_separate_nps_int')
    geos2wrf_utils_path = confbasic("geos2wrf_utils_path")
    # Set up parallelization and logging
    run_parallel = True
    rank = 0
    try:
        from mpi4py import MPI as mpi
        from asaptools import simplecomm,partition,timekeeper
        global_communicator = mpi.COMM_WORLD
        rank = global_communicator.Get_rank()
        # move old log file if it exists
        log_file = "log_{id}_rank{r}.txt".format(id=expt_id, r=rank)
        if os.path.exists(log_file):
            i = 1
            while os.path.exists(log_file+'.'+i):
                continue
            os.path.rename(log_file, log_file+'.'+i)
        log2file_lev = log_level
    except:
        run_parallel = False
        log_file = None # process-specific logger not needed
        log2file_lev = None

    logger = _default_log(name='nr_input_generator', log2stdout=log_level,
                          log2file=log2file_lev, 
                          logFile=log_file)
    if not run_parallel:
        logger.debug("Exception while obtaining rank. Will run serial")
    else:
        logger.debug("My rank == {}".format(rank))

    
    # Create directories
    if rank == 0:
        if not os.path.exists(os.path.join(outdir, expt_id)):
            os.makedirs(os.path.join(outdir, expt_id))
        else:
            logger.warn("Output path '{}' already exists. Data will be overwritten"
                        .format(os.path.join(outdir, expt_id)))
    combined_nc_outdir = os.path.join(outdir, expt_id, "combined_nc")
    nps_int_outdir = os.path.join(outdir, expt_id, 'nps_int')
    out_path = os.path.join(combined_nc_outdir, outfile)
    if rank == 0 and not os.path.exists(nps_int_outdir):
        os.makedirs(nps_int_outdir)
    if rank == 0 and not os.path.exists(combined_nc_outdir):
        os.makedirs(combined_nc_outdir)
    #'g5nr_combined_hires{sfx}'.format(sfx=".%Y%m%d_%H%Mz.nc4"))
    #met_vars = ['TT']
    
    # Run it
    met_vars = nps_params.NPS_REQUIRED_MET_PARAMS
    met_vars.extend(nps_params.NPS_DIAGNOSTIC_MET_PARAMS)
    lsm_vars = nps_params.NPS_REQUIRED_LAND_PARAMS
    generate_input(start_date, duration, frequency, out_path, nps_int_outdir,
                   metInputDir=metInputTopdir, lsmInputDir=lsmInputTopdir, 
                   metVars=met_vars, lsmVars=lsm_vars, makeIsobaric=make_isobaric,
                   extraNpsInt=extra_nps_int, log=logger, 
                   geos2wrf_utils_path=geos2wrf_utils_path)
