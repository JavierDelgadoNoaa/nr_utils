"""
This module contains the datatypes used by multiple utilities. 
They include:
 Field: An encapsulation of a field/variable obtainable from an input source. Currently,
        only netCDF input sources are supported.
   Subclasses:
       SoilField: A field from a land surface model. Only netCDF fields from LIS supported
       MetField: A meteorological field from an atrmosphic model. Only
                 netCDF files from G5NR are supported.
"""
import sys
import os
import logging

from params import G5NR_Params as g5nr
from params import GFS_Params as gfs
from params import LIS_Params as lis
from params import NPS_Params as nps_params
from nps import nps_utils
from nps import nps_int_utils

from datetime import timedelta as tdelta
from datetime import datetime as dtime

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

def get_met_field(nps_name, topdir, log=None):
    '''
    @return a MetField object for the variable with the given `nps_name`. Use
            the params.G5NR_Params.NPS_2_G5NR mappings to determine the 
            corresponding  NPS parameter and params.G5NR_Params.PARAM_2_COLLECTION
            to determine what collection it belongs to.
            If it is a derived parameter (i.e. multiple G5NR Met fields are used
            to create the NPS field, the `derived' attribute of the MetField 
            will be set.
    '''
    if not nps_name in g5nr.NPS_2_G5NR.keys():
        raise Exception("{} has no mapping in params.G5NR_Params.NPS_2_G5NR"
                        .format(nps_name))
    g5nr_name = g5nr.NPS_2_G5NR[nps_name]
    if log is None:
        log = _default_log()

    log.debug("Will use g5nr variable '{}' for NPS variable {}"
              .format(g5nr_name, nps_name))
    if not g5nr_name in g5nr.VAR_2_COLLECTION.keys():
        raise Exception("{} has no mapping in params.G5NR_Params.VAR_2_COLLECTION"
                        .format(g5nr_name))
    collection = g5nr.VAR_2_COLLECTION[g5nr_name]
    fld = MetField(g5nrName=g5nr_name, srcDataset=collection, wpsName=nps_name,
                   topdir=topdir)
    if isinstance(g5nr_name, list):
        fld.derived = True
    else:
        fld.derived = False
    return fld

def get_soil_field(nps_prefix, topdir, log=None):
    """
    @param nps_prefix Prefix of parameter in NPS (e.g. SM, ST)
    @return a SoilField object for the variable with the given `nps_prefix'. 
            
    """
    if log is None:
        log = _default_log()
    try:
        lis_name = lis.NPS_2_LIS[nps_prefix]
    except:
        raise Exception("{} has no mapping in params.LIS_Params.NPS_2_LIS"
                        .format(nps_prefix))
    log.debug("Using {} for NPS variable {}".format(lis_name, nps_prefix))
    fld = SoilField(lisName=lis_name, topdir=topdir)
    return fld


class Field(object):
    def __init__(self, log=None):
        if log is not None:
            self.log = log
        else:
            self.log=_default_log()

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
        el_path = os.path.join(self.data_topdir, subdir, fileName)
        self.log.debug("Reading file {}".format(el_path))
        return el_path

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
        self.derived = derived

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
        el_path = os.path.join(self.input_data_topdir, self.src_dataset, fileName)
        self.log.debug("input_file_path = {}".format(el_path))
        return el_path

