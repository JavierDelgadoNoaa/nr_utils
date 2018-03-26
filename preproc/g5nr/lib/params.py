import numpy as np
from nps import nps_int_utils, nps_int_generators

class G5NR_Params:
    '''
    This class stores parameters related to the G5NR, including mappings
    between NPS/WPS variables and G5NR/GEOS5 parameters/functions. See
    comments for individual parameters for details.
    '''

    # Define mappings to g5nr field name from WPS/NPS field name.
    # For derived variables, the value should be None and there should
    # be a corresponding entry in DERIVED_VAR_DEPENDENCIES and 
    # DERIVED_VAR_GENERATOR so specify how the variable is created.
    # Note that this means that derived fields cannot be functions of 
    # avariable with the same name. e.g. RH cannot depend on RH. If
    # that's the case, this scheme (of using None) won't work
    NPS_2_G5NR = { 
        #'GHT' : None,   # to use derived
        'HGT' : "H",  
        'TT' : 'T',
        'UU' : 'U',
        'VV' : 'V',
        'PMSL' : 'SLP',
        'PSFC' : 'PS',
        'QC' : 'QL', 
        'SPECHUMD' : 'QV',
        'RH' : 'RH',
        'SKINTEMP' : 'TS',
        #'LANDSEA' : 'FRLAND', # needs landice too
        "LANDSEA" : None,
        'SNOW' : 'SNOMAS', 
        #'SOILHGT' : 'HLML', # this appears to be geometric hgt
        "SOILHGT" : None,
        #'PINT': None,  # TODO : add generator for this; I don't think it's used with non-spectral anyway
        #'SEAICE': ['FRLAKE', 'FROCEAN'],
        'SEAICE': 'FRSEAICE', #tavg30mn_2d_met2_Nx
        #'TSOIL1': 'ST000010', #tavg30mn_2d_met2_Nx  <- not compatible with WPS
        "DELP": "DELP", # doesn't exist in NPS, but needed to create HGT
        "U10M": "U10M", # ditto ; diagnostic
        "V10M": "V10M", # ditto ; diagnostic
        "PRECCON": "PRECCON", # diagnostic
        "PRECTOT": "PRECTOT", # ditto
        "TQL": "TQL", # ditto
        "W": "W", # ditto
        "FRLAND" : "FRLAND", # dependency
        "FRLANDICE" : "FRLANDICE", # dependency
        "PHIS" : "PHIS", # dependency
                 }
    # I don't know if these have mappings in NPS. I just need them for LIS
    for fld in ["SWLAND", 'TLML', 'QLML', 'SWGDN', "LWGAB", 'PS',
                     "PRECTOT", "PRECSNO", "PRECCON", "HLML", "PARDR",
                     "PARDF", "ULML", "VLML"]:
        NPS_2_G5NR[fld] = fld
        

    # Map NPS variables to the functions that create  them using the
    # parameters specified in the mapping for said NPS variable in 
    # NPS_2_G5NR.
    # These mappings should point to functions that create the derived
    # values in netCDF.
    NC_2_NC = {}

    # Map NPS variables to the functions that create  them using the
    # parameters specified in the mapping for said NPS variable in
    # NPS_2_G5NR.
    # These mappings should point to functions that go from netCDF directly
    # to the intermediate file format (e.g. the createVAR utils from geos2wrf)
    NC_TO_INT = \
      {
        #'SEAICE' : nps_int_utils.create_seaice    
        'foo':'bar'
      }

    # Map NPS variables that are not directly available in the source dataset
    # (i.e. derived variables) to the functions that create them.
    # Variables that work directly with the netCDF files _MUST_ use
    # functions from a submodule of 'nps.conversions'. Otherwise, modifications to 
    # nr_input_generator are necessary. See the get_met_field() function
    DERIVED_VAR_GENERATOR = \
        { 
            #"GHT": nps_int_generators.create_ght_geos2wrf // BROKEN; just using native H from g5nr
            "LANDSEA": "nps.conversions.from_geos5.create_landsea",
            "SOILHGT": "nps.conversions.from_geos5.create_soilhgt",
            #"foo":"bar"
        }
    
    # Map derived variables to the variables they depend on. For derived variables
    # that work on the nps_int data, use the NPS variable name (since that's
    # what it'll be in the nps_int file.
    # For variables that work on the G5NR data, use G5NR variable names of the 
    # variables they depend on
    DERIVED_VAR_DEPENDENCIES = \
      { 
        "GHT": ["TT", "DELP", "SOILHGT", "SPECHUMD"],
        "LANDSEA": ["FRLAND", "FRLANDICE"],
        "SOILHGT": ["PHIS"],
      }

    # specify the indices of the levels from the g5nr that we want to use
    # in the output files
    HYBRID_LEVELS = 72
    G5NR_PRES_LEVELS = range(1,49)

    # Prefix of input file to use (c1440_NR. to use full res input file)
    FILE_PREFIX = "c1440_NR."

    VAR_2_COLLECTION = {
        'H' : 'inst30mn_3d_H_Nv', 
        'T' : 'inst30mn_3d_T_Nv', 
        'U' : 'inst30mn_3d_U_Nv', 
        'V' : 'inst30mn_3d_V_Nv', 
        'SLP' : 'inst30mn_2d_met1_Nx', 
        'PS' : 'inst30mn_3d_DELP_Nv', 
        'PL' : 'inst30mn_3d_PL_Nv', 
        'QL' : 'inst30mn_3d_QL_Nv', 
        "QV" : "inst30mn_3d_QV_Nv", 
        "RH" : "inst30mn_3d_RH_Nv", 
        "TS" : "inst30mn_2d_met1_Nx",
        "SNOMAS" : "tavg30mn_2d_met2_Nx",
        "HLML" : "inst30mn_2d_met1_Nx", # Need be processed w createSurfaceGeo
        "FRSEAICE" : "tavg30mn_2d_met2_Nx",
        "FRLAND" : "const_2d_asm_Nx", # need to be processed w createLANDSEA
		"FROCEAN" : "const_2d_asm_Nx",
        "FRLAKE" : "const_2d_asm_Nx",
        'DELP' : 'inst30mn_3d_DELP_Nv',
        "SWLAND" : "tavg30mn_2d_met2_Nx",
        "TLML" : 'inst30mn_2d_met1_Nx',
        "QLML" : 'inst30mn_2d_met1_Nx',
        "SWGDN" : "tavg30mn_2d_met3_Nx",
        "LWGAB" : "tavg30mn_2d_met3_Nx",
        "PS" : "inst30mn_3d_DELP_Nv",
        "PRECTOT" : "inst30mn_2d_met1_Nx",
        "PRECSNO" : "inst30mn_2d_met1_Nx",
        "PRECCON" : "inst30mn_2d_met1_Nx",
        "HLML" : "inst30mn_2d_met1_Nx",
        "PARDR" : "tavg30mn_2d_met3_Nx",
        "PARDF" : "tavg30mn_2d_met3_Nx",
        "ULML" : "inst30mn_2d_met1_Nx",
        "VLML" : "inst30mn_2d_met1_Nx",
        "U10M": "inst30mn_2d_met1_Nx",
        "V10M": "inst30mn_2d_met1_Nx",
        "PRECCON": "tavg30mn_2d_met3_Nx", # also in inst_met1
        "PRECTOT": "tavg30mn_2d_met3_Nx", # ditto
        "TQL": "inst30mn_2d_met1_Nx", # diag
        "W": "inst30mn_3d_W_Nv",  # diag
        "FRLANDICE" : "const_2d_asm_Nx",
        "PHIS" : "const_2d_asm_Nx", 
       }

class GFS_Params:
    # Isobaric pressure levels for which GFS produces temperature data. 
    # Obtained from the grib and gribB files for PROD 2015 on hwrfdata
    GFS_LEVELS = np.array([1., 2., 3., 5., 7., 10., 20., 30., 50., 70., 100.,
                  125., 150., 175., 200., 225., 250., 275., 300., 325., 
                  350., 375., 400., 425., 450., 475., 500., 525., 550., 
                  575., 600., 625., 650., 675., 700., 725., 750., 775., 
                  800., 825., 850., 875., 900., 925., 950., 975., 1000. ], 
                  dtype=np.dtype('float32'))
    # Everything is in Pa, so multiply
    GFS_LEVELS = GFS_LEVELS * 100.

class LIS_Params(object):
    # Map variables from their LIS name to their NPS name
    LIS_TO_NPS_MAPPINGS = {}
    # Pattern of output files
    OUTPUT_FILE_PATTERN  = "LIS_HIST_{:%Y%m%d%H%M}.d{domNum:02d}.nc"
    # Map NPS field names to their LIS counterparts
    NPS_2_LIS = {
        "ST":"SoilTemp_tavg", # suffix will be set in nc2nps
        "SM":"SoilMoist_tavg" # ditto
                }

class NPS_Params(object):
    # Mandatory NPS atmospheric fields
    #NPS_REQUIRED_MET_PARAMS = ['HGT', 'TT', 'UU', 'VV', 'PMSL', 'PSFC', 'QC',
    #NPS_REQUIRED_MET_PARAMS = ['TT', 'UU', 'VV', 'PMSL', 'PSFC', 'QC',
    #NPS_REQUIRED_MET_PARAMS = ['HGT', 'TT', 'UU', 'VV', 'PMSL', 'PSFC', 'QC',
    NPS_REQUIRED_MET_PARAMS = ['HGT', 'TT', 'UU', 'VV', 'PMSL', 'PSFC',  # Do we need QC?
                               #'RH', 'SKINTEMP', 'LANDSEA',
                               'SPECHUMD', 'RH', 'SKINTEMP', 'LANDSEA',
                               'SNOW', 'SOILHGT', 'SEAICE']
                               #'SNOW', 'SOILHGT', 'PINT', 'SEAICE']
                               #'SNOW', 'SOILHGT', 'PINT']
    #NPS_REQUIRED_MET_PARAMS = ["TT"]                               
    NPS_DIAGNOSTIC_MET_PARAMS = [ "U10M", "V10M", "PRECTOT", "PRECCON", "TQL" ] #, "W" ]
    #NPS_DIAGNOSTIC_MET_PARAMS = []
    # TODO? SPECHUMD should be diagnostic, since I think it's only used by NPS when using spectral data

    # For Testing
    #NPS_REQUIRED_MET_PARAMS = ["RH", 'HGT', 'PMSL', 'PSFC', 'SPECHUMD', 'SKINTEMP'] # test
    #NPS_REQUIRED_MET_PARAMS = ["SKINTEMP", "SOILHGT", "LANDSEA"]
    #NPS_REQUIRED_MET_PARAMS = ["SKINTEMP"] # !! Need at least one MetField (i.e. not a derived field)
    #NPS_DIAGNOSTIC_MET_PARAMS = []
    
    # Mandatory NPS land surface fields
    #NPS_REQUIRED_LAND_PARAMS = ['SM', 'ST']
    NPS_REQUIRED_LAND_PARAMS = []
    
