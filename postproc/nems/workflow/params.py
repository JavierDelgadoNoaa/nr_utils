"""
Settings for portal.py and for the omapy wrapper script
"""

import os


#
# CONSTANTS
#

# Path to omapy
OMAPY_DIST = "/home/Javier.Delgado/apps/omapy/stable"


#
# PARAMETERS
#

# Storm identifier
storm_id = "08L"

# Directory to put things under
omapy_outdir = "./bsnr/hwrf_physics"

# Standard levels to plot for 3-d plots - will be used by any 
# plotset with a "levs" key
std_levs = 250, 500, 850


"""
Define plotsets. Each plotset is a dictionary with the following keys:
REQUIRED
plotConfName - Name of the plot configuration section to be processed 
               (i.e. in main.conf or auxconf).
               You may specify {lev} here and it will be expanded according
               to the "lev" paramter
fname - (Omapy) output file name. The following strings are interplated:
          fhr: forecst hour
OPTIONAL
outpath - Output path. The following strings are interpolated:
              fname - The omapy output file name
          NOTE: Must be a full path including file name, not just a directory
auxconf - Aux. config file containing the plot config. If specified, it
          will be passed as a command line argument.
mapTypes - Map type to generate. Can be any of the map_extents_setting 
           supported by Omapy
levs - List of levels to generate plots for for this plotset
"""
t_basin = dict(plotConfName="t_filled_{levValue}", levs=std_levs, 
         auxconf=os.path.join(OMAPY_DIST,"conf/plots/temp.conf"), 
         fname="air_temp_{levValue}_f{fhr:04d}.png", mapType="config",
         outpath="basin/{fname}")
mslp_basin = dict(plotConfName="mslp_filled", fname="mslp_f{fhr:04d}.png", 
         mapType="config", outpath="basin/{fname}")
mslp_storm = dict(plotConfName="mslp_filled", fname="mslp_f{fhr:04d}.png", 
         mapType="storm_centric", outpath="storm/{fname}")
ght500_basin = dict(plotConfName="ght_line", levs=[500],
         auxconf=os.path.join(OMAPY_DIST, "conf/plots/geoheight.conf"),
         fname="ght_f{fhr:04d}.png", mapType="config",
         outpath="basin/{fname}")
prate_storm = dict(plotConfName="precip_rate", fname="prate_f{fhr:04d}.png",
                   mapType="storm_centric", outpath="storm/{fname}")
mslpLine_storm = dict(plotConfName="mslp_line", fname="mslpLine_f{fhr:04d}.png", 
         mapType="storm_centric", outpath="storm/{fname}")

plotsets = [  mslp_basin, mslp_storm, mslpLine_storm, ght500_basin, prate_storm ]

# override to do just storm-centric ones
#plotsets = [p for p in plotsets if p["mapType"] == "storm_centric"]


#plotsets = [   mslpLine_storm ]

