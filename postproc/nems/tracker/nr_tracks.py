#!/usr/bin/env python
'''
Draw tracks using the track_plotter module.
This example uses the absolute times of the forecast offsets
instead of the forecast offset itself (i.e. forecast hour)
This is a requirement for g5nrtrk since it has no fhr.
Also plots maxwind and MSLP, using actual dates as x axis labels.
'''

import sys
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import logging as log
from datetime import datetime as dtime
from datetime import timedelta as tdelta
import copy
import matplotlib.dates as mpl_dates

from nwpy.viz.map import bling 
from pycane.postproc.tracker import utils as trkutils 
from pycane.postproc.viz.tcv import tcv_plot_helper, track_plotter 

if __name__ == '__main__':

    names = ['BSNR, NAM physics (GFDL tracker)',
             'BSNR, HWRF physics (GFDL tracker)',
             'G5NR (G5NR tracker)',
            ]
    paths = [ 
              "/home/Javier.Delgado/scratch/nems/g5nr/data/gamma/2j_5500x4000/2006090400/nam_physics/tracker_rundir/fort.69",
              "/scratch4/NAGAPE/aoml-osse/Javier.Delgado/nems/g5nr/data/gamma/2j_5500x4000/2006090400/hwrf_physics/tracker/fort.69",
              "/home/Javier.Delgado/scratch/nems/g5nr/tc_stuff/tc_vitals/geos5trk/2006_08L.txt",
            ]
    colors = [
              "blue",
              "red",
              "green",
             ]
    line_widths = [2.0, 2.0, 4.0]
    #extents = [10, -90, 40, -80] 
    extents = [20, -95, 40, -80] 
    
    #test
    #start_date = dtime(year=2006, month=9, day=8, hour=7)
    #end_date = dtime(year=2006, month=9, day=10, hour=6)
    #interval = tdelta(hours=3)

    first_fhr = 90
    #start_date = dtime(year=2006, month=9, day=7, hour=18)
    start_date = dtime(year=2006, month=9, day=4, hour=00) + tdelta(hours=first_fhr)
    #end_date = dtime(year=2006, month=9, day=13, hour=1) # bsnr last
    #end_date = dtime(year=2006, month=9, day=12, hour=3) # g5nr ends 9/12@3z
    end_date = dtime(year=2006, month=9, day=11, hour=22) # ...and has gap @ 9/11@23z
    interval = tdelta(hours=1)

    # Set the "initialization" dates, which will be used to calculate the 
    # absolute dates.
    # get_geos5trk_track_data() will automatically set start_date to the first
    # date in the 'atcf'
    #paths[0].start_date = ...
    #paths[1].start_date = dtime(year=2006, month=9, day=4, hour=0) # set for gfdltrk too
    # how frequently to mark the line showing the track
    TIME_MARKER_INTERVAL = 6

    log.basicConfig(level=log.DEBUG)

    for i,label in enumerate(names):
        log.info("processing %s @ %s" %(names[i], paths[i]))
        forecast_track = trkutils.get_track_data(paths[i])
        times = []
        datedict = forecast_track.fcst_date_dict
        trkdata_filtered = []
        currdate = copy.copy(start_date)
        while currdate < end_date:
            trkdata = datedict[currdate]
            trkdata_filtered.append(trkdata)
            times.append(mpl_dates.date2num(currdate))
            currdate += interval
        forecast_track.tracker_entries = trkdata_filtered
        #day_idc = [ idx for idx,val in enumerate(fhrs) if val%TIME_MARKER_INTERVAL == 0 ]
        plt.figure("tracks")
        #(lats,lons) = [(trkentry.lat,trkentry.lon) for trkentry in forecast_track.tracker_entries]
        lats = [trkentry.lat for trkentry in forecast_track.tracker_entries]
        lons = [trkentry.lon for trkentry in forecast_track.tracker_entries]
        m = track_plotter.plot_track(lats, lons, 
                                 #windspeeds=maxwinds, 
                                 #indicator_freq=day_idc,
                                 line_color=colors[i], ns_gridline_freq=10, we_gridline_freq=10,
                                 #flagged_idc=flagged_idc,
                                 label=label, 
                                 extents=extents,
                                 line_width=line_widths[i])
        #plt.figure("mslp")
        #plt.plot(mslp_values, color=colors[i], lw=line_width, label=label)
        for metric,yLabel in zip(["mslp_value","maxwind_value"], 
                                 ["MSLP (hPa)", "Max 10m Wind (kts)"]):
            # NOTE : Passing x_label messes up x axis formatting                                
            plt.figure(metric)
            tcv_plot_helper.plot_tcv_metric_vs_fhr(forecast_track, metric, y_label=yLabel,
                                               time_axis_parameter="epoch_zeta",
                                               label=label, color=colors[i],
                                               lw=line_widths[i])
        #plt.legend()
        #plt.figure("maxwind_value")
        #plt.plot(maxwinds, color=colors[i], lw=line_width, label=label)
        #plt.legend()
    plt.figure("tracks")
    bling.decorate_map(m)
    # options worth testing: water_color='#99ffff',lake_color='#99ffff', 
    #continent_color='#cc9966'
    
    # plot legend. The skip_duplicates arg must be True since the plot_track()
    # function actually plots multiple lines for each track in order to vary 
    # the intensity of the color based on the max wind value
    bling.create_simple_legend(skip_duplicates=True, position='upper right')
    
    plt.savefig('tracks.png')

    plt.figure("mslp_value")
    plt.legend(loc='best')
    plt.savefig("mslp.png")

    plt.figure("maxwind_value")
    plt.legend(loc='best')
    plt.savefig("maxwind.png")

