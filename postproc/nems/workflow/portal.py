"""
Make a simple static HTML web portal to display a set of images
previously generated (e.g. with omapy/omapy_wrapper).
The fields for which pages are generated are controlled by 
the settings in params.py.
The time settings and output directory of the generated files
are set in this script under "Set params"
"""

import markup
import os

from params import plotsets, OMAPY_DIST

##
# Set params
##
# What forecast hour to start at
first_fhr = 0
# Duration in hours of forecast
duration = 300
# Frequency in hours between inputs
interval = 3
# Wehere to put generated html files
topdir = "./portal"

def path_to_anchor(path):
    """ Convert '/' separated path to something better suited for anchor tag """
    return path.replace("/","__")

def html_page_header(title=None, cssFiles=None):
    page.init( css=cssFiles, title=title)

def get_plotset_filename(plotset, lev):
    '''
    if lev: 
       s = plotset["plotConfName"] + "_" + str(lev) + "__" + plotset["mapType"] + ".htm"
    else:
       s = plotset["plotConfName"] + "__" + plotset["mapType"] + ".htm"
    '''
    s = plotset["plotConfName"] + "__" + plotset["mapType"] + ".htm"
    return s.format(levValue=lev)

def add_top_nav(first_fhr, duration, interval, filePattern, filePath, lev):
    """
    See doc of add_main_content() for interpolated stuff
    """
    page.div(id_="header")
    for fhr in range(first_fhr, duration, interval):
        fname = filePattern.format(fhr=fhr, levValue=lev)
        fpath = filePath.format(fname=fname)
        #anchor = "#" + fname + fpath
        anchor = "#" + path_to_anchor(fpath)
        page.a("{0:03d}".format(fhr), href=anchor)
    page.div.close()

def add_main_content(first_fhr, duration, interval, filePattern, filePath, lev):
    """
    filePattern interpolated args: 
     - fhr - forecast hour
     - levValue - Level (passed in argument)
    filePath interpolated args:
     - fname - filePattern with args interpolated
    """
    page.div(id_="content")
    page.table()
    for fhr in range(first_fhr, duration, interval):
        fname = filePattern.format(fhr=fhr, levValue=lev)
        fpath = filePath.format(fname=fname)
        #anchor = fname + fpath # NOTE : should match convention in add_top_nav
        anchor = path_to_anchor(fpath) # NOTE : should match convention in add_top_nav
        page.tr()
        page.td("fhr {0:03d}".format(fhr))
        page.td()
        page.a(name=anchor) 
        #page.img(src="{pfx}f{fhr:04d}{sfx}".format(pfx=prefix, fhr=fhr, sfx=suffix))
        page.img(src="{0}".format(fpath))
        page.td.close()
        page.tr.close()
    page.table.close()
    page.div.close()

def add_bottom_nav():
    page.div(id_="footer")
    for plotset in plotsets:
        if "levs" in plotset and plotset["levs"][0] is not None:
            for lev in plotset["levs"]:
                #text = plotset["plotConfName"] + "_" + str(lev) + "(" + plotset["mapType"] + ")"
                text = plotset["plotConfName"] + "(" + plotset["mapType"] + ")"
                text = text.replace("config", "static") # mapType of 'config' is not intuitive
                text = text.format(levValue=lev)
                #link = plotset["plotConfName"] + "_" + str(lev) + "__" + plotset["mapType"] + ".htm"
                link = get_plotset_filename(plotset, lev)
                page.a(text, href=link)
        else:
            text = plotset["plotConfName"] + "(" + plotset["mapType"] + ")"
            text = text.replace("config", "static") # mapType of 'config' is not intuitive
            #link = plotset["plotConfName"] + "__" + plotset["mapType"] + ".htm"
            link = get_plotset_filename(plotset, None)
            page.a(text, href=link)
    page.div.close()

def main():
    global page
    # set params
    #first_fhr = 0
    #duration = 300
    #interval = 3
    # Wehere to put generated html files
    #topdir = "./portal"
    # Make HTML
    #import pdb ; pdb.set_trace()
    for plotset in plotsets:
        # hack: Have something to iterate through
        if not "levs" in plotset: plotset["levs"] = [None]
        for lev in plotset["levs"]:
            page = markup.page()
            html_page_header(title="Nature Run Portal", cssFiles=("nature.css"))
            #import pdb ; pdb.set_trace()
            add_top_nav(first_fhr, duration, interval, plotset["fname"], plotset["outpath"], lev)
            add_main_content(first_fhr, duration, interval, plotset["fname"], plotset["outpath"], lev)
            add_bottom_nav()

            filename = get_plotset_filename(plotset, lev)
            #with open("portal.htm", 'w') as f:
            filepath = os.path.join(topdir, filename)
            with open(filepath, 'w') as f:
                f.write(str(page))

if __name__ == "__main__":
   main()
