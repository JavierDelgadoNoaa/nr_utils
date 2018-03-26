#!/usr/bin/env python
"""
Download a given set of g5nr data. The data that is downloaded is specified
via variables. To minimize hits to the file system, a "database" is used to
keep track of files that have been successfully downloaded on previous runs.
Also, the script will check if a file to be downloaded already exists and if so,
skips it. Therefore, if you want to overwrite files, you must delete them 
first.

HTTP and FTP are supported, although HTTP has not been tested in a while.

NOTE: The directory structure is organized into year/month/day

USAGE: 
  collection_retriever.py <database file>. The database file is a pickled List
  containing files that have already been downloaded. This is done to reduce
  the queries to the filesystem. If the file does not exist, it will be created.


KNOWN ISSUES
- The 'const' datasets are only provided once a day. The current code
  does not work around this, which results in a  lot of "skipping..." messages
"""

import sys
import os
from datetime import datetime as dtime
from datetime import timedelta as tdelta
from urlparse import urlparse
import time
import ftplib
import pickle

# Note : Slight path difference with HTTP and FTP
http_host = "g5nr.nccs.nasa.gov"
http_topdir = "data/DATA"
ftp_host = 'ftp.nccs.nasa.gov'
ftp_user = "G5NR"
ftp_topdir = 'Ganymed/7km/c1440_NR/DATA/'

use_http = False

output_directory = '/scratch4/NAGAPE/aoml-osse/Javier.Delgado/nems/g5nr/data/raw_collections'
res = '0.0625_deg'
prefix = "c1440_NR."

year= 2006 #/Y2006/

month= 9 #M09
#month= 9 #M09
#month= 7 #M09

day= 1 #D10/

start_time = 0

interval = 3 * 3600
#interval = 1 * 3600

end_time = 10 * 3600 * 24
#end_time = 26 * 3600 * 24

## !!
# tavg30mn_2d_met2_Nx/ only available at 15 and 45 passed the hour
# There is no hour on the const_2d_asm_Nx - e.g. c1440_NR.const_2d_asm_Nx.20060910.nc4  
# - gotta download separately in the t loop. 

# for NEMS
datasets = [
            "inst30mn_3d_H_Nv",
            "inst30mn_3d_T_Nv", 
            "inst30mn_3d_U_Nv", 
            "inst30mn_3d_V_Nv", 
            "inst30mn_3d_RH_Nv", 
            "inst30mn_3d_QL_Nv",
            "inst30mn_3d_QV_Nv",
            "inst30mn_2d_met1_Nx",
            "inst30mn_3d_DELP_Nv",
            "tavg30mn_2d_met2_Nx",
            "tavg30mn_2d_met3_Nx",
            "const_2d_asm_Nx",
            "inst30mn_3d_PL_Nv",
           ]
# for LIS
#datasets =  [
#    "tavg30mn_2d_met2_Nx",
#    "inst30mn_2d_met1_Nx",
#    "tavg30mn_2d_met3_Nx",
#    "inst30mn_3d_DELP_Nv",
#            ]

# make sure database file was passed in
if len(sys.argv) < 2:
    print "USAGE: {} <database (hint: pickle file)>".format(sys.argv[0])
    sys.exit(1)
db_file = sys.argv[1]

if use_http is False: # assume FTP
    for i in range(10):
        ftp = None
        try:    
            ftp = ftplib.FTP(ftp_host, user=ftp_user)
            break
        except ftplib.error_temp:
            print 'Error establishing FTP connection. Will retry in 5 mins'
            time.sleep(300)
    if ftp is None:
        print 'Unable to establish FTP connection. Giving up'
        sys.exit(1)

already_there = []
if os.path.exists(db_file):
    print "Loading 'database' of downloaded datasets"
    already_there = pickle.load(open(db_file, 'rb'))
ymd = dtime(year=year, month=month, day=day)
for dataset in datasets:
    if dataset.startswith("inst"):
        tag = "inst"
    elif dataset.startswith("tavg"):
        tag = "tavg"
    elif dataset.startswith("const"):
        tag = "const"
    else:
        print 'unknown tag'
        sys.exit(3)
    
    out_dir = os.path.join(output_directory, dataset)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    for t in range(start_time, end_time, interval):
        fcstDate = ymd + tdelta(seconds=t)
        if tag == "tavg":
	    # Time-averaged data is at 15 and 45 passed the hour, as it 
	    # reflects the average for the 30 minute period
	    # TODO : Do we need to adjust the values to account for the 
	    # fact that it it is an average?
    	    fcstDate = fcstDate - tdelta(minutes=15)

    	if tag == 'const':
            suffix = ".%Y%m%d.nc4"
        else:
            suffix = ".%Y%m%d_%H%Mz.nc4"

        fileName = fcstDate.strftime("{pfx}{ds}{sfx}".format(pfx=prefix, sfx=suffix, ds=dataset))
        if use_http:
            host=http_host
            topdir=http_topdir
            scheme = "http"
        else:
            host = ftp_host
            topdir = ftp_topdir
            scheme = "ftp"
        url = fcstDate.strftime("{scheme}://{host}/{topdir}/{res}/{tag}/{ds}/Y%Y/M%m/D%d/{fileName}".format(
                                 scheme=scheme, host=host, topdir=topdir, 
                                 res=res, tag=tag, ds=dataset, fileName=fileName))
        parsed = urlparse(url)
        src_fileName  = os.path.basename(parsed.path)
        dest_fileName = os.path.join(out_dir, src_fileName)
        
        if src_fileName in already_there:
            print '%s already downloaded according to database. Skipping' %fileName
            continue
        elif os.path.exists(dest_fileName):
            print '%s File exists. Adding to database and skipping' %fileName
            already_there.append(src_fileName)
            continue
        else:
            if use_http:
                os.system("wget -P {} {}".format(out_dir, url))
            else:
                #p = parsed.path

                sys.stdout.write('get: {}\n'.format(parsed.path))
                #import pdb ; pdb.set_trace()
                try:
                    ftp.cwd(os.path.dirname(parsed.path))
                    #'/Ganymed/7km/c1440_NR/DATA/0.5000_deg/inst/inst01hr_3d_T_Cv/Y2006/M09/D18')
                    #'250 OK. Current directory is /Ganymed/7km/c1440_NR/DATA/0.5000_deg/inst/inst01hr_3d_T_Cv/Y2006/M09/D18'
                    #>>> ftp.retrlines("LIST")
                    cmd = 'RETR {srcFile}'.format(srcFile=src_fileName)
                    #temp_fileName = '.tmp_{}'.format(src_fileName)
                    temp_fileName = dest_fileName + ".tmp"
                    callback = open(temp_fileName, 'wb').write
                    ftp.retrbinary(cmd, callback)
                    os.rename(temp_fileName, dest_fileName)
                    already_there.append(src_fileName)
                except Exception as e:
                    print 'Exception occurred: {}'.format(e) #.strerror)
                    print 'dumping database'
                    pickle.dump(already_there, open(db_file, 'wb'))
                    sys.exit(1)
                    #raise e
            # Dump the database - in case lightning strikes
            pickle.dump(already_there, open(db_file, 'wb'))
