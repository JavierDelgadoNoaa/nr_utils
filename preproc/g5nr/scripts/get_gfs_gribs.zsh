#!/usr/bin/env zsh
##
# Extract the grib files for forecast hours 00-18 from a series of GFS 
# forecasts located in <TOPDIR> and with corresponding initialization date
# and duration and interval.
#
# In our basin scale nature run, we use the GFS soil moisture and soil 
# temperature fields since LIS has not been producing reasonable values.
##

zmodload zsh/datetime

TOPDIR="/scratch3/NAGAPE/jcsda-osse/Sean.Casey/archive/baseline0/nocosmic"
START_DATE="9/6/2006 00:00"
DURATION=$(( 10 * 24 * 3600 ))
INTERVAL=$(( 24 * 3600 )) # GFS runs daily

start_epoch=`date --date="$START_DATE" +%s`

for (( t=0 ; t<=$DURATION ; t+=$INTERVAL )) ; do
    curr_epoch=$(( $start_epoch + $t ))
    year=`strftime "%Y" $curr_epoch`
    month=`strftime "%m" $curr_epoch`
    day=`strftime "%d" $curr_epoch`
    hour=`strftime "%H" $curr_epoch`
    
    fil=gfs$year$month$day$hour.tar
    tar xvf $TOPDIR/$fil --exclude=a\* --exclude=b\* --exclude=f\* --exclude=s\* --exclude=S\* --exclude=pgbanl\* --exclude=gsi\* --exclude="pgbf???.gfs*" --exclude=pgbq\* --exclude=pgbf2\* --exclude=pgbf3\* --exclude=pgbf4\* --exclude=pgbf5\* --exclude=pgbf6\* --exclude=pgbf7\* --exclude=pgbf8\* --exclude=pgbf9\* --exclude=tcv\*

done
