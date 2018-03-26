#!/usr/bin/env zsh
##
# Run the UPP script until all tasks succeed. This wrapper script is needed
# since the UPP script itself submits multiple jobs. This script will
# run iteratively in case some of said jobs fail
##


[[ -z $14 ]] && echo "USAGE: $0 <UPP default config file> <UPP case-specific config file> <startYear> <startMonth> <startDay> <startHour> <StartMinute> <endYear> <endMonth> <endDay> <endHour>  <endMinute> <freq(hours)> <duration(days:hours:mins:secs)>" && exit 1

set -aeux

# Constants
#RUN_UPP_DIST=/home/Javier.Delgado/scratch/era_run_upp_test
RUN_UPP_DIST=/home/Javier.Delgado/scripts/run_upp/stable
RUN_UPP="$RUN_UPP_DIST/run_upp.py"
MAX_TRIES=4

# Set cmdline params
default_upp_conf=$1
case_upp_conf=$2
start_year=$3 ; start_month=$4 ; start_day=$5 ; start_hour=$6 ; start_minute=$7
end_year=$8 ; end_month=$9 ; end_day=$10 ; end_hour=$11 ; end_minute=$12
freq_hours=$13
duration=$14
# get duration in hours (from DD:HH:MM:SS)
[[ -z $duration[11] ]] && echo "Duration should be in DD:HH:MM:SS" && exit 2
[[ $duration[1,2] != 00 ]] && echo "Only hours and minutes allowed for duration" && exit 5
[[ $duration[10,11] != 00 ]] && echo "Only hours and minutes allowed for duration" && exit 5
duration_hours=$duration[4,5]
mins=$duration[7,8]
if [[ $mins == 00 ]] ; then
    duration_hours=$duration_hours 
elif [[ $mins == 30 ]] ; then
    duration_hours=$(( $duration_hours + 0.5 ))
elif [[ $mins == 15 ]] ; then
    duration_hours=$(( $duration_hours + 0.25 ))
elif [[ $mins == 6 ]] ; then
    duration_hours=$(( $duration_hours + 0.1 ))
else
    echo "Unsupported value for 'minutes'. Sorry this stupid script was done in a hurry"
    exit 6
fi

# Calculate fhr
init_date=`date --date="$start_month/$start_day/$start_year $start_hour:$start_minute" +%s`
curr_date=`date --date="$end_month/$end_day/$end_year $end_hour:$end_minute" +%s`
start_fhr=$(( ( $curr_date - $init_date ) / 3600 ))
minute_frac=$(( $end_minute / 60. ))
start_fhr=$(( $start_fhr + $minute_frac ))
#end_fhr=$(( $start_fhr + $duration_hours )) # NOTE : UPP is not like omapy, duration is relative to first_fhr
end_fhr=$duration_hours 


# Run UPP script iteratively until it succeeds or MAX_TRIES is reached
# Assume success if it takes less than 60 seconds to do run_upp (which indicates
# that all tasks are in the "succeeded" database
#

# run_upp.py is currently wired to run in it's own directory only
curr_dir=`pwd`
cd `dirname $RUN_UPP_DIST`

source $RUN_UPP_DIST/env.sh
success=0
tries=0
while [[ 1 == 1 ]] ; do
    start=`date +%s`
    start_mmddyyyy_hhmm="$start_month-$start_day-$start_year $start_hour:$start_minute"
    python $RUN_UPP db.pickle $default_upp_conf $case_upp_conf -o BASIC.first_fhr=$start_fhr \
           -o BASIC.duration=$end_fhr -o BASIC.interval=$freq_hours \
           -o BASIC.start_date="$start_mmddyyyy_hhmm"
    end=`date +%s`
    elapsed=$(( $end - $start ))
    tries=$(( $tries + 1 ))
    [[ $elapsed < 60 ]] && success=1 && break
    [[ $tries -eq $MAX_TRIES ]] && break
done

cd $curr_dir

if [[ $success -eq 1 ]] ; then
    exit 0
else
    exit 2
fi
