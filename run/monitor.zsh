#!/usr/bin/env zsh
##
# Run NMM-B in bursts of a few forecast hours per job.
# This script is needed becaause if we let it run freely the I/O 
# servers ultimately run out of memory.
# This script will ensure each run succeeds before goining on to the next
# short run and exits if the job fails.
##

set -aexu

# First forecast hour to process
first_fhr=168
# Last fhr to process
last_fhr=180
# How many hours is each forecast?
run_duration=6
# How long in forecast hours do we run for each job?
duration_per_job=6
# Expected size of the restart file
#expected_restart_file_size=871358476580 # bin8 nemsio
#expected_restart_file_size=436158476580 # bin4 nemsio
expected_restart_file_size=435358375972 # binary

for (( fhr=$first_fhr ; fhr<=$last_fhr ; fhr+=$duration_per_job )) ; do 
    
    typeset -Z4 fhr4=$fhr
    #curr_restart_file="nmmb_rst_01_nio_${fhr4}h_00m_00.00s"
    curr_restart_file="nmmb_rst_01_bin_${fhr4}h_00m_00.00s"
    typeset -Z4 next_rst_fhr4=$(( $fhr + $duration_per_job ))
    #next_restart_file="nmmb_rst_01_nio_${next_rst_fhr4}h_00m_00.00s"
    next_restart_file="nmmb_rst_01_bin_${next_rst_fhr4}h_00m_00.00s"

    if [[ $first_fhr != 0 && ! -e $curr_restart_file ]] ; then
        echo "$curr_restart_file does not exist. Is \$first_fhr set properly? Cowardly bailing"
        exit 2
    fi
    if [[ -e $next_restart_file ]] ; then
        echo "$next_restart_file already exists. Is \$first_fhr set properly? Cowardly bailing"
        exit 2
    fi
    
    jobid=`qsub job.zsh` # SUBMIT JOB
    jobid=`echo $jobid | sed "s#\..*##"` # remove '.bqs3' suffix
    # Update forecast duration for next 6-hour interval in 
    sed -i "s#nhours_fcst.*#nhours_fcst:   $(( $fhr + $run_duration ))#" configure_file_01
    

    # Wait for restart file to appear
    while [[ ! -e $next_restart_file ]] ; do
        sleep 1800
    done
    # Monitor restart file size
    curr_size=`stat -c %s $next_restart_file`
    while [[ $curr_size != $expected_restart_file_size ]] ; do
        sleep 300
        curr_size=`stat -c %s $next_restart_file`
    done

    # Wait for job to complete
    while [[ 0 == 0 ]] ; do
        c=`/bin/grep -c "End Epilogue" bsnr_fcst_5k.o${jobid} || true`
        if [[ $c == 1 ]] ; then
            break
        fi
        sleep 60
    done

    # See job exit status
    #exit_code=`grep "exit code" bsnr_fcst_5k.o${jobid} | awk '{print $13}'` #<-- 'exit code' may appear due to initial ssh errors
    exit_code=`grep "Job ${jobid}.bqs3 finished for user" bsnr_fcst_5k.o${jobid} | awk '{print $13}'`
    if [[ $exit_code != 0 ]] ; then
        echo "The job did not exit properly. Check the problem and remember to re-set \$first_fhr according to the new start time."
        exit 1
    fi

    # Perform final processing
    mv restart_file_01 $curr_restart_file
    mkdir "profile_${fhr4}"    
    mv profile.* profile_${fhr4}/
    cd  profile_${fhr4}
    cp ../configure_file_01 .
    ln -s ../bsnr_fcst_5k.o${jobid} logfile
    cd ..

done


