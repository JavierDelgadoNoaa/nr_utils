./run_upp_wrapper.zsh /home/Javier.Delgado/scripts/nr_workflow/sfc1.conf /home/Javier.Delgado/scripts/nr_workflo
w/case.conf 2006 09 04 00 00 2006 09 11 00 00 0.5 00:72:00:00 &> oeHourly_newExec_sfc_hybrid_168_240


####
# CONTENTS OF case.conf
#
[DEFAULT]
fields_per_unipost_job = 4

[BASIC]
model_rundir = /home/Javier.Delgado/scratch/nems/g5nr/data/gamma/2j_5500x4000/{init_date:%Y%m%d%H}/atmos_from180

[hpc]
nodes = 4
walltime_per_fieldset = 0.8
##
