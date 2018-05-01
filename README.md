Utilities for preprocessing and postprocessing the NMM-B Nature Run initialized from G5NR
This package consists of tools for preprocessing and postprocessing
nature run data. It also contains scripts for running NMM-B on large grids. It's implementation is geared towards preprocessing
data from G5NR for the NEMS Preprocessing System (NPS).
The postprocessing is geared towards using NEMS/NMM-B model and UPP 
output.


DEPENDENCIES
 nwpy - Use for utilities that help generate NPS intermediate format files.
        Also used in postprocessing for the specdata library in order to obtain
        the file names of the UPP files contianing the fields needed by the tracker.
 run_upp - Used for Running UPP as part of the postprocessing workflow
 OmapPy - Use by the postprocessing workflow to generate figures


DESCRIPTION OF SCRIPTS

    PREPROCESSING
        - collection_retriever*py - For downloading G5NR collections via FTP
        - nr_input_generator.py - Generate NPS intermediate files from G5NR 
                                  (and optionally LIS) data
        - params.py - Contains configuration settings (among other things)
                      used by nr_input_generator



SETTING UP
 - Preprocessing
    - Set paths in env.sh
 - Running NMM-B
    - Set up directory as usual and use the scripts under run/
      -> may need to tweak job scheduling parameters

