# DALES with chemistry

This directory is for pre-processing the periodic-boundary DALES with chemistry data from Arseni's simulations.

---

There are two Python scripts and a Slurm script to run them (as processing requires some time).

**Box Profile Extraction Script**:
This script extracts the box profile from data by latitude and longitude, including the location of interest within (in my script, I included the Cabauw tower location). You can also set the vertical boundaries of your profile to extract the required data.

**Map Movie Plotting Script**:
This script generates a map animation for a specific height over a chosen time period from the selected tracer data. You can customize the time period, temporal interpolation, and tracer names.

**Additional Notes**:

- The fielddump and profile files provide instantaneous data at intervals of 15 minutes and 1 minute, respectively.
- To use the scripts:
  - Copy the folder containing the DALES data from my archive folder to your scratch (as you cannot run the Slurm job directly from the archive folder).
  - Update the file paths in the Python scripts to point to your input and output directories.
  - Choose the variable you need (in my scripts, you’ll find sv001, sv002, sv003, and sv004, which are CO2 tracers and some other meteorological variables). CO2 tracers:
    - sv001: Background (CAMS).
    - sv002: Background + anthropogenic emissions.
    - sv003: Background, emissions, + soil respiration.
    - sv004: CO2sum (all previous contributions + loss through photosynthesis).

For example, to calculate NEE, you can subtract sv002 from sv004 to get the biogenic contribution. sv004 represents the total CO2 sum, including all contributions.