#!/bin/bash

#SBATCH --time=18:00:00 # days
#SBATCH --job-name=catrine_SPP_ifs_processing
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2 # Number of CPU cores (do I actually need 2?)
#SBATCH --mem=16GB  # Memory allocation


set -e  # Exit immediately if a command exits with a non-zero status

cd /home/paaa/ || { echo "Failed to change directory to /home/paaa"; exit 1; }
# Activate the virtual environment
source venv/test_env/bin/activate || { echo "Failed to activate virtual environment"; exit 1; }

#############################################
#############################################
# Define the variables once here
subdomain="netherlands"
exp_id="j0c6"
exp_name="SPP_all"
exp_type="pf"
levels="srf" # pl / srf 
lead_time="0"
stream="enfo" # oper/lwda/enfo

# Date range for retrieval (inclusive). Format: YYYYMMDD
yyyymmdd_start="20220515"
yyyymmdd_end="20220520"
#############################################
#############################################

shopt -s nullglob  # allow empty glob to return nothing instead of literal

scratch_path_glob="/scratch/$USER/IFS/${subdomain}/${exp_id}/${exp_id}_${exp_name}_${levels}_*_${lead_time}.nc"
files=($scratch_path_glob)
echo "${scratch_path_glob}"

if [ ${#files[@]} -gt 0 ]; then
  echo "#### Data already extracted and available at ${files[0]} (and others) ####"

else
  echo "#### Running Mars retrieval for $exp_id $levels ####"
  sbatch --job-name=mars_spp --output=/home/paaa/python_scripts/processing_IFS/%x_%j.out --wrap="bash python_scripts/processing_IFS/scripts/mars_spp_automated.sh \
    --subdomain \"$subdomain\" \
    --exp_id \"$exp_id\" \
    --exp_name \"$exp_name\" \
    --levels \"$levels\" \
    --lead_time \"$lead_time\" \
    --stream \"$stream\" \
    --yyyymmdd_start \"$yyyymmdd_start\" \
    --yyyymmdd_end \"$yyyymmdd_end\""
  
  echo "#### Come back after Mars extraction is done. ####"
  exit 0
fi


echo "#### Running S1: Deaccumulate and combine days together ####"
python3 -u python_scripts/processing_IFS/scripts/S1_SPP_deaccumulate_combine.py \
  --subdomain "$subdomain" \
  --exp_id "$exp_id"_"$exp_name" \
  --exp_type "$exp_type" \
  --levels "$levels" \
  --lead_time "$lead_time"

levels_for_interp="$levels"
if [ "$levels" = "pl" ]; then
  echo "#### Running S2: Convert from pressure levels to height ####"
  python3 -u python_scripts/processing_IFS/scripts/S2_SPP_from_pl_to_height.py \
    --subdomain "$subdomain" \
    --exp_id "$exp_id"_"$exp_name" \
    --exp_type "$exp_type" \
    --lead_time "$lead_time"
  levels_for_interp="z"
else
  echo "Skipping S2 (not pressure levels)."
fi

echo "#### Running S3: Interpolate to single location ####" 
python3 -u python_scripts/processing_IFS/scripts/S3_SPP_interpolate_to_single_location.py \
  --subdomain "$subdomain" \
  --exp_id "$exp_id"_"$exp_name" \
  --exp_type "$exp_type" \
  --levels "$levels_for_interp" \
  --lead_time "$lead_time"


echo "All scripts completed successfully." 