#!/bin/bash

#SBATCH --time=18:00:00 # days
#SBATCH --job-name=catrine_ifs_processing
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2 # Number of CPU cores (do I actually need 2?)
#SBATCH --mem=16GB  # Memory allocation


set -e  # Exit immediately if a command exits with a non-zero status

cd /home/paaa/ || { echo "Failed to change directory to /home/paaa"; exit 1; }
# Activate the virtual environment
source venv/test_env/bin/activate || { echo "Failed to activate virtual environment"; exit 1; }

# echo "Script started. Waiting for 5 hour..."
# sleep 5h
# echo "5 hour has passed. Continuing..."

#############################################
#############################################
# Define the variables once here
subdomain="netherlands"
exp_id="iyw7"
exp_name="ctrl_49r2b"
exp_type="fc"
levels="srf" # ml / pl / srf 
ldiagflx="False"
ldiagflx_49r2b="True"
ldiagghg="False"
lbud23="False"
lead_time="0"
stream="oper" # oper/lwda
#############################################
#############################################
# if [ "$levels" = "srf" ]; then
#   ldiagflx="False"
#   ldiagflx_49r2b="False"
#   ldiagghg="False"
#   lbud23="False"
# fi

shopt -s nullglob  # allow empty glob to return nothing instead of literal

scratch_path_glob="/scratch/$USER/IFS/${subdomain}/${exp_id}/${exp_id}_${exp_name}_${levels}_*_${lead_time}.nc"
files=($scratch_path_glob)
echo "${scratch_path_glob}"

if [ ${#files[@]} -gt 0 ]; then
  echo "#### Data already extracted and available at ${files[0]} (and others) ####"
else
  echo "#### Running Mars retrieval for $exp_id $levels ####"
  sbatch --job-name=mars_automated --output=/home/paaa/python_scripts/processing_IFS/%x_%j.out --wrap="python_scripts/processing_IFS/scripts/mars_ghg_fc_automated \
    --subdomain "$subdomain" \
    --exp_id "$exp_id" \
    --exp_name "$exp_name" \
    --exp_type "$exp_type" \
    --levels "$levels" \
    --ldiagflx "$ldiagflx" \
    --ldiagflx_49r2b "$ldiagflx_49r2b" \
    --ldiagghg "$ldiagghg" \
    --lbud23 "$lbud23" \
    --lead_time "$lead_time" \
    --stream "$stream"
    "
  
  echo "#### Come back after Mars extraction is done. ####"
  exit 0
fi


echo "#### Running S1: Rename and deaccumulate ####"
python3 -u python_scripts/processing_IFS/scripts/S1_rename_vars_deaccumulate.py \
  --subdomain "$subdomain" \
  --exp_id "$exp_id"_"$exp_name" \
  --exp_type "$exp_type" \
  --levels "$levels" \
  --ldiagflx "$ldiagflx" \
  --ldiagghg "$ldiagghg" \
  --lbud23 "$lbud23" \
  --ldiagflx_49r2b "$ldiagflx_49r2b" \
  --lead_time "$lead_time"

if [ "$levels" = "ml" ]; then
    echo "#### Running S2: Convert from model levels to height ####"
    python3 -u python_scripts/processing_IFS/scripts/S2_from_ml_to_height.py \
      --subdomain "$subdomain" \
      --exp_id "$exp_id"_"$exp_name" \
      --exp_type "$exp_type" \
      --lead_time "$lead_time"
else
  echo "Skipping S2 (not ml level)."
fi

echo "#### Running S3: Interpolate to single location ####"
python3 -u python_scripts/processing_IFS/scripts/S3_interpolate_to_single_location.py \
  --subdomain "$subdomain" \
  --exp_id "$exp_id"_"$exp_name" \
  --exp_type "$exp_type" \
  --levels "$levels" \
  --lead_time "$lead_time"


echo "All scripts completed successfully."
