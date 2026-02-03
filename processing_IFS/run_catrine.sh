#!/bin/bash

#SBATCH --time=1-05:00:00 # 2 days
#SBATCH --job-name=catrine_ifs_processing
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2 # Number of CPU cores (do I actually need 2?)
#SBATCH --mem=64GB  # Memory allocation


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
subdomain="cabauw"
exp_id="j0a2"
exp_name="SPP_noIC_noBFAS"
exp_type="fc"
levels="pl" # ml / pl / srf 
ldiagflx="False"
ldiagghg="False"
lbud23="False"
lead_time="0"
#############################################
#############################################
if [ "$levels" = "srf" ]; then
  ldiagflx="False"
  ldiagghg="False"
  lbud23="False"
fi

shopt -s nullglob  # allow empty glob to return nothing instead of literal

scratch_path_glob="/scratch/$USER/IFS/${subdomain}/${exp_id}/${exp_id}_${exp_name}_${levels}_*_${lead_time}.nc"
files=($scratch_path_glob)
echo "${scratch_path_glob}"

if [ ${#files[@]} -gt 0 ]; then
  echo "#### Data already extracted and available at ${files[0]} (and others) ####"
else
  echo "#### Running Mars retrieval for $exp_id $levels ####"
  sbatch --job-name=mars_automated --output=/home/paaa/mars/%x_%j.out --wrap="/home/paaa/mars/mars_ghg_fc_automated \
    --subdomain "$subdomain" \
    --exp_id "$exp_id" \
    --exp_name "$exp_name" \
    --exp_type "$exp_type" \
    --levels "$levels" \
    --ldiagflx "$ldiagflx" \
    --ldiagghg "$ldiagghg" \
    --lbud23 "$lbud23" \
    --lead_time "$lead_time" \
    "
  
  echo "#### Come back after Mars extraction is done. ####"
  exit 0
fi


echo "#### Running S1: Rename and deaccumulate ####"
python3 -u python_scripts/processing_IFS/S1_rename_vars_deaccumulate.py \
  --subdomain "$subdomain" \
  --exp_id "$exp_id"_"$exp_name" \
  --exp_type "$exp_type" \
  --levels "$levels" \
  --ldiagflx "$ldiagflx" \
  --ldiagghg "$ldiagghg" \
  --lbud23 "$lbud23" \
  --lead_time "$lead_time"

if [ "$levels" = "ml" ]; then
    echo "#### Running S2: Convert from model levels to height ####"
    python3 -u python_scripts/processing_IFS/S2_from_ml_to_height.py \
      --subdomain "$subdomain" \
      --exp_id "$exp_id"_"$exp_name" \
      --exp_type "$exp_type" \
      --lead_time "$lead_time"
else
  echo "Skipping S2 (not ml level)."
fi

echo "#### Running S3: Interpolate to single location ####"
python3 -u python_scripts/processing_IFS/S3_interpolate_to_single_location.py \
  --subdomain "$subdomain" \
  --exp_id "$exp_id"_"$exp_name" \
  --exp_type "$exp_type" \
  --levels "$levels" \
  --lead_time "$lead_time"


echo "All scripts completed successfully."


# # Define the date range
# yyyymmdd1="20220515"
# yyyymmdd2="20220420"

# # Define the list of parameters
# params=("co2" "q" "u","v",)

# # Loop through parameters and run the srun command
# for param in "${params[@]}"; do
#     echo "Running srun for parameter: $param"
#     srun -c 4 --mem=64GB python3 catrine_wp7_ifs_output_netcdf4_ugrid.py "$param" "$yyyymmdd1" "$yyyymmdd2"
# done