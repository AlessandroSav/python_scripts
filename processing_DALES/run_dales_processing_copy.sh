#!/bin/bash
#SBATCH --mem=120G  
#SBATCH --cpus-per-task=24   # Number of CPU cores per task (adjust based on your script's parallelism)
#SBATCH --time=12:00:00      # Max time limit (adjust as needed)
#SBATCH --ntasks=1           # Number of tasks (usually 1 for Dask, unless you want to run on multiple nodes)
#SBATCH --job-name=save_dales   # Job name (for tracking purposes)


cd /home/paaa/
source venv/test_env/bin/activate || { echo "Failed to activate virtual environment"; exit 1; }
# python3 -u python_scripts/processing_DALES/find_location_save_nc_DALES_old.py || { echo "Python script failed"; exit 1; }
# python3 -u python_scripts/processing_DALES/first_process_dales.py || { echo "Python script failed"; exit 1; }

python3 -u python_scripts/processing_DALES/filter_dales.py || { echo "Python script failed"; exit 1; }
# python3 -u python_scripts/processing_DALES/filter_dales_byTime.py || { echo "Python script failed"; exit 1; }



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