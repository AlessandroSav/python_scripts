#!/bin/bash

#SBATCH --time=1-12:00:00 
#SBATCH --job-name=lbud23_ifs_processing
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4 # Number of CPU cores (do I actually need 2?)
#SBATCH --mem=64GB  # Memory allocation


set -e  # Exit immediately if a command exits with a non-zero status

cd /home/paaa/ || { echo "Failed to change directory to /home/paaa"; exit 1; }
# Activate the virtual environment
source venv/test_env/bin/activate || { echo "Failed to activate virtual environment"; exit 1; }


# -----------------------------------------
# 1. Run the Python script
# -----------------------------------------
cd /home/paaa/python_scripts/processing_IFS/ || { echo "Failed to change directory to /home/paaa/python_scripts/processing_IFS"; exit 1; }
echo "Starting job at $(date)"
python3 from_ml_to_height.py
echo "Job finished at $(date)"