#!/bin/bash

#SBATCH --job-name=create_dales_input
#SBATCH --time=04:00:00
#SBATCH --mem=64G
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4

# Optional: load modules or activate virtualenv
source /home/paaa/venv/test_env/bin/activate

cd /home/paaa/python_scripts

echo "Starting create_dales_input.py on $(hostname) at $(date)"
python3 create_dales_input.py
STATUS=$?
echo "Finished create_dales_input.py with exit code ${STATUS} at $(date)"
exit ${STATUS}
