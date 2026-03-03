## This scripts extracts basic variabels from mars and produces a couple of basic plots which are then saved and can be compared
## you can also plot more than one experiment at the time 

import subprocess
import xarray as xr
import os
import glob
import matplotlib.pyplot as plt
import sys

###################################
###################################
############# Setting #############
# Define the variables once here
# variables to pass
# exps = ['itvc','iyob']
exps = ['itvc','iyw7']
fig_suffix = 'ctrl49R1_ctrl49R2'
# exps = ['iu8c','iu8j']
# fig_suffix = 'iu8c_iu8j'
dir_figure = '/perm/paaa/images/quick'

###################################
###################################
srf = {}
ml = {}
srf_mean = {}
ml_mean  = {}

## 1) MARS extraction
stop = False
for exp in exps:
    print(f"##############################################")
    print(f"###### Processing {exp} ######")
    mars_var = os.environ.copy()
    mars_var["exp_id"] = exp
    mars_var["year"] = '2022'
    mars_var["d"] = "0516"
    mars_var["t"] = "0"
    mars_var["base_dir"] = "/scratch/paaa/IFS/netherlands"
    
    expected_srf_file = f"{exp}_quick_srf_{mars_var['year']}{mars_var['d']}_{mars_var['t']}.nc"
    expected_ml_file  = f"{exp}_quick_ml_{mars_var['year']}{mars_var['d']}_{mars_var['t']}.nc"
    search_srf_pattern = os.path.join(mars_var["base_dir"], '**', expected_srf_file)
    search_ml_pattern = os.path.join(mars_var["base_dir"], '**', expected_ml_file)
    matching_srf_files = glob.glob(search_srf_pattern, recursive=True)
    matching_ml_files = glob.glob(search_ml_pattern, recursive=True)

    if matching_srf_files and matching_ml_files:
        print(f"Found {expected_srf_file} at {matching_srf_files[0]}.")
        print(f"Found {expected_ml_file} at {matching_ml_files[0]}.") 
        ## 2) Open files 
        print(f"#######################")    
        print(f"###### Opening datasets ######")
        srf[exp] = xr.open_mfdataset(matching_srf_files[0])
        ml[exp] = xr.open_mfdataset(matching_ml_files[0])
        srf_mean[exp] = srf[exp].mean(['latitude','longitude'], keep_attrs=True)
        ml_mean[exp]  = ml[exp].mean(['latitude','longitude'], keep_attrs=True)


        if exp == exps[0]:
            print(f"SURFACE VARIABLES:")
            for var in srf[exp].data_vars:
                long_name = srf[exp][var].attrs.get('long_name', '(no long_name)')
                print(f"{var}: {long_name}")
            print(f"MODEL LEVEL VARIABLES:")
            for var in ml[exp].data_vars:
                long_name = ml[exp][var].attrs.get('long_name', '(no long_name)')
                print(f"{var}: {long_name}")

    else: # Check if a job is already running for this exp_id
        try:
            result = subprocess.run(
                ["squeue", "--user", os.environ["USER"], "--noheader", "--format=%j"],
                stdout=subprocess.PIPE,
                check=True,
                text=True
            )
            running_jobs = [j.strip() for j in result.stdout.strip().splitlines()]
            
        except subprocess.CalledProcessError as e:
            print(f"Warning: Could not check squeue: {e}")


        if any(exp in name for name in running_jobs):
            print(f"MARS extraction already running for {exp}. File {expected_srf_file} not available yet.")
            
        else :
            print(f" Submitting MARS extraction for {exp}. It will create {expected_srf_file} and {expected_ml_file}.")
            # Submit sbatch script
            subprocess.run([
                "sbatch", 
                f"--job-name={exp}_mars",
                "--export=ALL",  # <-- ensure env vars are passed
                "/home/paaa/mars/mars_ghg_quick"
            ], env=mars_var)
        stop = True
if stop:
    sys.exit("Stopping script, come back later when MARS extraction is done.")
#############################################

## 3) Plot
#####  Evolution of Standard surface variables  #####
print(f"#######################")    
print(f"###### Plotting ######")
vars = ['u10', 'v10', 't2m', 'blh', 'aco2nee', 'tcco2','fsr']
fig, axes = plt.subplots(len(vars), 1, figsize=(10, 2*len(vars)), sharex=True)
# If only one variable, axes is not a list
if len(vars) == 1:
    axes = [axes]
# Loop through variables
for ax, var in zip(axes, vars):
    for exp in exps:
        srf_mean[exp][var].plot(x='time', ax=ax, label=exp)
    # Use metadata from the first experiment
    data_var = srf_mean[exps[0]][var]
    ax.set_title(data_var.attrs.get('long_name', var))
    ax.set_ylabel(data_var.attrs.get('units', ''))
    ax.legend()

plt.tight_layout()

plt.savefig(f"{dir_figure}/mean_srf_{fig_suffix}_{mars_var['t']}lead.pdf")
print(f"###### Saved {dir_figure}/mean_srf_{fig_suffix}_{mars_var['t']}lead.pdf ######")

#############################################
#####  Mean profiles  at specific hour #####
lead_time = 0
vars = ['t', 'q', 'co2', 'clwc', 'u', 'v']
vars = ['t', 'q', 'co2', 'u', 'v']
fig, axes = plt.subplots(1,len(vars), figsize=(3 * len(vars), 9), sharey=True)
# If only one variable, axes is not a list
if len(vars) == 1:
    axes = [axes]
for i, (ax, var) in enumerate(zip(axes, vars)):
    for exp in exps:
        da = ml_mean[exp][var].isel(time=lead_time)
        da.plot(y='level', ax=ax, label=exp,yincrease=False)
    # Use metadata from the first experiment
    data_var = ml_mean[exps[0]][var]
    ax.set_title(data_var.attrs.get('long_name', var))
    ax.set_xlabel(data_var.attrs.get('units', ''))
    ax.legend()
    if i == 0:
        ax.set_ylabel("Model level")
    else:
        ax.set_ylabel('')
        ax.tick_params(labelleft=False)

plt.tight_layout()
sel_date = ml_mean[exps[0]]['time'].isel(time=lead_time).dt.strftime('%Y%m%d_%H').item()

plt.savefig(f"{dir_figure}/mean_ml_{fig_suffix}_{sel_date}_{mars_var['t']}lead.pdf")
print(f"###### Saved {dir_figure}/mean_ml_{fig_suffix}_{sel_date}_{mars_var['t']}lead.pdf ######")

#############################################
#####  Mean profiles  of the dataset #####
vars = ['t', 'q', 'co2', 'clwc', 'u', 'v']
vars = ['t', 'q', 'co2', 'u', 'v']
fig, axes = plt.subplots(1,len(vars), figsize=(3 * len(vars), 9), sharey=True)
# If only one variable, axes is not a list
if len(vars) == 1:
    axes = [axes]
for i, (ax, var) in enumerate(zip(axes, vars)):
    for exp in exps:
        da = ml_mean[exp][var].mean('time')
        da.plot(y='level', ax=ax, label=exp,yincrease=False)
    # Use metadata from the first experiment
    data_var = ml_mean[exps[0]][var]
    ax.set_title(data_var.attrs.get('long_name', var))
    ax.set_xlabel(data_var.attrs.get('units', ''))
    ax.legend()
    if i == 0:
        ax.set_ylabel("Model level")
    else:
        ax.set_ylabel('')
        ax.tick_params(labelleft=False)

plt.tight_layout()

plt.savefig(f"{dir_figure}/mean_ml_{fig_suffix}_alltime_{mars_var['t']}lead.pdf")
print(f"###### Saved {dir_figure}/mean_ml_{fig_suffix}_alltime_{mars_var['t']}lead.pdf ######")


#############################################
#####  Spatial heterogeneity  #####

