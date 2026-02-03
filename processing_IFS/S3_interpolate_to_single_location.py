import xarray as xr
import os
import numpy as np
from datetime import timedelta
import sys
sys.path.append(".")  # Ensures the current directory is in the Python path
sys.path.append("/home/paaa/python_scripts/")
import my_functions as mfun
import argparse

################################################################################################
parser = argparse.ArgumentParser()
parser.add_argument("--subdomain", type=str, required=True)
parser.add_argument("--exp_id", type=str, required=True)
parser.add_argument("--exp_type", type=str, required=True)
parser.add_argument("--levels", type=str, required=True)
parser.add_argument("--lead_time", type=int, default=24)

args = parser.parse_args()

subdomain = args.subdomain
exp_id = args.exp_id
exp_type = args.exp_type
levels = args.levels
lead_time = args.lead_time

dir_in       = f"/perm/paaa/IFS/{subdomain}/{exp_type}"
dir_out      = dir_in
if levels == 'ml':
    levels = 'z'
    print('using height instead of model levels for interpolation to single location')

################################################################################################
## open dataset 
files = [os.path.join(dir_in, f) for f in os.listdir(dir_in) if exp_id+f'_{levels}' in f and f.endswith(f't{lead_time}.nc')]
print('Reading these files:')
print(files)
ds    = xr.open_mfdataset(files,combine='by_coords')
################################################
UTC_to_LT = +2 # hours
locations = [
    {
        "name": "cabauw",
        "lat": 51.971,
        "lon": 4.927,
        "z": [5, 60, 100, 180]
    },
    {
        "name": "loobos",
        "lat": 52.166,
        "lon": 5.744,
        "z": [24]
    }
]
################################################
################################################################################################
### Interpolate spatially using Inverse Distance Weighting 
def inverse_distance_weighting(ds,target_lat,target_lon):
    # Get latitude and longitude arrays (assumed to be 1D or 2D broadcastable)
    lat = ds['latitude']
    lon = ds['longitude']
    # Compute distance in degrees
    dist = np.sqrt((lat - target_lat)**2 + (lon - target_lon)**2)
    # Avoid division by zero (e.g. exact match)
    dist = dist.where(dist != 0, other=1e-10)
    # Inverse distance weights
    weights = 1 / dist
    weights = weights / weights.sum(dim=('latitude', 'longitude'))
    # Now apply the weights to average over lat/lon
    # Broadcast weights to match dimensions
    weighted = ds.weighted(weights)
    data_idw = weighted.mean(dim=('latitude', 'longitude'))
    return data_idw
################################################################################################

################################################
## Process data using my_classes.py
##
# Convert UTC to local time (+2 hours)
ds['time'] = ds['time'].astype('datetime64[ns]') + np.timedelta64(UTC_to_LT, 'h')

for var in ds.data_vars:
    if 'co2_flx' in var:
        print(f"Filling up units for {var}")
        ds[var].attrs['units'] = ds['co2'].attrs['units']+r" ms$^{-1}$"
    if 'units' in ds[var].attrs:
        if 'co2' in var and 'PPM' not in ds[var].attrs['units']:
            old_units = ds[var].attrs['units']
            if 'g kg$^{-1}$' in old_units:
                ds[var] = mfun.concentration_to_ppm('co2', ds[var] / 1000)
                ds[var].attrs['units'] = old_units.replace('g kg$^{-1}$', 'PPM')
            # elif 'kg m**-2 s**-1' in old_units:
            #     print(f"converting {old_units} to PPM m/s for variable {var}, assumption on (surface?) density, careful!")
            #     rho = mfun.calc_rho(ds['sp'],ds['t'],ds['q']/1000)#kg/m3
            #     rho=1.245#kg/m3
            #     print(f"rho near the surface: {rho.sel(height=0,method='nearest').isel(time=1).values}")
            #     ds[var] = mfun.concentration_to_ppm('co2', ds[var]/rho)
            #     ds[var].attrs['units'] = old_units.replace('kg m**-2$', 'PPM m')
            elif "g m$^{-2}$ h$^{-1}$" in old_units:
                print(f"converting {old_units} to PPM m/s for variable {var}, assumption on density, careful!")
                rho = mfun.calc_rho(ds['p'],ds['t'],ds['q']/1000)#kg/m3
                print(f"rho near the surface: {rho.sel(height=0,method='nearest').isel(time=1).values}")
                ds[var] = mfun.concentration_to_ppm('co2', ds[var]/1000/rho)
                ds[var].attrs['units'] = old_units.replace('g m$^{-2}$', 'PPM m')
            else: 
                print(f"Cannot convert {old_units} to PPM for variable {var}")
    else:
        print(f"Units unknown for variable {var}")

if levels == 'z':  
    print(f"Computing total tendencies .")
    if 'wspd' not in ds:
        ds['wspd'] = mfun.pitagora_fun(ds['u'], ds['v'])
    
    # Combine tendencies into total, only if they don't already exist
    for var in ['q', 'T']:
        var_name = f'd{var}dt_tot'
        if var_name not in ds and f'd{var}dt_diff' in ds:
            ds[var_name] = (ds[f'd{var}dt_dyn'] +
                                ds[f'd{var}dt_diff'] +
                                ds[f'd{var}dt_conv'] +
                                ds[f'd{var}dt_cloud'])
            # Copy over attributes (at least 'units')
            ds[var_name].attrs = ds[f'd{var}dt_dyn'].attrs.copy()
    
    for var in ['u', 'v', 'co2', 'ch4']:
        var_name = f'd{var}dt_tot'
        if var_name not in ds and f'd{var}dt_diff' in ds:
            ds[var_name] = (ds[f'd{var}dt_dyn'] +
                                ds[f'd{var}dt_diff'] +
                                ds[f'd{var}dt_conv'])
            # Copy over attributes (at least 'units')
            ds[var_name].attrs = ds[f'd{var}dt_dyn'].attrs.copy()
    

################################################
#### Save processed data ####
file_name = exp_id+f"_{levels}_t{lead_time}_processed.nc"   # Define the file name
file_path = os.path.join(dir_out, file_name)                # Combine the directory and file name
print(f"Saving {file_path} .")
ds.to_netcdf(file_path,compute=True)                        # Save the dataset to NetCD

#### Save slab ####
ds_processed_slab = ds.mean(('latitude', 'longitude'), keep_attrs=True)
file_name = exp_id+f"_{levels}_t{lead_time}_slab.nc"    # Define the file name
file_path = os.path.join(dir_out, file_name)            # Combine the directory and file name
print(f"Saving {file_path} .")
ds_processed_slab.to_netcdf(file_path,compute=True)     # Save the dataset to NetCDF

#### Save one location ####
for location in locations: 
    point_location = inverse_distance_weighting(ds,location['lat'],location['lon'])

    if levels == 'z':
        ### interpolate to tower's heights 
        all_heights = np.unique(np.concatenate([ds['height'].values, location['z']]))
        point_location = point_location.interp(height=all_heights)

    file_name = exp_id+f"_{levels}_t{lead_time}_{location['name']}.nc"  # Define the file name
    file_path = os.path.join(dir_out, file_name)                        # Combine the directory and file name
    print(f"Saving {file_path} .")
    point_location.to_netcdf(file_path,compute=True)                    # Save the dataset to NetCDF

print(f"Done_____________")
