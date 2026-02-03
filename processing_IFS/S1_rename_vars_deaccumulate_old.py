import xarray as xr
import numpy as np
import os
import sys
import dask
import h5netcdf
import scipy
import netCDF4
import argparse

#######################################################
#######################################################
parser = argparse.ArgumentParser()
parser.add_argument("--subdomain", type=str, required=True)
parser.add_argument("--exp_id", type=str, required=True)
parser.add_argument("--exp_type", type=str, required=True)
parser.add_argument("--levels", type=str, required=True)
parser.add_argument("--ldiagflx", type=str, default='False')
parser.add_argument("--ldiagghg", type=str, default='True')
parser.add_argument("--lbud23", type=str, default='False')
parser.add_argument("--lead_time", type=int, default=24)

args = parser.parse_args()

subdomain = args.subdomain
exp_id = args.exp_id
exp_type = args.exp_type
levels = args.levels
ldiagflx = args.ldiagflx
ldiagghg = args.ldiagghg
lbud23 = args.lbud23
lead_time = args.lead_time

dir_in = f"/scratch/paaa/IFS/{subdomain}/{exp_id[0:4]}"
dir_out = f'/perm/paaa/IFS/{subdomain}/{exp_type}'
exp_name_in = f'{exp_id}'
exp_name_out = f'{exp_name_in}'

#######################################################
#######################################################
# List all files in the directory that start with the exp_name, include the specified level name, and end with .nc

target_lead_times = [lead_time, lead_time + 12]

print(f"Looking for data in {dir_in}")
files = []
for lt in target_lead_times:
    matching_files = [
        os.path.join(dir_in, f)
        for f in os.listdir(dir_in)
        if exp_name_in in f
        and levels in f
        and f'_{lt}' in f
        # and f.lower().endswith(f'{subdomain.lower()}.nc')
        and f.lower().endswith('.nc')
    ]
    files.extend(matching_files)

files.sort()
print(f"Found these files for lead time {lead_time} (+12): {files}")

if files:
    ds = xr.open_mfdataset(
        files,
        combine='by_coords',
        chunks={'longitude': -1, 'latitude': -1, 'time': 50}
    )
else:
    print(f"No files found for lead time {lead_time} or {lead_time+12}.")
    sys.exit(0)

###################################
for var in ['co2','q','clwc','ra','co','ch4']:
    if var in ds:
        ds[var] *= 1000 
        ds[var].attrs['units'] = r'g kg$^{-1}$'

#### RENAME VARIABLES ####
###
########################################
if lbud23=='True':
    LBUD23 = {
        'p91' : ('dudt_dyn',     'm s$^{-2}$ *s'),
        'p92' : ('dvdt_dyn',     'm s$^{-2}$ *s'),
        'p93' : ('dTdt_dyn',     'K s$^{-1}$ *s'),
        'p94' : ('dqdt_dyn',     'g kg$^{-1}$ s$^{-1}$ *s'),
        
        'p95' : ('dTdt_rad',     'K s$^{-1}$ *s'),
        
        'p96' : ('dudt_diff',    'm s$^{-2}$ *s'),
        'p97' : ('dvdt_diff',    'm s$^{-2}$ *s'),
        'p98' : ('dTdt_diff',    'K s$^{-1}$ *s'),
        'p99' : ('dqdt_diff',    'g kg$^{-1}$ s$^{-1}$ *s'),
        
        'p100': ('dudt_gwave',   'm s$^{-2}$ *s'),
        'p101': ('dvdt_gwave',   'm s$^{-2}$ *s'),
        'p102': ('dTdt_gwave',   'K s$^{-1}$ *s'),
        
        'p103': ('dudt_conv',    'm s$^{-2}$ *s'),
        'p104': ('dvdt_conv',    'm s$^{-2}$ *s'),
        'p105': ('dTdt_conv',    'K s$^{-1}$ *s'),
        'p106': ('dqdt_conv',    'g kg$^{-1}$ s$^{-1}$ *s'),
        
        'p107': ('prflx_conv_l', 'kg m$^{-2}$ s$^{-1}$ *s'),
        'p108': ('prflx_conv_i', 'kg m$^{-2}$ s$^{-1}$ *s'),
        
        'p109': ('dTdt_cloud',   'K s$^{-1}$ *s'),
        'p110': ('dqdt_cloud',   'g kg$^{-1}$ s$^{-1}$ *s'),
        'p111': ('dqldt_cloud',  'g kg$^{-1}$ s$^{-1}$ *s'),
        'p112': ('dqidt_cloud',  'g kg$^{-1}$ s$^{-1}$ *s'),
        
        'p113': ('prflx_stra_l', 'kg m$^{-2}$ s$^{-1}$ *s'),
        'p114': ('prflx_stra_i', 'kg m$^{-2}$ s$^{-1}$ *s'),
        
        'p115': ('dTdt_rad_SW',  'K s$^{-1}$ *s'),
    }
    #####
    ds = ds.rename(LBUD23)
####################
if ldiagflx=='True':
    LDIAGFLX = {
        # dynamics
        'p212001' : 'dudt_dyn'    , 'p212002' : 'uflx_dyn'   ,
        'p212003' : 'dvdt_dyn'    , 'p212004' : 'vflx_dyn'   ,
        'p212005' : 'dTdt_dyn'    , 'p212006' : 'Tflx_dyn'   ,
        'p212007' : 'dqdt_dyn'    , 'p212008' : 'qflx_dyn'   ,
        'p212009' : 'dco2dt_dyn'  , 'p212010' : 'co2flx_dyn' ,
        'p212011' : 'dch4dt_dyn'  , 'p212012' : 'ch4flx_dyn' ,
        # diffusion
        'p212013' : 'dudt_diff'   , 'p212014' : 'uflx_diff'  ,
        'p212015' : 'dvdt_diff'   , 'p212016' : 'vflx_diff'  ,
        'p212017' : 'dTdt_diff'   , 'p212018' : 'Tflx_diff'  ,
        'p212019' : 'dqdt_diff'   , 'p212020' : 'qflx_diff'  ,
        'p212021' : 'dco2dt_diff' , 'p212022' : 'co2flx_diff',
        'p212023' : 'dch4dt_diff' , 'p212024' : 'ch4flx_diff',
        # convection
        'p212025' : 'dudt_conv'   , 'p212026' : 'uflx_conv'  ,
        'p212027' : 'dvdt_conv'   , 'p212028' : 'vflx_conv'  ,
        'p212029' : 'dTdt_conv'   , 'p212030' : 'Tflx_conv'  ,
        'p212031' : 'dqdt_conv'   , 'p212032' : 'qflx_conv'  ,
        'p212033' : 'dco2dt_conv' , 'p212034' : 'co2flx_conv',
        'p212035' : 'dch4dt_conv' , 'p212036' : 'ch4flx_conv',
        # cloud
        'p212037' : 'dTdt_cloud'  , 'p212038' : 'Tflx_cloud' ,
        'p212039' : 'dqdt_cloud'  , 'p212040' : 'qflx_cloud' ,
    }
    #####
    ds = ds.rename(LDIAGFLX)
####################
if ldiagghg=='True':
    LLDIAGGHG = {
        'p91' : 'dudt_dyn'   , 'p92' : 'dvdt_dyn'   , 'p93' : 'dTdt_dyn' , 'p94' : 'dqdt_dyn',
        'p95' : 'dudt_diff'  , 'p96' : 'dvdt_diff'  , 'p97' : 'dTdt_diff', 'p98' : 'dqdt_diff',
        'p99' : 'dudt_conv'  , 'p100': 'dvdt_conv'  , 'p101': 'dTdt_conv', 'p102': 'dqdt_conv',
        'p103': 'dTdt_cloud' , 'p104': 'dqdt_cloud' ,
        'p105': 'dco2dt_dyn' , 'p106': 'dch4dt_dyn' ,
        'p107': 'dco2dt_diff', 'p108': 'dch4dt_diff',
        'p109': 'dco2dt_conv', 'p110': 'dch4dt_conv',
        # 'p111': 'dco2dt_tot', 'p112': 'dch4dt_tot',
        # 'p111': 'dryMF',
        # 'p112': 'udrate_dry',
    }
    # LLDIAGGHG = {
    #     'p91': 'dudt_dyn', 'p92': 'dvdt_dyn', 'p93': 'dTdt_dyn', 'p94': 'dqdt_dyn',
    #     'p95': 'dudt_diff', 'p96': 'dvdt_diff', 'p97': 'dTdt_diff', 'p98': 'dqdt_diff',
    #     'p99': 'dudt_conv', 'p100': 'dvdt_conv', 'p101': 'dTdt_conv', 'p102': 'dqdt_conv',
    #     'p103': 'dTdt_cloud', 'p104': 'dqdt_cloud',
    #     'p105': 'dco2dt_dyn', 'p106': 'dch4dt_dyn',
    #     'p107': 'dco2dt_diff', 'p108': 'dch4dt_diff',
    #     'p109': 'dco2dt_conv', 'p110': 'dch4dt_conv',
    #     'p111': 'dryMF','p112': 'dry_moistMF','p113': 'CFL_fraction'
    #     # 'p112': 'udrate_dry',
    # }
    #####
    ds = ds.rename(LLDIAGGHG)
########################################

## Loop through the variables
for var, var_data in ds.variables.items():
    ## Assign units
    if 'dqdt' in var or 'dco2dt' in var or 'dch4dt' in var :
        ds[var] *= 1000 
        ds[var].attrs['units'] = r'g kg$^{-1}$'
    elif 'dTdt' in var:
        ds[var].attrs['units'] = r'K'
    elif 'dudt' in var:
        ds[var].attrs['units'] = r'm s$^{-1}$'
    elif 'dvdt' in var:
        ds[var].attrs['units'] = r'm s$^{-1}$'
    elif 'MF' in var:
        ds[var].attrs['units'] = r'kg m$^{-2}$s$^{-1}$'

    ## Assign long name 
    if var == 'dryMF':
        ds[var].attrs['long_name'] = 'Dry mass-flux'
    if var == 'dry_moistMF':
        ds[var].attrs['long_name'] = 'Combined dry and moist mass-flux'
    ## moisture 
    if var == 'dqdt_dyn':
        ds[var].attrs['long_name'] = 'Moisture tendency from dynamics'
    elif var == 'dqdt_diff':
        ds[var].attrs['long_name'] = 'Moisture tendency from diffusion and gravity waves'
    elif var == 'dqdt_conv':
        ds[var].attrs['long_name'] = 'Moisture tendency from convection'
    elif var == 'dqdt_cloud':
        ds[var].attrs['long_name'] = 'Moisture tendency from cloud'
    ## temperature 
    if var == 'dTdt_dyn':
        ds[var].attrs['long_name'] = 'Temperature tendency from dynamics'
    elif var == 'dTdt_diff':
        ds[var].attrs['long_name'] = 'Temperature tendency from diffusion and gravity waves'
    elif var == 'dTdt_conv':
        ds[var].attrs['long_name'] = 'Temperature tendency from convection'
    elif var == 'dTdt_cloud':
        ds[var].attrs['long_name'] = 'Temperature tendency from cloud'
    ## zonal wind 
    if var == 'dudt_dyn':
        ds[var].attrs['long_name'] = 'Zonal wind tendency from dynamics'
    elif var == 'dudt_diff':
        ds[var].attrs['long_name'] = 'Zonal wind tendency from diffusion and gravity waves'
    elif var == 'dudt_conv':
        ds[var].attrs['long_name'] = 'Zonal wind tendency from convection'
    ## meridional wind 
    if var == 'dvdt_dyn':
        ds[var].attrs['long_name'] = 'Meridional wind tendency from dynamics'
    elif var == 'dvdt_diff':
        ds[var].attrs['long_name'] = 'Meridional wind tendency from diffusion and gravity waves'
    elif var == 'dvdt_conv':
        ds[var].attrs['long_name'] = 'Meridional wind tendency from convection'
    ## CO2 
    if var == 'dco2dt_dyn':
        ds[var].attrs['long_name'] = 'CO2 tendency from dynamics'
    elif var == 'dco2dt_diff':
        ds[var].attrs['long_name'] = 'CO2 tendency from diffusion and gravity waves'
    elif var == 'dco2dt_conv':
        ds[var].attrs['long_name'] = 'CO2 tendency from convection'
    elif var == 'co2flx_dyn':
        ds[var].attrs['long_name'] = 'Time-integrated CO2 flux from dynamics'
    elif var == 'co2flx_diff':
        ds[var].attrs['long_name'] = 'Time-integrated CO2 flux from diffusion'
    elif var == 'co2flx_conv':
        ds[var].attrs['long_name'] = 'Time-integrated CO2 flux from convection'
    ## CH4
    if var == 'dch4dt_dyn':
        ds[var].attrs['long_name'] = 'CH4 tendency from dynamics'
    elif var == 'dch4dt_diff':
        ds[var].attrs['long_name'] = 'CH4 tendency from diffusion and gravity waves'
    elif var == 'dch4dt_conv':
        ds[var].attrs['long_name'] = 'CH4 tendency from convection'
    elif var == 'ch4flx_dyn':
        ds[var].attrs['long_name'] = 'Time-integrated CH4 flux from dynamics'
    elif var == 'ch4flx_diff':
        ds[var].attrs['long_name'] = 'Time-integrated CH4 flux from diffusion'
    elif var == 'ch4flx_conv':
        ds[var].attrs['long_name'] = 'Time-integrated CH4 flux from convection'

##### DEACCUMULATE #####
print(f"Deaccumulating...")
# Select variables to deaccumulate (that contain 'dt')
vars_with_dt = [var for var in ds.variables if 'dt' in var]

# Select variables to deaccumulate (Time-integrated) from model level output
for var in ds:
    if var in ['srta','trta','umfa','dmfa','udra','ddra','tdcha','dryMF','dry_moistMF']:
        vars_with_dt.append(var)
        ds[var].attrs['long_name'] = ds[var].attrs['long_name'].replace('Time-integrated', 'Deaccumulated').capitalize() # remove the string "Time-integrated..."
    if var in ['co2flx_dyn','co2flx_diff','co2flx_conv','ch4flx_dyn','ch4flx_diff','ch4flx_conv']:
        ds[var].attrs['units'] = 'g m$^{-2}$'
        vars_with_dt.append(var)
        ds[var].attrs['long_name'] = ds[var].attrs['long_name'].replace('Time-integrated', 'Deaccumulated').capitalize()


# define accumulation period dt in hours
# dt = ds['time'].diff('time').dt.total_seconds() # seconds 
dt =1 #hour
if vars_with_dt:
    # Identify reset points where time.hour == 1
    reset_indices = ds.time.dt.hour == 0 ## This needs to be changed if the forecast is longer than 24 hours !!!
    # Compute the difference along the 'time' dimension for those variables
    d_ds = ds[vars_with_dt].diff('time')
    # Apply reset correction across selected variables
    d_ds_corrected = d_ds.where(~reset_indices.isel(time=slice(1, None)), other=ds[vars_with_dt].isel(time=slice(1, None)))
    # Add back the first values (lost due to diff operation)
    ds_deaccumulated = xr.concat([ds[vars_with_dt].isel(time=0), d_ds_corrected], dim='time')/dt

    # Copy attributes from the original dataset
    for var in ds_deaccumulated.data_vars:
        ds_deaccumulated[var].attrs = ds[var].attrs.copy()
        ds_deaccumulated[var].attrs['units'] = ds_deaccumulated[var].attrs['units'] +r' h$^{-1}$'

    # Combine the deaccumulated variables with the rest of the dataset
    # ds_final = ds.copy()
    ds[vars_with_dt] = ds_deaccumulated[vars_with_dt]

###
## I don't know why this is needed, but without it the saving goes wrong. Might be becasue of existing NaN values
ds = ds.map(lambda x: x.astype('float64'))

## Save to perm what you need ##

# Ensure the directory exists
os.makedirs(dir_out, exist_ok=True)
# Define the file name
file_name = exp_name_out+"_"+levels+f"_t{lead_time}.nc"
# Combine the directory and file name
file_path = os.path.join(dir_out, file_name)
# Save the dataset to NetCDF
print(f"Saving...")
ds.to_netcdf(file_path,compute=True)

print(f"Dataset saved to: {file_path}")

print("Done.")