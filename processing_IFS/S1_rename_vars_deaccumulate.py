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
########
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

## loop through the files and deaccumulate the fields that need it
# remove the variable ds_combined if it exists
if 'ds_combined' in locals():
    del ds_combined
for file in files:
    print(f"Processing file {file}")
    ds_single = xr.open_dataset(file, chunks={'longitude': -1, 'latitude': -1, 'time': 50})
    time_values = ds_single['time'].values
    # Calculate the time difference between consecutive time steps
    if len(time_values) > 1:
        time_diffs = np.diff(time_values) # in numpy timedelta64[ns] format
        # Assuming uniform time steps, take the first difference
        time_step = time_diffs[0].astype('timedelta64[h]')
    else:
        print("Not enough time points to calculate time step.")
    print(f"Time step between consecutive records: {time_step} hour")
    # update units of some variables
    for var in ['co2','q','clwc','ra','co','ch4']:
        if var in ds_single:
            ds_single[var] *= 1000 
            ds_single[var].attrs['units'] = r'g kg$^{-1}$'
        else:
            print(f"Variable {var} not found in dataset.")
    
    ###### rename experimental variables ######
    if lbud23=='True':
        ds_single = ds_single.rename(LBUD23)
    if ldiagflx=='True':
        ds_single = ds_single.rename(LDIAGFLX)
    if ldiagghg=='True':
        ds_single = ds_single.rename(LLDIAGGHG)
    ########################################

    ## Loop through the variables
    for var, var_data in ds_single.variables.items():
        ## Assign units
        if 'dqdt' in var or 'dco2dt' in var or 'dch4dt' in var :
            ds_single[var] *= 1000 
            ds_single[var].attrs['units'] = r'g kg$^{-1}$'
        elif 'dTdt' in var:
            ds_single[var].attrs['units'] = r'K'
        elif 'dudt' in var:
            ds_single[var].attrs['units'] = r'm s$^{-1}$'
        elif 'dvdt' in var:
            ds_single[var].attrs['units'] = r'm s$^{-1}$'
        elif 'MF' in var:
            ds_single[var].attrs['units'] = r'kg m$^{-2}$s$^{-1}$'
        ## Assign long name 
        if var == 'dryMF':
            ds_single[var].attrs['long_name'] = 'Dry mass-flux'
        if var == 'dry_moistMF':
            ds_single[var].attrs['long_name'] = 'Combined dry and moist mass-flux'
        ## moisture 
        if var == 'dqdt_dyn':
            ds_single[var].attrs['long_name'] = 'Moisture tendency from dynamics'
        elif var == 'dqdt_diff':
            ds_single[var].attrs['long_name'] = 'Moisture tendency from diffusion and gravity waves'
        elif var == 'dqdt_conv':
            ds_single[var].attrs['long_name'] = 'Moisture tendency from convection'
        elif var == 'dqdt_cloud':
            ds_single[var].attrs['long_name'] = 'Moisture tendency from cloud'
        ## temperature 
        if var == 'dTdt_dyn':
            ds_single[var].attrs['long_name'] = 'Temperature tendency from dynamics'
        elif var == 'dTdt_diff':
            ds_single[var].attrs['long_name'] = 'Temperature tendency from diffusion and gravity waves'
        elif var == 'dTdt_conv':
            ds_single[var].attrs['long_name'] = 'Temperature tendency from convection'
        elif var == 'dTdt_cloud':
            ds_single[var].attrs['long_name'] = 'Temperature tendency from cloud'
        ## zonal wind 
        if var == 'dudt_dyn':
            ds_single[var].attrs['long_name'] = 'Zonal wind tendency from dynamics'
        elif var == 'dudt_diff':
            ds_single[var].attrs['long_name'] = 'Zonal wind tendency from diffusion and gravity waves'
        elif var == 'dudt_conv':
            ds_single[var].attrs['long_name'] = 'Zonal wind tendency from convection'
        ## meridional wind 
        if var == 'dvdt_dyn':
            ds_single[var].attrs['long_name'] = 'Meridional wind tendency from dynamics'
        elif var == 'dvdt_diff':
            ds_single[var].attrs['long_name'] = 'Meridional wind tendency from diffusion and gravity waves'
        elif var == 'dvdt_conv':
            ds_single[var].attrs['long_name'] = 'Meridional wind tendency from convection'
        ## CO2 
        if var == 'dco2dt_dyn':
            ds_single[var].attrs['long_name'] = 'CO2 tendency from dynamics'
        elif var == 'dco2dt_diff':
            ds_single[var].attrs['long_name'] = 'CO2 tendency from diffusion and gravity waves'
        elif var == 'dco2dt_conv':
            ds_single[var].attrs['long_name'] = 'CO2 tendency from convection'
        elif var == 'co2flx_dyn':
            ds_single[var].attrs['long_name'] = 'Time-integrated CO2 flux from dynamics'
        elif var == 'co2flx_diff':
            ds_single[var].attrs['long_name'] = 'Time-integrated CO2 flux from diffusion'
        elif var == 'co2flx_conv':
            ds_single[var].attrs['long_name'] = 'Time-integrated CO2 flux from convection'
        ## CH4
        if var == 'dch4dt_dyn':
            ds_single[var].attrs['long_name'] = 'CH4 tendency from dynamics'
        elif var == 'dch4dt_diff':
            ds_single[var].attrs['long_name'] = 'CH4 tendency from diffusion and gravity waves'
        elif var == 'dch4dt_conv':
            ds_single[var].attrs['long_name'] = 'CH4 tendency from convection'
        elif var == 'ch4flx_dyn':
            ds_single[var].attrs['long_name'] = 'Time-integrated CH4 flux from dynamics'
        elif var == 'ch4flx_diff':
            ds_single[var].attrs['long_name'] = 'Time-integrated CH4 flux from diffusion'
        elif var == 'ch4flx_conv':
            ds_single[var].attrs['long_name'] = 'Time-integrated CH4 flux from convection'

    ################################################
    ### Define a function to deaccumulate a variable
    def deaccumulate(var):
        var_diff = var.diff(dim='time', label='upper')
        # divide by the time step in hours to get rate per hour
        var_deaccumulated = var_diff / time_step.astype(float)
        # Copy attributes from the original dataset
        var_deaccumulated.attrs = var.attrs.copy()
        var_deaccumulated.attrs['units'] = var_deaccumulated.attrs['units'] +r' h$^{-1}$'
        return var_deaccumulated
    ################################################

    print(f"Deaccumulating...")
    # Select variables to deaccumulate (that contain 'dt')
    vars_to_deaccumulate = [var for var in ds_single.variables if 'dt' in var]
    # Select variables to deaccumulate (Time-integrated) from model level output
    for var in ds_single:
        if var in ['srta','trta','umfa','dmfa','udra','ddra','tdcha','dryMF','dry_moistMF']:
            vars_to_deaccumulate.append(var)
            ds_single[var].attrs['long_name'] = ds_single[var].attrs['long_name'].replace('Time-integrated', 'Deaccumulated').capitalize() # remove the string "Time-integrated..."
        if var in ['co2flx_dyn','co2flx_diff','co2flx_conv','ch4flx_dyn','ch4flx_diff','ch4flx_conv']:
            ds_single[var].attrs['units'] = 'g m$^{-2}$' # Are you sure? is it not (kg/kg m/s) ?
            vars_to_deaccumulate.append(var)
            ds_single[var].attrs['long_name'] = ds_single[var].attrs['long_name'].replace('Time-integrated', 'Deaccumulated').capitalize()

    # Deaccumulate each selected variable
    for var_name in vars_to_deaccumulate:
        if var_name in ds_single:
            # print(f"Deaccumulating variable: {var_name}")
            ds_single[var_name] = deaccumulate(ds_single[var_name])
        else:
            print(f"Variable {var_name} not found in dataset.")

    # After processing, drop the first time and concatenate all ds_single datasets
    ###
    ds_single = ds_single.isel(time=slice(1, None)) ### !!
    ###
    if 'ds_combined' in locals():
        ds_combined = xr.concat([ds_combined, ds_single], dim='time')
    else:
        ds_combined = ds_single
print(f"Finished processing files________")

###
## I don't know why this is needed, but without it the saving goes wrong. Might be becasue of existing NaN values
ds_combined = ds_combined.map(lambda x: x.astype('float64'))

## Save to perm what you need ##

# Ensure the directory exists
os.makedirs(dir_out, exist_ok=True)
# Define the file name
file_name = exp_name_out+"_"+levels+f"_t{lead_time}.nc"
# Combine the directory and file name
file_path = os.path.join(dir_out, file_name)
# Save the dataset to NetCDF
print(f"Saving...")
ds_combined.to_netcdf(file_path,compute=True)

print(f"Dataset saved to: {file_path}")
print("Done.")