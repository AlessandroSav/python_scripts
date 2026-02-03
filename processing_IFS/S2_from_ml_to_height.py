import xarray as xr
import os
import numpy as np
from datetime import timedelta
import sys
sys.path.append(".")  # Ensures the current directory is in the Python path
sys.path.append("/home/paaa/python_scripts/")
import my_functions as mfun
import argparse

################################################
parser = argparse.ArgumentParser()
parser.add_argument("--subdomain", type=str, required=True)
parser.add_argument("--exp_id", type=str, required=True)
parser.add_argument("--exp_type", type=str, required=True)
parser.add_argument("--lead_time", type=int, default=24)

args = parser.parse_args()

subdomain = args.subdomain
exp_id = args.exp_id
exp_type = args.exp_type
lead_time = args.lead_time

dir_in       = f"/perm/paaa/IFS/{subdomain}/{exp_type}"
dir_out      = dir_in
top_lev      = 50
################################################

compute_flx=False # decide if integrating tendencies and compute fluxes
def compute_flx_from_tend(ds_ml,p_half,vars,schemes,top_lev=3,dt=3600 ):
    dp = p_half.diff('level')
    dp = dp.isel(level=slice(top_lev-3,None)).assign_coords(level=np.arange(top_lev-1, 138))

    # Integrate d(var)/dt over height
    for scheme in schemes:
        for var in vars:
            flux_name = f"{var}_flx_{scheme}"
            dvar_dt = ds_ml[f"d{var}dt_{scheme}"]

            # Cumulative trapezoidal integration
            integrated_flux = (dvar_dt* dp/mfun.g).cumsum(dim="level") / dt
            
            ds_ml[flux_name] = integrated_flux
    return ds_ml

##################
##################
print('Processing '+exp_id+' .')
print(f'Processing lead time {lead_time} .')

hlevs = [0,10,20,40,60,80,100,120,140,160,180,200,250,300,350,400,450,500,
        570,640,710,880,950,1050,1150,1250,1350,1450,1550,1700,1900,2200,2500,2800,3100,3400,3700,4000,4500,5000,5500]

## open dataset 
files = [os.path.join(dir_in, f) for f in os.listdir(dir_in) if exp_id+'_ml' in f and f.endswith(f't{lead_time}.nc')]
print('Reading these files:')
print(files)
ds_ml    = xr.open_mfdataset(files,combine='by_coords')
top_lev = max(top_lev,min(ds_ml.level.values))

a= np.array([ 2.000365, 3.102241, 4.666084, 6.827977, 9.746966, 13.605424, 18.608931, 24.985718, 32.985710, 42.879242, 54.955463, 69.520576, 86.895882, 107.415741, 131.425507, 159.279404, 191.338562, 227.968948, 269.539581, 316.420746, 368.982361, 427.592499, 492.616028, 564.413452, 643.339905, 729.744141, 823.967834, 926.344910, 1037.201172, 1156.853638, 1285.610352, 1423.770142, 1571.622925, 1729.448975, 1897.519287, 2076.095947, 2265.431641, 2465.770508, 2677.348145, 2900.391357, 3135.119385, 3381.743652, 3640.468262, 3911.490479, 4194.930664, 4490.817383, 4799.149414, 5119.895020, 5452.990723, 5798.344727, 6156.074219, 6526.946777, 6911.870605, 7311.869141, 7727.412109, 8159.354004, 8608.525391, 9076.400391, 9562.682617, 10065.978516, 10584.631836, 11116.662109, 11660.067383, 12211.547852, 12766.873047, 13324.668945, 13881.331055, 14432.139648, 14975.615234, 15508.256836, 16026.115234, 16527.322266, 17008.789062, 17467.613281, 17901.621094, 18308.433594, 18685.718750, 19031.289062, 19343.511719, 19620.042969, 19859.390625, 20059.931641, 20219.664062, 20337.863281, 20412.308594, 20442.078125, 20425.718750, 20361.816406, 20249.511719, 20087.085938, 19874.025391, 19608.572266, 19290.226562, 18917.460938, 18489.707031, 18006.925781, 17471.839844, 16888.687500, 16262.046875, 15596.695312, 14898.453125, 14173.324219, 13427.769531, 12668.257812, 11901.339844, 11133.304688, 10370.175781, 9617.515625, 8880.453125, 8163.375000, 7470.343750, 6804.421875, 6168.531250, 5564.382812, 4993.796875, 4457.375000, 3955.960938, 3489.234375, 3057.265625, 2659.140625, 2294.242188, 1961.500000, 1659.476562, 1387.546875, 1143.250000, 926.507812, 734.992188, 568.062500, 424.414062, 302.476562, 202.484375, 122.101562, 62.781250, 22.835938, 3.757813, 0.000000, 0.000000 ])
b = np.array([ 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000007, 0.000024, 0.000059, 0.000112, 0.000199, 0.000340, 0.000562, 0.000890, 0.001353, 0.001992, 0.002857, 0.003971, 0.005378, 0.007133, 0.009261, 0.011806, 0.014816, 0.018318, 0.022355, 0.026964, 0.032176, 0.038026, 0.044548, 0.051773, 0.059728, 0.068448, 0.077958, 0.088286, 0.099462, 0.111505, 0.124448, 0.138313, 0.153125, 0.168910, 0.185689, 0.203491, 0.222333, 0.242244, 0.263242, 0.285354, 0.308598, 0.332939, 0.358254, 0.384363, 0.411125, 0.438391, 0.466003, 0.493800, 0.521619, 0.549301, 0.576692, 0.603648, 0.630036, 0.655736, 0.680643, 0.704669, 0.727739, 0.749797, 0.770798, 0.790717, 0.809536, 0.827256, 0.843881, 0.859432, 0.873929, 0.887408, 0.899900, 0.911448, 0.922096, 0.931881, 0.940860, 0.949064, 0.956550, 0.963352, 0.969513, 0.975078, 0.980072, 0.984542, 0.988500, 0.991984, 0.995003, 0.997630, 1.000000 ])
# Expand dimensions of 'b' to match surface_pressure
a_expanded = xr.DataArray(a, dims="level")  # Assign 'levels' as the dimension
b_expanded = xr.DataArray(b, dims="level")  # Assign 'levels' as the dimension

##########################################################################################
if 'lnsp' in ds_ml.variables:
    print('Using lnsp to compute pressure levels.')
    lnsp = ds_ml['lnsp'].sel(level=1).reset_coords('level', drop=True)
    ## Pressure 
    p_half = a_expanded + (b_expanded * np.exp(lnsp))
else:
    print('lnsp not available. Using sp to compute pressure levels.')
    ## open dataset 
    files_srf = [os.path.join(dir_in, f) for f in os.listdir(dir_in) if exp_id+'_srf' in f and f.endswith(f't{lead_time}.nc')]
    ds_srf    = xr.open_mfdataset(files_srf,combine='by_coords')

    sp = ds_srf['sp']  #surface pressure (and lnsp is it's natural logarithm).
    ## Pressure 
    p_half = a_expanded + b_expanded *sp
##########################################################################################

p_ml = (p_half.isel(level=slice(top_lev-2,-1)) + p_half.isel(level=slice(top_lev-1,None)))/2
p_ml = p_ml.assign_coords(level=np.arange(top_lev, 138))  # Levels from 1 to 137
ds_ml['p'] = p_ml
ds_ml['p'].attrs['long_name'] = 'pressure'
#### Compute fluxes from tendencies ####
if compute_flx == True:
    compute_flx_from_tend(ds_ml,p_half,['u','v','co2'],['conv','diff'],top_lev,dt=3600)      

## hydrostatic balance 
dp =  p_ml.diff(dim="level")
T  = ds_ml['t'].sel(level=p_ml['level'])
q = ds_ml['q'].sel(level=p_ml['level'])/1000 # kg/kg
rho = mfun.calc_rho(p_ml,T,q)
dz = dp/(rho.sel(level=p_ml['level'])*mfun.g)

# h_0 = ds_srf['z'] / mfun.g      # include orography
h_0 = 0                         # surface level = 0
dz_reversed = dz.sortby("level", ascending=False)  # Reverse the level order
z_accumulated = dz_reversed.cumsum(dim="level")    # Accumulate from the top
z_accumulated = z_accumulated.sortby("level")      # Restore original order
h_levels = h_0 + z_accumulated

ds_z = xr.Dataset(
    coords={
        "time": ds_ml.time,
        "latitude": ds_ml.latitude,
        "longitude": ds_ml.longitude,
        "height": hlevs,  # Assuming hlevs is your target height levels
    }
)

# Add empty variables for each data variable in ds_ml, initialised with NaNs
for var in ds_ml.data_vars:
    ds_z[var] = xr.DataArray(
        np.full((len(ds_ml.time), len(ds_ml.latitude), len(ds_ml.longitude), len(hlevs)), np.nan),
        dims=("time", "latitude", "longitude", "height"),
        coords=ds_z.coords,
        attrs=ds_ml[var].attrs  # Preserve attributes
    )
ds_z['height'].attrs.update({'units': 'm', 'long_name': 'Height from the surface'})

for it in ds_ml.time:
    if it.dt.hour == 0 or it.dt.hour == 12:
        print('time:', str(it.values)[0:10])
    for ilat in ds_ml.latitude:
        # print('lat:', str(ilat.values)[0:5])
        for ilon in ds_ml.longitude:
            # print('lon:', str(ilon.values)[0:5])
            
            # Select data at current time, lat, lon
            ifs_it = ds_ml.sel(level=slice(top_lev+1, 137), time=it, latitude=ilat, longitude=ilon)
            # Assign height as a coordinate and swap dims
            ifs_it = ifs_it.assign_coords(
                {"height": ("level", h_levels.sel(time=it, latitude=ilat, longitude=ilon).values)}
            ).swap_dims({"level": "height"})

            # Interpolate to fixed heights
            ifs_it = ifs_it.interp(height=hlevs, kwargs={"fill_value": "extrapolate"})

            # Assign interpolated values into the preallocated dataset
            ds_z.loc[dict(time=it, latitude=ilat, longitude=ilon)] = ifs_it

# Define the file name
file_name = exp_id+f"_z_t{lead_time}.nc"

# Combine the directory and file name
file_path = os.path.join(dir_out, file_name)

# Save the dataset to NetCDF
ds_z.to_netcdf(file_path,compute=True)

print(f"Dataset saved to: {file_path} .")
print('Done.')