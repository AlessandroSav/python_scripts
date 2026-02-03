import os
import xarray as xr
import numpy as np
import my_functions as mfun

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

############################
############################
class DatasetProcessor:
    def __init__(self, exp_name, dir_in,lead_time='',UTC_to_LT=+2):
        self.exp_name = exp_name
        self.dir_in = dir_in
        self.l_time = lead_time
        self.UTC_to_LT = UTC_to_LT
        
        # Initialize the dataset attributes as None
        self.ds_srf_slab = None
        self.ds_z_slab = None
        self.ds_srf = None 
        self.ds_z = None
        
        # Process the datasets immediately after instantiation
        self.process_datasets()
    
    def process_datasets(self):
        print(f"Processing {self.exp_name}")
        for levels in ['srf', 'z']:
            # Open dataset
            files = [os.path.join(self.dir_in, f) for f in os.listdir(self.dir_in) if self.exp_name + '_' + levels in f and f.endswith(f'{self.l_time}.nc')]
            if not files:  # Check if the list is empty
                print(f"Cannot find .nc file for {self.exp_name}_{levels} .")
                continue

            ds = xr.open_mfdataset(files, combine='by_coords', chunks={'time': -1, 'latitude': -1, 'longitude': -1, 'heigth': 18})
            
            # Convert UTC to local time (+2 hours)
            ds['time'] = ds['time'].astype('datetime64[ns]') + np.timedelta64(self.UTC_to_LT, 'h')


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
                        elif 'kg m**-2 s**-1' in old_units:
                            print(f"converting {old_units} to PPM for variable {var}, careful!")
                            ds[var] = mfun.concentration_to_ppm('co2', ds[var])
                            ds[var].attrs['units'] = old_units.replace('kg', 'PPM')
                        else: 
                            print(f"trying to convert {old_units} to PPM for variable {var}")
                else:
                    print(f"Units unknown for variable {var}")

            if levels == 'z':
                self.ds_z = ds
                self.ds_z_slab = ds.mean(('latitude', 'longitude'), keep_attrs=True)
                
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
                
                for var in ['u', 'v', 'co2', 'ch4']:
                    var_name = f'd{var}dt_tot'
                    if var_name not in ds and f'd{var}dt_diff' in ds:
                        ds[var_name] = (ds[f'd{var}dt_dyn'] +
                                         ds[f'd{var}dt_diff'] +
                                         ds[f'd{var}dt_conv'])
                
                self.ds_z_slab = ds.mean(('latitude', 'longitude'), keep_attrs=True)
                
            elif levels == 'srf':
                self.ds_srf = ds
                self.ds_srf_slab = ds.mean(('latitude', 'longitude'), keep_attrs=True)
        print(f"Done_____________")

    def get_ds_srf_slab(self):
        return self.ds_srf_slab

    def get_ds_z_slab(self):
        return self.ds_z_slab

    def get_ds_srf(self):
        return self.ds_srf

    def get_ds_z(self):
        return self.ds_z
    
    def get_ds_z_location(self,target_lat,target_lon,target_heights):
        point_location_z = inverse_distance_weighting(self.ds_z,target_lat,target_lon)
        ### interpolate to tower's heights 
        point_location_z = point_location_z.interp(height=target_heights)
        return point_location_z
    
    def get_ds_srf_location(self,target_lat,target_lon,target_heights):
        point_location_srf = inverse_distance_weighting(self.ds_srf,target_lat,target_lon)
        return point_location_srf