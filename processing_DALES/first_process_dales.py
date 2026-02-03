import xarray as xr
import numpy as np
import os
import dask
from dask.diagnostics import ProgressBar
import h5netcdf
import scipy
import dask
import netCDF4
from pyproj import Transformer, CRS, Proj, transform

dir_in   = "/scratch/paaa/dales/CATRINE"
# dir_out  = '/perm/paaa/Les/Cases/Catrine'
dir_out  = "/scratch/paaa/dales/CATRINE"


########################
# tstart = datetime.datetime(year=2022, month=5, day=17, hour=0)
# tref = datetime.datetime(year=2022,month=1,day=1)
#######################


# List all files in the directory that start with the exp_name, include the specified level name, and end with .nc
files = [os.path.join(dir_in, f) for f in os.listdir(dir_in) if 'merged_' in f and f.endswith('.nc')]
files.sort()
print(files)

## Open and combine the files 
ds = xr.open_mfdataset(files,combine='by_coords', chunks={'xt': 500,'yt': 500,'time': 20,'zt': 5},engine="h5netcdf")
del ds.attrs["history"]
# Extract time values (assumes they are cftime objects)
time_values = ds.time.values  # This is an array of cftime objects

# Extract day, hour, and minute as NumPy arrays
days = np.array([t.day for t in time_values]) - 1  # Subtract 1 to keep 15-May-2022 as the base
hours = np.array([t.hour for t in time_values])
minutes = np.array([t.minute for t in time_values])

# Define correct reference date as numpy.datetime64
correct_ref_date = np.datetime64("2022-05-15") ### WRONG?????

# Compute new time values using vectorised timedelta operations
new_time_values = correct_ref_date + days.astype("timedelta64[D]") + hours.astype("timedelta64[h]") + minutes.astype("timedelta64[m]")
new_time_values = new_time_values.astype("datetime64[ns]")
ds = ds.assign_coords(time=new_time_values)

ds = ds.where(ds.time.dt.minute == 0, drop=True).sel(zt=slice(0,7000))

# Create a copy of the dataset to modify
ds_interp = ds.copy()

# Interpolate each variable separately
if "u" in ds:
    ds_interp["u"] = ds["u"].interp(xm=ds.xt)
if "v" in ds:
    ds_interp["v"] = ds["v"].interp(ym=ds.yt)
if "w" in ds:
    ds_interp["w"] = ds["w"].interp(zm=ds.zt)

# Drop the unnecessary dimensions and coordinates
ds_interp = ds_interp.drop_dims(["xm", "ym", "zm"])


class Transform:
    def __init__(self, parameters):
        self.parameters = parameters
        self.crs_latlon = 'epsg:4326'
        
        # Construct CRS for the custom projection
        self.crs_proj = CRS.from_proj4(
            f"+proj=lcc +lon_0={parameters['lon_0']} +lat_0={parameters['lat_0']} "
            f"+lat_1={parameters['lat_1']} +lat_2={parameters['lat_2']} "
            f"+x_0={parameters['x_0']} +y_0={parameters['y_0']} "
            f"+a={parameters['a']} +b={parameters['b']} +k_0={parameters['k_0']} "
            f"+units={parameters['units']}"
        )
        
        # Construct transformation objects
        self.latlon_to_xy_transform = Transformer.from_crs(self.crs_latlon, self.crs_proj)
        self.xy_to_latlon_transform = Transformer.from_crs(self.crs_proj, self.crs_latlon)

    def latlon_to_xy(self, lat, lon):
        return self.latlon_to_xy_transform.transform(lat, lon)

    def xy_to_latlon(self, x, y):
        return self.xy_to_latlon_transform.transform(x, y)
    
# Instantiate the Transform class with corrected projection parameters
transform = Transform({
    'proj' :'lcc',
            'lon_0':    0,
            'lat_0': 52.5,
            'lat_1': 52.5,
            'lat_2': 52.5,
            'x_0':  649536.512574,
            'y_0': 1032883.739533,
            'a':   6371220.000000,
            'b':   6371220.000000,
            'k_0' : 1.0,
    'units': 'm'
})


# target lat/lon is provided
target_lat = 51.9703
target_lon = 4.9264

# Test the transformation
x_sw, y_sw = 910000, 940000

print(f"Target latitude: {target_lat}, Target longitude: {target_lon}")

# Sets to store unique xt and yt values
xt_set = set()
yt_set = set()

xt_set.update(ds_interp.variables['xt'][:].values)
yt_set.update(ds_interp.variables['yt'][:].values)

# Convert sets to sorted lists (optional, for ordered data)
xt_list = sorted(xt_set)
yt_list = sorted(yt_set)

# print(xt_list)
# print(yt_list)

xt_list = [x + x_sw for x in xt_list]
yt_list = [y + y_sw for y in yt_list]

#Reprojecting x and y to lon an lat 
x_mesh, y_mesh = np.meshgrid(xt_list, yt_list)

lat, lon = transform.xy_to_latlon(x_mesh, y_mesh)

print(f"lat shape: {lat.shape}, lon shape: {lon.shape}")


# Compute the distance from each grid point to the target location
distances = np.sqrt((lat - target_lat)**2 + (lon - target_lon)**2)

# Find the index of the closest grid point
lat_index, lon_index = np.unravel_index(np.argmin(distances), distances.shape)

##
ds_coords = ds_interp.assign_coords(lat=(("yt", "xt"), lat), lon=(("yt", "xt"), lon))
ds_coords = ds_coords.set_coords(["lat", "lon"])  # Ensure lat/lon are treated as coordinates

#####  Cabauw Domain #####
# Latitude:   51.72 -  52.22
# Longitude:  4.68  -  5.18
lat_min = 51.72
lat_max = 52.22
lon_min = 4.68
lon_max = 5.18 
##
ds_subset_cabauw = ds_coords.where(
    (ds_coords.lat > lat_min) & (ds_coords.lat < lat_max) &
    (ds_coords.lon > lon_min) & (ds_coords.lon < lon_max),
    drop=True
)

#####  Cabauw and Loobos Domain #####
lat_min = 51.72
lat_max = 52.27
lon_min = 4.68
lon_max = 5.75 
##
ds_subset_loobos = ds_coords.where(
    (ds_coords.lat > lat_min) & (ds_coords.lat < lat_max) &
    (ds_coords.lon > lon_min) & (ds_coords.lon < lon_max),
    drop=True
)


## Save to perm what you need ##
# Ensure the directory exists
os.makedirs(dir_out, exist_ok=True)

# # Define the file name
# file_name = "dales_cabauw_3d.nc"
# # Combine the directory and file name
# file_path = os.path.join(dir_out, file_name)
# print("Saving "+file_path+" ...")
# # Save the dataset to NetCDF
# ds_subset_cabauw.to_netcdf(file_path,compute=True)
# print(f"Dataset saved to: {file_path}")
# # Define the file name
# file_name = "dales_loobos_3d.nc"
# # Combine the directory and file name
# file_path = os.path.join(dir_out, file_name)
# print("Saving "+file_path+" ...")
# # Save the dataset to NetCDF
# ds_subset_loobos.to_netcdf(file_path,compute=True)
# print(f"Dataset saved to: {file_path}")


##################
# for domain in ['cabauw','loobos']:
for domain in ['loobos',]:
    for var in ['u','v','w','thl','sv004']:
    # for var in ds_coords.data_vars:
    # Define the file name
        file_name = "dales_"+domain+"_"+var+"_3d.nc"
        # Combine the directory and file name
        file_path = os.path.join(dir_out, file_name)
        print("Saving "+file_path+" ...")
        # Save the dataset to NetCDF
        if domain == 'cabauw':
            ds_subset_cabauw[var].to_netcdf(file_path,compute=True)
            print(f"Dataset saved to: {file_path}")
        if domain == 'loobos':
            ds_subset_loobos[var].to_netcdf(file_path,compute=True)
            print(f"Dataset saved to: {file_path}")
##################
