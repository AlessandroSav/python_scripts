import numpy as np
import netCDF4 as netc
from pyproj import Transformer, CRS, Proj, transform
import xarray as xr
import datetime
import time

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

# Test the transformation
x_sw, y_sw = 910000, 940000
print(f"x: {x_sw}, y: {y_sw}")


rundir='/scratch/ecme2192/DALES/' #put folder May_17_18_2022_dales from archive/adoyenne/ to your scratch-shared folder
outdir='/scratch/paaa/dales/CATRINE/' #set your output folder with / at the end


nx_tot = 1728
ny_tot = 1152
nz = 128


nprocx_tot   = 19   #nprocx + 1

nprocx_start = 0
nprocx_end   = 17  #nprocx - 1

nprocx = nprocx_end - nprocx_start + 1
nx = nx_tot//nprocx_tot*nprocx

nprocy_tot   = 25 #nprocy + 1
nprocy_start = 0

nprocy_end   = 23 #nprocy - 1
nprocy = nprocy_end - nprocy_start + 1
ny = ny_tot//nprocy_tot*nprocy

dnx = nx_tot//nprocx
dny = ny_tot//nprocy


# target lat/lon is provided
target_lat = 51.9703
target_lon = 4.9264

print(f"Target latitude: {target_lat}, Target longitude: {target_lon}")


tstart = datetime.datetime(year=2022, month=5, day=17, hour=0)
tref = datetime.datetime(year=2022,month=1,day=1)

# Format the datetime object as "dd/mm/yyyy"
formatted_date = tstart.strftime("%d/%m/%Y")


#Read air density profile:
fname = rundir+'profiles.001.nc'
with netc.Dataset(fname) as fobj: 
    rhof = fobj.variables['rhof'][:]
    


dt = 4 #we save hourly data for saving space reasons (output is set to 15min in this simulation)

zlow_idx = 0 # see zt in profile or fielddump (ncdump -c profile.001.nc)  to chose the index of the altitude to be the lower border 
zheight_idx=125 # see zt in profile or fielddump (ncdump -c profile.001.nc)  to chose the index of the altitude to be the higher border 
    
spinup_shift=(24*4)-1 #exclude first day, also for the size reason
    
with netc.Dataset(rundir+'fielddump.000.000.001.nc') as fobj: 
    t = fobj.variables['time'][spinup_shift::dt] 
    zt= fobj.variables['zt'][zlow_idx:zheight_idx]
    print('start time of output', tstart + datetime.timedelta(seconds=float(t[0])))
    print('output heights in meter', zt)

# Loading data! Note that this might take a while!

field3 = []

# Create a list to store all the extracted data
extracted_data = []

svname0 = 'sv001' 
svname1 = 'sv002'
svname2 = 'sv003'
svname3 = 'sv004' #CO2sum

# Specify the variable name of interest to var_name_to_extract:
#'sv001', 'sv002', 'sv003', 'sv004' 
#4 CO2 tracers: sv001 - background from CAMS; 
#sv002 BG+anthropogenic emiss; 
#sv003 - BG, emiss, + soil respiration; 
#sv004 - CO2sum (all previous+ loss through photosynthesis )

var_name_to_extract=svname3


# Sets to store unique xt and yt values
xt_set = set()
yt_set = set()

print(f'nprocx:{nprocx}, nprocy:{nprocy}')

# Iterate through the field dump files
for ix in range(nprocx_start,nprocx_end+1):
    for iy in range(nprocy_start,nprocy_end+1):
        # Open the fielddump file
        with netc.Dataset(rundir + f'fielddump.{ix:03d}.{iy:03d}.001.nc') as fobj: 
            # Extract xt and yt
            xt = fobj.variables['xt'][:]
            yt = fobj.variables['yt'][:]
            
            # Append xt and yt to the lists
            xt_set.update(xt)
            yt_set.update(yt)
            
            if len(field3) == 0:
                field3 = np.zeros([len(t),zheight_idx, ny_tot, nx_tot])
                

            field3[:,:, (iy - nprocy_start) * dny:(iy - nprocy_start + 1) * dny, (ix - nprocx_start) * dnx:(ix - nprocx_start + 1) * dnx] += 28.9647 / 44.01 * fobj.variables[var_name_to_extract][spinup_shift::dt, zlow_idx:zheight_idx] 

print('Data are placed to field arrays.')

# Convert sets to sorted lists (optional, for ordered data)
xt_list = sorted(xt_set)
yt_list = sorted(yt_set)

print(xt_list)
print(yt_list)

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

# --- Step 2: Determine the next boundary (2nd corner of the grid cell) ---

# Check the target point's location relative to the closest grid point

# Case for latitude
if target_lat > lat[lat_index, lon_index]:  # Target is "above" the closest point
    lat_next_index = lat_index + 1 if lat_index + 1 < lat.shape[0] else lat_index  # Stay within bounds
else:  # Target is "below" or equal to the closest point
    lat_next_index = lat_index - 1 if lat_index - 1 >= 0 else lat_index

# Case for longitude
if target_lon > lon[lat_index, lon_index]:  # Target is "right" of the closest point
    lon_next_index = lon_index + 1 if lon_index + 1 < lon.shape[0] else lon_index  # Stay within bounds
                    
else:  # Target is "left" or equal to the closest point
    lon_next_index = lon_index - 1 if lon_index - 1 >= 0 else lon_index

# --- Step 3: Extract the 2x2 grid box corners ---
    
# Ensure the latitudes and longitudes are ordered from smaller to larger
lat_indices = [min(lat_index, lat_next_index), max(lat_index, lat_next_index)]
lon_indices = [min(lon_index, lon_next_index), max(lon_index, lon_next_index)]
                
print(lat_indices)   
print(lon_indices)


# Ensure the latitudes are from smaller to larger by checking indices
lat_c = lat[[min(lat_index, lat_next_index), max(lat_index, lat_next_index)], 
        [min(lon_index, lon_next_index), max(lon_index, lon_next_index)]]

# Ensure the longitudes are from smaller to larger by checking indices
lon_c = lon[[min(lat_index, lat_next_index), max(lat_index, lat_next_index)], 
        [min(lon_index, lon_next_index), max(lon_index, lon_next_index)]]
                
print(lat_c)      
print(lon_c)

# Extract requested variable and slice it by the lat/lon indices
extracted_data = field3[:, :, lat_indices[0]:lat_indices[1]+1, lon_indices[0]:lon_indices[1]+1]

              
# Convert them to 1D arrays for proper representation
lat_1d = np.unique(lat_c.flatten())  # Extract unique latitude values
lon_1d = np.unique(lon_c.flatten())  # Extract unique longitude values

# Define output filename
output_filename = outdir + f"DALES_extracted_data_for_Cabauw_loc_fielddump_{var_name_to_extract}_{ix:03d}_{iy:03d}.nc"

# Save to NetCDF
with netc.Dataset(output_filename, 'w', format='NETCDF4') as out_fobj:
    # Create dimensions
    out_fobj.createDimension('time', len(t))
    out_fobj.createDimension('zt', len(zt))
    out_fobj.createDimension('lat', len(lat_1d))
    out_fobj.createDimension('lon', len(lon_1d))

    # Create and populate the time variable
    time_var = out_fobj.createVariable('time', t.dtype, ('time',))
    time_var[:] = t

    # Create and populate the zt variable
    zt_var = out_fobj.createVariable('zt', zt.dtype, ('zt',))
    zt_var[:] = zt

    # Create latitude and longitude variables
    lat_var = out_fobj.createVariable('lat', lat_1d.dtype, ('lat',))
    lon_var = out_fobj.createVariable('lon', lon_1d.dtype, ('lon',))
    lat_var[:] = lat_1d
    lon_var[:] = lon_1d

    # Define the variable in the NetCDF file
    out_var = out_fobj.createVariable(var_name_to_extract, extracted_data.dtype, ('time', 'zt', 'lat', 'lon'))
    out_var[:, :, :, :] = extracted_data

print(f"Extracted data saved in {output_filename}")