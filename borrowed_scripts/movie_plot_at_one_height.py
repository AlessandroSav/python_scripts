import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import netCDF4 as netc
import xarray as xr
import datetime
import time
from pyproj import Transformer, CRS, Proj, transform
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from matplotlib.animation import FuncAnimation
import matplotlib.animation as animation1
import cartopy as cart
import cartopy.crs as ccrs
from matplotlib.axes import Axes
from cartopy.mpl.geoaxes import GeoAxes
GeoAxes._pcolormesh_patched = Axes.pcolormesh
from matplotlib.colors import LinearSegmentedColormap


rivers_10m = cart.feature.NaturalEarthFeature('physical', 'rivers_lake_centerlines', '10m')
lakes_10m  = cart.feature.NaturalEarthFeature('physical', 'lakes', '10m')
border_10m = cart.feature.NaturalEarthFeature('cultural', 'admin_0_countries', '10m')
grad_10m  = cart.feature.NaturalEarthFeature('physical', 'graticules_1', '10m')
urban_10m = cart.feature.NaturalEarthFeature('cultural', 'urban_areas', '10m')
places_10m = cart.feature.NaturalEarthFeature('cultural', 'populated_places', '10m')
admin_10m = cart.feature.NaturalEarthFeature('cultural', 'admin_1_states_provinces', '10m')
ocean_110m = cart.feature.NaturalEarthFeature('physical', 'ocean', '110m',edgecolor="none", facecolor=np.array([0.59375, 0.71484375, 0.8828125]))



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
  
    


x_sw = 910000
y_sw = 940000

print(f"x: {x_sw}, y: {y_sw}")


nx_tot = 1728
ny_tot = 1152
nz = 128

    
# Settings:

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


tstart = datetime.datetime(year=2022, month=5, day=17, hour=0)
tref = datetime.datetime(year=2022,month=1,day=1)

# Format the datetime object as "dd/mm/yyyy"
formatted_date = tstart.strftime("%d/%m/%Y")

rundir='/scratch-shared/adoyenne/May_17_18_2022_dales/' #put folder May_17_18_2022_dales from archive/adoyenne/ to your scratch-shared folder
outdir='/' #set your output folder with / at the end

Save_to_nc = False

# Specify the variables of interest
#'sv001', 'sv002', 'sv003', 'sv004' 
#4 CO2 tracers: sv001 - background from CAMS; 
#sv002 BG+anthropogenic emiss; 
#sv003 - BG, emiss, + soil respiration; 
#sv004 - CO2sum (all previous+ loss through photosynthesis )


#Read air density profile:
fname = rundir+'profiles.001.nc'
with netc.Dataset(fname) as fobj: 
    rhof = fobj.variables['rhof'][:]
    

    
#Due to extremely high size of DALES output, we can extract the smacial data at one height:


dt = 1 #if you need hourly use 4 here as data are 15-min average

z_idx = 1 # see zt in profile or fielddump (ncdump -c profile.001.nc)  to chose the index of the altitude your are interested in
    
spinup_shift=(24*4)-1
    
with netc.Dataset(rundir+'fielddump.000.000.001.nc') as fobj: 
    t = fobj.variables['time'][spinup_shift::dt] #I add (6*60)-1 to exclude first 6 hours due to spin-up from harmonie and DALES
    print('start time of output', tstart + datetime.timedelta(seconds=float(t[0])))
    
#with netc.Dataset(rundir+'000/cape.x000y000.001.nc') as fobj:
#with netc.Dataset(rundir+'000/radfield.000.000.001.nc') as fobj: 
    #t = fobj.variables['time'][spinup_shift::dt] #I add (6*60)-1 to exclude first 6 hours due to spin-up from harmonie and DALES
    #print('start time of output', tstart + datetime.timedelta(seconds=float(t[0])))
    
# Loading data! Note that this might take a while!

field3 = []

svname0 = 'sv001' 
svname1 = 'sv002'
svname2 = 'sv003'
svname3 = 'sv004'

# Sets to store unique xt and yt values
xt_set = set()
yt_set = set()

iz = z_idx
print(f'nprocx:{nprocx}, nprocy:{nprocy}')

# Iterate through the field dump files
for ix in range(nprocx_start,nprocx_end+1):
    for iy in range(nprocy_start,nprocy_end+1):
        # Open the fielddump file
        #with netc.Dataset(rundir + f'{iy:03d}/cape.x{ix:03d}y{iy:03d}.001.nc') as fobj:
        with netc.Dataset(rundir + f'fielddump.{ix:03d}.{iy:03d}.001.nc') as fobj: 
        #with netc.Dataset(rundir + f'{iy:03d}/radfield.{ix:03d}.{iy:03d}.001.nc') as fobj:
            # Extract xt and yt
            xt = fobj.variables['xt'][:]
            yt = fobj.variables['yt'][:]
            
            # Append xt and yt to the lists
            xt_set.update(xt)
            yt_set.update(yt)
            
            if len(field3) == 0:
                field3 = np.zeros([len(t),ny_tot, nx_tot])
                

            field3[:, (iy - nprocy_start) * dny:(iy - nprocy_start + 1) * dny, (ix - nprocx_start) * dnx:(ix - nprocx_start + 1) * dnx] += 28.9647 / 44.01 * fobj.variables[svname3][spinup_shift::dt, z_idx] 
            #field3[:, (iy - nprocy_start) * dny:(iy - nprocy_start + 1) * dny, (ix - nprocx_start) * dnx:(ix - nprocx_start + 1) * dnx] += 28.9647 / 44.01 * fobj.variables[svname3][spinup_shift::dt] 


            
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

#with netc.Dataset(rundir + f'initfields.inp.001.nc') as fobj:
    #lat = fobj.variables['lat'][:]
    #lon = fobj.variables['lon'][:]




if Save_to_nc: #if you need to save your field to nc
    
    output_filename = outdir + f"extracted_field.nc"

    with netc.Dataset(output_filename, 'w', format='NETCDF4') as out_fobj:
        # Create dimensions
        out_fobj.createDimension('time', len(t))
        out_fobj.createDimension('lon', lon.shape[0])
        out_fobj.createDimension('lat', lat.shape[1])

        # Create and populate the time variable
        time_var = out_fobj.createVariable('time', t.dtype, ('time',))
        time_var[:] = t

        # Create latitude and longitude variables
        lat_var = out_fobj.createVariable('lat', lat.dtype, ('lon', 'lat'))
        lon_var = out_fobj.createVariable('lon', lon.dtype, ('lon', 'lat'))
        lat_var[:, :] = lat
        lon_var[:, :] = lon


            
        # Define the variable in the NetCDF file
        out_var = out_fobj.createVariable(svname3, field3.dtype, ('time', 'lon', 'lat'))
        out_var[:, :, :] = field3

    print(f"Extracted data saved in {output_filename}")
    
    
    
#Animation creation is below:


# Define the colors for the custom colormap
colors = [
    (1.0, 1.0, 1.0),  # White (lightest blue)
    (0.9, 0.9, 1.0),  # Very light blue
    (0.7, 0.7, 1.0),  # Light blue
    (0.5, 0.5, 1.0),  # Soft blue
    (0.0, 0.0, 1.0),  # Blue
    (0.0, 1.0, 0.0),  # Green
    (1.0, 1.0, 0.0),  # Yellow
    (1.0, 0.5, 0.0),  # Orange
    (1.0, 0.25, 0.0),  # Darker orange
]


# Create the custom colormap
custom_cmap = LinearSegmentedColormap.from_list("custom_rainbow", colors, N=256)


plotvals = field3


# Define your colormap and color ticks
cmin = int(420)
cmax = int(455) 

print('CO2sum:','max:',cmax ,'min:',cmin)


lat_min=51.7 #np.min(lat)
lat_max=52.5 #np.max(lat)
lon_min=3.95 #np.min(lon)
lon_max=6.2 #np.max(lon)


ncol = (cmax - cmin) // 1
# Use matplotlib.colormaps.get_cmap
#cmap = plt.get_cmap('viridis', ncol)
cmap = plt.get_cmap(custom_cmap, ncol)

cticks = np.linspace(cmin, cmax, ncol + 1)

fig = plt.figure(figsize=(8, 6), dpi=300)

sphere = ccrs.PlateCarree(globe=ccrs.Globe(datum='WGS84', ellipse='sphere'))


# Assuming you have your data and variables defined
# Define lon, lat, plotvals, etc.

# -- Map
ax = fig.add_subplot(1, 1, 1, projection=ccrs.Orthographic(
    central_longitude=((lon_max - lon_min) / 2) + lon_min,
    central_latitude=((lat_max - lat_min) / 2) + lat_min
))


#--- Base field
vls = ax.pcolormesh(lon, lat, plotvals[0], transform=ccrs.PlateCarree(), vmin=cmin, vmax=cmax, cmap=cmap)

gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                  linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
gl.top_labels = False
gl.right_labels = False
gl.left_labels = True
gl.bottom_labels = True
gl.xformatter = LONGITUDE_FORMATTER
gl.yformatter = LATITUDE_FORMATTER
gl.xlabel_style = {'size': 14, 'color': 'gray'}
gl.ylabel_style = {'size': 14, 'color': 'gray'}
gl.xlabel_style = {'color': 'grey', 'weight': 'bold'}
gl.ylabel_style = {'color': 'grey', 'weight': 'bold'}

ax.add_feature(cfeature.COASTLINE, linewidth=1, edgecolor='black')
ax.add_feature(cfeature.LAKES, linewidth=1, edgecolor='black',  facecolor='None')
ax.add_feature(cfeature.BORDERS, linewidth=1, linestyle='--', edgecolor='black')

#cbar_ax = fig.add_axes([0.14, 0.1, 0.75, 0.028])  # Adjust the position as needed

cbar_ax = fig.add_axes([0.11, 0.05, 0.8, 0.028])  # Adjust the position as needed

# Add colorbar
cbar = fig.colorbar(vls, cax=cbar_ax, orientation="horizontal")
cbar.ax.tick_params(labelsize=10)
cbar.ax.get_yaxis().labelpad = 10
cbar.set_label('CO2sum (ppm)', rotation=0, fontsize=12)

# Set the map extent (you can adjust this as needed)
ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())

# ---- Animation

plt.tight_layout()

# Linear interpolation of data to create additional frames
def interpolate_data(data, num_interpolated_frames):
    interpolated_data = []
    for i in range(len(data) - 1):
        interpolated_data.append(data[i])
        # Generate interpolated frames between data[i] and data[i+1]
        for j in range(1, num_interpolated_frames + 1):
            interpolated_frame = data[i] + (data[i + 1] - data[i]) * (j / (num_interpolated_frames + 1))
            interpolated_data.append(interpolated_frame)
    interpolated_data.append(data[-1])  # Append the last frame
    return np.array(interpolated_data)

# Define the number of additional frames between original frames (180s steps)
num_interpolated_frames = 4  # (900s original interval / 180s new interval - 1)
interpolated_plotvals = interpolate_data(plotvals, num_interpolated_frames)

time_step=900

# Animation function
def animate(i):
    # Calculate the current time in seconds with interpolation
    current_seconds = t[i // (num_interpolated_frames + 1)] + \
                      (i % (num_interpolated_frames + 1)) * time_step / (num_interpolated_frames + 1)
    current_time = tstart + datetime.timedelta(seconds=current_seconds)
    
    # Check the shape of the array before updating
    #print(f"Frame {i} shape: {interpolated_plotvals[i].shape}")
    
    # Update the data array for the frame
    vls.set_array(interpolated_plotvals[i, :, :].ravel().flatten())
    ax.set_title(f'{current_time} UTC', fontweight='bold')
    
    
# create animation
anim = FuncAnimation(fig, animate, interval=500, frames=len(interpolated_plotvals)-1)


# saving to m4 using ffmpeg writer

writervideo = animation1.FFMpegWriter(fps=20)
anim.save(outdir + 'Test_save_DALES_old_'+str(svname3)+'.mp4', writer=writervideo)




plt.close()                   # avoid plotting a spare static plot

