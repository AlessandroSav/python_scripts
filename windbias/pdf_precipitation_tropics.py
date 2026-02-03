import numpy as np
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
import xarray as xr
import sys
import matplotlib as mpl
from matplotlib.colors import BoundaryNorm
import os
import matplotlib.ticker as ticker
import json
from datetime import datetime, timedelta
import matplotlib.ticker as mticker
from matplotlib.ticker import FuncFormatter
from xhistogram.xarray import histogram

readdir = '/perm/paaa/IFS/maritime_c'
savedir = '/perm/paaa/images'

def deacc_variable(ds,dt=3600,restart=1):
    # ds must be an xarray with dimension 'time'
    # dt is interval in seconds
    ds_deaccumulated = ds.diff('time')/dt
    return ds_deaccumulated


exp_info = {
    "IMERG": {"color": "black",        "name": "IMERG",                                           "pattern": "solid"},
    "iuze":  {"color": "midnightblue", "name": "50r1, dryMF (0x mom, 1x scalars)",                "pattern": "solid"},
    "iurj":  {"color": "red",          "name": "50r1, dryMF (2x mom, 2x scalars)",                "pattern": "solid"},
    "iuv9":  {"color": "c",            "name": "50r1, dryMF (2x mom, 1x scalars)",                "pattern": "solid"},
    "iv93":  {"color": "orange",       "name": "50r1, dryMF (2x momentum, 2x scalars) allPBL",    "pattern": "solid"},
    "isv0":  {"color": "pink",         "name": "50r1, red. downdrafts (0.05), DM rain evap",      "pattern": "solid"},


}
exp_list      = ["IMERG","iuze","iurj","iuv9",'iv93','isv0']
color_list   = [exp_info[exp]["color"]   for exp in exp_list]
name_list    = [exp_info[exp]["name"]    for exp in exp_list]
pattern_list = [exp_info[exp]["pattern"] for exp in exp_list]

min_lon = 0
min_lat = -20
max_lon = 360
max_lat = 20
# For the histogram
nbins = 100
bin_edges = np.logspace(-3, 3, nbins + 1)  # 10^-3 to 10^3
# Compute bin centers
bin_centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])
bin_widths = np.diff(bin_edges)

date='20250101'
name_dic = {}
color_dic = {}
data = {}
longitudes_2d = {}
latitudes_2d = {}
counts = {}
prob = {}
pdf_weighted={}
start_time = datetime.strptime("01.01.2025, 00", "%d.%m.%Y, %H")
end_time   = datetime.strptime("06.01.2025, 00", "%d.%m.%Y, %H")
for e,runid in enumerate(exp_list):
    print(f'Opening {runid}')
    name_dic[runid]=name_list[e]
    color_dic[runid]=color_list[e] 
    if runid=="IMERG":   
        file=f'/perm/natb/OBS/GPM_IMERG/GPM_IMERG_hourly_{date}_tropics.nc'
        ds = xr.open_dataset(file,engine='netcdf4')['precipitation'].load()
        ds = ds.sel(time=slice(start_time,end_time))
        data[e]=ds
        data[e] = data[e].sel(lat=slice(min_lat,max_lat),lon=slice(min_lon,max_lon))  

    else:
        file=f'{readdir}/hres_{runid}_2D_precip_{date}_tropics.nc'
        ds = xr.open_dataset(file,engine='netcdf4')['tp'].load()
        ds = ds.sel(time=slice(start_time,end_time))
        data[e]=ds
        data[e] = deacc_variable(data[e]) # deaccumulate variable
        data[e] = data[e]*1000 # from meters to mm
        data[e] = data[e]*3600 # from mm/seconds to mm/hour
        data[e] = data[e].sel(latitude=slice(max_lat,min_lat),longitude=slice(min_lon,max_lon))

    
    # Flatten all dimensions into 1D array
    data[e] = data[e].values.flatten()
    # Remove NaNs
    data[e] = data[e][~np.isnan(data[e])]
    # Compute histogram counts
    counts[e], _ = np.histogram(data[e], bins=bin_edges, density=False)
    # Convert counts to probability of each bin
    prob[e] = counts[e] / counts[e].sum()  # sum(prob) = 1
    # tp * f(tp) = bin_center * probability_density
    pdf_weighted[e] = bin_centers * prob[e] 
    pdf_weighted[e] = bin_centers * counts[e] / len(data[e])


print(f'Saving figure in {savedir}/pdf_precipitation_tropics.png')
# Plot
plt.figure(figsize=(10,4))
for e,runid in enumerate(exp_list):
   plt.plot(bin_centers, pdf_weighted[e], lw=2,c=color_list[e],label=name_list[e])
plt.xlabel("pr (mm/hour)")
plt.ylabel("pr × f(pr)")
plt.title("Distribution of total precipitation")
plt.xscale("log")
plt.legend()
plt.grid(True, alpha=0.3)
plt.savefig(f'{savedir}/pdf_precipitation_tropics.png')
plt.close()  