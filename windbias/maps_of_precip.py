import numpy as np
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
import xarray as xr
import xarray
import sys
import matplotlib as mpl
from matplotlib.colors import BoundaryNorm
import os
import matplotlib.ticker as ticker
import json
from datetime import datetime, timedelta
import matplotlib.ticker as mticker
from matplotlib.ticker import FuncFormatter

readdir = '/perm/paaa/IFS/maritime_c'
savedir = '/perm/paaa/images/temp_for_movies'



def get_my_colobar(name):
    if name=='precip_9':
        cmap = mpl.colors.LinearSegmentedColormap.from_list("", ["#FFFFFF","#00ffff","#a687d3","#643B9F","#371F76","#FFD700","#ff6500","#FF0000","#FF00FF"])
        norm = BoundaryNorm(np.array([0.,0.01,0.03,0.1,0.3,1,3,10,30,100]), ncolors=cmap.N, clip=True)
    return cmap,norm

def plot_adjust(pcm):
    ax.coastlines(linewidth=1.0)
    gl = ax.gridlines(draw_labels=True, linestyle='--', alpha=0.)
    gl.xlabel_style = {'color': 'black', 'weight': 'light', 'size': 10}
    gl.ylabel_style = {'color': 'black', 'weight': 'light', 'size': 10}
    gl.top_labels = False
    gl.right_labels = False
    cb=plt.colorbar(pcm,orientation='horizontal',aspect=50)
    cb.set_label(label='Precipitation [mm/hour]',size=10)
    cb.ax.tick_params(labelsize=10)
    ticks = [0., 0.01, 0.03, 0.1, 0.3, 1, 3, 10, 30, 100]
    cb.set_ticks(ticks)
    cb.set_ticklabels([str(t) for t in ticks])  # Convert numbers to strings

    #formatter = ticker.ScalarFormatter(useMathText=True)
    #formatter.set_powerlimits((-2, 2))  # Controls when scientific notation is used
    #formatter.set_scientific(True)
    #cb.ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.3f'))
    plt.tight_layout()


def conditional_formatter(x, _):
    if abs(x) < 0.1:
        return f"{x:.2f}"
    else:
        return f"{x:.1f}"
    
def deacc_variable(ds,dt=3600,restart=1):
    # ds must be an xarray with dimension 'time'
    # dt is interval in seconds
    ds_deaccumulated = ds.diff('time')/dt
    return ds_deaccumulated



exp_info = {
    "IMERG": {"color": "black",        "name": "IMERG",                                                "pattern": "solid"},
    "iuze":  {"color": "midnightblue", "name": "50r1, dryMF (0x momentum, 1x scalars)",                "pattern": "solid"},
    "iurj":  {"color": "red",          "name": "50r1, dryMF (2x momentum, 2x scalars)",                "pattern": "solid"},
    "iuv9":  {"color": "purple",       "name": "50r1, dryMF (2x momentum, 1x scalars)",                 "pattern": "solid"},
    "iv93":  {"color": "orange",       "name": "50r1, dryMF (2x momentum, 2x scalars) allPBL",          "pattern": "solid"},

    # "iq78":  {"color": "midnightblue", "name": "49r1",                                                 "pattern": "solid"},
    # "isry":  {"color": "dodgerblue",   "name": "50r1",                                                 "pattern": "solid"},
    # "iu33":  {"color": "lightblue",    "name": "50r1, no moisture convergence",                        "pattern": "solid"},
    # "iu34":  {"color": "cadetblue",    "name": "50r1, RMFDPREC=1.0",                                   "pattern": "solid"},    
    # "isrp":  {"color": "cyan",         "name": "50r1, no downdrafts",                                  "pattern": "solid"},
    # "issd":  {"color": "orange",       "name": "50r1, no downdrafts, DM rain evap",                    "pattern": "solid"},
    # "iswy":  {"color": "saddlebrown",  "name": "50r1, no downdrafts, DM rain evap, RMFDPREC=0.5",      "pattern": "solid"},
    # "isxm":  {"color": "red",          "name": "50r1, no downdrafts, DM rain evap, RMFDPREC=1.0",      "pattern": "solid"},
    # "isv0":  {"color": "pink",         "name": "50r1, red. downdrafts (0.05), DM rain evap",           "pattern": "solid"},
    # "isse":  {"color": "purple",       "name": "50r1, red. downdrafts (0.1), DM rain evap",            "pattern": "solid"},
    # "issf":  {"color": "lime",         "name": "50r1, red. downdrafts (0.15), DM rain evap",           "pattern": "solid"},
    # "isro":  {"color": "forestgreen",  "name": "50r1, red. downdrafts (0.2), DM rain evap",            "pattern": "solid"},
    # "itck":  {"color": "grey",         "name": "50r1, red. downdrafts (0.15), DM (x0.5)",              "pattern": "solid"},
    # "itcl":  {"color": "lightgrey",    "name": "50r1, red. downdrafts (0.15), DM (x0.5), RNUM=0.6",    "pattern": "solid"},     
    # "iupb":  {"color": "darkred",      "name": "50r1, ENTRORG=0.0035",                                 "pattern": "solid"},     
    # "itg9":  {"color": "thistle",      "name": "50r1, DM, ENTRORG=0.0035",                             "pattern": "solid"},   
    # "itnb":  {"color": "plum",         "name": "50r1, DM, ENTRORG=0.0035, DETRPEN=0.00015",            "pattern": "solid"},        
    # "itn9":  {"color": "olive",        "name": "50r1, DM, ENTRORG=0.00233",                            "pattern": "solid"}, 
    # "itna":  {"color": "darkkhaki",    "name": "50r1, DM, ENTRORG=0.00233, DETRPEN=0.0001",            "pattern": "solid"},  
    # "iu4j":  {"color": "greenyellow",  "name": "50r1, DD(0.1),DM(0.5),RMFDPREC=1,ENTRORG=0.00233",     "pattern": "solid"},     
    # "itbs":  {"color": "yellow",       "name": "50r1, red. DD (0.05), DM, ENTRORG=0.0035",             "pattern": "solid"},
    # "itfm":  {"color": "lightcoral",   "name": "50r1, red. DD (0.15), DM, ENTRORG=0.0025",             "pattern": "solid"},
    # "ity2":  {"color": "slateblue",   "name": "50r1, entr: RH squared + neutral value=0.6",            "pattern": "solid"},    
    # "ium0":  {"color": "lightcoral",  "name": "50r1, T and vor nudged",                                "pattern": "solid"},
    # "ium1":  {"color": "slateblue",   "name": "50r1, T and vor nudged, DM, ENTRORG=0.0035",            "pattern": "solid"},      
}
#    "iti4":  {"color": "plum",         "name": "50r1, DM rain evap, ENTRORG=0.0035, DETRPEN=0.0015",   "pattern": "solid"},        
#    "itmg":  {"color": "olive",        "name": "50r1, DM rain evap, ENTRORG=0.00233, DETRPEN=0.00075", "pattern": "solid"}, 
#    "itme":  {"color": "darkkhaki",    "name": "50r1, DM rain evap, ENTRORG=0.00233, DETRPEN=0.001",   "pattern": "solid"},    

#,"itnb","ity2","itcl"
exp_list      = ["IMERG","iuze","iurj","iuv9","iv93"]

color_list   = [exp_info[exp]["color"]   for exp in exp_list]
name_list    = [exp_info[exp]["name"]    for exp in exp_list]
pattern_list = [exp_info[exp]["pattern"] for exp in exp_list]

date='20250101'

name_dic = {}
color_dic = {}
data = {}
longitudes_2d = {}
latitudes_2d = {}

for e,runid in enumerate(exp_list):
 name_dic[runid]=name_list[e]
 color_dic[runid]=color_list[e] 
 if runid=="IMERG":   
    file=f'/perm/natb/OBS/GPM_IMERG/GPM_IMERG_hourly_{date}_tropics.nc'
    ds = xr.open_dataset(file,engine='netcdf4')['precipitation'].load()
    data[e]=ds
    longitudes = data[e].lon
    latitudes = data[e].lat
    latitudes_2d[e], longitudes_2d[e] = np.meshgrid(latitudes,longitudes)     

 elif runid in ['isrp',]: ## this is how Tobias is calling his files  
    file=f'{readdir}/hres_{runid}_2D_precip_1hafter_{date}_remap_0.1x0.1_tropics.nc'
    ds = xr.open_dataset(file,engine='netcdf4')['var228'].load()
    data[e]=ds
    longitudes = data[e].lon
    latitudes = data[e].lat
    longitudes_2d[e], latitudes_2d[e] = np.meshgrid(longitudes,latitudes)  
 else:
    file=f'{readdir}/hres_{runid}_2D_precip_{date}_tropics.nc'
    ds = xr.open_dataset(file,engine='netcdf4')['tp'].load()
    data[e]=ds
    longitudes = data[e].longitude
    latitudes = data[e].latitude
    longitudes_2d[e], latitudes_2d[e] = np.meshgrid(longitudes,latitudes)  
    data[e] = deacc_variable(data[e]) # deaccumulate variable
    data[e] = data[e]*1000 # from meters to mm
    data[e] = data[e]*3600 # from mm/seconds to mm/hour

min_lon = 120
min_lat = -20
max_lon = 180
max_lat = 20
 


# Starting time
start_time = datetime.strptime("01.01.2025, 00 UTC", "%d.%m.%Y, %H UTC")
# Generate list of time strings for the next 48 hours
time = [(start_time + timedelta(hours=i)).strftime("%d.%m.%Y, %H UTC") for i in range(120)]

 
for t in range(0,120):
 #if t==48:   
  print(t)  

  # Define title strings
  Title = [name_list[0] + f", {time[t]}"] + name_list[1:]   
  fig, ax = plt.subplots(nrows=2,ncols=3,dpi=200,subplot_kw={'projection':ccrs.PlateCarree()},figsize=(18,8))
  axes = ax.flatten()
  n=0
    
  for e in range(len(exp_list)): 
        extent = [min_lon, max_lon, min_lat, max_lat] 
        axes[n].set_extent(extent, crs=ccrs.PlateCarree())  
        cmap,norm=get_my_colobar('precip_9')
        cnf=axes[n].pcolormesh(longitudes_2d[e], latitudes_2d[e], data[e][t,:,:].squeeze(),shading='nearest', norm=norm, cmap=cmap, transform=ccrs.PlateCarree())
        #cnf = axes[n].contourf(longitudes_2d,latitudes_2d,data[e][t,:,:].squeeze(),transform = ccrs.PlateCarree(),transform_first=True,cmap=cmap,extend='max')

        #cbar1_ax = fig.add_axes([0.17, 0.17, 0.66, 0.015],autoscalex_on=True)
        #cbar2_ax = fig.add_axes([0.92, 0.15, 0.01, 0.3])
        #cbar1=fig.colorbar(cnf, cax=cbar1_ax,orientation='horizontal',ticks=ticks)
        #if e ==2 or e==5:
        #    cbar1 = fig.colorbar(cnf, ax=axes[n], orientation='vertical', shrink=0.8)
        #    cbar1.set_label("Precipitation [mm/h]",fontsize=14)
            # Increase tick label size on the colorbar
        #    cbar1.ax.tick_params(labelsize=14)
        axes[n].set_title(Title[e],fontsize=14)
        axes[n].coastlines(color='black')    
        gl = axes[n].gridlines(draw_labels=True)
        gl.xlocator = mticker.FixedLocator(range(-180, 181, 10))
        gl.ylocator = mticker.FixedLocator(range(-90, 91, 10))        
        gl.xlines = False
        gl.ylines = False  # hides the gridlines
        gl.top_labels = False
        gl.left_labels = True
        gl.bottom_labels = True
        gl.right_labels = False
        gl.xlabel_style = {'size': 11}
        gl.ylabel_style = {'size': 11}

        n=n+1

  # Add a single colorbar to the right
  cbar_ax = fig.add_axes([0.83, 0.2, 0.02, 0.6])  # [left, bottom, width, height]
  cbar=fig.colorbar(cnf, cax=cbar_ax)     
  cbar.set_label("Precipitation [mm/h]",fontsize=14)
  cbar.ax.yaxis.set_major_formatter(FuncFormatter(conditional_formatter)) 
  fig.subplots_adjust(bottom=0.1, top=0.9, left=0.1, right=0.8,
                    wspace=0.15, hspace=0.15)
    
  plt.savefig(f'{savedir}/map_downdrafts_onlyMC_ENTR_nudged_{date}_{t}.png')
  plt.close()   