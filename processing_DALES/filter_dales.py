import xarray as xr
import os
import dask
import h5netcdf
import numpy as np
from datetime import timedelta
import gc
from dask.diagnostics import ProgressBar
import sys
sys.path.append(".")
sys.path.append("/home/paaa/python_scripts/")
import my_functions as mfun

#%% FUNCTIONS
def load_dales_var(var_name,dir_dales,levels):
    str_time = '2022-05-17T02'
    end_time = '2022-05-19T00'
    xmin, xmax = 62000, 112000
    ymin, ymax = 21000, 71000
    zmin = levels[0]
    zmax = levels[1]

    files_var = [os.path.join(dir_dales, f) for f in os.listdir(dir_dales) if 'loobos_' in f and f"{var_name}_3d" in f and f.endswith('.nc')]
    dset_var = xr.open_mfdataset(files_var,
                                combine='by_coords',
                                chunks={'time': 12, 'zt': 8, 'xt': -1, 'yt': -1},
                                engine='h5netcdf'
                                )
    dset_var = dset_var.sel(xt=slice(xmin-1, xmax+1), yt=slice(ymin-1, ymax+1), zt=slice(zmin, zmax))
    dset_var['time'] = dset_var['time'].astype('datetime64[ns]') + np.timedelta64(2, 'D')
    dset_var = dset_var.sel(time=slice(str_time, end_time))
    return dset_var.compute()
#%%

##############################
# Height selection
id_z = [300,600,900,1200,1500]
##############################
for zmax in id_z:
    zmin = zmax-300
    print(f"Height to process: {str(zmax)}")
    # Paths and configuration
    dir_dales = "/perm/paaa/Les/Cases/Catrine"
    save_dir = dir_dales
    suffix = '_50km_z'+str(zmax)

    # Variables to process
    # vars = ['sv004','u','v','thl',]
    vars = ['sv004',]
    if 'w' not in vars:
        vars.append('w')
    print(f"Variables to process: {vars}")

    # Load w first to compute w_p for all variables
    dset_w = load_dales_var('w',dir_dales,[zmin,zmax])
    dset_w_mean = dset_w.mean(('xt', 'yt')).chunk({'time': -1, 'zt': -1})
    w_p = dset_w['w'] - dset_w_mean['w']

    # Grid size
    xsize = float(dset_w.xt[-1] - dset_w.xt[0])
    ysize = float(dset_w.yt[-1] - dset_w.yt[0])
    nr_klps = 50
    xsize_m = xsize
    xsize_km = xsize_m / 1000
    # Logarithmically spaced 'space' values between 0.1 km and xsize_km
    space = np.logspace(np.log10(0.1), np.log10(xsize_km), nr_klps)
    # Compute klps from space
    klps = xsize_km / (2 * space)

    # Main loop per variable
    for var in vars:
        if var == 'w':
            continue  # Already loaded
        print(f"Processing variable: {var}")

        dset_var = load_dales_var(var,dir_dales,[zmin,zmax])
        var_mean = dset_var.mean(('xt', 'yt')).chunk({'time': -1, 'zt': -1})
        var_p = dset_var[var] - var_mean[var]
        unfiltered_flux = (var_p * w_p).mean(dim=['xt', 'yt'])  # result has dims time, zt

        # Prepare for scale separation
        var_pfw_pf_all = []
        var_psfw_psf_all = []

        for klp in klps:
            print(f"  Filtering scale: klp={klp:.2f}")
            mask = mfun.create_radial_mask((dset_var.sizes["yt"], dset_var.sizes["xt"]), cutoff=klp)
            # up filter
            var_pf = mfun.low_pass_filter(var_p, mask)
            w_pf = mfun.low_pass_filter(w_p, mask)
            # sub filter 
            var_psf = var_p - var_pf
            w_psf = w_p - w_pf
            # compute flux
            pfw_pf = (var_pf * w_pf).mean(dim=['xt', 'yt'])#.compute()
            psfw_psf = (var_psf * w_psf).mean(dim=['xt', 'yt'])#.compute()

            var_pfw_pf_all.append(pfw_pf)
            var_psfw_psf_all.append(psfw_psf)

            del var_pf, w_pf, var_psf, w_psf, pfw_pf, psfw_psf
            gc.collect()
        print(f"  Concatenating...")
        klp_coord    = xr.DataArray(klps, dims="klp", name="klp")
        filter_coord = xr.DataArray(space, dims="klp", name="filter")
        temp = xr.concat(var_pfw_pf_all, dim=klp_coord).assign_coords(filter=filter_coord)
        var_pfw_pf = xr.Dataset(
            {
                f"{var}w_filtered": temp,
                f"{var}w_unfiltered": unfiltered_flux,
            }
        )
        print(f"  Saving...")
        with ProgressBar():
            var_pfw_pf.to_netcdf(f"{save_dir}/{var}_flx_filter{suffix}.nc", compute=True)

        del dset_var, var_p, var_mean, var_pfw_pf_all, var_psfw_psf_all, temp
        gc.collect()

print("Done.")
