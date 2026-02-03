#%% Libraries 
import numpy as np
import xarray as xr
from scipy.fft import fft2, ifft2, fftshift
from xarray import apply_ufunc
from scipy.stats import binned_statistic

#%% Define constants
Rd      =   287.04         # J/(K*kg)
Rv      =   461.5          # J/kg*K
Lv      =   2.53e6        # J/kg
cp      =   1004.7090      # J/(K*kg)
Cpd     =   1005.7
Cpv     =   1850.
Cl      =   4190.
g       =   9.80665 #m/s2
p0      =   1e5         # Pa         
es0     =   610.78 
T0      =   273.15       # K
bt      =   35.86 
at      =   17.27
v_Kar   =   0.41            # von Karman constant
omega   =   7.27 * 10**-5      # Angular speed [1/s]

def concentration_to_ppm(gas,concentration):
    """
    Convert concentration of co2 to ppm 
    Input:
         gas = name of the gas 
         concentration    = concentration in g/g
    """
    if gas == 'co2':
        mmgas=44.0095 # g/mol
    elif gas =='ra':
        mmgas=222 # g/mol
    elif gas =='ch4':
        mmgas=16.04 # g/mol
    else:
        raise ValueError("Gas wrongly specified or not known.")
    
    mmair=28.9644 #g/mol # assumption of dry air
    ppm = 10**6 *concentration * mmair/mmgas 
    return ppm

def VertInterpolMeter (pres, t, q, ghg, nh) :
    """
    Vertical interpolation for coordinate in meters
    Input:
         pres = pressure at half levels
         t    = temperature
         q    = specific humidity
         nh   = height in m
    """
    #
    # Initialisation
    #
    rd=287.06
    rg=9.8066
    #
    # Height on half levels
    #
    nlev = len(t)
    zh = np.zeros(nlev+1, float)
    #
    # Loop over the levels
    #
    for l in range(nlev-1,-1,-1) :
      zh[l] = zh[l+1] + rd/rg * t[l] * (1.0 + 0.61 * q[l]) * np.log(pres[l+1]/pres[l])
    #
    # Height on full levels
    #
    zh = np.array(.5 * (zh[1:] + zh[0:-1]))
    if max(nh) > max(zh) :
      print()
      print('WARNING: need more vertical levels for interpolation')
      print('Highest level = %7.2f for an observation at %7.2f' % (max(zh),max(nh)))
    #
    # need to reverse before interpolation (as IFS vertical profile is from top to bottom).
    #
    zh = zh[-1::-1]
    ghg = ghg[-1::-1]
    #
    # Interpolation
    #
    return np.interp(nh, zh, ghg)



def pitagora_fun(a,b):
    i = (a**2 + b**2)**(1/2)
    return i

def calc_rho(p,T,q=0):
    # p in Pa
    # T in K
    # q in kg/kg
    rho = p/(Rd*T*(1+0.61*q)) # kg/m3
    return rho

def calc_pz_hypsometric(ps,z,Tv):
    # ps surface pressure in Pa
    # z height in m
    # Tv virtual temperature in K
    p = ps * np.exp(-g*z / (Rd * Tv))
    return p

#%% Calculation of saturation vapor pressure (in Pa)
def calc_es(T):
    # T in K
    ### Tetens formula
    # es = es0 * np.exp((Lv/Rv)*((1/T0)-(1/T))) # Pa
    es = es0 * np.exp(at*(T-T0) / (T-bt))       # Pa
    ###
    # es = 611*np.exp(0.067*(T-T0))             # Pa
    return(es)
#%% Calculation of saturation mixing ratio (in kg/kg)
def calc_rs(T,p):
    es = calc_es(T)
    rs = Rd/Rv*es/(p - es)
    return(rs)

#%% Calculation of virtual temperature (K) for environment (assuming ql ~ 0)
def calc_Tv(T,p,RH):
    rs = calc_rs(T,p)
    r = (RH/100.)*rs
    Tv = T*(1+ 0.61 * r)
    return(Tv)

def calc_T(th,p):
    # th in K
    # p in Pa
    T = th * calc_exner(p)
    return T

def calc_th(T,p):
    # T in K
    # p in Pa
    th = T / calc_exner(p)
    return th

def calc_thl(th,ql,p):
    # th in K
    # ql in 
    # p in Pa
    thetal = th - Lv / (cp * calc_exner(p)) * ql
    return thetal

def calc_exner(p):
    # p in Pa
    exner = (p/p0)**(Rd/cp)
    return exner

def calc_th_from_thl(thl,ql,p):
    # th in K
    # ql in 
    # p in Pa
    th = thl + Lv / (cp * calc_exner(p)) * ql
    return th

    
#%%
def compute_isotropic_spectrum(field, dx, dy=None, nbins=50):
    """
    Compute the isotropic 1D power spectrum from a 2D field,
    accounting for non-square domains with grid spacings dx and dy.

    Parameters
    ----------
    field : 2D ndarray
        Input field (e.g., velocity or cloud field)
    dx : float
        Grid spacing in x-direction (m)
    dy : float, optional
        Grid spacing in y-direction (m). If None, assumed equal to dx.
    nbins : int
        Number of radial bins for isotropic averaging

    Returns
    -------
    k_bin_centres : 1D ndarray
        Wavenumber bin centres (cycles per m)
    power_spectrum_1d : 1D ndarray
        Isotropic 1D power spectrum (variance density)
    """
    if dy is None:
        dy = dx

    nx, ny = field.shape

    # 2D FFT
    ft_field = np.fft.fft2(field)
    ft_field = np.fft.fftshift(ft_field)
    power_spectrum_2d = np.abs(ft_field)**2 / (nx * ny)

    # Wavenumber arrays
    kx = np.fft.fftshift(np.fft.fftfreq(nx, d=dx))
    ky = np.fft.fftshift(np.fft.fftfreq(ny, d=dy))
    kxm, kym = np.meshgrid(kx, ky, indexing='ij')
    wavenumber = np.sqrt(kxm**2 + kym**2)

    # Bin power spectrum isotropically
    k_flat = wavenumber.flatten()
    ps_flat = power_spectrum_2d.flatten()

    k_bins = np.linspace(0, k_flat.max(), nbins+1)

    bin_means, _, _ = binned_statistic(
        k_flat, ps_flat, statistic='mean', bins=k_bins
    )

    # Return bin centres, not edges
    k_bin_centres = 0.5 * (k_bins[1:] + k_bins[:-1])

    return k_bin_centres, bin_means

def compute_only_spectrum(field_2d, dx, dy, nbins):
    k1d, spec_1d = compute_isotropic_spectrum(field_2d, dx=dx, dy=dy, nbins=nbins)
    return spec_1d
  
def create_radial_mask(shape, cutoff):
    h, w = shape
    hc, wc = h // 2, w // 2
    Y, X = np.ogrid[:h, :w]
    r = np.hypot(X - wc, Y - hc)
    return (r <= cutoff).astype(float)

def low_pass_filter(
    field: xr.DataArray,
    mask: np.ndarray,
    core_dims: tuple[str, str] = ("yt", "xt")
) -> xr.DataArray:
    """Apply low-pass filter to 2D slices of field with given mask.

    Parameters:
    -----------
    field : xr.DataArray
        Input data array.
    mask : np.ndarray
        2D frequency-domain mask to apply.
    core_dims : tuple of str
        Names of the 2D spatial dimensions (e.g. ("yt", "xt")).

    Returns:
    --------
    xr.DataArray
        Filtered array.
    """

    def _filter(field_2d):
        F = fft2(field_2d)
        F = fftshift(F)
        F *= mask
        F = fftshift(F)
        return np.real(ifft2(F))

    return apply_ufunc(
        _filter,
        field,
        input_core_dims=[list(core_dims)],
        output_core_dims=[list(core_dims)],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[float],
    )

#%%
def deacc_variable(ds,dt=3600,restart=1):
    # ds must be an xarray with dimension 'time'
    # dt is interval in seconds
    # restart is the hour at which the accumulation restarts every day 
    
    # Identify reset points where time.hour == 1
    reset_indices = ds.time.dt.hour == restart ## This needs to be changed if the forecast is longer than 24 hours !!!
    # Compute the difference along the 'time' dimension for those variables
    d_ds = ds.diff('time')
    # Apply reset correction across selected variables
    d_ds_corrected = d_ds.where(~reset_indices.isel(time=slice(1, None)), other=ds.isel(time=slice(1, None)))
    # Add back the first values (lost due to diff operation)
    ds_deaccumulated = xr.concat([ds.isel(time=0), d_ds_corrected], dim='time')/dt
    
    return ds_deaccumulated

#%% Compute BLH from Ri number
def interp_pblh(Ri, zt, threshold=0.25):
    # Difference from threshold
    diff = Ri - threshold
    # Condition: Ri > 0.25
    mask = diff > 0
    # Find index along zt where Ri first exceeds threshold
    ri_ind = mask.argmax(dim="zt")
    # Gather values just before and after the threshold crossing
    ri_low = Ri.isel(zt=ri_ind - 1)
    ri_high = Ri.isel(zt=ri_ind)
    zt_low = zt.isel(zt=ri_ind - 1)
    zt_high = zt.isel(zt=ri_ind)
    # Linear interpolation to where Ri == threshold
    pblh = zt_low + (zt_high - zt_low) * ((threshold - ri_low) / (ri_high - ri_low))
    return pblh