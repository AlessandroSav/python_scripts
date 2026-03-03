import argparse
import os
from typing import Optional, Tuple

import numpy as np
import xarray as xr


G = 9.80665
RD = 287.05


def _find_level_dim(ds: xr.Dataset) -> Tuple[str, xr.DataArray]:
    for name in ("level", "isobaricInhPa", "plev", "pressure"):
        if name in ds.dims or name in ds.coords:
            lev = ds[name]
            return name, lev
    raise ValueError("Could not find a pressure level coordinate (tried: level/isobaricInhPa/plev/pressure).")


def _pressure_pa_from_level(lev: xr.DataArray) -> xr.DataArray:
    lev_vals = lev.values
    if np.nanmax(lev_vals) <= 2000:  # hPa
        return lev.astype("float64") * 100.0
    return lev.astype("float64")


def _get_T_q(ds: xr.Dataset) -> Tuple[xr.DataArray, Optional[xr.DataArray]]:
    T = None
    for name in ("t", "T", "130.128"):
        if name in ds:
            T = ds[name]
            break
    if T is None:
        raise ValueError("Temperature variable not found (expected 't' or 'T').")

    q = None
    for name in ("q", "133.128"):
        if name in ds:
            q = ds[name]
            break
    return T, q


def _height_from_geopotential(ds: xr.Dataset) -> Optional[xr.DataArray]:
    for name in ("z", "129.128", "gh", "geopotential"):
        if name in ds:
            return ds[name] / G
    return None


def _height_from_hypsometric(p_pa: xr.DataArray, T: xr.DataArray, q: Optional[xr.DataArray], level_dim: str) -> xr.DataArray:
    # Virtual temperature
    if q is not None:
        # q in kg/kg is ideal, but some datasets store g/kg. Try to detect.
        qv = q
        if "units" in q.attrs and "g" in q.attrs["units"] and "/kg" in q.attrs["units"]:
            qv = q / 1000.0
        elif float(q.max()) > 0.5:  # heuristic: g/kg
            qv = q / 1000.0
        Tv = T * (1.0 + 0.61 * qv)
    else:
        Tv = T

    # Sort levels from bottom (high p) to top (low p)
    p_sorted = p_pa.sortby(p_pa, ascending=False)
    Tv_sorted = Tv.sortby(p_pa, ascending=False)

    # log-pressure thickness between adjacent levels
    ln_p = np.log(p_sorted)
    dlnp = ln_p.diff(level_dim)

    # mid-layer Tv
    Tv_mid = 0.5 * (Tv_sorted.isel({level_dim: slice(None, -1)}) + Tv_sorted.isel({level_dim: slice(1, None)}))

    # dz between adjacent levels (positive upwards because p decreases upward)
    dz = - (RD / G) * Tv_mid * dlnp

    # Integrate from the bottom-most level: set z(bottom)=0 and accumulate upwards
    zero = xr.zeros_like(Tv_sorted.isel({level_dim: 0})).astype("float64")
    zero = zero.expand_dims({level_dim: [p_sorted[level_dim].values[0]]})
    z_cum = dz.cumsum(dim=level_dim).astype("float64")
    z_sorted = xr.concat([zero, z_cum], dim=level_dim)

    # Return to original level ordering
    return z_sorted.sortby(p_pa)


def _interp_1d(height_1d: np.ndarray, values_1d: np.ndarray, target_heights: np.ndarray) -> np.ndarray:
    m = np.isfinite(height_1d) & np.isfinite(values_1d)
    if m.sum() < 2:
        return np.full_like(target_heights, np.nan, dtype="float64")

    h = height_1d[m]
    v = values_1d[m]
    order = np.argsort(h)
    h = h[order]
    v = v[order]

    # np.interp requires increasing x
    out = np.interp(target_heights, h, v, left=np.nan, right=np.nan)
    return out.astype("float64")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--subdomain", type=str, required=True)
    parser.add_argument("--exp_id", type=str, required=True)
    parser.add_argument("--exp_type", type=str, required=True)
    parser.add_argument("--lead_time", type=int, default=0)
    args = parser.parse_args()

    subdomain = args.subdomain
    exp_id = args.exp_id
    exp_type = args.exp_type
    lead_time = args.lead_time

    dir_in = f"/perm/paaa/IFS/{subdomain}/{exp_type}"
    in_path = os.path.join(dir_in, f"{exp_id}_pl_t{lead_time}.nc")
    out_path = os.path.join(dir_in, f"{exp_id}_z_t{lead_time}.nc")

    print(f"Reading: {in_path}")
    ds = xr.open_dataset(in_path)

    level_dim, lev = _find_level_dim(ds)
    if level_dim != "level":
        ds = ds.rename({level_dim: "level"})
        lev = ds["level"]
        level_dim = "level"

    p_pa = _pressure_pa_from_level(lev)

    # Target heights (meters)
    hlevs = np.array(
        [0, 10, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450, 500,
         570, 640, 710, 880, 950, 1050, 1150, 1250, 1350, 1450, 1550, 1700, 1900, 2200,
         2500, 2800, 3100, 3400, 3700, 4000, 4500, 5000, 5500],
        dtype="float64",
    )

    height = _height_from_geopotential(ds)
    if height is None:
        print("No geopotential variable found; falling back to hypsometric height from T/(q).")
        T, q = _get_T_q(ds)
        height = _height_from_hypsometric(p_pa, T, q, level_dim=level_dim)
    else:
        print("Using geopotential to derive geometric height.")

    # Build output dataset
    out_coords = {"height": hlevs}
    for dim in ds.dims:
        if dim == "level":
            continue
        if dim in ds.coords:
            out_coords[dim] = ds[dim]
        else:
            out_coords[dim] = np.arange(ds.sizes[dim])

    ds_out = xr.Dataset(coords=out_coords)
    ds_out["height"].attrs.update({"units": "m", "long_name": "Height above surface (interpolated from pressure levels)"})

    # Carry over non-level variables (e.g., ensemble member?)
    # Only keep variables that depend on level; others will be copied.
    for var in ds.data_vars:
        da = ds[var]
        if "level" in da.dims:
            print(f"Interpolating {var} to fixed heights")
            da_i = xr.apply_ufunc(
                _interp_1d,
                height,
                da,
                xr.DataArray(hlevs, dims=("height",)),
                input_core_dims=[["level"], ["level"], ["height"]],
                output_core_dims=[["height"]],
                vectorize=True,
                output_dtypes=["float64"],
            )
            da_i = da_i.assign_coords(height=hlevs)
            # Ensure dim order (preserve any extra dims like ensemble member 'number')
            non_level_dims = [d for d in da.dims if d != "level"]
            da_i = da_i.transpose(*non_level_dims, "height")
            da_i.attrs = da.attrs.copy()
            ds_out[var] = da_i
        else:
            ds_out[var] = da

    print(f"Saving: {out_path}")
    ds_out.to_netcdf(out_path)


if __name__ == "__main__":
    main()
