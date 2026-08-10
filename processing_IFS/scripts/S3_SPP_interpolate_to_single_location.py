import argparse
import os

import numpy as np
import xarray as xr


UTC_to_LT = +2  # hours


def inverse_distance_weighting(ds: xr.Dataset, target_lat: float, target_lon: float) -> xr.Dataset:
    # Get latitude and longitude arrays (assumed to be 1D or 2D broadcastable)
    lat = ds['latitude']
    lon = ds['longitude']
    # Compute distance in degrees
    dist = np.sqrt((lat - target_lat)**2 + (lon - target_lon)**2)
    # If there is an exact match, return it directly
    if (dist == 0).any():
        return ds.where(dist == 0, drop=True)
    # Otherwise perform inverse-distance weighting
    weights = 1 / dist
    weights = weights / weights.sum(dim=("latitude", "longitude"))
    return ds.weighted(weights).mean(dim=("latitude", "longitude"))


def _save(ds: xr.Dataset, path: str) -> None:
    """Delete any existing file before writing to avoid PermissionError on overwrite."""
    if os.path.exists(path):
        os.remove(path)
    ds.to_netcdf(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--subdomain", type=str, required=True)
    parser.add_argument("--exp_id", type=str, required=True)
    parser.add_argument("--exp_type", type=str, required=True)
    parser.add_argument("--levels", type=str, required=True)  # srf/pl/z
    parser.add_argument("--lead_time", type=int, default=0)
    args = parser.parse_args()

    subdomain = args.subdomain
    exp_id = args.exp_id
    exp_type = args.exp_type
    levels = args.levels
    lead_time = args.lead_time

    dir_in = f"/perm/paaa/IFS/{subdomain}/{exp_type}"
    in_path = os.path.join(dir_in, f"{exp_id}_{levels}_t{lead_time}.nc")

    if subdomain == 'netherlands' or subdomain == 'netherlands_2':
        locations = [
            {"name": "cabauw", "lat": 51.971, "lon": 4.927, "z": [5, 60, 100, 180]},
            {"name": "loobos", "lat": 52.166, "lon": 5.744, "z": [24]},
        ]
    elif subdomain == 'HPB_tower':
        locations = [
        {"name": "HPB_tower","lat":47.8,"lon":11.017,"z":[50,93,131]},
    ]


    print(f"Reading: {in_path}")
    ds = xr.open_dataset(in_path)

    # Convert UTC to local time (+2 hours)
    if "time" in ds:
        ds["time"] = ds["time"].astype("datetime64[ns]") + np.timedelta64(UTC_to_LT, "h")

    # Save processed full-field file
    processed_path = os.path.join(dir_in, f"{exp_id}_{levels}_t{lead_time}_processed.nc")
    print(f"Saving processed: {processed_path}")
    _save(ds, processed_path)

    # Save slab-mean
    slab = ds.mean(("latitude", "longitude"), keep_attrs=True)
    slab_path = os.path.join(dir_in, f"{exp_id}_{levels}_t{lead_time}_slab.nc")
    print(f"Saving slab: {slab_path}")
    _save(slab, slab_path)

    # Save point locations
    for loc in locations:
        point = inverse_distance_weighting(ds, loc["lat"], loc["lon"])

        if levels == "z" and "height" in point.coords:
            all_heights = np.unique(np.concatenate([point["height"].values, np.array(loc["z"], dtype=float)]))
            point = point.interp(height=all_heights)

        out_path = os.path.join(dir_in, f"{exp_id}_{levels}_t{lead_time}_{loc['name']}.nc")
        print(f"Saving point: {out_path}")
        _save(point, out_path)

    print("Done.")


if __name__ == "__main__":
    main()
