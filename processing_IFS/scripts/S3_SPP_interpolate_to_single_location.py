import argparse
import os

import numpy as np
import xarray as xr


UTC_to_LT = +2  # hours

locations = [
    {"name": "cabauw", "lat": 51.971, "lon": 4.927, "z": [5, 60, 100, 180]},
    {"name": "loobos", "lat": 52.166, "lon": 5.744, "z": [24]},
]


def inverse_distance_weighting(ds: xr.Dataset, target_lat: float, target_lon: float) -> xr.Dataset:
    lat = ds["latitude"]
    lon = ds["longitude"]
    dist = np.sqrt((lat - target_lat) ** 2 + (lon - target_lon) ** 2)
    dist = dist.where(dist != 0, other=1e-10)
    weights = 1.0 / dist
    weights = weights / weights.sum(dim=("latitude", "longitude"))
    return ds.weighted(weights).mean(dim=("latitude", "longitude"))


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

    print(f"Reading: {in_path}")
    ds = xr.open_dataset(in_path)

    # Convert UTC to local time (+2 hours)
    if "time" in ds:
        ds["time"] = ds["time"].astype("datetime64[ns]") + np.timedelta64(UTC_to_LT, "h")

    # Save processed full-field file
    processed_path = os.path.join(dir_in, f"{exp_id}_{levels}_t{lead_time}_processed.nc")
    print(f"Saving processed: {processed_path}")
    ds.to_netcdf(processed_path)

    # Save slab-mean
    slab = ds.mean(("latitude", "longitude"), keep_attrs=True)
    slab_path = os.path.join(dir_in, f"{exp_id}_{levels}_t{lead_time}_slab.nc")
    print(f"Saving slab: {slab_path}")
    slab.to_netcdf(slab_path)

    # Save point locations
    for loc in locations:
        point = inverse_distance_weighting(ds, loc["lat"], loc["lon"])

        if levels == "z" and "height" in point.coords:
            all_heights = np.unique(np.concatenate([point["height"].values, np.array(loc["z"], dtype=float)]))
            point = point.interp(height=all_heights)

        out_path = os.path.join(dir_in, f"{exp_id}_{levels}_t{lead_time}_{loc['name']}.nc")
        print(f"Saving point: {out_path}")
        point.to_netcdf(out_path)

    print("Done.")


if __name__ == "__main__":
    main()
