import argparse
import glob
import os

import xarray as xr


LEVITRAC = {
    'p216090': 'co2flx_tot',
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--subdomain", type=str, required=True)
    parser.add_argument("--exp_id", type=str, required=True)
    parser.add_argument("--exp_type", type=str, required=True)
    parser.add_argument("--levels", type=str, required=True)
    parser.add_argument("--lead_time", type=int, default=0)
    parser.add_argument("--levitrac", type=str, default='False')
    args = parser.parse_args()

    subdomain = args.subdomain
    exp_id = args.exp_id
    exp_type = args.exp_type
    levels = args.levels
    lead_time = args.lead_time
    levitrac = args.levitrac

    # exp_id is expected as "<expver>_<exp_name>"; expver is first 4 chars.
    expver = exp_id[:4]

    user = os.environ.get("USER", "paaa")
    dir_in = f"/scratch/{user}/IFS/{subdomain}/{expver}"
    dir_out = f"/perm/paaa/IFS/{subdomain}/{exp_type}"

    pattern = os.path.join(dir_in, f"{expver}_*_{levels}_*_{lead_time}.nc")
    files = sorted(glob.glob(pattern))

    print(f"Looking for input files with pattern: {pattern}")
    if not files:
        raise SystemExit(f"No files found in {dir_in} for {levels} lead_time={lead_time}.")

    # Also ensure we only take the requested experiment label.
    files = [f for f in files if exp_id in os.path.basename(f)]
    print(f"Found {len(files)} files:")
    for f in files:
        print(f"  {f}")

    # Load each daily file separately and drop the trailing step=24 (= step=0 of
    # the next day) from all but the last file to avoid duplicate timestamps.
    # Using explicit concat instead of open_mfdataset(combine="by_coords") because
    # the latter produces incorrect values when materialised via to_netcdf() with
    # a lazy isel dedup index (xarray lazy-graph bug with non-contiguous indices).
    datasets = []
    for i, f in enumerate(files):
        ds_day = xr.open_dataset(f).load()  # eager load to avoid int16 encoding overflow on write
        if i < len(files) - 1:
            # Drop the last time step (step=24 h = step=0 of next day's file)
            ds_day = ds_day.isel(time=slice(None, -1))
        datasets.append(ds_day)
    ds = xr.concat(datasets, dim="time")
    if levels == "srf" and levitrac == "True":
        ds = ds.rename(LEVITRAC)
    if "time" in ds:
        ds = ds.sortby("time")

    # Drop inherited int16 encoding from the source files. The combined dataset
    # spans a wider value range than any single daily file, so the per-file
    # scale_factor/add_offset would silently clip values that fall outside the
    # encodable range. Writing as float32 avoids this and is still compact enough.
    for var in ds.data_vars:
        ds[var].encoding.clear()

    os.makedirs(dir_out, exist_ok=True)
    out_path = os.path.join(dir_out, f"{exp_id}_{levels}_t{lead_time}.nc")
    print(f"Saving combined dataset to: {out_path}")
    if os.path.exists(out_path):
        os.remove(out_path)
    ds.to_netcdf(out_path)


if __name__ == "__main__":
    main()
