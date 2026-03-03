import argparse
import glob
import os

import xarray as xr


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--subdomain", type=str, required=True)
    parser.add_argument("--exp_id", type=str, required=True)
    parser.add_argument("--exp_type", type=str, required=True)
    parser.add_argument("--levels", type=str, required=True)
    parser.add_argument("--lead_time", type=int, default=0)
    args = parser.parse_args()

    subdomain = args.subdomain
    exp_id = args.exp_id
    exp_type = args.exp_type
    levels = args.levels
    lead_time = args.lead_time

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

    ds = xr.open_mfdataset(files, combine="by_coords")
    if "time" in ds:
        ds = ds.sortby("time")

    os.makedirs(dir_out, exist_ok=True)
    out_path = os.path.join(dir_out, f"{exp_id}_{levels}_t{lead_time}.nc")
    print(f"Saving combined dataset to: {out_path}")
    ds.to_netcdf(out_path)


if __name__ == "__main__":
    main()
