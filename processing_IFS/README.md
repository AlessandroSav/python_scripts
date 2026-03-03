# `processing_IFS/`

This folder contains the workflow to retrieve and post-process IFS output (including SPP ensemble runs) into analysis-ready NetCDF.

Typical pipeline:
1. Retrieve raw fields from ECMWF MARS and convert GRIB → NetCDF (writes to `/scratch/$USER/IFS/...`).
2. Stage 1 (S1): rename experiment/diagnostic variables and de-accumulate accumulated fields; for SPP optionally combine daily files into one time series.
3. Stage 2 (S2, optional): convert vertical coordinate to fixed heights (either from model levels or pressure levels).
4. Stage 3 (S3): produce “processed” files, slab means, and site time series (Cabauw/Loobos) using simple spatial interpolation.

## Entry points (SLURM)

- `run_catrine.sh`
  - Forecast-style (`fc`) workflow.
  - If data are not yet present in `/scratch`, it submits the MARS retrieval job and exits.
  - Then runs S1 (rename/deaccumulate), optionally S2 (if `levels=ml`), and S3.

- `run_catrine_spp.sh`
  - SPP/ensemble (`pf`) workflow.
  - If data are not yet present in `/scratch`, it submits the MARS retrieval job and exits.
  - Then runs S1 (combine days), optional S2 (if `levels=pl`), and S3.

Both scripts define the experiment ID/name, date range, levels (`ml`/`pl`/`srf`), and diagnostic switches near the top.

## Pipeline-step scripts (`scripts/`)

The `scripts/` subfolder contains the smaller scripts used by the SLURM entry points:

### MARS retrieval
- `scripts/mars_spp_automated.sh`
  - MARS retrieval helper for SPP (ensemble) data (`--levels pl|srf`).
  - Writes daily NetCDF files in `/scratch/$USER/IFS/<subdomain>/<exp_id>/`.

- `scripts/mars_ghg_fc_automated`
  - SLURM batch script to retrieve forecasts (FC) for model levels (`ml`) and/or surface (`srf`).
  - Switches like `--ldiagghg`, `--ldiagflx`, `--lbud23`, `--ldiagflx_49r2b` change the requested diagnostic parameters.

### Stage 1 (S1)
- `scripts/S1_rename_vars_deaccumulate.py`
  - Renames diagnostic variables, adjusts units for some tracers, and de-accumulates accumulated fields.
  - Reads from `/scratch/...` and writes under `/perm/paaa/IFS/<subdomain>/<exp_type>/`.

- `scripts/S1_SPP_deaccumulate_combine.py`
  - SPP helper: combines per-day files for a given lead time into a single NetCDF time series.

### Stage 2 (S2)
- `scripts/S2_from_ml_to_height.py`
  - Converts from model levels (`ml`) to fixed height levels using hydrostatic balance.

- `scripts/S2_SPP_from_pl_to_height.py`
  - Converts from pressure levels (`pl`) to fixed height levels.
  - Uses geopotential if available, otherwise hypsometric height from `T` (and optionally `q`).

### Stage 3 (S3)
- `scripts/S3_interpolate_to_single_location.py`
  - Produces `*_processed.nc`, `*_slab.nc`, and site point files (Cabauw/Loobos) using inverse-distance weighting.

- `scripts/S3_SPP_interpolate_to_single_location.py`
  - Same Stage 3 idea, in a more minimal/clean CLI form.

## Tests / scratch code

- `tests/` contains notebooks and scripts used to validate intermediate processing steps.
