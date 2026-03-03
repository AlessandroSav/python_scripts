# python_scripts

Collection of notebooks and helper scripts used for:
- retrieving IFS (and SPP ensemble) data from ECMWF MARS,
- post-processing IFS outputs to analysis-ready NetCDF (including interpolation to fixed heights and site extraction),
- creating DALES input from IFS fields,
- processing / filtering DALES outputs,
- retrieving supporting observations from the KNMI Data Platform,
- plotting and analysis notebooks (Cabauw / Loobos and other case studies).

This repository is not a packaged Python library; most code is run as standalone scripts (`python ...`), SLURM jobs (`sbatch ...`), or interactive notebooks.

## Repository structure

- `processing_IFS/`
  - SLURM entry points for IFS processing.
  - `run_catrine.sh`: forecast (`fc`) style workflow.
  - `run_catrine_spp.sh`: SPP/ensemble (`pf`) style workflow.
  - See `processing_IFS/README.md` for the workflow and script overview.

- `processing_DALES/`
  - DALES preprocessing/postprocessing helpers.
  - See `processing_DALES/README.md` for the DALES-chemistry notes.

- `data_retrieval_KNMI/`
  - Small scripts to download observational datasets from the KNMI Data Platform.

- `windbias/`
  - Analysis/plot scripts and notebooks focused on wind/precip diagnostics.

- `borrowed_scripts/`
  - External/borrowed utilities used in some workflows (keep provenance in mind).

- `my_functions.py`
  - Shared math/physics utilities (thermodynamics, conversions, filtering helpers, etc.).

- `my_classes.py`
  - Convenience classes/helpers for common processing patterns (e.g., slab means, IDW site extraction).

- `*.ipynb`
  - Interactive analysis notebooks (quick looks, comparisons between DALES and IFS, storyline notebooks, etc.).

## Common workflows

### 1) IFS retrieval + processing (SLURM)

The typical IFS processing flow is:
1. MARS retrieval (GRIB → NetCDF into `/scratch/$USER/IFS/...`)
2. S1: rename vars + deaccumulate + copy to `/perm/...`
3. S2 (optional): vertical interpolation to fixed heights
4. S3: “processed” file + slab mean + site time series

Entry points:
- `sbatch processing_IFS/run_catrine.sh`
- `sbatch processing_IFS/run_catrine_spp.sh`

These scripts contain the experiment IDs, date ranges, level type, and switch flags near the top.

### 2) Create DALES input from IFS

- Main script: `create_dales_input.py`
- SLURM wrapper: `sbatch run_create_dales_input.sh`

This script retrieves required surface/model-level fields (via MARS) and prepares files under a `for_dales/` output directory (paths are currently set to the author’s HPC layout).

### 3) KNMI observations download

Scripts in `data_retrieval_KNMI/` call the KNMI Open Data API and download NetCDF files to a configured output directory.

Token handling:
- Set your KNMI token via environment variable `KNMI_API_KEY`.
- Example:
  - `export KNMI_API_KEY="<your-token>"`

## Notes / assumptions

- Many scripts assume an HPC environment with:
  - access to ECMWF MARS (`mars` command),
  - `ecmwf-toolbox` (e.g., `grib_to_netcdf`),
  - and a SLURM scheduler (`sbatch`).

- Several paths are currently hard-coded for a specific environment (e.g. `/scratch/...`, `/perm/...`, `/home/paaa/...`). If you run elsewhere, expect to edit the path variables at the top of the scripts.

## Minimal Python dependencies (typical)

Exact requirements depend on the workflow, but commonly used packages include:
- `numpy`, `xarray`, `scipy`, `dask`, `netCDF4`, `h5netcdf`
- `requests` (KNMI downloads)
- `pyproj` (some DALES reprojection utilities)
- `matplotlib`, `cartopy` (plotting)
