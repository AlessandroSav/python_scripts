#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: mars_spp_automated.sh \
  --subdomain <name> \
  --exp_id <expver> \
  --exp_name <label> \
  --levels <pl|srf> \
  --lead_time <int> \
  --stream <enfo|...> \
  --yyyymmdd_start <YYYYMMDD> \
  --yyyymmdd_end <YYYYMMDD>

Creates NetCDF files in:
  /scratch/$USER/IFS/<subdomain>/<exp_id>/
Named like:
  <exp_id>_<exp_name>_<levels>_<YYYYMMDD>_<lead_time>.nc
EOF
}

subdomain=""
exp_id=""
exp_name=""
levels=""
lead_time=""
stream=""
yyyymmdd_start=""
yyyymmdd_end=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --subdomain) subdomain="$2"; shift 2;;
    --exp_id) exp_id="$2"; shift 2;;
    --exp_name) exp_name="$2"; shift 2;;
    --levels) levels="$2"; shift 2;;
    --lead_time) lead_time="$2"; shift 2;;
    --stream) stream="$2"; shift 2;;
    --yyyymmdd_start) yyyymmdd_start="$2"; shift 2;;
    --yyyymmdd_end) yyyymmdd_end="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 2;;
  esac
done

if [[ -z "$subdomain" || -z "$exp_id" || -z "$exp_name" || -z "$levels" || -z "$lead_time" || -z "$stream" || -z "$yyyymmdd_start" || -z "$yyyymmdd_end" ]]; then
  echo "Missing required args."
  usage
  exit 2
fi

if [[ "$levels" != "pl" && "$levels" != "srf" ]]; then
  echo "--levels must be 'pl' or 'srf' (got: $levels)"
  exit 2
fi

# Ensure the module command exists (typical on HPC).
if command -v module >/dev/null 2>&1; then
  :
elif [[ -f /etc/profile.d/modules.sh ]]; then
  # shellcheck disable=SC1091
  source /etc/profile.d/modules.sh
fi

if command -v module >/dev/null 2>&1; then
  module load eclib || true
  module load ecmwf-toolbox || true
fi

out_dir="/scratch/${USER}/IFS/${subdomain}/${exp_id}"
mkdir -p "$out_dir"
cd "$out_dir"

# Domain/grid defaults: keep these minimal and editable.
# NOTE: Update AREA/GRID as needed for your case.
area="52.22/4.68/51.72/5.75"   # N/W/S/E
grid="0.2/0.2"

# Pressure level request example (include z/t/q so pl->height is feasible).
pl_levelist="100/200/250/300/400/500/550/600/650/700/750/800/850/900/950/1000"
pl_param="61.210/129.128/130.128" #/133.128



# Surface request example (as provided; you can trim further).
srf_param="20.3/29.228/34.128/47.128/64.210/78.128/79.128/80.228/81.228/82.228/83.228/84.228/85.228/129.128/134.128/136.128/137.128/139.128/142.128/143.128/145.128/146.128/147.128/148.128/151.128/159.128/164.128/165.128/166.128/167.128/168.128/169.128/170.128/173.128/175.128/176.128/177.128/178.128/179.128/180.128/181.128/182.128/186.128/187.128/188.128/206.128/218.228/219.228/228.128/235.128/244.128/251.228/228235/228236/260015/260121"

# Date loop (inclusive). Requires GNU date.
cur="$yyyymmdd_start"
end="$yyyymmdd_end"

while :; do
  yyyy=${cur:0:4}
  mm=${cur:4:2}
  dd=${cur:6:2}
  iso_date="${yyyy}-${mm}-${dd}"

  out_nc="${exp_id}_${exp_name}_${levels}_${cur}_${lead_time}.nc"
  if [[ -f "$out_nc" ]]; then
    echo "Already exists: $out_nc"
  else
    echo "Retrieving ${levels} for ${iso_date} -> ${out_nc}"

    grb="mars_${levels}_${exp_id}_${cur}_${lead_time}.grb"
    req="mars_${levels}_${exp_id}_${cur}_${lead_time}.req"

    if [[ "$levels" == "pl" ]]; then
      cat >"$req" <<EOF
retrieve,
class=rd,
date=${iso_date},
expver=${exp_id},
levelist=${pl_levelist},
levtype=pl,
number=1/2/3/4/5/6/7/8,
param=${pl_param},
step=0/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15/16/17/18/19/20/21/22/23/24,
stream=${stream},
time=00:00:00,
type=pf,
grid=${grid},
area=${area},
target="${grb}"
EOF
    else
      cat >"$req" <<EOF
retrieve,
class=rd,
date=${iso_date},
expver=${exp_id},
levtype=sfc,
number=1/2/3/4/5/6/7/8,
param=${srf_param},
step=0/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15/16/17/18/19/20/21/22/23/24,
stream=${stream},
time=00:00:00,
type=pf,
grid=${grid},
area=${area},
target="${grb}"
EOF
    fi

    if mars "$req"; then
      :
    else
      echo "WARNING: MARS returned non-zero exit code (often due to missing fields)."
      echo "WARNING: Will still convert '${grb}' if it was written."
    fi

    if [[ -f "$grb" ]]; then
      grib_to_netcdf -R "${cur}" "$grb" -o "$out_nc"
      rm -f "$grb" "$req"
    else
      echo "Expected GRIB not found: $grb"
      exit 1
    fi
  fi

  if [[ "$cur" == "$end" ]]; then
    break
  fi
  cur=$(date -d "${iso_date} + 1 day" +%Y%m%d)
done

echo "Done. Files are in: $out_dir"