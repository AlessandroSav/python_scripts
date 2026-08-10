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
number_members=""
lead_time=""
stream=""
yyyymmdd_start=""
yyyymmdd_end=""
levitrac=""
force="False"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --subdomain) subdomain="$2"; shift 2;;
    --exp_id) exp_id="$2"; shift 2;;
    --exp_name) exp_name="$2"; shift 2;;
    --levels) levels="$2"; shift 2;;
    --number_members) number_members="$2"; shift 2;;
    --lead_time) lead_time="$2"; shift 2;;
    --stream) stream="$2"; shift 2;;
    --yyyymmdd_start) yyyymmdd_start="$2"; shift 2;;
    --yyyymmdd_end) yyyymmdd_end="$2"; shift 2;;
    --levitrac) levitrac="$2"; shift 2;;
    --force) force="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 2;;
  esac
done

if [[ -z "$subdomain" || -z "$exp_id" || -z "$exp_name" || -z "$levels" || -z "$number_members" || -z "$lead_time" || -z "$stream" || -z "$yyyymmdd_start" || -z "$yyyymmdd_end" || -z "$levitrac" ]]; then
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
# Area selection
if [[ "$subdomain" == "netherlands" ]]; then
  area='52.22/4.68/51.72/5.75'  # Cabauw + Loobos
  echo "area: $area"
elif [[ "$subdomain" == "netherlands_2" ]]; then
  area='52.5/4.8/51.5/5.8'  # Cabauw + Loobos
  echo "area: $area"
elif [[ "$subdomain" == "HPB_tower" ]] ; then
  area='48.3/10.5/47.3/11.5'
  echo "area: $area"
else
  echo "subdomain not known. Extracting around Cabauw instead"
  area='52.22/4.68/51.72/5.18'
  echo "area: $area"
fi
grid="0.2/0.2"

# Pressure level request example (include z/t/q 
pl_levelist="300/400/500/550/600/650/700/750/800/820/840/850/860/880/900/910/920/925/930/940/950/960/970/980/990/1000/1005/1010/1015/1020"
pl_param="61.210/129.128/130.128/133.128" #/clwc



# Surface request example (as provided; you can trim further).
srf_param="20.3/29.228/34.128/47.128/64.210/78.128/79.128/80.228/81.228/82.228/83.228/84.228/85.228/129.128/134.128/136.128/137.128/139.128/142.128/143.128/145.128/146.128/147.128/148.128/151.128/159.128/164.128/165.128/166.128/167.128/168.128/169.128/170.128/173.128/175.128/176.128/177.128/178.128/179.128/180.128/181.128/182.128/186.128/187.128/188.128/206.128/218.228/219.228/228.128/235.128/244.128/251.228/228235/228236/260015/260121"
if [[ "$levitrac" == "True" ]]; then
  srf_param+='/216090'
fi

# Date loop (inclusive). Requires GNU date.
cur="$yyyymmdd_start"
end="$yyyymmdd_end"

while :; do
  yyyy=${cur:0:4}
  mm=${cur:4:2}
  dd=${cur:6:2}
  iso_date="${yyyy}-${mm}-${dd}"

  out_nc="${exp_id}_${exp_name}_${levels}_${cur}_${lead_time}.nc"
  if [[ -f "$out_nc" && "$force" != "True" ]]; then
    echo "Already exists: $out_nc"
  else
    echo "Retrieving ${levels} for ${iso_date} -> ${out_nc}"

    grb="mars_${levels}_${exp_id}_${cur}_${lead_time}.grb"
    grb_cf="mars_${levels}_${exp_id}_${cur}_${lead_time}_cf.grb"
    grb_pf="mars_${levels}_${exp_id}_${cur}_${lead_time}_pf.grb"
    req_cf="mars_${levels}_${exp_id}_${cur}_${lead_time}_cf.req"
    req_pf="mars_${levels}_${exp_id}_${cur}_${lead_time}_pf.req"

    # Remove any stale intermediate files from previous interrupted runs to prevent MARS appending
    rm -f "$grb" "$grb_cf" "$grb_pf" "${grb_cf%.grb}_n0.grb" "$req_cf" "$req_pf"

    if [[ "$levels" == "pl" ]]; then
      cat >"$req_cf" <<EOF
retrieve,
class=rd,
date=${iso_date},
expver=${exp_id},
levelist=${pl_levelist},
levtype=pl,
param=${pl_param},
step=$(($lead_time+0))/$(($lead_time+1))/$(($lead_time+2))/$(($lead_time+3))/$(($lead_time+4))/$(($lead_time+5))/$(($lead_time+6))/$(($lead_time+7))/$(($lead_time+8))/$(($lead_time+9))/$(($lead_time+10))/$(($lead_time+11))/$(($lead_time+12))/$(($lead_time+13))/$(($lead_time+14))/$(($lead_time+15))/$(($lead_time+16))/$(($lead_time+17))/$(($lead_time+18))/$(($lead_time+19))/$(($lead_time+20))/$(($lead_time+21))/$(($lead_time+22))/$(($lead_time+23))/$(($lead_time+24)),
stream=${stream},
time=00:00:00,
type=cf,
grid=${grid},
area=${area},
interpolation=grid-box-average,
target="${grb_cf}"
EOF
      cat >"$req_pf" <<EOF
retrieve,
class=rd,
date=${iso_date},
expver=${exp_id},
levelist=${pl_levelist},
levtype=pl,
number=1/to/${number_members},
param=${pl_param},
step=$(($lead_time+0))/$(($lead_time+1))/$(($lead_time+2))/$(($lead_time+3))/$(($lead_time+4))/$(($lead_time+5))/$(($lead_time+6))/$(($lead_time+7))/$(($lead_time+8))/$(($lead_time+9))/$(($lead_time+10))/$(($lead_time+11))/$(($lead_time+12))/$(($lead_time+13))/$(($lead_time+14))/$(($lead_time+15))/$(($lead_time+16))/$(($lead_time+17))/$(($lead_time+18))/$(($lead_time+19))/$(($lead_time+20))/$(($lead_time+21))/$(($lead_time+22))/$(($lead_time+23))/$(($lead_time+24)),
stream=${stream},
time=00:00:00,
type=pf,
grid=${grid},
area=${area},
interpolation=grid-box-average,
target="${grb_pf}"
EOF
    else
      cat >"$req_cf" <<EOF
retrieve,
class=rd,
date=${iso_date},
expver=${exp_id},
levtype=sfc,
param=${srf_param},
step=$(($lead_time+0))/$(($lead_time+1))/$(($lead_time+2))/$(($lead_time+3))/$(($lead_time+4))/$(($lead_time+5))/$(($lead_time+6))/$(($lead_time+7))/$(($lead_time+8))/$(($lead_time+9))/$(($lead_time+10))/$(($lead_time+11))/$(($lead_time+12))/$(($lead_time+13))/$(($lead_time+14))/$(($lead_time+15))/$(($lead_time+16))/$(($lead_time+17))/$(($lead_time+18))/$(($lead_time+19))/$(($lead_time+20))/$(($lead_time+21))/$(($lead_time+22))/$(($lead_time+23))/$(($lead_time+24)),
stream=${stream},
time=00:00:00,
type=cf,
grid=${grid},
area=${area},
interpolation=grid-box-average,
target="${grb_cf}"
EOF
      cat >"$req_pf" <<EOF
retrieve,
class=rd,
date=${iso_date},
expver=${exp_id},
levtype=sfc,
number=1/to/${number_members},
param=${srf_param},
step=$(($lead_time+0))/$(($lead_time+1))/$(($lead_time+2))/$(($lead_time+3))/$(($lead_time+4))/$(($lead_time+5))/$(($lead_time+6))/$(($lead_time+7))/$(($lead_time+8))/$(($lead_time+9))/$(($lead_time+10))/$(($lead_time+11))/$(($lead_time+12))/$(($lead_time+13))/$(($lead_time+14))/$(($lead_time+15))/$(($lead_time+16))/$(($lead_time+17))/$(($lead_time+18))/$(($lead_time+19))/$(($lead_time+20))/$(($lead_time+21))/$(($lead_time+22))/$(($lead_time+23))/$(($lead_time+24)),
stream=${stream},
time=00:00:00,
type=pf,
grid=${grid},
area=${area},
interpolation=grid-box-average,
target="${grb_pf}"
EOF
    fi

    # Retrieve control forecast (member 0) then perturbed forecasts
    if mars "$req_cf"; then
      :
    else
      echo "WARNING: MARS cf returned non-zero exit code (often due to missing fields)."
    fi
    if mars "$req_pf"; then
      :
    else
      echo "WARNING: MARS pf returned non-zero exit code (often due to missing fields)."
    fi

    # Merge cf (member 0) and pf into a single GRIB before converting
    if [[ -f "$grb_cf" && -f "$grb_pf" ]]; then
      grb_cf_n0="${grb_cf%.grb}_n0.grb"
      # Set type=pf and number=0 on the cf fields so grib_to_netcdf treats them as member 0
      grib_set -s type=pf,number=0 "$grb_cf" "$grb_cf_n0"
      cat "$grb_cf_n0" "$grb_pf" > "$grb"
      rm -f "$grb_cf" "$grb_cf_n0" "$grb_pf" "$req_cf" "$req_pf"
    elif [[ -f "$grb_pf" ]]; then
      echo "WARNING: cf GRIB not found; proceeding with pf only."
      mv "$grb_pf" "$grb"
      rm -f "$grb_cf" "$req_cf" "$req_pf"
    else
      echo "Expected GRIB not found: $grb_pf"
      exit 1
    fi

    if [[ -f "$grb" ]]; then
      grib_to_netcdf -R "${cur}" "$grb" -o "$out_nc"
      rm -f "$grb"
    else
      echo "Expected merged GRIB not found: $grb"
      exit 1
    fi
  fi

  if [[ "$cur" == "$end" ]]; then
    break
  fi
  cur=$(date -d "${iso_date} + 1 day" +%Y%m%d)
done

echo "Done. Files are in: $out_dir"