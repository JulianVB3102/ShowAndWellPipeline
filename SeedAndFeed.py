#!/usr/bin/env bash
set -euo pipefail
# -e: exit on error
# -u: error on using unset vars
# -o pipefail: pipeline fails if any command fails

# ---------------------------
# Load environment variables
# ---------------------------
# Reads .env and exports keys like GCP_PROJECT_ID, GCS_BUCKET, API keys, etc.
set -a; source .env; set +a

# Run-date: first arg (YYYY-MM-DD) or default to today's date
TODAY="${1:-$(date +%F)}"

echo "[info] Project: $GCP_PROJECT_ID  Bucket: gs://$GCS_BUCKET  Run: $TODAY"

# ---------------------------------------
# 0) Ensure required BigQuery datasets exist
#    (idempotent; ignore error if they do)
# ---------------------------------------
bq mk -d sw_bronze 2>/dev/null || true
bq mk -d sw_silver 2>/dev/null || true
bq mk -d sw_gold   2>/dev/null || true

# -------------------------------------------------------------------
# 1) Seed today's providers.csv if the input file isn't present
#    Allows the pipeline to run end-to-end without a manual seed
# -------------------------------------------------------------------
if [[ ! -f "data/bronze/$TODAY/providers.csv" ]]; then
  echo "[warn] data/bronze/$TODAY/providers.csv not found; creating a minimal one"
  mkdir -p "data/bronze/$TODAY"
  cat > "data/bronze/$TODAY/providers.csv" <<CSV
provider_id,display_name,category,website,phone
p1,Revival PT,Rehabilitation,https://revivalpt.net,612-605-7594
p2,Twin Cities Nutritionists,Nutrition,https://twincitiesnutritionist.com/,612-202-8703
p3,MN Fat Loss,Weight Loss,https://mnfatloss.com/,(763)710-7499
CSV
fi

# -------------------------------------------------------------------
# 2) Fetch ratings via Google Places "searchText" → ratings.csv (BRONZE)
#    - Python script reads providers.csv, calls the API, writes ratings.csv
#    - Copy that file into partitioned paths in GCS
# -------------------------------------------------------------------
echo "[step] ratings → places:searchText"
python3 scripts/fetch_places_ratings.py "data/bronze/$TODAY/providers.csv" "data/bronze" "$TODAY"

gsutil cp "data/bronze/$TODAY/ratings.csv" \
  "gs://$GCS_BUCKET/bronze/ratings/source=google_places/run_date=$TODAY/ratings.csv"

# -------------------------------------------------------------------
# Build/refresh a single external table over ALL ratings CSVs in GCS
# - Enumerate all ratings CSV URIs
# - Write an external table definition JSON (autodetect CSV schema)
# - Update if it exists; otherwise create it
# -------------------------------------------------------------------
# Turn the gsutil listing into a comma-separated list of quoted URIs
RATE_URIS=$(gsutil ls "gs://$GCS_BUCKET/bronze/ratings/source=*/run_date=*/ratings.csv" | sed 's/^/    "&/; s/$/"/' | paste -sd, -)

# External table definition JSON:
# - autodetect CSV schema
# - skip header row
# - hivePartitioningOptions points at "bronze/ratings/" (keeps partitions logical)
# - sourceUris includes all historical rating CSVs from all runs
cat > /tmp/ratings_def.json <<JSON
{
  "autodetect": true,
  "sourceFormat": "CSV",
  "csvOptions": { "skipLeadingRows": 1 },
  "hivePartitioningOptions": { "mode": "STRINGS", "sourceUriPrefix": "gs://$GCS_BUCKET/bronze/ratings/" },
  "sourceUris": [
$RATE_URIS
  ]
}
JSON

# Update existing external table; if not present, create it
bq update --external_table_definition=/tmp/ratings_def.json sw_bronze.ratings_ext || \
bq mk --external_table_definition=/tmp/ratings_def.json sw_bronze.ratings_ext

# -------------------------------------------------------------------
# 3) Fetch place details (addresses, coordinates, etc.) → locations.csv
#    - Also can enrich providers (fill phone/site if missing)
#    - Upload locations.csv to GCS under Bronze
# -------------------------------------------------------------------
echo "[step] locations → places details"
python3 scripts/fetch_place_details.py "data/bronze/$TODAY" "$TODAY"

gsutil cp "data/bronze/$TODAY/locations.csv" \
  "gs://$GCS_BUCKET/bronze/locations/source=places_details/run_date=$TODAY/locations.csv"

# -------------------------------------------------------------------
# Load locations into a native BigQuery table (typed) for downstream use
# - Using a native table avoids CSV parsing quirks at query time
# - --replace to keep this run's snapshot deterministic
# -------------------------------------------------------------------
bq load --replace --skip_leading_rows=1 --source_format=CSV --autodetect \
  "$GCP_PROJECT_ID:sw_bronze.locations_load" \
  "gs://$GCS_BUCKET/bronze/locations/source=places_details/run_date=$TODAY/locations.csv"

# -------------------------------------------------------------------
# 4) Create/refresh Silver & Gold layers (views/materializations)
#    - Silver: cleaned/typed/joined intermediate layer
#    - Gold  : analytics-ready views for consumption (e.g., map)
# -------------------------------------------------------------------
bq query --use_legacy_sql=false < sql/10_silver_views.sql
bq query --use_legacy_sql=false < sql/20_gold_views.sql

# -------------------------------------------------------------------
# 5) Sanity check: Gold provider map view must have rows
#    - If zero rows, exit nonzero to fail the run (detects data issues)
# -------------------------------------------------------------------
echo "[test] gold rows exist"
bq query --use_legacy_sql=false --format=csv --quiet \
'SELECT COUNT(*) FROM `sw_gold.provider_map_v`' | tail -n1 | awk -F, '{ if($1<1){ print "no rows"; exit 1 } else { print "[ok] rows=" $1 } }'

# -------------------------------------------------------------------
# 6) Export a single CSV (Gold map view) for Tableau consumption
#    - Materialize view to a temp table
#    - Extract to GCS as CSV (with header)
#    - Copy to Cloud Shell home for easy local download
# -------------------------------------------------------------------
echo "[step] export to CSV"
bq query --use_legacy_sql=false \
  --destination_table="$GCP_PROJECT_ID:sw_gold.provider_map_tmp" --replace \
  'SELECT * FROM `sw_gold.provider_map_v`'

bq extract --destination_format=CSV --field_delimiter=',' --print_header=true \
  "$GCP_PROJECT_ID:sw_gold.provider_map_tmp" \
  "gs://$GCS_BUCKET/exports/provider_directory/$TODAY/provider_map.csv"

# Copy the exported CSV to Cloud Shell home (so user can download via UI)
gsutil cp "gs://$GCS_BUCKET/exports/provider_directory/$TODAY/provider_map.csv" ~/

echo "[done] Download with: cloudshell download ~/provider_map.csv"
