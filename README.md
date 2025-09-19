# Show&Well • Places → BigQuery → Tableau

A compact, cloud-first pipeline that pulls provider ratings & details from the **Google Places API (New)**, lands raw CSVs in **GCS**, models them in **BigQuery** using a **Bronze / Silver / Gold** pattern, and exports a single CSV for a **Tableau** dashboard.

**Stack:** Python, Google Cloud (GCS + BigQuery), SQL, Tableau.  
**One command:** `./scripts/run_all.sh` — fetches ratings, enriches phone/website/lat-lon, computes distance from Minneapolis, assigns **Gold/Silver/Bronze** tiers, and exports for viz.

## Quickstart (Cloud Shell)
```bash
git clone <your repo> show-and-well-pipeline-starter
cd show-and-well-pipeline-starter

cp .env.example .env
# edit .env with your PROJECT, BUCKET, and GOOGLE_MAPS_API_KEY
set -a; source .env; set +a
gcloud config set project "$GCP_PROJECT_ID"

chmod +x scripts/run_all.sh
./scripts/run_all.sh | tee run_all_$(date +%F_%H%M).log

# Get the exported CSV to your machine (Cloud Shell):
cloudshell download ~/provider_map.csv
```

**Outputs**

BigQuery Datasets: sw_bronze, sw_silver, sw_gold

Views: tiered directory and map-ready view with lat/lon + distance

CSV: ~/provider_map.csv for Tableau

scripts/
  fetch_places_ratings.py      # ratings via Places (New) searchText
  fetch_place_details.py       # place details → locations.csv (+enrich providers)
  run_all.sh                   # orchestrates end-to-end
sql/
  10_silver_views.sql          # typed views + geo
  20_gold_views.sql            # tiering + map view
.github/workflows/
  shellcheck.yml               # CI lint for shell scripts
