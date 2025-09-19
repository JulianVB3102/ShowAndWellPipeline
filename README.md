# ShowAndWellPipeline
A compact, cloud-first pipeline that scrapes provider ratings/details from the Google Places API (New), lands raw CSVs in GCS, models them in BigQuery using a Bronze/Silver/Gold pattern, and publishes a single CSV to power a Tableau dashboard. Stack: Python, Google Cloud (GCS + BigQuery), SQL, Tableau.
