-- ----------------------------
-- Providers (latest snapshot)
-- ----------------------------
-- Simple passthrough of the current canonical providers view.
-- Keep only the columns downstream consumers need.
CREATE OR REPLACE VIEW `sw_silver.providers_latest_v` AS
SELECT provider_id, display_name, category, website, phone
FROM `sw_silver.providers_v`;

-- ---------------------------------------------------------
-- Ratings (pick ONE best row per provider)
-- Logic:
--   1) Rank each provider’s ratings rows by fetched date (newest first).
--   2) If there’s a tie on date, break ties deterministically by source.
--   3) Keep rn=1 as the “latest” record.
-- ---------------------------------------------------------
CREATE OR REPLACE VIEW `sw_silver.ratings_latest_v` AS
WITH ranked AS (
  SELECT
    provider_id,
    rating,
    reviews_count,
    rating_source,
    fetched_at,
    ROW_NUMBER() OVER (
      PARTITION BY provider_id
      ORDER BY DATE(fetched_at) DESC, rating_source
    ) AS rn
  FROM `sw_bronze.ratings_ext`
)
SELECT
  provider_id,
  rating,
  reviews_count,
  rating_source,
  fetched_at
FROM ranked
WHERE rn = 1;

-- -------------------------------------------
-- Locations (typed view over native load)
-- Notes:
--  - Enforce types explicitly (stable schema).
--  - Surface run_date/source for lineage/debugging.
-- -------------------------------------------
CREATE OR REPLACE VIEW `sw_silver.locations_v` AS
SELECT
  CAST(provider_id AS STRING)       AS provider_id,
  CAST(place_id AS STRING)          AS place_id,
  CAST(formatted_address AS STRING) AS formatted_address,
  CAST(street AS STRING)            AS street,
  CAST(city AS STRING)              AS city,
  CAST(state AS STRING)             AS state,
  CAST(postal_code AS STRING)       AS postal_code,
  CAST(lat AS FLOAT64)              AS lat,
  CAST(lon AS FLOAT64)              AS lon,
  CAST(source AS STRING)            AS source,
  CAST(run_date AS STRING)          AS run_date
FROM `sw_bronze.locations_load`;

-- -------------------------------------------
-- Provider geo metrics
-- Compute distance (miles) from downtown Minneapolis
-- using BigQuery GEOGRAPHY (ST_DISTANCE on lon/lat).
-- -------------------------------------------
CREATE OR REPLACE VIEW `sw_silver.provider_geo_v` AS
WITH g AS (
  SELECT
    l.provider_id,
    l.lat,
    l.lon,
    ST_DISTANCE(
      ST_GEOGPOINT(CAST(l.lon AS FLOAT64), CAST(l.lat AS FLOAT64)),
      ST_GEOGPOINT(-93.2650, 44.9778)
    ) / 1609.344 AS distance_mi  -- meters → miles
  FROM `sw_silver.locations_v` l
  WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
)
SELECT * FROM g;
