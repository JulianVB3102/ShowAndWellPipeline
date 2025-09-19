-- ----------------------------------------------
-- Tiered directory (Gold layer)
-- Joins clean providers with latest ratings,
-- then assigns a tier based on rating + volume.
-- ----------------------------------------------
CREATE OR REPLACE VIEW `sw_gold.provider_directory_tiered_v` AS
WITH base AS (
  SELECT
    p.provider_id,
    p.display_name,
    p.category,
    p.website,
    p.phone,
    r.rating,
    r.reviews_count
  FROM `sw_silver.providers_latest_v` p
  LEFT JOIN `sw_silver.ratings_latest_v` r USING (provider_id)  -- keep providers even without ratings
)
SELECT
  provider_id,
  display_name,
  category,
  website,
  phone,
  rating,
  reviews_count,
  -- Tier rules:
  -- Gold   = strong rating (≥3.8) AND solid volume (≥30 reviews)
  -- Silver = decent rating (≥3.0) but misses one of the Gold thresholds
  -- Bronze = everything else (no ratings or low scores)
  CASE
    WHEN rating IS NOT NULL
      AND reviews_count IS NOT NULL
      AND rating >= 3.8
      AND reviews_count >= 30
      THEN "Gold"
    WHEN rating IS NOT NULL
      AND rating >= 3.0
      AND (rating < 3.8 OR reviews_count < 30)
      THEN "Silver"
    ELSE "Bronze"
  END AS tier
FROM base;

-- ----------------------------------------------------
-- Map view (Gold layer)
-- Adds geospatial fields (lat/lon), distance in miles
-- from downtown Minneapolis, and a 35-mile flag.
-- ----------------------------------------------------
CREATE OR REPLACE VIEW `sw_gold.provider_map_v` AS
SELECT
  d.provider_id,
  d.display_name,
  d.category,
  d.website,
  d.phone,
  d.rating,
  d.reviews_count,
  d.tier,
  g.lat,
  g.lon,
  g.distance_mi,
  CASE WHEN g.distance_mi <= 35 THEN TRUE ELSE FALSE END AS within_35_miles
FROM `sw_gold.provider_directory_tiered_v` d
LEFT JOIN `sw_silver.provider_geo_v` g USING (provider_id);  -- keep rows even if we lack geo
