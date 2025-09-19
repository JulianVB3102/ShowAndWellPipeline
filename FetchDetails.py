import os, sys, csv, time, json, pathlib, requests
from typing import Dict, Any

# ------------------------------
# Config / constants
# ------------------------------
API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")  # must be set in environment
CENTER_LAT, CENTER_LON = 44.9778, -93.2650       # Minneapolis downtown (search center)

def search_places_v1(query: str) -> Dict[str, Any]:
    """
    Call Google Places API v1 (searchText) for a free-text query,
    biased to a 50 km circle around the CENTER_LAT/LON.
    Returns the raw JSON dict from the API.
    """
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        # Required API key header for Places v1
        "X-Goog-Api-Key": API_KEY,
        # Field mask to limit payload size (only fetch what we need)
        "X-Goog-FieldMask": "places.id,places.displayName,places.rating,places.userRatingCount,places.location",
        "Content-Type": "application/json",
    }
    body = {
        "textQuery": query,
        # Location bias narrows results to ~50km radius around downtown Minneapolis
        "locationBias": {
            "circle": {"center": {"latitude": CENTER_LAT, "longitude": CENTER_LON}, "radius": 50000}
        }
    }
    r = requests.post(url, headers=headers, json=body, timeout=20)
    r.raise_for_status()  # raise on non-2xx responses
    return r.json()

def main(providers_csv: str, out_dir: str, today: str):
    """
    Read providers from providers_csv, query Places for each,
    and write a ratings CSV to {out_dir}/{today}/ratings.csv with columns:
      provider_id,rating,reviews_count,rating_source,places_id,fetched_at
    """
    if not API_KEY:
        # Fail fast if the API key isn't present
        raise RuntimeError("Set GOOGLE_MAPS_API_KEY")

    # Output path like: data/bronze/2025-09-19/ratings.csv
    out_path = pathlib.Path(out_dir) / today / "ratings.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows_out = []

    # Open the providers CSV and iterate rows as dicts
    with open(providers_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid   = row.get("provider_id")
            name  = row.get("display_name") or ""
            # If no city column is present/filled, fall back to a broad "Minnesota"
            city  = row.get("city") or "Minnesota"

            # Build a pragmatic search string (e.g., "Revival PT Minneapolis MN")
            query = f"{name} {city} MN".strip()

            try:
                # Call Places API for this provider
                js = search_places_v1(query)

                # Take the top match (if any)
                place = (js.get("places") or [None])[0]
                if not place:
                    print(f"!! No place for {name}")
                    continue

                # Pull the fields we care about (may be missing)
                rating = place.get("rating")
                urc    = place.get("userRatingCount")
                pid_g  = place.get("id")

                # Append a normalized output row
                rows_out.append({
                    "provider_id": pid,
                    "rating": rating if rating is not None else "",
                    "reviews_count": urc if urc is not None else "",
                    "rating_source": "google_places_v1",
                    "places_id": pid_g,
                    "fetched_at": today
                })

            except Exception as e:
                # Don't kill the whole run if one lookup fails; log and continue
                print(f"!! Error for {name}: {e}")

    # Write results CSV (header + rows)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["provider_id","rating","reviews_count","rating_source","places_id","fetched_at"]
        )
        w.writeheader()
        for r in rows_out:
            w.writerow(r)

    print(f"Wrote {out_path}")

if __name__ == "__main__":
    # Expecting: python fetch_places_ratings.py providers.csv out_dir YYYY-MM-DD
    main(sys.argv[1], sys.argv[2], sys.argv[3])
