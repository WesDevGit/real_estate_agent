# Databricks notebook source
# DBTITLE 1,Ingest amenities (OpenStreetMap Overpass)
# MAGIC %md
# MAGIC # Ingest Amenities
# MAGIC
# MAGIC Source: [OpenStreetMap Overpass API](https://wiki.openstreetmap.org/wiki/Overpass_API) —
# MAGIC free, no API key. Global coverage.
# MAGIC
# MAGIC Pulls grocery, hospital, pharmacy, school, restaurant, park, and transit stops
# MAGIC for each supported city's bounding box.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and config
import time
from datetime import datetime
from typing import Optional

import requests

TARGET_TABLE = "realestate.bronze.amenities"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_TIMEOUT_SECONDS = 180
SLEEP_BETWEEN_CITIES_SECONDS = 2.0
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 10

CO_BBOXES = [
    {"city": "Bogota",   "country_code": "CO", "south": 3.7, "west": -74.3, "north": 4.9, "east": -73.9},
    {"city": "Medellin", "country_code": "CO", "south": 6.1, "west": -75.7, "north": 6.4, "east": -75.5},
]

# US fallbacks if listings table is empty (Austin/Miami/Atlanta/Houston metros).
US_FALLBACK_BBOXES = [
    {"city": "Austin",  "country_code": "US", "south": 30.0, "west": -98.0, "north": 30.6, "east": -97.5},
    {"city": "Miami",   "country_code": "US", "south": 25.5, "west": -80.5, "north": 26.0, "east": -80.0},
    {"city": "Atlanta", "country_code": "US", "south": 33.5, "west": -84.7, "north": 34.0, "east": -84.1},
    {"city": "Houston", "country_code": "US", "south": 29.5, "west": -95.8, "north": 30.0, "east": -95.0},
]

# OSM tag → normalized amenity_type.
TAG_MAP = {
    "supermarket": "grocery",
    "hospital": "hospital",
    "pharmacy": "pharmacy",
    "school": "school",
    "restaurant": "restaurant",
    "park": "park",
    "stop_position": "transit_stop",
}

# COMMAND ----------

# DBTITLE 1,Discover US city bounding boxes from listings
def discover_us_bboxes():
    try:
        rows = spark.sql(
            """
            SELECT city,
                   MIN(lat) AS south, MAX(lat) AS north,
                   MIN(lon) AS west,  MAX(lon) AS east
            FROM realestate.bronze.listings_us
            WHERE lat IS NOT NULL AND lon IS NOT NULL
            GROUP BY city
            HAVING COUNT(*) >= 1
            """
        ).collect()
    except Exception:
        rows = []

    if not rows:
        return US_FALLBACK_BBOXES

    boxes = []
    for r in rows:
        south, north = float(r["south"]), float(r["north"])
        west, east = float(r["west"]), float(r["east"])
        # 10% buffer.
        lat_pad = max((north - south) * 0.1, 0.05)
        lon_pad = max((east - west) * 0.1, 0.05)
        boxes.append({
            "city": r["city"],
            "country_code": "US",
            "south": south - lat_pad,
            "north": north + lat_pad,
            "west":  west - lon_pad,
            "east":  east + lon_pad,
        })
    return boxes

# COMMAND ----------

# DBTITLE 1,Overpass query template
def build_overpass_query(south, west, north, east) -> str:
    bbox = f"{south},{west},{north},{east}"
    return f"""
[out:json][timeout:120];
(
  node["amenity"~"hospital|pharmacy|school|restaurant"]({bbox});
  node["shop"="supermarket"]({bbox});
  node["amenity"="supermarket"]({bbox});
  node["leisure"="park"]({bbox});
  node["public_transport"="stop_position"]({bbox});
);
out body;
""".strip()


def fetch_overpass(query: str) -> list[dict]:
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=OVERPASS_TIMEOUT_SECONDS)
        except Exception as e:
            print(f"  Overpass request error attempt {attempt+1}: {e}")
            time.sleep(BACKOFF_BASE_SECONDS * (attempt + 1))
            continue

        if resp.status_code == 200:
            return resp.json().get("elements") or []
        if resp.status_code in (429, 504, 503):
            wait = BACKOFF_BASE_SECONDS * (attempt + 1) * 3
            print(f"  Overpass {resp.status_code}, backing off {wait}s")
            time.sleep(wait)
            continue
        print(f"  Overpass returned {resp.status_code}: {resp.text[:200]}")
        return []
    return []

# COMMAND ----------

# DBTITLE 1,Map OSM node to amenity row
def node_to_row(node: dict, city: str, country_code: str) -> Optional[dict]:
    tags = node.get("tags") or {}
    amenity_type = None
    if tags.get("amenity") == "supermarket" or tags.get("shop") == "supermarket":
        amenity_type = "grocery"
    elif tags.get("amenity") in ("hospital", "pharmacy", "school", "restaurant"):
        amenity_type = TAG_MAP[tags["amenity"]]
    elif tags.get("leisure") == "park":
        amenity_type = "park"
    elif tags.get("public_transport") == "stop_position":
        amenity_type = "transit_stop"

    if amenity_type is None:
        return None

    return {
        "amenity_id": str(node.get("id")),
        "country_code": country_code,
        "city": city,
        "lat": node.get("lat"),
        "lon": node.get("lon"),
        "amenity_type": amenity_type,
        "name": tags.get("name"),
        "address": tags.get("addr:full") or tags.get("addr:street"),
    }

# COMMAND ----------

# DBTITLE 1,Run ingestion
def run_ingest() -> int:
    cities = CO_BBOXES + discover_us_bboxes()
    print(f"Fetching amenities for {len(cities)} cities")

    total = 0
    now = datetime.utcnow()
    for c in cities:
        print(f"\n{c['city']} ({c['country_code']})")
        query = build_overpass_query(c["south"], c["west"], c["north"], c["east"])
        elements = fetch_overpass(query)
        rows = [r for r in (node_to_row(n, c["city"], c["country_code"]) for n in elements) if r]

        if not rows:
            print("  no amenities returned")
            time.sleep(SLEEP_BETWEEN_CITIES_SECONDS)
            continue

        for r in rows:
            r["ingest_time"] = now

        df = spark.createDataFrame(rows)
        df.createOrReplaceTempView("_new_amenities")
        spark.sql(
            f"""
            MERGE INTO {TARGET_TABLE} t
            USING _new_amenities s ON t.amenity_id = s.amenity_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

        by_type = {}
        for r in rows:
            by_type[r["amenity_type"]] = by_type.get(r["amenity_type"], 0) + 1
        print(f"  upserted {len(rows)} amenities: {by_type}")
        total += len(rows)
        time.sleep(SLEEP_BETWEEN_CITIES_SECONDS)

    print(f"\nTotal amenities upserted: {total}")
    return total


run_ingest()
