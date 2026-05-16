# Databricks notebook source
# DBTITLE 1,Real Estate Agent - Ingest Hazards
# MAGIC %md
# MAGIC # 08 — Ingest Hazards
# MAGIC
# MAGIC Loads natural-hazard risk data for US and Colombia into
# MAGIC `realestate.bronze.hazards` from four sources. Each block writes its own
# MAGIC slice (filtered by `source` + `hazard_type`) using overwriteByPartition-style
# MAGIC deletes followed by appends so re-runs are idempotent.
# MAGIC
# MAGIC **Sources covered:**
# MAGIC 1. **FEMA NFHL Flood Hazard Zones** — Layer 28 ArcGIS REST, queried per state
# MAGIC    bounding box derived from `bronze.listings_us` centroids (fallback hardcoded).
# MAGIC 2. **FEMA Disaster Declarations** — OpenFEMA API; aggregated per county over
# MAGIC    the last 20 years.
# MAGIC 3. **USGS PGA Earthquake Hazard** — CSV staged in volume; spatially joined to
# MAGIC    US ZCTA centroids (`bronze.demographics_us`) and CO municipios.
# MAGIC 4. **UNGRD (Colombia)** — risk maps staged in volume; municipio-level flood and
# MAGIC    landslide classifications mapped to risk scores.
# MAGIC
# MAGIC **NOAA SPC tornado** is optional and currently a markdown placeholder.
# MAGIC
# MAGIC `hazard_id = f"{source}_{hazard_type}_{geo}"` is the dedup key within each block.
# MAGIC
# MAGIC **Schedule:** quarterly.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and catalog
import csv
import json
import math
import time
from datetime import date, datetime, timezone
from typing import Iterable

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DateType,
)

spark.sql("USE CATALOG realestate")

# COMMAND ----------

# DBTITLE 1,Config
HAZARDS_TABLE = "realestate.bronze.hazards"

# FEMA NFHL Map Server — Layer 28 = Flood Hazard Zones.
FEMA_NFHL_URL = (
    "https://msc.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query"
)
FEMA_NFHL_PAGE_SIZE = 1000
FEMA_NFHL_TIMEOUT = 60

# OpenFEMA disaster declarations.
FEMA_DISASTERS_URL = (
    "https://www.fema.gov/api/open/v1/disasterDeclarationsSummaries"
)
FEMA_DISASTERS_TOP = 1000
FEMA_DISASTERS_LOOKBACK_YEARS = 20
FEMA_DISASTERS_TIMEOUT = 60

# Volume paths where the operator stages raw bulk files for sources that have
# no clean REST endpoint. If the file/directory is missing, the corresponding
# block logs a warning and skips.
USGS_PGA_VOLUME_DIR = "/Volumes/realestate/bronze/raw/usgs_pga/"
UNGRD_VOLUME_DIR = "/Volumes/realestate/bronze/raw/ungrd/"

# Hardcoded fallback bounding boxes (xmin, ymin, xmax, ymax in WGS84) used when
# bronze.listings_us has no rows for that state yet. Boxes are intentionally
# generous so we capture flood zones in the surrounding metro area.
FALLBACK_STATE_BBOXES = {
    "TX": (-106.65, 25.84, -93.51, 36.50),
    "CA": (-124.48, 32.53, -114.13, 42.01),
    "FL": (-87.63, 24.52, -80.03, 31.00),
    "NY": (-79.76, 40.50, -71.86, 45.02),
    "AZ": (-114.82, 31.33, -109.05, 37.00),
    "CO": (-109.06, 36.99, -102.04, 41.00),
    "GA": (-85.61, 30.36, -80.84, 35.00),
    "NC": (-84.32, 33.84, -75.46, 36.59),
    "WA": (-124.85, 45.54, -116.92, 49.00),
    "IL": (-91.51, 36.97, -87.50, 42.51),
}

# Map of OpenFEMA incidentType -> hazard_type vocabulary used downstream.
FEMA_INCIDENT_TYPE_MAP = {
    "Flood": "flood",
    "Coastal Storm": "flood",
    "Severe Storm": "flood",
    "Severe Storm(s)": "flood",
    "Hurricane": "hurricane",
    "Tropical Storm": "hurricane",
    "Typhoon": "hurricane",
    "Tornado": "tornado",
    "Fire": "wildfire",
    "Earthquake": "earthquake",
    "Mud/Landslide": "landslide",
    "Severe Ice Storm": "flood",
}

# UNGRD classification -> 0-1 risk score.
UNGRD_RISK_LEVELS = {
    "bajo": 0.2,
    "medio": 0.5,
    "alto": 0.8,
    "muy alto": 0.95,
    "muy_alto": 0.95,
}

# FEMA flood zone code -> 0-1 risk score.
FEMA_FLOOD_ZONE_RISK = {
    "AE": 0.9,
    "A": 0.9,
    "AH": 0.9,
    "AO": 0.9,
    "V": 0.9,
    "VE": 0.9,
    "X500": 0.4,
    "X": 0.1,
}

# Schema for staging rows before write. Keeping this explicit avoids costly
# schema inference and ensures every block writes the same shape.
HAZARDS_SCHEMA = StructType([
    StructField("hazard_id", StringType(), False),
    StructField("source", StringType(), False),
    StructField("country_code", StringType(), True),
    StructField("state_or_dept", StringType(), True),
    StructField("county_or_municipio", StringType(), True),
    StructField("zip_or_zone", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("hazard_type", StringType(), False),
    StructField("risk_level", StringType(), True),
    StructField("risk_score", DoubleType(), True),
    StructField("data_date", DateType(), True),
    StructField("details_json", StringType(), True),
    StructField("ingested_at", TimestampType(), False),
])

print(f"Target table: {HAZARDS_TABLE}")
print(f"FEMA NFHL URL: {FEMA_NFHL_URL}")
print(f"OpenFEMA URL : {FEMA_DISASTERS_URL}")

# COMMAND ----------

# DBTITLE 1,Shared write + helpers
def _score_to_label(score: float) -> str:
    """Map a 0-1 risk_score to the canonical risk_level vocabulary."""
    if score is None:
        return None
    if score >= 0.8:
        return "very_high"
    if score >= 0.6:
        return "high"
    if score >= 0.3:
        return "medium"
    return "low"


def _now_ts():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _write_block(rows: list, source: str, hazard_type: str) -> int:
    """
    Write a list of dict rows to bronze.hazards, replacing any existing rows
    with the same (source, hazard_type) tuple so re-runs are idempotent.

    Within `rows`, duplicates by hazard_id are collapsed (last write wins).
    """
    if not rows:
        # Nothing to write, but still clear any stale rows for this slice so
        # an empty re-run doesn't leave old data behind.
        spark.sql(
            f"DELETE FROM {HAZARDS_TABLE} "
            f"WHERE source = '{source}' AND hazard_type = '{hazard_type}'"
        )
        return 0

    # Deduplicate within the batch by hazard_id.
    by_id = {row["hazard_id"]: row for row in rows}
    deduped = list(by_id.values())

    df = spark.createDataFrame(deduped, schema=HAZARDS_SCHEMA)

    # Replace just this (source, hazard_type) slice.
    spark.sql(
        f"DELETE FROM {HAZARDS_TABLE} "
        f"WHERE source = '{source}' AND hazard_type = '{hazard_type}'"
    )
    df.write.mode("append").saveAsTable(HAZARDS_TABLE)
    return len(deduped)


def _state_bbox_from_listings() -> dict:
    """
    Compute (xmin, ymin, xmax, ymax) per US state from listings centroids.
    Falls back to the hardcoded bbox map for any state not yet covered by
    listings (or when the table is empty).
    """
    bboxes = dict(FALLBACK_STATE_BBOXES)
    try:
        df = spark.sql(
            "SELECT state, MIN(lon) AS xmin, MIN(lat) AS ymin, "
            "MAX(lon) AS xmax, MAX(lat) AS ymax "
            "FROM realestate.bronze.listings_us "
            "WHERE lat IS NOT NULL AND lon IS NOT NULL "
            "GROUP BY state"
        )
        for row in df.collect():
            if row["state"] is None:
                continue
            # Pad the centroid envelope by ~0.5 degrees so we capture flood
            # zones a few miles beyond the listing footprint.
            pad = 0.5
            bboxes[row["state"]] = (
                float(row["xmin"]) - pad,
                float(row["ymin"]) - pad,
                float(row["xmax"]) + pad,
                float(row["ymax"]) + pad,
            )
    except Exception as exc:  # noqa: BLE001
        print(f"  (could not read bronze.listings_us yet: {exc}; using fallback bboxes)")
    return bboxes


print("Helpers ready.")

# COMMAND ----------

# DBTITLE 1,Block 1 - FEMA NFHL Flood Hazard Zones (US)
def _zone_risk(zone_code: str) -> float:
    """Look up the 0-1 risk score for a FEMA flood-zone code."""
    if not zone_code:
        return 0.5
    code = zone_code.strip().upper()
    if code in FEMA_FLOOD_ZONE_RISK:
        return FEMA_FLOOD_ZONE_RISK[code]
    # Some zones come back as 'AE99' etc — match by prefix as a fallback.
    for prefix, score in FEMA_FLOOD_ZONE_RISK.items():
        if code.startswith(prefix):
            return score
    return 0.5


def _fetch_fema_flood_for_state(state: str, bbox: tuple) -> list:
    """Page through the FEMA NFHL Layer 28 query for one state bbox."""
    xmin, ymin, xmax, ymax = bbox
    rows = []
    offset = 0
    ingest_ts = _now_ts()
    data_dt = date.today()

    while True:
        params = {
            "geometry": f"{xmin},{ymin},{xmax},{ymax}",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": FEMA_NFHL_PAGE_SIZE,
        }
        try:
            resp = requests.get(
                FEMA_NFHL_URL, params=params, timeout=FEMA_NFHL_TIMEOUT
            )
            if resp.status_code != 200:
                print(f"    {state} offset={offset}: HTTP {resp.status_code}, stopping")
                break
            payload = resp.json()
        except Exception as exc:  # noqa: BLE001
            print(f"    {state} offset={offset}: error {exc}, stopping")
            break

        features = payload.get("features", []) or []
        if not features:
            break

        for feat in features:
            attrs = feat.get("attributes", {}) or {}
            zone_code = (
                attrs.get("FLD_ZONE")
                or attrs.get("ZONE_SUBTY")
                or attrs.get("ZONE")
            )
            score = _zone_risk(zone_code)
            # FEMA uses DFIRM_ID / FLD_AR_ID / OBJECTID for stable identity.
            zone_id = (
                attrs.get("FLD_AR_ID")
                or attrs.get("DFIRM_ID")
                or attrs.get("OBJECTID")
            )
            if zone_id is None:
                continue
            geo = f"{state}_{zone_id}"
            rows.append({
                "hazard_id": f"fema_flood_{geo}",
                "source": "fema",
                "country_code": "US",
                "state_or_dept": state,
                "county_or_municipio": attrs.get("COUNTY") or attrs.get("CO_FIPS"),
                "zip_or_zone": str(zone_code) if zone_code else None,
                "lat": None,
                "lon": None,
                "hazard_type": "flood",
                "risk_level": _score_to_label(score),
                "risk_score": float(score),
                "data_date": data_dt,
                "details_json": json.dumps(attrs, default=str),
                "ingested_at": ingest_ts,
            })

        # ArcGIS REST reports `exceededTransferLimit=true` when more pages
        # remain. If we got fewer than a full page we're done.
        if not payload.get("exceededTransferLimit") and len(features) < FEMA_NFHL_PAGE_SIZE:
            break
        offset += FEMA_NFHL_PAGE_SIZE
        # Be polite to FEMA's public ArcGIS instance.
        time.sleep(0.25)

    return rows


print("Querying FEMA NFHL Flood Hazard Zones per US state bbox...")
state_bboxes = _state_bbox_from_listings()
all_flood_rows = []
for state, bbox in sorted(state_bboxes.items()):
    print(f"  {state} bbox={bbox}")
    state_rows = _fetch_fema_flood_for_state(state, bbox)
    print(f"    fetched {len(state_rows)} flood-zone features")
    all_flood_rows.extend(state_rows)

written = _write_block(all_flood_rows, source="fema", hazard_type="flood")
print(f"Wrote {written} rows to bronze.hazards (source=fema, hazard_type=flood)")

# COMMAND ----------

# DBTITLE 1,Block 2 - FEMA Disaster Declarations (US)
def _fetch_fema_disasters(state: str) -> list:
    """Fetch up to FEMA_DISASTERS_TOP recent declarations for one state."""
    cutoff = date.today().replace(year=date.today().year - FEMA_DISASTERS_LOOKBACK_YEARS)
    cutoff_str = cutoff.isoformat() + "T00:00:00.000z"
    params = {
        "$filter": (
            f"state eq '{state}' and "
            f"declarationDate ge '{cutoff_str}'"
        ),
        "$orderby": "declarationDate desc",
        "$top": FEMA_DISASTERS_TOP,
    }
    try:
        resp = requests.get(
            FEMA_DISASTERS_URL, params=params, timeout=FEMA_DISASTERS_TIMEOUT
        )
        if resp.status_code != 200:
            print(f"  {state}: HTTP {resp.status_code}")
            return []
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        print(f"  {state}: error {exc}")
        return []
    return payload.get("DisasterDeclarationsSummaries", []) or []


# Aggregate disaster count per (state, county, mapped hazard_type).
print("Querying OpenFEMA disaster declarations per state...")
state_codes = sorted(set(FALLBACK_STATE_BBOXES.keys()))
# Per-county, per-hazard accumulators.
agg: dict = {}
raw_samples: dict = {}
for state in state_codes:
    declarations = _fetch_fema_disasters(state)
    print(f"  {state}: {len(declarations)} declarations")
    for dec in declarations:
        county = (
            dec.get("designatedArea")
            or dec.get("declaredCountyArea")
            or "STATEWIDE"
        )
        incident_type = dec.get("incidentType") or "Other"
        hazard_type = FEMA_INCIDENT_TYPE_MAP.get(incident_type)
        if not hazard_type:
            continue
        key = (state, county, hazard_type)
        agg[key] = agg.get(key, 0) + 1
        # Keep one sample declaration for details_json so we can audit later.
        raw_samples.setdefault(key, dec)

ingest_ts = _now_ts()
data_dt = date.today()
disaster_rows_by_type: dict = {}
for (state, county, hazard_type), count in agg.items():
    # Normalize count to 0-1: 50 events in 20 years = max risk.
    score = min(count / 50.0, 1.0)
    geo = f"{state}_{county}".replace(" ", "_").replace("'", "")
    row = {
        "hazard_id": f"fema_{hazard_type}_{geo}",
        "source": "fema",
        "country_code": "US",
        "state_or_dept": state,
        "county_or_municipio": county,
        "zip_or_zone": None,
        "lat": None,
        "lon": None,
        "hazard_type": hazard_type,
        "risk_level": _score_to_label(score),
        "risk_score": float(score),
        "data_date": data_dt,
        "details_json": json.dumps({
            "disaster_count": count,
            "lookback_years": FEMA_DISASTERS_LOOKBACK_YEARS,
            "sample": raw_samples.get((state, county, hazard_type)),
        }, default=str),
        "ingested_at": ingest_ts,
    }
    disaster_rows_by_type.setdefault(hazard_type, []).append(row)

# Write each disaster-derived hazard_type as its own slice so we can re-run
# them independently (each block-by-block overwrite is keyed on hazard_type).
total_disaster_rows = 0
for hazard_type, rows in disaster_rows_by_type.items():
    written = _write_block(rows, source="fema", hazard_type=hazard_type)
    total_disaster_rows += written
    print(f"  wrote {written} rows for hazard_type={hazard_type}")

# Ensure the disaster-derived hazard types that weren't seen this run get
# their stale slice cleared (so re-runs with fewer events stay consistent).
for hazard_type in {"hurricane", "tornado", "wildfire", "earthquake", "landslide"}:
    if hazard_type not in disaster_rows_by_type and hazard_type != "earthquake":
        # earthquake is fully owned by the USGS block below; don't wipe it here.
        spark.sql(
            f"DELETE FROM {HAZARDS_TABLE} "
            f"WHERE source = 'fema' AND hazard_type = '{hazard_type}'"
        )

print(
    f"Wrote {total_disaster_rows} rows to bronze.hazards "
    f"(source=fema, disaster-derived hazard_types)"
)

# COMMAND ----------

# DBTITLE 1,Block 3 - USGS PGA Earthquake Hazard (US + CO)
# MAGIC %md
# MAGIC ### USGS PGA earthquake bulk import
# MAGIC
# MAGIC The USGS National Seismic Hazard Model PGA grid does not have a clean REST
# MAGIC endpoint suitable for incremental scraping, so the operator downloads the
# MAGIC 2%-in-50-year PGA grid as CSV from
# MAGIC <https://earthquake.usgs.gov/hazards/hazmaps/> and stages it at:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/realestate/bronze/raw/usgs_pga/
# MAGIC ```
# MAGIC
# MAGIC Expected CSV columns: `lat,lon,pga` (PGA in g). For Colombia, use the global
# MAGIC GSHAP / global hazard grid from the same site.
# MAGIC
# MAGIC This cell spatially joins each PGA grid point to the nearest US ZCTA centroid
# MAGIC (from `bronze.demographics_us`) and CO municipio centroid. If the directory
# MAGIC is empty/missing, the block logs a warning and skips.

# COMMAND ----------

def _list_volume_csvs(path: str) -> list:
    """Return CSV files in a volume directory, or [] if the path is missing."""
    try:
        return [
            f.path for f in dbutils.fs.ls(path)  # noqa: F821
            if f.path.lower().endswith(".csv")
        ]
    except Exception as exc:  # noqa: BLE001
        print(f"  volume {path} not accessible: {exc}")
        return []


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in km."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _read_pga_points(csv_files: list) -> list:
    """Stream PGA points from one or more CSVs. Tolerant of column naming."""
    points = []
    for path in csv_files:
        try:
            df = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(path)
            )
            cols = {c.lower(): c for c in df.columns}
            lat_c = cols.get("lat") or cols.get("latitude")
            lon_c = cols.get("lon") or cols.get("longitude") or cols.get("lng")
            pga_c = (
                cols.get("pga")
                or cols.get("pga_g")
                or cols.get("acc")
                or cols.get("value")
            )
            if not (lat_c and lon_c and pga_c):
                print(f"    {path}: missing lat/lon/pga columns; skipping")
                continue
            for row in df.select(lat_c, lon_c, pga_c).collect():
                lat, lon, pga = row[0], row[1], row[2]
                if lat is None or lon is None or pga is None:
                    continue
                points.append((float(lat), float(lon), float(pga)))
        except Exception as exc:  # noqa: BLE001
            print(f"    {path}: read error {exc}")
    return points


def _spatial_join_pga(centroids: list, pga_points: list) -> dict:
    """
    For each centroid, find the nearest PGA grid point and return its PGA in g.
    `centroids` is a list of `(geo_id, state_or_dept, area_name, lat, lon)`.
    O(n*m) — acceptable for ~33k ZCTAs * a sparse PGA grid; for very large
    grids the operator should pre-filter the CSV.
    """
    results = {}
    if not pga_points:
        return results
    for geo_id, state, area, lat, lon in centroids:
        if lat is None or lon is None:
            continue
        best = None
        best_d = None
        for plat, plon, pga in pga_points:
            d = _haversine_km(lat, lon, plat, plon)
            if best_d is None or d < best_d:
                best_d = d
                best = (plat, plon, pga)
        if best is not None:
            results[geo_id] = (state, area, lat, lon, best[2])
    return results


usgs_csvs = _list_volume_csvs(USGS_PGA_VOLUME_DIR)
if not usgs_csvs:
    print(
        f"WARNING: no USGS PGA CSVs found in {USGS_PGA_VOLUME_DIR}; "
        "skipping earthquake hazard ingestion."
    )
else:
    print(f"Found {len(usgs_csvs)} USGS PGA CSV file(s):")
    for f in usgs_csvs:
        print(f"  {f}")
    pga_points = _read_pga_points(usgs_csvs)
    print(f"Loaded {len(pga_points)} PGA grid points.")

    # Pull US ZCTA centroids from bronze.demographics_us (uses listings_us
    # lat/lon as a proxy when no centroid is stored on demographics).
    us_centroids = []
    try:
        df = spark.sql(
            "SELECT d.zip AS geo_id, d.state, d.city, "
            "       AVG(l.lat) AS lat, AVG(l.lon) AS lon "
            "FROM realestate.bronze.demographics_us d "
            "LEFT JOIN realestate.bronze.listings_us l "
            "  ON d.zip = l.zip "
            "WHERE d.zip IS NOT NULL "
            "GROUP BY d.zip, d.state, d.city"
        )
        us_centroids = [
            (r["geo_id"], r["state"], r["city"], r["lat"], r["lon"])
            for r in df.collect()
        ]
    except Exception as exc:  # noqa: BLE001
        print(f"  could not read bronze.demographics_us: {exc}")

    # Colombia municipios from bronze.listings_co.
    co_centroids = []
    try:
        df = spark.sql(
            "SELECT city AS geo_id, departamento AS state, city, "
            "       AVG(lat) AS lat, AVG(lon) AS lon "
            "FROM realestate.bronze.listings_co "
            "WHERE lat IS NOT NULL AND lon IS NOT NULL "
            "GROUP BY city, departamento"
        )
        co_centroids = [
            (r["geo_id"], r["state"], r["city"], r["lat"], r["lon"])
            for r in df.collect()
        ]
    except Exception as exc:  # noqa: BLE001
        print(f"  could not read bronze.listings_co: {exc}")

    print(f"  US centroids: {len(us_centroids)}, CO centroids: {len(co_centroids)}")
    us_join = _spatial_join_pga(us_centroids, pga_points)
    co_join = _spatial_join_pga(co_centroids, pga_points)

    ingest_ts = _now_ts()
    data_dt = date.today()
    eq_rows = []

    for country_code, joined in (("US", us_join), ("CO", co_join)):
        for geo_id, (state, area, lat, lon, pga) in joined.items():
            score = min(float(pga) / 0.5, 1.0)
            eq_rows.append({
                "hazard_id": f"usgs_earthquake_{country_code}_{geo_id}",
                "source": "usgs",
                "country_code": country_code,
                "state_or_dept": state,
                "county_or_municipio": area,
                "zip_or_zone": geo_id if country_code == "US" else None,
                "lat": float(lat) if lat is not None else None,
                "lon": float(lon) if lon is not None else None,
                "hazard_type": "earthquake",
                "risk_level": _score_to_label(score),
                "risk_score": float(score),
                "data_date": data_dt,
                "details_json": json.dumps({"pga_g": float(pga)}),
                "ingested_at": ingest_ts,
            })

    written = _write_block(eq_rows, source="usgs", hazard_type="earthquake")
    print(f"Wrote {written} rows to bronze.hazards (source=usgs, hazard_type=earthquake)")

# COMMAND ----------

# DBTITLE 1,Block 4 - UNGRD (Colombia) flood and landslide
# MAGIC %md
# MAGIC ### UNGRD risk maps (Colombia)
# MAGIC
# MAGIC The operator downloads municipio-level risk classifications from
# MAGIC <https://portal.gestiondelriesgo.gov.co> and stages CSV files (one per
# MAGIC hazard) at:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/realestate/bronze/raw/ungrd/
# MAGIC ```
# MAGIC
# MAGIC The filename determines the hazard_type:
# MAGIC * `flood*.csv` -> `flood`
# MAGIC * `landslide*.csv` or `deslizamiento*.csv` -> `landslide`
# MAGIC * `earthquake*.csv` or `sismo*.csv` -> `earthquake`
# MAGIC
# MAGIC Each CSV should have columns roughly matching:
# MAGIC `municipio, departamento, classification` (where classification is one of
# MAGIC `bajo / medio / alto / muy alto`, case-insensitive).
# MAGIC
# MAGIC If the directory is empty/missing, the block logs a warning and skips.

# COMMAND ----------

def _ungrd_hazard_type_from_filename(name: str) -> str:
    n = name.lower()
    if "flood" in n or "inundac" in n:
        return "flood"
    if "landslide" in n or "deslizam" in n or "remoc" in n:
        return "landslide"
    if "earthquake" in n or "sismo" in n or "amenaza_sis" in n:
        return "earthquake"
    return None


ungrd_csvs = _list_volume_csvs(UNGRD_VOLUME_DIR)
if not ungrd_csvs:
    print(
        f"WARNING: no UNGRD CSVs found in {UNGRD_VOLUME_DIR}; "
        "skipping Colombia hazard ingestion."
    )
else:
    print(f"Found {len(ungrd_csvs)} UNGRD CSV file(s).")
    ingest_ts = _now_ts()
    data_dt = date.today()
    ungrd_rows_by_type: dict = {}

    for path in ungrd_csvs:
        fname = path.rsplit("/", 1)[-1]
        hazard_type = _ungrd_hazard_type_from_filename(fname)
        if not hazard_type:
            print(f"  {fname}: unrecognized hazard from filename, skipping")
            continue
        try:
            df = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(path)
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  {fname}: read error {exc}")
            continue

        cols = {c.lower(): c for c in df.columns}
        muni_c = (
            cols.get("municipio")
            or cols.get("city")
            or cols.get("nombre")
            or cols.get("nombre_municipio")
        )
        dept_c = (
            cols.get("departamento")
            or cols.get("dept")
            or cols.get("state")
        )
        class_c = (
            cols.get("classification")
            or cols.get("clasificacion")
            or cols.get("nivel")
            or cols.get("riesgo")
            or cols.get("amenaza")
        )
        if not (muni_c and class_c):
            print(f"  {fname}: missing municipio/classification columns; skipping")
            continue

        rows = []
        for row in df.collect():
            muni = row[muni_c]
            if muni is None:
                continue
            dept = row[dept_c] if dept_c else None
            raw_class = row[class_c]
            if raw_class is None:
                continue
            key = str(raw_class).strip().lower()
            score = UNGRD_RISK_LEVELS.get(key)
            if score is None:
                # Try collapsing whitespace ("muy  alto" -> "muy alto").
                score = UNGRD_RISK_LEVELS.get(" ".join(key.split()))
            if score is None:
                continue
            muni_clean = str(muni).strip()
            geo = f"{(dept or 'CO').strip()}_{muni_clean}".replace(" ", "_")
            rows.append({
                "hazard_id": f"ungrd_{hazard_type}_{geo}",
                "source": "ungrd",
                "country_code": "CO",
                "state_or_dept": str(dept).strip() if dept else None,
                "county_or_municipio": muni_clean,
                "zip_or_zone": None,
                "lat": None,
                "lon": None,
                "hazard_type": hazard_type,
                "risk_level": _score_to_label(score),
                "risk_score": float(score),
                "data_date": data_dt,
                "details_json": json.dumps({
                    "source_file": fname,
                    "classification": str(raw_class),
                }),
                "ingested_at": ingest_ts,
            })
        ungrd_rows_by_type.setdefault(hazard_type, []).extend(rows)
        print(f"  {fname}: parsed {len(rows)} rows ({hazard_type})")

    # NOTE: UNGRD writes share hazard_type=flood/landslide/earthquake with
    # other sources. We key the dedup slice on (source='ungrd', hazard_type)
    # so this only replaces UNGRD rows, leaving FEMA/USGS rows for the same
    # hazard_type untouched.
    total_ungrd = 0
    for hazard_type, rows in ungrd_rows_by_type.items():
        # Custom delete keeping the source filter strict.
        spark.sql(
            f"DELETE FROM {HAZARDS_TABLE} "
            f"WHERE source = 'ungrd' AND hazard_type = '{hazard_type}'"
        )
        if rows:
            # Deduplicate by hazard_id within the batch.
            by_id = {r["hazard_id"]: r for r in rows}
            df = spark.createDataFrame(list(by_id.values()), schema=HAZARDS_SCHEMA)
            df.write.mode("append").saveAsTable(HAZARDS_TABLE)
            total_ungrd += len(by_id)
            print(f"  wrote {len(by_id)} rows for ungrd {hazard_type}")
    print(f"Wrote {total_ungrd} rows total from UNGRD.")

# COMMAND ----------

# DBTITLE 1,Optional - NOAA SPC tornado tracks
# MAGIC %md
# MAGIC ### NOAA SPC tornado tracks (optional)
# MAGIC
# MAGIC Tornado history is currently *not* ingested. To add it later:
# MAGIC
# MAGIC 1. Download the tornado track shapefile from
# MAGIC    <https://www.spc.noaa.gov/gis/svrgis/>
# MAGIC    (e.g. `1950-YYYY_torn.zip`).
# MAGIC 2. Stage the unzipped shapefile (`.shp`, `.dbf`, `.shx`) at
# MAGIC    `/Volumes/realestate/bronze/raw/noaa_spc/`.
# MAGIC 3. Parse with `geopandas.read_file(...)`, aggregate tornado counts per
# MAGIC    county over the last 50 years, normalize counts to a 0-1 score
# MAGIC    (e.g. `min(count / 100, 1.0)`), and write with
# MAGIC    `source='noaa_spc'`, `hazard_type='tornado'` using the same `_write_block`
# MAGIC    helper from this notebook.
# MAGIC
# MAGIC FEMA disaster declarations already provide a tornado proxy at the county
# MAGIC level (block 2), so the SPC shapefile is additive, not blocking.

# COMMAND ----------

# DBTITLE 1,Counts and sanity check
counts_df = spark.sql(
    f"""
    SELECT source, hazard_type, COUNT(*) AS row_count
    FROM {HAZARDS_TABLE}
    GROUP BY source, hazard_type
    ORDER BY source, hazard_type
    """
)
counts_df.show(truncate=False)

total = spark.sql(f"SELECT COUNT(*) AS c FROM {HAZARDS_TABLE}").first()["c"]
print(f"bronze.hazards total rows: {total}")
