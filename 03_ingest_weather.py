# Databricks notebook source
# DBTITLE 1,Ingest Weather History
# MAGIC %md
# MAGIC # 03 — Ingest Weather History
# MAGIC
# MAGIC Pulls 10 years of daily weather history from the **Open-Meteo Archive API**
# MAGIC (`https://archive-api.open-meteo.com/v1/archive`) for every city we cover:
# MAGIC
# MAGIC * Bogotá and Medellín (Colombia, hard-coded centroids).
# MAGIC * US cities discovered from distinct `(city, state, lat, lon)` in
# MAGIC   `realestate.bronze.listings_us`. If the listings table is empty (first
# MAGIC   run before `02_ingest_listings_us` has populated anything), we fall back
# MAGIC   to a hard-coded list of US target cities: Austin TX, Miami FL,
# MAGIC   Atlanta GA, Houston TX.
# MAGIC
# MAGIC Open-Meteo Archive is keyless and accepts ~10-year date ranges in a single
# MAGIC request, so this notebook is **incremental**:
# MAGIC
# MAGIC 1. For each location, look up the max `date` already in
# MAGIC    `realestate.bronze.weather_history` for that `location_key`.
# MAGIC 2. Fetch from `(max_date + 1)` to **yesterday**. If the table has no rows
# MAGIC    for the location, go back **10 years**.
# MAGIC 3. Compute `is_extreme_day` per row:
# MAGIC    `precip > 50mm OR wind > 80km/h OR temp_max > 38°C OR temp_min < -15°C`.
# MAGIC 4. **Append** new rows to `realestate.bronze.weather_history` (no upsert —
# MAGIC    historical daily values don't change once the day is closed).
# MAGIC
# MAGIC **Output:** `realestate.bronze.weather_history`.
# MAGIC
# MAGIC **Schedule:** daily.

# COMMAND ----------

# DBTITLE 1,Load shared helpers
# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Use catalog
spark.sql("USE CATALOG realestate")

# COMMAND ----------

# DBTITLE 1,Imports
import time
import traceback
from datetime import date, timedelta

import requests

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    DateType, BooleanType,
)

# COMMAND ----------

# DBTITLE 1,Config
PIPELINE_NAME = "03_ingest_weather"

WEATHER_TABLE = "realestate.bronze.weather_history"
LISTINGS_US_TABLE = "realestate.bronze.listings_us"

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Daily variables to request. The Open-Meteo response is a parallel-array JSON
# object: `daily.time[i]` aligns with `daily.<var>[i]` for every variable.
DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
    "weathercode",
]

# Years of history to backfill on the very first run for a location.
HISTORY_YEARS = 10

# Extreme-day thresholds (used to populate `is_extreme_day`).
EXTREME_PRECIPITATION_MM = 50.0
EXTREME_WIND_KMH = 80.0
EXTREME_TEMP_MAX_C = 38.0
EXTREME_TEMP_MIN_C = -15.0

# HTTP behaviour. Open-Meteo's free tier is generous but not unlimited; a short
# delay between locations keeps us well within their fair-use guidance.
REQUEST_TIMEOUT_SECONDS = 60
INTER_REQUEST_SLEEP_SECONDS = 0.2

# Hard-coded Colombia cities (always fetched).
CO_CITIES = [
    {"city": "Bogota",   "country_code": "CO", "lat": 4.7110, "lon": -74.0721},
    {"city": "Medellin", "country_code": "CO", "lat": 6.2442, "lon": -75.5812},
]

# Fallback US cities used when bronze.listings_us is empty on the first run.
# Approximate city-centroid lat/lon.
US_FALLBACK_CITIES = [
    {"city": "Austin",  "country_code": "US", "lat": 30.2672, "lon":  -97.7431},
    {"city": "Miami",   "country_code": "US", "lat": 25.7617, "lon":  -80.1918},
    {"city": "Atlanta", "country_code": "US", "lat": 33.7490, "lon":  -84.3880},
    {"city": "Houston", "country_code": "US", "lat": 29.7604, "lon":  -95.3698},
]

# COMMAND ----------

# DBTITLE 1,Build city list (CO + US listings or fallback)
def load_us_cities_from_listings() -> list[dict]:
    """
    Pull distinct (city, lat, lon) from bronze.listings_us, computing a simple
    centroid per city when listings carry slightly different lat/lon values.

    Returns an empty list if the table is empty or doesn't exist yet.
    """
    try:
        df = spark.sql(
            f"""
            SELECT
                city,
                AVG(lat) AS lat,
                AVG(lon) AS lon
            FROM {LISTINGS_US_TABLE}
            WHERE city IS NOT NULL
              AND lat IS NOT NULL
              AND lon IS NOT NULL
            GROUP BY city
            """
        )
    except Exception:
        # Table missing entirely — treat as empty so the fallback kicks in.
        return []

    rows = df.collect()
    return [
        {
            "city": r["city"],
            "country_code": "US",
            "lat": float(r["lat"]),
            "lon": float(r["lon"]),
        }
        for r in rows
    ]


us_cities = load_us_cities_from_listings()
if not us_cities:
    print(
        f"  {LISTINGS_US_TABLE} is empty — using hard-coded US fallback "
        f"({len(US_FALLBACK_CITIES)} cities)."
    )
    us_cities = US_FALLBACK_CITIES

ALL_CITIES = CO_CITIES + us_cities

print(f"Locations to fetch: {len(ALL_CITIES)}")
for c in ALL_CITIES:
    print(f"  {c['city']}_{c['country_code']:<2}  lat={c['lat']:.4f}  lon={c['lon']:.4f}")

# COMMAND ----------

# DBTITLE 1,Date-range planner (incremental per location)
def determine_start_date(location_key: str, today: date) -> date:
    """
    Decide the first date we still need to fetch for `location_key`:
    `max(existing_date) + 1`, or `today - 10 years` if we have no rows yet.
    """
    df = spark.sql(
        f"""
        SELECT MAX(date) AS max_date
        FROM {WEATHER_TABLE}
        WHERE location_key = '{location_key}'
        """
    )
    row = df.first()
    max_date = row["max_date"] if row is not None else None

    if max_date is None:
        # First run for this location → 10-year backfill.
        return today.replace(year=today.year - HISTORY_YEARS)
    return max_date + timedelta(days=1)

# COMMAND ----------

# DBTITLE 1,Open-Meteo fetch + parse
def is_extreme(
    temp_max_c: float | None,
    temp_min_c: float | None,
    precipitation_mm: float | None,
    wind_speed_max_kmh: float | None,
) -> bool:
    """Apply the extreme-day rules. NULL on a field means 'not extreme via that field'."""
    if precipitation_mm is not None and precipitation_mm > EXTREME_PRECIPITATION_MM:
        return True
    if wind_speed_max_kmh is not None and wind_speed_max_kmh > EXTREME_WIND_KMH:
        return True
    if temp_max_c is not None and temp_max_c > EXTREME_TEMP_MAX_C:
        return True
    if temp_min_c is not None and temp_min_c < EXTREME_TEMP_MIN_C:
        return True
    return False


def fetch_weather_history(
    city: dict,
    start: date,
    end: date,
) -> list[Row]:
    """
    Call Open-Meteo Archive for `city` over [start, end] inclusive and return
    one Spark Row per day. Returns an empty list when the response carries no
    daily data (e.g. an empty range or an API error after retries).
    """
    params = {
        "latitude":   f"{city['lat']:.4f}",
        "longitude":  f"{city['lon']:.4f}",
        "start_date": start.isoformat(),
        "end_date":   end.isoformat(),
        "daily":      ",".join(DAILY_VARS),
        "timezone":   "auto",
    }
    headers = {
        "User-Agent": "realestate-agent-databricks/1.0",
        "Accept": "application/json",
    }

    response = requests.get(
        OPEN_METEO_ARCHIVE_URL,
        params=params,
        headers=headers,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    body = response.json()

    daily = body.get("daily") or {}
    dates = daily.get("time") or []
    if not dates:
        return []

    temp_max  = daily.get("temperature_2m_max")  or [None] * len(dates)
    temp_min  = daily.get("temperature_2m_min")  or [None] * len(dates)
    precip    = daily.get("precipitation_sum")   or [None] * len(dates)
    wind_max  = daily.get("windspeed_10m_max")   or [None] * len(dates)
    wcodes    = daily.get("weathercode")         or [None] * len(dates)

    location_key = f"{city['city']}_{city['country_code']}"
    rows: list[Row] = []
    for i, day_str in enumerate(dates):
        try:
            day = date.fromisoformat(day_str)
        except (TypeError, ValueError):
            continue

        t_max = float(temp_max[i]) if temp_max[i] is not None else None
        t_min = float(temp_min[i]) if temp_min[i] is not None else None
        # Daily mean isn't returned directly; derive from max/min when possible.
        t_mean = (
            (t_max + t_min) / 2.0
            if (t_max is not None and t_min is not None)
            else None
        )
        p_mm   = float(precip[i])   if precip[i]   is not None else None
        w_kmh  = float(wind_max[i]) if wind_max[i] is not None else None
        wcode  = int(wcodes[i])     if wcodes[i]   is not None else None

        rows.append(Row(
            location_key=location_key,
            city=city["city"],
            country_code=city["country_code"],
            lat=float(city["lat"]),
            lon=float(city["lon"]),
            date=day,
            temp_max_c=t_max,
            temp_min_c=t_min,
            temp_mean_c=t_mean,
            precipitation_mm=p_mm,
            wind_speed_max_kmh=w_kmh,
            weather_code=wcode,
            is_extreme_day=is_extreme(t_max, t_min, p_mm, w_kmh),
        ))
    return rows

# COMMAND ----------

# DBTITLE 1,Run fetch for every location
WEATHER_SCHEMA = StructType([
    StructField("location_key",       StringType(),  True),
    StructField("city",               StringType(),  True),
    StructField("country_code",       StringType(),  True),
    StructField("lat",                DoubleType(),  True),
    StructField("lon",                DoubleType(),  True),
    StructField("date",               DateType(),    True),
    StructField("temp_max_c",         DoubleType(),  True),
    StructField("temp_min_c",         DoubleType(),  True),
    StructField("temp_mean_c",        DoubleType(),  True),
    StructField("precipitation_mm",   DoubleType(),  True),
    StructField("wind_speed_max_kmh", DoubleType(),  True),
    StructField("weather_code",       IntegerType(), True),
    StructField("is_extreme_day",     BooleanType(), True),
])

today = date.today()
yesterday = today - timedelta(days=1)

results: list[dict] = []

for city in ALL_CITIES:
    location_key = f"{city['city']}_{city['country_code']}"
    print(f"\n{location_key}  lat={city['lat']:.4f}  lon={city['lon']:.4f}")

    try:
        start = determine_start_date(location_key, today)
    except Exception:
        # Most likely the bronze table doesn't exist yet — surface a clear hint.
        error_message = traceback.format_exc()
        print(f"  Could not read {WEATHER_TABLE}: {error_message.splitlines()[-1]}")
        results.append({
            "location_key": location_key,
            "start_date": None,
            "end_date": None,
            "rows_written": 0,
            "status": "FAILED",
            "error": error_message.splitlines()[-1],
        })
        continue

    if start > yesterday:
        print(f"  up to date — last loaded date is {start - timedelta(days=1)}, skipping")
        results.append({
            "location_key": location_key,
            "start_date": start.isoformat(),
            "end_date": yesterday.isoformat(),
            "rows_written": 0,
            "status": "SKIPPED",
            "error": "",
        })
        continue

    print(f"  fetching {start} → {yesterday}  ({(yesterday - start).days + 1} day(s))")

    try:
        rows = fetch_weather_history(city, start, yesterday)
        if rows:
            df = spark.createDataFrame(rows, schema=WEATHER_SCHEMA)
            df.write.format("delta").mode("append").saveAsTable(WEATHER_TABLE)
        rows_written = len(rows)
        print(f"  appended {rows_written} row(s)")
        results.append({
            "location_key": location_key,
            "start_date": start.isoformat(),
            "end_date": yesterday.isoformat(),
            "rows_written": rows_written,
            "status": "SUCCESS",
            "error": "",
        })
    except Exception:
        error_message = traceback.format_exc()
        print(f"  FAILED: {error_message.splitlines()[-1]}")
        results.append({
            "location_key": location_key,
            "start_date": start.isoformat(),
            "end_date": yesterday.isoformat(),
            "rows_written": 0,
            "status": "FAILED",
            "error": error_message.splitlines()[-1],
        })

    # Be polite between locations even though Open-Meteo is generous.
    time.sleep(INTER_REQUEST_SLEEP_SECONDS)

# COMMAND ----------

# DBTITLE 1,Run summary
results_schema = StructType([
    StructField("location_key",  StringType(),  True),
    StructField("start_date",    StringType(),  True),
    StructField("end_date",      StringType(),  True),
    StructField("rows_written",  IntegerType(), True),
    StructField("status",        StringType(),  True),
    StructField("error",         StringType(),  True),
])

results_df = spark.createDataFrame(results, schema=results_schema)
display(results_df.orderBy("location_key"))

# COMMAND ----------

# DBTITLE 1,Sanity counts
total_locations = len(results)
successes = sum(1 for r in results if r["status"] == "SUCCESS")
skipped   = sum(1 for r in results if r["status"] == "SKIPPED")
failures  = sum(1 for r in results if r["status"] == "FAILED")
rows_written_total = sum(r["rows_written"] for r in results)

print(f"Locations attempted: {total_locations}")
print(f"  successes:          {successes}")
print(f"  already up to date: {skipped}")
print(f"  failures:           {failures}")
print(f"Rows appended:        {rows_written_total}")

if failures > 0 and successes == 0 and skipped == 0:
    raise RuntimeError(
        "All weather locations failed — check error column in the run summary."
    )

# COMMAND ----------

# DBTITLE 1,Bronze counts by location
display(
    spark.sql(f"""
        SELECT
            location_key,
            country_code,
            city,
            COUNT(*)                                    AS row_count,
            MIN(date)                                   AS first_date,
            MAX(date)                                   AS last_date,
            SUM(CASE WHEN is_extreme_day THEN 1 ELSE 0 END) AS extreme_day_count
        FROM {WEATHER_TABLE}
        GROUP BY location_key, country_code, city
        ORDER BY country_code, city
    """)
)

# COMMAND ----------

# DBTITLE 1,Most recent rows just appended
display(
    spark.sql(f"""
        SELECT
            location_key,
            date,
            temp_max_c,
            temp_min_c,
            temp_mean_c,
            precipitation_mm,
            wind_speed_max_kmh,
            weather_code,
            is_extreme_day
        FROM {WEATHER_TABLE}
        WHERE date >= current_date() - INTERVAL 14 DAYS
        ORDER BY location_key, date DESC
        LIMIT 50
    """)
)
