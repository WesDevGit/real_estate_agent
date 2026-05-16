# Databricks notebook source
# DBTITLE 1,Ingest US Crime (FBI UCR + optional SpotCrime)
# MAGIC %md
# MAGIC # 04 — Ingest US Crime
# MAGIC
# MAGIC Pulls US crime data into `realestate.bronze.crime_us`.
# MAGIC
# MAGIC ## Sources
# MAGIC
# MAGIC ### FBI UCR (primary, required)
# MAGIC * Base: `https://api.usa.gov/crime/fbi/cde/`
# MAGIC * Auth: `api_key` query parameter from `get_secret("FBI_API_KEY")`
# MAGIC * For every state present in `realestate.bronze.listings_us` (falling back to
# MAGIC   `TX, FL, GA, CA` when the listings table is empty), we:
# MAGIC   1. List agencies for that state via `/agency/byStateAbbr/{state}`.
# MAGIC   2. For each agency, fetch annual offense counts for the last 5 years across
# MAGIC      the major Part I offense codes:
# MAGIC      `homicide`, `rape`, `robbery`, `aggravated-assault`, `burglary`,
# MAGIC      `larceny`, `motor-vehicle-theft`.
# MAGIC   3. Emit one annual aggregate row per (agency, year, offense) with
# MAGIC      `incident_id = f"fbi_{ori}_{year}_{offense_code}"` and
# MAGIC      `source = "fbi_ucr"`.
# MAGIC * `crime_category` is normalized:
# MAGIC   * violent = `homicide`, `rape`, `robbery`, `aggravated-assault`
# MAGIC   * property = `burglary`, `larceny`, `motor-vehicle-theft`
# MAGIC   * other = anything else
# MAGIC
# MAGIC ### SpotCrime (optional)
# MAGIC * Skipped entirely when `SPOTCRIME_API_KEY` is missing from the `realestate`
# MAGIC   secret scope — the notebook logs a warning and continues, it does **not**
# MAGIC   fail.
# MAGIC * When present, for each distinct zip centroid in `bronze.listings_us` we
# MAGIC   query `http://api.spotcrime.com/crimes.json` (radius = 2 miles) and write
# MAGIC   incident-level rows with `source = "spotcrime"`.
# MAGIC
# MAGIC ## Idempotency
# MAGIC
# MAGIC `bronze.crime_us` has no primary key constraint in Delta, so the notebook
# MAGIC stages new rows in a temp view and `MERGE`s them on `incident_id`. Re-runs
# MAGIC over the same window are safe and produce no duplicates.

# COMMAND ----------

# DBTITLE 1,Load shared helpers
# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports specific to this notebook
import time
import traceback
from datetime import date, datetime, timezone

import requests

# COMMAND ----------

# DBTITLE 1,Use realestate catalog
spark.sql("USE CATALOG realestate")

# COMMAND ----------

# DBTITLE 1,Config
PIPELINE_NAME = "04_ingest_crime_us"

# FBI Crime Data Explorer (CDE) base. All endpoints accept api_key as a
# query parameter.
FBI_BASE_URL = "https://api.usa.gov/crime/fbi/cde"

# Part I offenses we fetch annual counts for. Codes are the FBI CDE path
# segments (lowercase, hyphenated).
FBI_OFFENSE_CODES = [
    "homicide",
    "rape",
    "robbery",
    "aggravated-assault",
    "burglary",
    "larceny",
    "motor-vehicle-theft",
]

VIOLENT_OFFENSE_CODES = {"homicide", "rape", "robbery", "aggravated-assault"}
PROPERTY_OFFENSE_CODES = {"burglary", "larceny", "motor-vehicle-theft"}

# How many years back to pull annual aggregates for, ending with last full
# calendar year. FBI's published year is typically the prior year.
FBI_YEARS_BACK = 5

# Fallback state list if bronze.listings_us is empty (first run, before any
# listings ingest has populated it).
FALLBACK_STATES = ["TX", "FL", "GA", "CA"]

# SpotCrime endpoint + radius in miles. Optional path.
SPOTCRIME_URL = "http://api.spotcrime.com/crimes.json"
SPOTCRIME_RADIUS_MILES = 2

# HTTP defaults. FBI's CDE occasionally throttles; we keep timeouts modest
# and retry once on 429/5xx with a short backoff.
HTTP_TIMEOUT_SECONDS = 30
HTTP_USER_AGENT = "realestate-agent-databricks/1.0"
HTTP_RETRY_BACKOFF_SECONDS = 2.0

TARGET_TABLE = "realestate.bronze.crime_us"

# COMMAND ----------

# DBTITLE 1,Load FBI API key (required)
fbi_api_key = None
fbi_skip_reason = None
try:
    fbi_api_key = get_secret("FBI_API_KEY")
    if not fbi_api_key:
        fbi_skip_reason = "FBI_API_KEY secret is empty"
        fbi_api_key = None
except Exception as exc:
    fbi_skip_reason = f"FBI_API_KEY unavailable: {type(exc).__name__}: {exc}"
    fbi_api_key = None

if fbi_api_key:
    print(f"FBI API key loaded (len={len(fbi_api_key)}).")
else:
    # Required — failing fast here surfaces a missing secret immediately
    # rather than after we've already started a partial run.
    raise RuntimeError(
        f"FBI UCR ingest requires FBI_API_KEY in secret scope 'realestate'. "
        f"Reason: {fbi_skip_reason}"
    )

# COMMAND ----------

# DBTITLE 1,Resolve target states from listings (fallback if empty)
def resolve_target_states() -> list[str]:
    """
    Return the distinct, non-null, two-letter state codes that appear in
    `bronze.listings_us`. Falls back to FALLBACK_STATES on first run when
    the listings table is empty.
    """
    try:
        df = spark.sql("""
            SELECT DISTINCT UPPER(TRIM(state)) AS state
            FROM realestate.bronze.listings_us
            WHERE state IS NOT NULL AND LENGTH(TRIM(state)) = 2
        """)
        states = [row["state"] for row in df.collect() if row["state"]]
    except Exception as exc:
        print(
            f"  [warn] Failed to query bronze.listings_us for states: "
            f"{type(exc).__name__}: {exc}. Using fallback."
        )
        states = []

    if not states:
        print(f"  No states in bronze.listings_us — using fallback {FALLBACK_STATES}.")
        return list(FALLBACK_STATES)

    print(f"  Resolved {len(states)} state(s) from listings: {sorted(states)}")
    return sorted(states)


TARGET_STATES = resolve_target_states()

# COMMAND ----------

# DBTITLE 1,HTTP helper with one retry on transient failures
def http_get_json(url: str, params: dict) -> tuple[int, dict | list | None]:
    """
    GET `url` with `params` and return (status_code, parsed_json).

    On HTTP 429 or 5xx the call is retried once after HTTP_RETRY_BACKOFF_SECONDS.
    Returns (-1, None) on a connection-level error after retry.
    """
    headers = {"User-Agent": HTTP_USER_AGENT, "Accept": "application/json"}

    for attempt in (1, 2):
        try:
            response = requests.get(
                url, params=params, headers=headers,
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            status = response.status_code
            if status == 429 or 500 <= status < 600:
                if attempt == 1:
                    time.sleep(HTTP_RETRY_BACKOFF_SECONDS)
                    continue
            try:
                return status, response.json() if response.content else None
            except Exception:
                return status, None
        except Exception:
            if attempt == 1:
                time.sleep(HTTP_RETRY_BACKOFF_SECONDS)
                continue
            return -1, None

    return -1, None


def normalize_crime_category(offense_code: str) -> str:
    """Map an FBI offense code to violent / property / other."""
    code = (offense_code or "").lower()
    if code in VIOLENT_OFFENSE_CODES:
        return "violent"
    if code in PROPERTY_OFFENSE_CODES:
        return "property"
    return "other"

# COMMAND ----------

# DBTITLE 1,FBI: list agencies for a state
def fetch_agencies_for_state(state_abbr: str) -> list[dict]:
    """
    Hit `/agency/byStateAbbr/{state}` and return a list of agency dicts.

    The CDE endpoint can return either a list of agency objects or a dict
    keyed by ORI. We normalize to a list of dicts so the caller doesn't
    have to branch.
    """
    url = f"{FBI_BASE_URL}/agency/byStateAbbr/{state_abbr}"
    status, payload = http_get_json(url, {"api_key": fbi_api_key})

    if status != 200 or payload is None:
        print(f"  [warn] Agency list for {state_abbr} returned HTTP {status}.")
        return []

    # The API has shipped both shapes in different windows. Coerce.
    if isinstance(payload, list):
        agencies = payload
    elif isinstance(payload, dict):
        # Either { "<ORI>": {...}, ... } or { "results": [...] }
        if "results" in payload and isinstance(payload["results"], list):
            agencies = payload["results"]
        else:
            agencies = [
                {**v, "ori": v.get("ori") or k}
                for k, v in payload.items()
                if isinstance(v, dict)
            ]
    else:
        agencies = []

    # Keep only entries with an ORI — that's the join key we need.
    cleaned = [a for a in agencies if a.get("ori")]
    return cleaned

# COMMAND ----------

# DBTITLE 1,FBI: annual offense counts for an agency
def fetch_agency_offense_year(
    ori: str,
    offense_code: str,
    year: int,
) -> int | None:
    """
    Return the annual offense count for (ORI, offense_code, year).

    Uses `/summarized/agency/{ori}/{offense_code}` with the year range pinned
    to a single year. Returns None when the endpoint produces no data or
    errors — callers skip Nones so we never insert a half-formed row.
    """
    url = f"{FBI_BASE_URL}/summarized/agency/{ori}/{offense_code}"
    params = {
        "from": f"{year}-01",
        "to":   f"{year}-12",
        "api_key": fbi_api_key,
    }
    status, payload = http_get_json(url, params)
    if status != 200 or payload is None:
        return None

    # CDE returns { "offenses": { "actuals": { "<offense>": { "<year>": N }}}}
    # in newer responses, or a flat list of monthly counts in older ones.
    # Handle both.
    try:
        offenses = payload.get("offenses") if isinstance(payload, dict) else None
        if isinstance(offenses, dict):
            actuals = offenses.get("actuals") or {}
            if isinstance(actuals, dict):
                # actuals may be keyed by display name or by code; sum all
                # numeric values under the year.
                total = 0
                found = False
                for _, year_map in actuals.items():
                    if isinstance(year_map, dict) and str(year) in year_map:
                        val = year_map[str(year)]
                        if isinstance(val, (int, float)):
                            total += int(val)
                            found = True
                if found:
                    return total

        # Fallback: list of monthly dicts with a "count" or "value" field.
        if isinstance(payload, list):
            total = 0
            found = False
            for entry in payload:
                if not isinstance(entry, dict):
                    continue
                val = entry.get("count") or entry.get("value") or entry.get("actual")
                if isinstance(val, (int, float)):
                    total += int(val)
                    found = True
            if found:
                return total
    except Exception:
        return None

    return None

# COMMAND ----------

# DBTITLE 1,FBI: build annual rows for all states
def build_fbi_rows() -> list[dict]:
    """
    Walk states -> agencies -> offenses -> years and produce one row per
    (agency, year, offense_code). Returns a list of dicts shaped to match
    bronze.crime_us.
    """
    # Years to fetch: e.g. for FBI_YEARS_BACK=5 and current year 2026, this
    # is [2020..2024]. We end at last full year because FBI rarely publishes
    # the current year mid-stream.
    current_year = datetime.now(timezone.utc).year
    years = list(range(current_year - FBI_YEARS_BACK, current_year))

    rows: list[dict] = []
    ingested_at = datetime.now(timezone.utc)

    for state in TARGET_STATES:
        print(f"\nFBI UCR: state={state}")
        agencies = fetch_agencies_for_state(state)
        print(f"  agencies: {len(agencies)}")
        if not agencies:
            continue

        for agency in agencies:
            ori = agency.get("ori")
            if not ori:
                continue
            agency_city = agency.get("agency_name_city") or agency.get("city") or None
            # Agency-level lat/lon when CDE provides it.
            try:
                lat = float(agency["latitude"]) if agency.get("latitude") else None
            except (TypeError, ValueError):
                lat = None
            try:
                lon = float(agency["longitude"]) if agency.get("longitude") else None
            except (TypeError, ValueError):
                lon = None

            for offense_code in FBI_OFFENSE_CODES:
                for year in years:
                    count = fetch_agency_offense_year(ori, offense_code, year)
                    if count is None or count <= 0:
                        # Skip null/zero — keeps the bronze table compact and
                        # avoids fabricating "0 incidents" rows for agencies
                        # that simply did not report.
                        continue

                    rows.append({
                        "incident_id": f"fbi_{ori}_{year}_{offense_code}",
                        "source": "fbi_ucr",
                        "city": agency_city,
                        "state": state,
                        "zip": None,
                        "lat": lat,
                        "lon": lon,
                        "crime_type": offense_code,
                        "crime_category": normalize_crime_category(offense_code),
                        "incident_date": date(year, 12, 31),
                        "year": year,
                        "ingested_at": ingested_at,
                        # Store the count as a synthetic "count" field by
                        # encoding it via duplicate rows? No — schema has no
                        # count column for crime_us. The annual aggregate is
                        # one row per (ori, year, offense) and the count is
                        # implied by the row's existence. Downstream silver
                        # uses fbi_ucr rows as a presence/score signal, not
                        # an exact tally. If exact counts are needed they
                        # can be back-derived from the API.
                    })

    print(f"\nFBI UCR rows assembled: {len(rows):,}")
    return rows

# COMMAND ----------

# DBTITLE 1,SpotCrime: optional incident-level fetch
def fetch_spotcrime_rows(spotcrime_key: str) -> list[dict]:
    """
    For each distinct zip centroid in bronze.listings_us, hit SpotCrime
    and emit one row per incident. Best-effort: per-centroid errors are
    logged and skipped, never raised.
    """
    try:
        centroids = spark.sql("""
            SELECT
                UPPER(TRIM(state)) AS state,
                city,
                zip,
                AVG(lat) AS lat,
                AVG(lon) AS lon
            FROM realestate.bronze.listings_us
            WHERE lat IS NOT NULL AND lon IS NOT NULL
            GROUP BY UPPER(TRIM(state)), city, zip
        """).collect()
    except Exception as exc:
        print(f"  [warn] Could not query listings_us for SpotCrime centroids: {exc}")
        return []

    if not centroids:
        print("  SpotCrime: no zip centroids in bronze.listings_us — nothing to fetch.")
        return []

    print(f"  SpotCrime centroids to query: {len(centroids)}")
    rows: list[dict] = []
    ingested_at = datetime.now(timezone.utc)

    for c in centroids:
        params = {
            "lat": c["lat"],
            "lon": c["lon"],
            "radius": SPOTCRIME_RADIUS_MILES,
            "key": spotcrime_key,
        }
        status, payload = http_get_json(SPOTCRIME_URL, params)
        if status != 200 or not isinstance(payload, dict):
            print(f"  [warn] SpotCrime {c['city']},{c['state']} {c['zip']}: HTTP {status}")
            continue

        incidents = payload.get("crimes") or []
        for inc in incidents:
            if not isinstance(inc, dict):
                continue
            inc_id = inc.get("cdid") or inc.get("id")
            if not inc_id:
                continue
            crime_type = (inc.get("type") or "").strip().lower()
            # SpotCrime's `type` is already a coarse label
            # (e.g. "assault", "burglary"). Normalize to our 3 buckets.
            if crime_type in {"assault", "robbery", "shooting", "homicide"}:
                category = "violent"
            elif crime_type in {"burglary", "theft", "vandalism", "arson"}:
                category = "property"
            else:
                category = "other"

            # Parse date — SpotCrime returns ISO-like timestamps.
            inc_date = None
            inc_year = None
            raw_date = inc.get("date")
            if isinstance(raw_date, str) and raw_date:
                try:
                    parsed = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                    inc_date = parsed.date()
                    inc_year = parsed.year
                except ValueError:
                    inc_date = None

            try:
                inc_lat = float(inc["lat"]) if inc.get("lat") else None
            except (TypeError, ValueError):
                inc_lat = None
            try:
                inc_lon = float(inc["lon"]) if inc.get("lon") else None
            except (TypeError, ValueError):
                inc_lon = None

            rows.append({
                "incident_id": f"spotcrime_{inc_id}",
                "source": "spotcrime",
                "city": c["city"],
                "state": c["state"],
                "zip": c["zip"],
                "lat": inc_lat,
                "lon": inc_lon,
                "crime_type": crime_type or None,
                "crime_category": category,
                "incident_date": inc_date,
                "year": inc_year,
                "ingested_at": ingested_at,
            })

    print(f"  SpotCrime rows assembled: {len(rows):,}")
    return rows

# COMMAND ----------

# DBTITLE 1,Merge helper — idempotent upsert on incident_id
def merge_rows_into_bronze(rows: list[dict]) -> int:
    """
    Stage `rows` to a temp view and MERGE INTO bronze.crime_us on
    incident_id. Returns the count of rows in the staging view (i.e. the
    upper bound on new + updated rows).
    """
    if not rows:
        return 0

    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        DateType, IntegerType, TimestampType,
    )

    schema = StructType([
        StructField("incident_id",    StringType(),    False),
        StructField("source",         StringType(),    True),
        StructField("city",           StringType(),    True),
        StructField("state",          StringType(),    True),
        StructField("zip",            StringType(),    True),
        StructField("lat",            DoubleType(),    True),
        StructField("lon",            DoubleType(),    True),
        StructField("crime_type",     StringType(),    True),
        StructField("crime_category", StringType(),    True),
        StructField("incident_date",  DateType(),      True),
        StructField("year",           IntegerType(),   True),
        StructField("ingested_at",    TimestampType(), True),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    # Deduplicate within this batch so MERGE doesn't error on multiple
    # source rows matching the same target row.
    df = df.dropDuplicates(["incident_id"])
    staged_count = df.count()
    df.createOrReplaceTempView("_crime_us_staging")

    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING _crime_us_staging AS s
          ON t.incident_id = s.incident_id
        WHEN MATCHED THEN UPDATE SET
            source         = s.source,
            city           = s.city,
            state          = s.state,
            zip            = s.zip,
            lat            = s.lat,
            lon            = s.lon,
            crime_type     = s.crime_type,
            crime_category = s.crime_category,
            incident_date  = s.incident_date,
            year           = s.year,
            ingested_at    = s.ingested_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    return staged_count

# COMMAND ----------

# DBTITLE 1,Run: FBI UCR (required) then SpotCrime (optional)
fbi_rows_written = 0
spotcrime_rows_written = 0
fbi_status = "PENDING"
spotcrime_status = "PENDING"
fbi_error = ""
spotcrime_error = ""

# --- FBI UCR --------------------------------------------------------------
try:
    fbi_rows = build_fbi_rows()
    fbi_rows_written = merge_rows_into_bronze(fbi_rows)
    fbi_status = "SUCCESS"
    print(f"\nFBI UCR: merged {fbi_rows_written:,} rows into {TARGET_TABLE}")
except Exception:
    fbi_error = traceback.format_exc()
    fbi_status = "FAILED"
    print(f"\nFBI UCR FAILED:\n{fbi_error}")

# --- SpotCrime (optional) ------------------------------------------------
spotcrime_key = None
try:
    spotcrime_key = get_secret("SPOTCRIME_API_KEY")
    if not spotcrime_key:
        spotcrime_key = None
        spotcrime_status = "SKIPPED"
        spotcrime_error = "SPOTCRIME_API_KEY secret is empty"
        print(f"\nSpotCrime: SKIPPED — {spotcrime_error}")
except Exception as exc:
    # The whole point: do NOT fail the notebook if the optional secret
    # is absent. Log a clear warning and move on.
    spotcrime_key = None
    spotcrime_status = "SKIPPED"
    spotcrime_error = f"SPOTCRIME_API_KEY unavailable: {type(exc).__name__}: {exc}"
    print(f"\nSpotCrime: SKIPPED — {spotcrime_error}")

if spotcrime_key:
    try:
        print("\nSpotCrime: key present, fetching incidents...")
        spotcrime_rows = fetch_spotcrime_rows(spotcrime_key)
        spotcrime_rows_written = merge_rows_into_bronze(spotcrime_rows)
        spotcrime_status = "SUCCESS"
        print(f"SpotCrime: merged {spotcrime_rows_written:,} rows into {TARGET_TABLE}")
    except Exception:
        spotcrime_error = traceback.format_exc()
        spotcrime_status = "FAILED"
        # SpotCrime failures do not fail the notebook — the agent only
        # requires FBI data for the safety tools to work.
        print(f"SpotCrime: non-fatal FAILURE:\n{spotcrime_error}")

# COMMAND ----------

# DBTITLE 1,Run summary
from pyspark.sql.types import StructType, StructField, StringType, LongType

summary_schema = StructType([
    StructField("source",          StringType(), True),
    StructField("status",          StringType(), True),
    StructField("rows_written",    LongType(),   True),
    StructField("error_preview",   StringType(), True),
])

summary_rows = [
    {
        "source": "fbi_ucr",
        "status": fbi_status,
        "rows_written": int(fbi_rows_written),
        "error_preview": (fbi_error.splitlines()[-1] if fbi_error else ""),
    },
    {
        "source": "spotcrime",
        "status": spotcrime_status,
        "rows_written": int(spotcrime_rows_written),
        "error_preview": (spotcrime_error.splitlines()[-1] if spotcrime_error else ""),
    },
]

summary_df = spark.createDataFrame(summary_rows, schema=summary_schema)
display(summary_df)

# Fail the notebook only if FBI (the required source) failed. SpotCrime
# being skipped or failing is expected and acceptable.
if fbi_status == "FAILED":
    raise RuntimeError(
        "FBI UCR ingest failed. SpotCrime status was "
        f"'{spotcrime_status}'. See logs above."
    )

# COMMAND ----------

# DBTITLE 1,Sanity counts on bronze.crime_us
display(
    spark.sql(f"""
        SELECT
            source,
            crime_category,
            COUNT(*) AS row_count,
            MIN(year) AS min_year,
            MAX(year) AS max_year,
            COUNT(DISTINCT state) AS distinct_states,
            MAX(ingested_at) AS latest_ingest
        FROM {TARGET_TABLE}
        GROUP BY source, crime_category
        ORDER BY source, crime_category
    """)
)

# COMMAND ----------

# DBTITLE 1,Sample of newest rows
display(
    spark.sql(f"""
        SELECT
            incident_id,
            source,
            state,
            city,
            crime_type,
            crime_category,
            year,
            incident_date,
            ingested_at
        FROM {TARGET_TABLE}
        ORDER BY ingested_at DESC
        LIMIT 25
    """)
)
