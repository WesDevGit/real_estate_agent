# Databricks notebook source
# DBTITLE 1,Real Estate Agent - Ingest US Schools (NCES via Urban Institute)
# MAGIC %md
# MAGIC # 06 — Ingest US Schools
# MAGIC
# MAGIC Fetches US public school directory data and grade-8 assessment results from the
# MAGIC **Urban Institute Education Data API** (which redistributes NCES / EDFacts data).
# MAGIC
# MAGIC The API is **open and requires no API key**.
# MAGIC
# MAGIC ## Endpoints
# MAGIC * Directory: `GET https://educationdata.urban.org/api/v1/schools/ccd/directory/{year}/?fips={state_fips}`
# MAGIC * Assessments: `GET https://educationdata.urban.org/api/v1/schools/edfacts/assessments/{year}/grade-8/?fips={state_fips}`
# MAGIC
# MAGIC ## Steps
# MAGIC 1. Resolve the list of US states from `bronze.listings_us` (fallback list if empty).
# MAGIC 2. Pick the most recent year for which the directory endpoint returns data
# MAGIC    (try 2022, fall back to 2021 / 2020).
# MAGIC 3. For each state, page through the directory and assessment endpoints by
# MAGIC    following the `next` URL until exhausted.
# MAGIC 4. Pivot assessment rows on subject (math + reading) and join to the directory
# MAGIC    on NCES school ID (`ncessch`).
# MAGIC 5. Merge upsert into `realestate.bronze.schools_us` using `school_id`.
# MAGIC
# MAGIC ## Target
# MAGIC `realestate.bronze.schools_us` — created by `00_setup_schema.py`.
# MAGIC
# MAGIC **Schedule:** Annual.

# COMMAND ----------

# DBTITLE 1,Load shared helpers
# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and catalog
from datetime import datetime
from typing import Iterator

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# `99_helpers` already runs `USE CATALOG realestate`, but we re-run it here so
# this notebook is safe to execute standalone (e.g. via a job task).
spark.sql("USE CATALOG realestate")

# COMMAND ----------

# DBTITLE 1,Configuration
# Urban Institute Education Data API — open, no key needed.
BASE_URL = "https://educationdata.urban.org/api/v1"
DIRECTORY_PATH = "schools/ccd/directory"
ASSESSMENTS_PATH = "schools/edfacts/assessments"

# Try years in descending order; the first one that returns 200 with rows wins.
# EDFacts/CCD typically lag the academic year by 2–3 years.
CANDIDATE_YEARS = [2022, 2021, 2020]

# Fallback list when `bronze.listings_us` is empty (early bootstrap runs).
FALLBACK_STATES = ["TX", "FL", "GA", "CA"]

# Two-letter state -> two-digit FIPS code. The Urban Institute API expects the
# FIPS *integer*; we cast at request time.
STATE_TO_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56",
}

# HTTP request settings.
REQUEST_TIMEOUT_SECONDS = 60
MAX_PAGES_PER_STATE = 200  # hard ceiling so a runaway `next` chain can't loop forever

print(f"Base URL          : {BASE_URL}")
print(f"Candidate years   : {CANDIDATE_YEARS}")
print(f"Fallback states   : {FALLBACK_STATES}")

# COMMAND ----------

# DBTITLE 1,Resolve target states
def resolve_states() -> list[str]:
    """
    Return the distinct two-letter state codes present in `bronze.listings_us`,
    falling back to `FALLBACK_STATES` when the listings table is empty (e.g. on
    a first-run before listings have been ingested).
    """
    try:
        rows = (
            spark.table("realestate.bronze.listings_us")
            .select("state")
            .where(F.col("state").isNotNull() & (F.length(F.col("state")) == 2))
            .distinct()
            .collect()
        )
        states = sorted({r["state"].upper() for r in rows if r["state"]})
    except Exception as exc:
        print(f"Could not read bronze.listings_us ({exc}); using fallback.")
        states = []

    if not states:
        print("No states found in bronze.listings_us; using fallback.")
        states = list(FALLBACK_STATES)

    # Keep only states we know how to translate to FIPS.
    states = [s for s in states if s in STATE_TO_FIPS]
    return states


TARGET_STATES = resolve_states()
print(f"Target states ({len(TARGET_STATES)}): {TARGET_STATES}")

# COMMAND ----------

# DBTITLE 1,Paginated fetch helper
def fetch_pages(url: str) -> Iterator[dict]:
    """
    Yield every record from a paginated Urban Institute endpoint.

    The API returns JSON shaped like:
        {"count": N, "next": "<url>" | null, "previous": ..., "results": [...]}

    We follow `next` until it is null. To guard against an infinite loop in the
    pathological case where `next` points back to itself, we cap iterations at
    `MAX_PAGES_PER_STATE`.

    Args:
        url: The initial fully-qualified URL (may include query string).

    Yields:
        Each record dict from `results`. A 404 on the *initial* URL is treated
        as "no data for this (year, state)" and yields nothing rather than
        raising; downstream callers can detect emptiness and try another year.
    """
    pages = 0
    current = url
    while current and pages < MAX_PAGES_PER_STATE:
        response = requests.get(current, timeout=REQUEST_TIMEOUT_SECONDS)
        if response.status_code == 404 and pages == 0:
            # Year/endpoint combo unavailable; signal empty.
            return
        response.raise_for_status()
        payload = response.json()
        for row in payload.get("results", []) or []:
            yield row
        current = payload.get("next")
        pages += 1
    if pages >= MAX_PAGES_PER_STATE:
        print(f"  WARN: hit MAX_PAGES_PER_STATE={MAX_PAGES_PER_STATE} for {url}")


def directory_url(year: int, state_fips: str) -> str:
    fips_int = int(state_fips)
    return f"{BASE_URL}/{DIRECTORY_PATH}/{year}/?fips={fips_int}"


def assessments_url(year: int, state_fips: str) -> str:
    fips_int = int(state_fips)
    return f"{BASE_URL}/{ASSESSMENTS_PATH}/{year}/grade-8/?fips={fips_int}"

# COMMAND ----------

# DBTITLE 1,Pick the most recent year that returns data
def pick_directory_year(probe_state_fips: str) -> int:
    """
    Probe the directory endpoint with `CANDIDATE_YEARS` (newest first) and return
    the first year that returns at least one record for `probe_state_fips`.

    Raises:
        RuntimeError: If no candidate year returns data.
    """
    for year in CANDIDATE_YEARS:
        url = directory_url(year, probe_state_fips)
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        except Exception as exc:
            print(f"  probe {year}: request error ({exc})")
            continue
        if response.status_code == 404:
            print(f"  probe {year}: 404")
            continue
        if response.status_code != 200:
            print(f"  probe {year}: HTTP {response.status_code}")
            continue
        payload = response.json()
        if payload.get("count", 0) > 0 or payload.get("results"):
            print(f"  probe {year}: OK (count={payload.get('count')})")
            return year
        print(f"  probe {year}: empty")
    raise RuntimeError(
        f"None of {CANDIDATE_YEARS} returned data from {DIRECTORY_PATH}; "
        "check the API status."
    )


probe_fips = STATE_TO_FIPS[TARGET_STATES[0]]
print(f"Probing directory endpoint with state {TARGET_STATES[0]} (fips={probe_fips})")
DIRECTORY_YEAR = pick_directory_year(probe_fips)
print(f"Using directory year: {DIRECTORY_YEAR}")

# COMMAND ----------

# DBTITLE 1,Pick a year for assessments (allowed to differ)
def pick_assessments_year(probe_state_fips: str) -> int | None:
    """
    Pick the most recent year that has assessment data. Returns `None` if no
    candidate year returns rows (we will still write directory rows with null
    proficiency columns).
    """
    for year in CANDIDATE_YEARS:
        url = assessments_url(year, probe_state_fips)
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        except Exception as exc:
            print(f"  probe {year}: request error ({exc})")
            continue
        if response.status_code == 404:
            print(f"  probe {year}: 404")
            continue
        if response.status_code != 200:
            print(f"  probe {year}: HTTP {response.status_code}")
            continue
        payload = response.json()
        if payload.get("count", 0) > 0 or payload.get("results"):
            print(f"  probe {year}: OK (count={payload.get('count')})")
            return year
        print(f"  probe {year}: empty")
    print("  No assessment year returned data; proficiency columns will be null.")
    return None


print(f"Probing assessments endpoint with state {TARGET_STATES[0]} (fips={probe_fips})")
ASSESSMENTS_YEAR = pick_assessments_year(probe_fips)
print(f"Using assessments year: {ASSESSMENTS_YEAR}")

# COMMAND ----------

# DBTITLE 1,Field mappers
# Reverse FIPS lookup for the `state` column on the output frame.
FIPS_TO_STATE = {v: k for k, v in STATE_TO_FIPS.items()}


def _to_bool(value) -> bool | None:
    """
    Map Urban Institute boolean-ish encodings to a real Python bool.

    The API uses many flavors: 1/0, "Yes"/"No", "1"/"0", True/False,
    "Not applicable", "Missing/not reported", -1 / -2 / -3 (sentinel codes).
    Anything we don't recognize maps to `None`.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
        return None  # -1/-2/-3 sentinel "missing" codes
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("1", "yes", "true", "y"):
            return True
        if v in ("0", "no", "false", "n"):
            return False
    return None


def _to_int(value) -> int | None:
    """Parse to int, mapping API sentinel negatives (-1/-2/-3) to None."""
    if value is None:
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    if n < 0:
        return None
    return n


def _to_double(value) -> float | None:
    """Parse to float, mapping negative sentinels to None."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f < 0:
        return None
    return f


def _format_grade(value) -> str | None:
    """Format a numeric grade code as a 2-char string ('KG', '01', ..., '12')."""
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip().upper()
        if v in ("", "-1", "-2", "-3", "NA", "N/A"):
            return None
        if v in ("KG", "PK", "K", "PRE"):
            return "KG" if v in ("KG", "K") else "PK"
        try:
            n = int(v)
        except ValueError:
            return v
    else:
        try:
            n = int(value)
        except (TypeError, ValueError):
            return None
    if n < 0:
        return None
    if n == 0:
        return "KG"
    return f"{n:02d}"


def derive_grade_levels(low, high) -> str | None:
    """Combine `lowest_grade_offered` + `highest_grade_offered` into `KG-08`."""
    lo = _format_grade(low)
    hi = _format_grade(high)
    if lo and hi:
        return f"{lo}-{hi}"
    return lo or hi


def derive_school_type(charter_indicator, magnet_indicator) -> str:
    """
    Decode the CCD `charter_indicator` and `magnet_indicator` flags into our
    coarse `school_type` enum. CCD only covers *public* schools, so anything
    that isn't a charter is bucketed as `public` (the directory does not
    contain private schools — those come from CCD's private survey, which we
    don't fetch here).
    """
    if _to_bool(charter_indicator) is True:
        return "charter"
    # Magnet schools are still public; we surface them as `public` for the
    # coarse enum but callers can refine later if needed.
    return "public"


def map_directory_row(raw: dict) -> dict:
    """Map one Urban Institute directory record to our bronze schema."""
    state_fips = str(raw.get("fips") or "").zfill(2)
    state_code = FIPS_TO_STATE.get(state_fips)
    return {
        "school_id": str(raw["ncessch"]) if raw.get("ncessch") is not None else None,
        "school_name": raw.get("school_name"),
        "city": raw.get("city_location") or raw.get("city_mailing"),
        "state": state_code,
        "zip": (str(raw.get("zip_location"))[:5] if raw.get("zip_location") else None),
        "lat": _to_double(raw.get("latitude")),
        "lon": _to_double(raw.get("longitude")),
        "grade_levels": derive_grade_levels(
            raw.get("lowest_grade_offered"),
            raw.get("highest_grade_offered"),
        ),
        "enrollment": _to_int(raw.get("enrollment")),
        "school_type": derive_school_type(
            raw.get("charter"),
            raw.get("magnet"),
        ),
        "title1_eligible": _to_bool(raw.get("title_i_eligible")),
    }

# COMMAND ----------

# DBTITLE 1,Fetch directory for all target states
def fetch_directory(states: list[str], year: int) -> list[dict]:
    """Fetch and map directory records for every state, in order."""
    out: list[dict] = []
    for state in states:
        fips = STATE_TO_FIPS[state]
        url = directory_url(year, fips)
        print(f"  directory: {state} (fips={fips}) {url}")
        count_before = len(out)
        for raw in fetch_pages(url):
            mapped = map_directory_row(raw)
            if mapped["school_id"]:
                out.append(mapped)
        print(f"    +{len(out) - count_before} rows (total={len(out)})")
    return out


directory_records = fetch_directory(TARGET_STATES, DIRECTORY_YEAR)
print(f"Total directory rows fetched: {len(directory_records)}")

# COMMAND ----------

# DBTITLE 1,Fetch assessments and pivot subject -> columns
def fetch_assessments(states: list[str], year: int | None) -> dict[str, dict]:
    """
    Fetch grade-8 math + reading assessment rows for each state and pivot them
    into a dict keyed by `ncessch` school id:

        { "120000100000": {"math": 67.5, "reading": 71.0}, ... }

    Assessment rows in EDFacts come per (school, subject) — we keep the most
    recent / highest-coverage `pct_proficient` we see, ignoring sentinel
    negatives.

    Returns an empty dict if `year is None`.
    """
    pivoted: dict[str, dict] = {}
    if year is None:
        return pivoted

    for state in states:
        fips = STATE_TO_FIPS[state]
        url = assessments_url(year, fips)
        print(f"  assessments: {state} (fips={fips}) {url}")
        count_before = len(pivoted)
        for raw in fetch_pages(url):
            ncessch = raw.get("ncessch")
            if not ncessch:
                continue
            ncessch = str(ncessch)
            # The EDFacts assessments endpoint exposes subject as either
            # `subject` ("math"/"read") or `discipline` depending on year.
            subject_raw = (raw.get("subject") or raw.get("discipline") or "")
            subject = str(subject_raw).strip().lower()
            if subject in ("math", "mathematics"):
                subject_key = "math"
            elif subject in ("read", "reading", "ela", "english"):
                subject_key = "reading"
            else:
                continue

            pct = _to_double(
                raw.get("pct_proficient")
                or raw.get("read_test_pct_prof_midpt")
                or raw.get("math_test_pct_prof_midpt")
            )
            if pct is None:
                continue

            bucket = pivoted.setdefault(ncessch, {})
            # Prefer the higher midpoint when we see multiple rows for the same
            # (school, subject) — EDFacts reports proficiency as a range with a
            # midpoint and ranges can overlap.
            existing = bucket.get(subject_key)
            if existing is None or pct > existing:
                bucket[subject_key] = pct
        print(f"    +{len(pivoted) - count_before} schools with scores (total={len(pivoted)})")
    return pivoted


assessment_scores = fetch_assessments(TARGET_STATES, ASSESSMENTS_YEAR)
print(f"Total schools with at least one assessment score: {len(assessment_scores)}")

# COMMAND ----------

# DBTITLE 1,Join directory + assessments into a DataFrame
def attach_scores(records: list[dict], scores: dict[str, dict]) -> list[dict]:
    """Left-join the pivoted assessment dict onto each directory record."""
    ingested_at = datetime.utcnow()
    out: list[dict] = []
    for r in records:
        sid = r["school_id"]
        s = scores.get(sid, {})
        out.append(
            {
                **r,
                "math_proficiency_pct": s.get("math"),
                "reading_proficiency_pct": s.get("reading"),
                "ingested_at": ingested_at,
            }
        )
    return out


joined_records = attach_scores(directory_records, assessment_scores)
print(f"Joined records ready for upsert: {len(joined_records)}")

# Explicit schema so empty / partial frames still get the right column types
# when we hand them to MERGE.
SCHEMA = StructType(
    [
        StructField("school_id", StringType(), nullable=False),
        StructField("school_name", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("zip", StringType(), nullable=True),
        StructField("lat", DoubleType(), nullable=True),
        StructField("lon", DoubleType(), nullable=True),
        StructField("grade_levels", StringType(), nullable=True),
        StructField("enrollment", IntegerType(), nullable=True),
        StructField("school_type", StringType(), nullable=True),
        StructField("title1_eligible", BooleanType(), nullable=True),
        StructField("math_proficiency_pct", DoubleType(), nullable=True),
        StructField("reading_proficiency_pct", DoubleType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=True),
    ]
)

if joined_records:
    schools_df = spark.createDataFrame(joined_records, schema=SCHEMA)
else:
    schools_df = spark.createDataFrame([], schema=SCHEMA)

# Drop dupes on school_id within this batch so MERGE doesn't see ambiguous
# match candidates.
schools_df = schools_df.dropDuplicates(["school_id"])
print(f"Distinct schools to upsert: {schools_df.count()}")

# COMMAND ----------

# DBTITLE 1,Upsert into bronze.schools_us
TARGET_TABLE = "realestate.bronze.schools_us"
STAGING_VIEW = "_schools_us_staging"

schools_df.createOrReplaceTempView(STAGING_VIEW)

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING {STAGING_VIEW} AS source
ON target.school_id = source.school_id
WHEN MATCHED THEN UPDATE SET
    target.school_name             = source.school_name,
    target.city                    = source.city,
    target.state                   = source.state,
    target.zip                     = source.zip,
    target.lat                     = source.lat,
    target.lon                     = source.lon,
    target.grade_levels            = source.grade_levels,
    target.enrollment              = source.enrollment,
    target.school_type             = source.school_type,
    target.title1_eligible         = source.title1_eligible,
    target.math_proficiency_pct    = source.math_proficiency_pct,
    target.reading_proficiency_pct = source.reading_proficiency_pct,
    target.ingested_at             = source.ingested_at
WHEN NOT MATCHED THEN INSERT (
    school_id, school_name, city, state, zip, lat, lon,
    grade_levels, enrollment, school_type, title1_eligible,
    math_proficiency_pct, reading_proficiency_pct, ingested_at
) VALUES (
    source.school_id, source.school_name, source.city, source.state,
    source.zip, source.lat, source.lon,
    source.grade_levels, source.enrollment, source.school_type, source.title1_eligible,
    source.math_proficiency_pct, source.reading_proficiency_pct, source.ingested_at
)
"""

spark.sql(merge_sql)
print(f"Merged {schools_df.count()} rows into {TARGET_TABLE}.")

# COMMAND ----------

# DBTITLE 1,Post-load counts
total_rows = spark.table(TARGET_TABLE).count()
per_state = (
    spark.table(TARGET_TABLE)
    .groupBy("state")
    .agg(
        F.count("*").alias("schools"),
        F.sum(F.when(F.col("math_proficiency_pct").isNotNull(), 1).otherwise(0)).alias("with_math"),
        F.sum(F.when(F.col("reading_proficiency_pct").isNotNull(), 1).otherwise(0)).alias("with_reading"),
    )
    .orderBy(F.col("schools").desc())
)

print(f"{TARGET_TABLE} total rows: {total_rows}")
per_state.show(60, truncate=False)
