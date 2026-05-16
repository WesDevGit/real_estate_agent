# Databricks notebook source
# DBTITLE 1,Ingest demographics (US Census + Colombia DANE)
# MAGIC %md
# MAGIC # Ingest Demographics
# MAGIC
# MAGIC **US:** US Census Bureau ACS 5-year API (live).
# MAGIC **Colombia:** DANE Censo 2018 — operator-staged bulk files.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and config
import os
from datetime import datetime
from typing import Optional

import requests

CENSUS_BASE_URL = "https://api.census.gov/data"
CENSUS_YEARS_TO_TRY = [2022, 2021, 2020]
CENSUS_VARS = [
    "B19013_001E",  # median household income
    "B01002_001E",  # median age
    "B15003_022E",  # bachelor's
    "B15003_023E",  # master's
    "B15003_024E",  # professional
    "B15003_025E",  # doctorate
    "B15003_001E",  # education denom (25+)
    "B25003_001E",  # housing units total
    "B25003_002E",  # owner-occupied
    "B25077_001E",  # median home value
]

US_FALLBACK_STATE_FIPS = ["48", "12", "13", "06"]  # TX, FL, GA, CA

CO_VOLUME = "/Volumes/realestate/bronze/raw/dane_censo/"

US_TARGET_TABLE = "realestate.bronze.demographics_us"
CO_TARGET_TABLE = "realestate.bronze.demographics_co"

# COMMAND ----------

# DBTITLE 1,Operator instructions
# MAGIC %md
# MAGIC ## Colombia bulk file staging
# MAGIC
# MAGIC The DANE Censo 2018 microdata or aggregate tables are not exposed via a stable
# MAGIC public JSON API. An operator must download the latest published files from:
# MAGIC * https://www.dane.gov.co/index.php/estadisticas-por-tema/demografia-y-poblacion/censo-nacional-de-poblacion-y-vivenda-2018
# MAGIC
# MAGIC Drop the files (`.xlsx` or `.csv`) into `/Volumes/realestate/bronze/raw/dane_censo/`.
# MAGIC This notebook will pick up any file whose name starts with `dane_censo_` and parse
# MAGIC the rows for Bogotá and Medellín municipalities.

# COMMAND ----------

# DBTITLE 1,Discover US state FIPS codes
# State FIPS lookup (subset — extend as listings grow).
STATE_NAME_TO_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "FL": "12", "GA": "13",
    "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
    "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24",
    "MA": "25", "MI": "26", "MN": "27", "MS": "28", "MO": "29",
    "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
    "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45",
    "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50",
    "VA": "51", "WA": "53", "WV": "54", "WI": "55", "WY": "56",
}


def discover_us_state_fips() -> list[str]:
    try:
        rows = spark.sql(
            "SELECT DISTINCT state FROM realestate.bronze.listings_us WHERE state IS NOT NULL"
        ).collect()
    except Exception:
        rows = []
    fips = [STATE_NAME_TO_FIPS.get(r["state"]) for r in rows]
    fips = [f for f in fips if f]
    return fips or US_FALLBACK_STATE_FIPS

# COMMAND ----------

# DBTITLE 1,Fetch US Census ACS for a state
def fetch_acs_for_state(year: int, state_fips: str, api_key: str) -> list[dict]:
    params = {
        "get": ",".join(CENSUS_VARS),
        "for": "zip code tabulation area:*",
        "in": f"state:{state_fips}",
        "key": api_key,
    }
    url = f"{CENSUS_BASE_URL}/{year}/acs/acs5"
    resp = requests.get(url, params=params, timeout=120)
    if resp.status_code != 200:
        print(f"  Census {year} {state_fips} returned {resp.status_code}: {resp.text[:200]}")
        return []

    data = resp.json()
    if not data or len(data) < 2:
        return []

    header = data[0]
    var_idx = {v: header.index(v) for v in CENSUS_VARS if v in header}
    state_idx = header.index("state")
    zip_idx = header.index("zip code tabulation area")

    out = []
    for row in data[1:]:
        def get(var):
            v = row[var_idx[var]] if var in var_idx else None
            if v in (None, "", "-666666666", "-999999999"):
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        edu_num = sum(filter(None, [get("B15003_022E"), get("B15003_023E"),
                                    get("B15003_024E"), get("B15003_025E")])) or None
        edu_denom = get("B15003_001E")
        pct_college = (100.0 * edu_num / edu_denom) if (edu_num and edu_denom) else None

        housing_total = get("B25003_001E")
        owner_occ = get("B25003_002E")
        pct_homeowner = (100.0 * owner_occ / housing_total) if (housing_total and owner_occ) else None

        zip_code = row[zip_idx]
        out.append({
            "geo_id": f"{row[state_idx]}_{zip_code}",
            "state": row[state_idx],
            "county": None,
            "zip": zip_code,
            "city": None,
            "total_population": None,  # not requested above; could add B01003_001E
            "median_household_income_usd": int(get("B19013_001E")) if get("B19013_001E") else None,
            "median_age": get("B01002_001E"),
            "pct_college_educated": pct_college,
            "pct_homeowner": pct_homeowner,
            "median_home_value_usd": int(get("B25077_001E")) if get("B25077_001E") else None,
            "year": year,
        })
    return out

# COMMAND ----------

# DBTITLE 1,Run US ingestion
def run_us_ingest() -> int:
    try:
        api_key = get_secret("CENSUS_API_KEY")
    except Exception as e:
        print(f"CENSUS_API_KEY not found in secrets: {e}. Skipping US block.")
        return 0

    fips_codes = discover_us_state_fips()
    print(f"Fetching ACS for state FIPS: {fips_codes}")

    chosen_year = None
    all_rows = []
    for year in CENSUS_YEARS_TO_TRY:
        # Probe with first state; if it returns data, use this year for all.
        probe = fetch_acs_for_state(year, fips_codes[0], api_key)
        if probe:
            chosen_year = year
            all_rows.extend(probe)
            for fips in fips_codes[1:]:
                all_rows.extend(fetch_acs_for_state(year, fips, api_key))
            break

    if not all_rows:
        print("No ACS data fetched.")
        return 0

    now = datetime.utcnow()
    for r in all_rows:
        r["ingest_time"] = now

    # Upsert via merge — but for first build we just overwrite by (geo_id, year).
    df = spark.createDataFrame(all_rows)
    df.createOrReplaceTempView("_new_demographics_us")
    spark.sql(
        f"""
        MERGE INTO {US_TARGET_TABLE} t
        USING _new_demographics_us s
        ON t.geo_id = s.geo_id AND t.year = s.year
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print(f"US ACS {chosen_year}: upserted {len(all_rows)} rows")
    return len(all_rows)


us_count = run_us_ingest()

# COMMAND ----------

# DBTITLE 1,Run Colombia ingestion (bulk files)
def run_co_ingest() -> int:
    try:
        files = [f.path for f in dbutils.fs.ls(CO_VOLUME) if f.path.endswith((".csv", ".xlsx"))]
    except Exception as e:
        print(f"CO volume not accessible ({e}). Skipping CO block.")
        return 0

    if not files:
        print(f"No DANE files staged in {CO_VOLUME}. Skipping CO block.")
        return 0

    print(f"Found {len(files)} CO files to parse: {[os.path.basename(f) for f in files]}")
    # Parsing exact columns depends on which DANE export was downloaded.
    # Implement here when files are staged. For now, log and exit cleanly.
    print("CO parser is a stub — operator should map downloaded file columns to the schema.")
    return 0


co_count = run_co_ingest()

# COMMAND ----------

print(f"Demographics ingestion complete. US rows: {us_count}, CO rows: {co_count}")
