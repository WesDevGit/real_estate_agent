# Databricks notebook source
# DBTITLE 1,Ingest economic indicators (US + Colombia)
# MAGIC %md
# MAGIC # Ingest Economic Indicators
# MAGIC
# MAGIC * **US:** BLS LAUS (local unemployment, live API) + FRED MORTGAGE30US.
# MAGIC * **Colombia:** DANE GEIH and Banco de la República — operator-staged bulk files.

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and config
import os
from datetime import datetime, date

import requests

US_TARGET_TABLE = "realestate.bronze.economic_us"
CO_TARGET_TABLE = "realestate.bronze.economic_co"

BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
FRED_URL = "https://api.stlouisfed.org/fred/series/observations"

# BLS LAUS area codes for fallback US metros. Build out as listings grow.
# Pattern for series_id: LAUMT{area_code}0000000003 (unemployment rate, MSA-level).
US_METROS = {
    "Austin":  {"state": "TX", "laus_msa": "1242420"},   # Austin-Round Rock-Georgetown MSA
    "Miami":   {"state": "FL", "laus_msa": "1233100"},   # Miami-Fort Lauderdale-Pompano Beach MSA
    "Atlanta": {"state": "GA", "laus_msa": "1212060"},   # Atlanta-Sandy Springs-Alpharetta MSA
    "Houston": {"state": "TX", "laus_msa": "1226420"},   # Houston-The Woodlands-Sugar Land MSA
}

DANE_GEIH_VOLUME = "/Volumes/realestate/bronze/raw/dane_geih/"
BANREP_VOLUME = "/Volumes/realestate/bronze/raw/banrep/"

# COMMAND ----------

# DBTITLE 1,BLS — local unemployment rate
def fetch_bls_unemployment(api_key: str, start_year: int, end_year: int) -> dict:
    """Return dict keyed by series_id with list of (date, value)."""
    series_ids = [f"LAU{m['laus_msa']}0000000003" for m in US_METROS.values()]
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": api_key,
    }
    resp = requests.post(BLS_URL, json=payload, timeout=60)
    if resp.status_code != 200:
        print(f"BLS returned {resp.status_code}: {resp.text[:200]}")
        return {}
    body = resp.json()
    if body.get("status") != "REQUEST_SUCCEEDED":
        print(f"BLS error: {body.get('message')}")
        return {}

    out = {}
    for series in body.get("Results", {}).get("series", []):
        sid = series["seriesID"]
        observations = []
        for d in series.get("data", []):
            try:
                year = int(d["year"])
                period = d["period"]  # e.g. 'M03'
                if not period.startswith("M"):
                    continue
                month = int(period[1:])
                if month > 12:
                    continue
                value = float(d["value"]) if d.get("value") not in (None, "") else None
                observations.append((date(year, month, 1), value))
            except (TypeError, ValueError, KeyError):
                continue
        out[sid] = sorted(observations)
    return out

# COMMAND ----------

# DBTITLE 1,FRED — 30-year mortgage rate
def fetch_fred_mortgage(api_key: str, start_year: int) -> list[tuple[date, float]]:
    params = {
        "series_id": "MORTGAGE30US",
        "api_key": api_key,
        "file_type": "json",
        "observation_start": f"{start_year}-01-01",
    }
    resp = requests.get(FRED_URL, params=params, timeout=60)
    if resp.status_code != 200:
        print(f"FRED returned {resp.status_code}: {resp.text[:200]}")
        return []
    obs = resp.json().get("observations", [])
    out = []
    for o in obs:
        try:
            dt = datetime.strptime(o["date"], "%Y-%m-%d").date()
            value = float(o["value"]) if o["value"] != "." else None
        except (TypeError, ValueError):
            continue
        if value is not None:
            out.append((dt, value))
    return out


def monthly_mortgage_avg(weekly: list[tuple[date, float]]) -> dict[date, float]:
    """Convert weekly observations to month-start averages."""
    by_month: dict[date, list[float]] = {}
    for d, v in weekly:
        key = date(d.year, d.month, 1)
        by_month.setdefault(key, []).append(v)
    return {k: sum(vs) / len(vs) for k, vs in by_month.items() if vs}

# COMMAND ----------

# DBTITLE 1,Run US ingestion
def run_us_ingest() -> int:
    try:
        bls_key = get_secret("BLS_API_KEY")
        fred_key = get_secret("FRED_API_KEY")
    except Exception as e:
        print(f"US economic keys missing ({e}). Skipping US block.")
        return 0

    end_year = date.today().year
    start_year = end_year - 5
    bls_data = fetch_bls_unemployment(bls_key, start_year, end_year)
    fred_weekly = fetch_fred_mortgage(fred_key, start_year)
    fred_monthly = monthly_mortgage_avg(fred_weekly)

    rows = []
    now = datetime.utcnow()
    for city, meta in US_METROS.items():
        sid = f"LAU{meta['laus_msa']}0000000003"
        for dt, unemp in bls_data.get(sid, []):
            mortgage = fred_monthly.get(date(dt.year, dt.month, 1))
            rows.append({
                "geo_id": meta["laus_msa"],
                "metro_area": city,
                "state": meta["state"],
                "date": dt,
                "unemployment_rate": unemp,
                "job_growth_yoy_pct": None,
                "median_wage_usd": None,
                "mortgage_rate_30yr": mortgage,
                "ingest_time": now,
            })

    if not rows:
        print("No US economic rows fetched.")
        return 0

    df = spark.createDataFrame(rows)
    df.createOrReplaceTempView("_new_economic_us")
    spark.sql(f"""
        MERGE INTO {US_TARGET_TABLE} t
        USING _new_economic_us s
        ON t.geo_id = s.geo_id AND t.date = s.date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"US economic: upserted {len(rows)} rows for {len(US_METROS)} metros")
    return len(rows)


us_count = run_us_ingest()

# COMMAND ----------

# DBTITLE 1,Operator instructions — Colombia
# MAGIC %md
# MAGIC ### Colombia file staging
# MAGIC
# MAGIC **DANE GEIH (Gran Encuesta Integrada de Hogares) — monthly unemployment:**
# MAGIC * Download from https://www.dane.gov.co/index.php/estadisticas-por-tema/mercado-laboral
# MAGIC * Drop Excel file(s) in `/Volumes/realestate/bronze/raw/dane_geih/`.
# MAGIC
# MAGIC **Banco de la República rate data:**
# MAGIC * Download mortgage / interest rate CSVs from https://www.banrep.gov.co/es/estadisticas
# MAGIC * Drop in `/Volumes/realestate/bronze/raw/banrep/`.

# COMMAND ----------

# DBTITLE 1,Run Colombia ingestion (bulk files)
def run_co_ingest() -> int:
    found_any = False
    for vol, label in [(DANE_GEIH_VOLUME, "DANE GEIH"), (BANREP_VOLUME, "Banco Rep")]:
        try:
            files = [f.path for f in dbutils.fs.ls(vol) if f.path.lower().endswith((".csv", ".xlsx"))]
        except Exception:
            files = []
        if files:
            found_any = True
            print(f"{label}: {len(files)} files staged in {vol}")
        else:
            print(f"{label}: no files in {vol}")

    if not found_any:
        return 0

    print("CO economic parser is a stub — operator should map downloaded file columns to "
          "bronze.economic_co schema once files are staged.")
    return 0


co_count = run_co_ingest()

# COMMAND ----------

print(f"Economic ingestion complete. US rows: {us_count}, CO rows: {co_count}")
