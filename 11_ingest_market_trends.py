# Databricks notebook source
# DBTITLE 1,Ingest market trends (US + Colombia)
# MAGIC %md
# MAGIC # Ingest Market Trends
# MAGIC
# MAGIC No live API. Loads operator-staged bulk CSV/Excel from Databricks volumes:
# MAGIC * **US:** Zillow Research and Redfin Data Center monthly CSVs.
# MAGIC * **Colombia:** DANE IPVN (Índice de Precios de Vivienda Nueva) quarterly Excel.

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and config
import os
from datetime import datetime, date
from typing import Optional

import pandas as pd

ZILLOW_VOLUME = "/Volumes/realestate/bronze/raw/zillow_research/"
REDFIN_VOLUME = "/Volumes/realestate/bronze/raw/redfin/"
DANE_IPVN_VOLUME = "/Volumes/realestate/bronze/raw/dane_ipvn/"

US_TARGET_TABLE = "realestate.bronze.market_trends_us"
CO_TARGET_TABLE = "realestate.bronze.market_trends_co"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operator file staging
# MAGIC
# MAGIC **Zillow Research (zillow.com/research/data/):**
# MAGIC * Download "ZHVI (All Homes - Smooth, Seasonally Adjusted)" by ZIP. Drop in
# MAGIC   `/Volumes/realestate/bronze/raw/zillow_research/`.
# MAGIC * The CSV is wide: columns are `RegionID`, `RegionName` (zip), `State`, `City`,
# MAGIC   then one column per month (`YYYY-MM-DD`). We melt to long format.
# MAGIC
# MAGIC **Redfin Data Center (redfin.com/news/data-center/):**
# MAGIC * Download "Market Tracker" CSV by ZIP. Drop in `/Volumes/realestate/bronze/raw/redfin/`.
# MAGIC * Long format already, one row per (zip, period).
# MAGIC
# MAGIC **DANE IPVN (dane.gov.co):**
# MAGIC * Download quarterly IPVN Excel. Drop in `/Volumes/realestate/bronze/raw/dane_ipvn/`.

# COMMAND ----------

# DBTITLE 1,Zillow ZHVI parser
def parse_zillow_zhvi(path: str) -> list[dict]:
    pdf = pd.read_csv(path)
    if pdf.empty:
        return []

    id_cols = [c for c in pdf.columns if not c.startswith(("19", "20"))]
    date_cols = [c for c in pdf.columns if c.startswith(("19", "20"))]
    if not date_cols:
        print(f"  {os.path.basename(path)}: no date columns detected, skipping")
        return []

    melted = pdf.melt(id_vars=id_cols, value_vars=date_cols,
                      var_name="date_str", value_name="median_list_price_usd")
    melted = melted.dropna(subset=["median_list_price_usd"])

    rows = []
    for _, r in melted.iterrows():
        zip_code = str(r.get("RegionName") or "").zfill(5)
        try:
            dt = pd.to_datetime(r["date_str"]).date()
        except Exception:
            continue
        rows.append({
            "geo_id": zip_code,
            "geo_type": "zip",
            "zip": zip_code,
            "city": r.get("City"),
            "state": r.get("StateName") or r.get("State"),
            "date": dt,
            "median_list_price_usd": int(round(r["median_list_price_usd"])),
            "median_sale_price_usd": None,
            "median_days_on_market": None,
            "homes_sold": None,
            "inventory_count": None,
            "months_of_supply": None,
            "price_reduced_pct": None,
            "source": "zillow_research",
        })
    return rows

# COMMAND ----------

# DBTITLE 1,Redfin parser
def parse_redfin_market(path: str) -> list[dict]:
    # Redfin tracker exports are tsv with .csv extension sometimes; try both.
    for sep in [",", "\t"]:
        try:
            pdf = pd.read_csv(path, sep=sep)
            if pdf.shape[1] > 3:
                break
        except Exception:
            continue
    else:
        return []

    rows = []
    for _, r in pdf.iterrows():
        zip_code = str(r.get("region_id") or r.get("zip_code") or "").zfill(5)
        if not zip_code or zip_code == "00000":
            continue
        period_end = r.get("period_end") or r.get("month")
        try:
            dt = pd.to_datetime(period_end).date()
        except Exception:
            continue

        homes_sold = r.get("homes_sold") or r.get("Homes Sold")
        inventory = r.get("inventory") or r.get("Inventory")
        months_supply = None
        try:
            if homes_sold and inventory:
                months_supply = float(inventory) / float(homes_sold)
        except (TypeError, ValueError, ZeroDivisionError):
            months_supply = None

        rows.append({
            "geo_id": zip_code,
            "geo_type": "zip",
            "zip": zip_code,
            "city": r.get("city"),
            "state": r.get("state_code"),
            "date": dt,
            "median_list_price_usd": int(r["median_list_price"]) if pd.notna(r.get("median_list_price")) else None,
            "median_sale_price_usd": int(r["median_sale_price"]) if pd.notna(r.get("median_sale_price")) else None,
            "median_days_on_market": int(r["median_dom"]) if pd.notna(r.get("median_dom")) else None,
            "homes_sold": int(homes_sold) if pd.notna(homes_sold) else None,
            "inventory_count": int(inventory) if pd.notna(inventory) else None,
            "months_of_supply": months_supply,
            "price_reduced_pct": float(r["price_drops"]) if pd.notna(r.get("price_drops")) else None,
            "source": "redfin",
        })
    return rows

# COMMAND ----------

# DBTITLE 1,DANE IPVN parser
def parse_dane_ipvn(path: str) -> list[dict]:
    """Best-effort parser for DANE IPVN quarterly Excel.

    Real-world IPVN Excel files have multiple sheets and merged header cells. This
    function assumes a 'BOG' and 'MED' city-level series exists; operator may need
    to point to the correct sheet name.
    """
    try:
        sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    except Exception as e:
        print(f"  IPVN read error: {e}")
        return []

    rows = []
    for sheet_name, pdf in sheets.items():
        # Heuristic: any sheet containing "Bogot" or "Medel" columns or rows
        text = pdf.to_string()
        if "Bogot" not in text and "Medel" not in text:
            continue
        # Operator must map columns here once file structure is known. Log and skip.
        print(f"  IPVN sheet '{sheet_name}' detected but column mapping not implemented. "
              "Update parse_dane_ipvn() with the actual schema.")
    return rows

# COMMAND ----------

# DBTITLE 1,Generic ingest runner
def ingest_files(volume: str, parser, table: str, label: str) -> int:
    try:
        files = [f.path for f in dbutils.fs.ls(volume) if f.path.lower().endswith((".csv", ".xlsx", ".tsv"))]
    except Exception as e:
        print(f"{label} volume not accessible ({e}). Skipping.")
        return 0

    if not files:
        print(f"{label}: no files staged in {volume}")
        return 0

    total = 0
    now = datetime.utcnow()
    for f in files:
        local = f.replace("dbfs:", "/dbfs") if f.startswith("dbfs:") else f
        print(f"{label}: parsing {os.path.basename(local)}")
        rows = parser(local)
        if not rows:
            continue
        for r in rows:
            r["ingested_at"] = now
        df = spark.createDataFrame(rows)
        df.createOrReplaceTempView("_new_market_trends")
        if "source" in df.columns:
            spark.sql(f"""
                MERGE INTO {table} t
                USING _new_market_trends s
                ON t.geo_id = s.geo_id AND t.date = s.date AND t.source = s.source
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        else:
            spark.sql(f"""
                MERGE INTO {table} t
                USING _new_market_trends s
                ON t.geo_id = s.geo_id AND t.date = s.date
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        total += len(rows)
        print(f"  upserted {len(rows)} rows")
    return total

# COMMAND ----------

# DBTITLE 1,Run
zillow_count = ingest_files(ZILLOW_VOLUME, parse_zillow_zhvi, US_TARGET_TABLE, "Zillow")
redfin_count = ingest_files(REDFIN_VOLUME, parse_redfin_market, US_TARGET_TABLE, "Redfin")
ipvn_count   = ingest_files(DANE_IPVN_VOLUME, parse_dane_ipvn, CO_TARGET_TABLE, "DANE IPVN")

print(f"\nMarket trends ingest complete. Zillow={zillow_count}, Redfin={redfin_count}, IPVN={ipvn_count}")
