# Databricks notebook source
# DBTITLE 1,Ingest exchange rates (COP/USD)
# MAGIC %md
# MAGIC # Ingest COP/USD Exchange Rates
# MAGIC
# MAGIC Source: **Yahoo Finance** via the `yfinance` library — free, no API key, supports
# MAGIC the `USDCOP=X` FX pair which returns COP per 1 USD (matching the bronze schema).
# MAGIC
# MAGIC (frankfurter.app does not support COP — it only covers ECB-reference currencies.)
# MAGIC
# MAGIC Incremental: on first run fetches 2 years of history. Subsequent runs fetch from
# MAGIC `max(date) + 1` to yesterday.

# COMMAND ----------

# DBTITLE 1,Install yfinance
# MAGIC %pip install yfinance
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and config
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import yfinance as yf

TARGET_TABLE = "realestate.bronze.exchange_rates"
FROM_CURRENCY = "COP"
TO_CURRENCY = "USD"
TICKER = "USDCOP=X"           # Yahoo: USD/COP — Close column is COP per 1 USD
INITIAL_HISTORY_DAYS = 365 * 2

# COMMAND ----------

# DBTITLE 1,Determine date range to fetch
def get_existing_max_date() -> Optional[date]:
    row = spark.sql(
        f"SELECT MAX(date) AS max_date FROM {TARGET_TABLE} "
        f"WHERE from_currency = '{FROM_CURRENCY}' AND to_currency = '{TO_CURRENCY}'"
    ).first()
    if row is None or row["max_date"] is None:
        return None
    return row["max_date"]


def compute_start_end() -> tuple[date, date]:
    yesterday = date.today() - timedelta(days=1)
    max_existing = get_existing_max_date()
    if max_existing is None:
        start = date.today() - timedelta(days=INITIAL_HISTORY_DAYS)
        print(f"Bootstrapping: fetching {INITIAL_HISTORY_DAYS} days of history "
              f"({start} → {yesterday})")
    else:
        start = max_existing + timedelta(days=1)
        print(f"Incremental: fetching from {start} through {yesterday}")
    return start, yesterday

# COMMAND ----------

# DBTITLE 1,Fetch USDCOP history from Yahoo Finance
def fetch_history(start: date, end: date) -> pd.DataFrame:
    # yfinance end date is exclusive — bump by 1 to include the requested end.
    end_exclusive = end + timedelta(days=1)
    ticker = yf.Ticker(TICKER)
    hist = ticker.history(start=start.isoformat(), end=end_exclusive.isoformat())
    if hist.empty:
        return hist
    # Keep only Close (COP per 1 USD) and ensure tz-naive date index.
    hist = hist[["Close"]].copy()
    hist.index = pd.to_datetime(hist.index).tz_localize(None).date
    return hist

# COMMAND ----------

# DBTITLE 1,Run ingestion
def run_ingest() -> int:
    start, end = compute_start_end()
    if start > end:
        print(f"Already current through {end}. Nothing to fetch.")
        return 0

    hist = fetch_history(start, end)
    if hist.empty:
        print("Yahoo Finance returned no rows. Try again later or widen the date range.")
        return 0

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for idx_date, row in hist.iterrows():
        try:
            rate = float(row["Close"])
        except (TypeError, ValueError):
            continue
        if not rate or rate != rate:   # filter NaN
            continue
        rows.append({
            "date": idx_date,
            "from_currency": FROM_CURRENCY,
            "to_currency": TO_CURRENCY,
            "rate": rate,
            "ingested_at": now,
        })

    if not rows:
        print("No usable rate rows after parsing.")
        return 0

    # Match the schema column order: date, from_currency, to_currency, rate, ingested_at
    df = spark.createDataFrame(rows).select("date", "from_currency", "to_currency", "rate", "ingested_at")
    df.write.mode("append").saveAsTable(TARGET_TABLE)
    print(f"Inserted {len(rows)} rate rows "
          f"(date range: {rows[0]['date']} → {rows[-1]['date']})")
    return len(rows)


inserted = run_ingest()

# COMMAND ----------

# DBTITLE 1,Verify latest rate
spark.sql(
    f"SELECT date, rate FROM {TARGET_TABLE} "
    f"WHERE from_currency='{FROM_CURRENCY}' AND to_currency='{TO_CURRENCY}' "
    f"ORDER BY date DESC LIMIT 5"
).show()
