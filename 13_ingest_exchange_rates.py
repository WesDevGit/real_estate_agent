# Databricks notebook source
# DBTITLE 1,Ingest exchange rates (COP/USD)
# MAGIC %md
# MAGIC # Ingest COP/USD Exchange Rates
# MAGIC
# MAGIC Source: [frankfurter.app](https://www.frankfurter.app/) — free, no API key.
# MAGIC
# MAGIC The bronze table stores the rate as **COP per 1 USD**. Frankfurter returns
# MAGIC USD per 1 COP, so we invert: `rate = 1 / response.rates['USD']`.
# MAGIC
# MAGIC Incremental: on first run fetches 2 years of history (skipping weekends —
# MAGIC frankfurter only publishes weekday rates). Subsequent runs fetch from
# MAGIC `max(date) + 1` to yesterday.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and config
import time
from datetime import date, datetime, timedelta
from typing import Optional

import requests

TARGET_TABLE = "realestate.bronze.exchange_rates"
FROM_CURRENCY = "COP"
TO_CURRENCY = "USD"
INITIAL_HISTORY_DAYS = 365 * 2
REQUEST_SLEEP_SECONDS = 0.2
REQUEST_TIMEOUT_SECONDS = 30

# COMMAND ----------

# DBTITLE 1,Fetch single-date rate from frankfurter
def fetch_rate_for_date(target_date: date) -> Optional[float]:
    """Return COP-per-USD rate for the given date, or None if no data.

    frankfurter returns USD-per-COP; we invert.
    """
    url = f"https://api.frankfurter.app/{target_date.isoformat()}"
    params = {"from": FROM_CURRENCY, "to": TO_CURRENCY}
    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    except Exception as e:
        print(f"  [{target_date}] request failed: {type(e).__name__}: {e}")
        return None

    if resp.status_code != 200:
        return None

    body = resp.json()
    usd_per_cop = body.get("rates", {}).get(TO_CURRENCY)
    if not usd_per_cop:
        return None
    return 1.0 / float(usd_per_cop)

# COMMAND ----------

# DBTITLE 1,Determine date range to fetch
def get_existing_max_date() -> Optional[date]:
    row = spark.sql(
        f"SELECT MAX(rate_date) AS max_date FROM {TARGET_TABLE} "
        f"WHERE from_currency = '{FROM_CURRENCY}' AND to_currency = '{TO_CURRENCY}'"
    ).first()
    if row is None or row["max_date"] is None:
        return None
    return row["max_date"]


def iter_dates_to_fetch():
    """Yield weekday dates from (max+1 or 2yr ago) through yesterday."""
    yesterday = date.today() - timedelta(days=1)
    max_existing = get_existing_max_date()
    if max_existing is None:
        start = date.today() - timedelta(days=INITIAL_HISTORY_DAYS)
        print(f"Bootstrapping: fetching {INITIAL_HISTORY_DAYS} days of history")
    else:
        start = max_existing + timedelta(days=1)
        print(f"Incremental: fetching from {start} through {yesterday}")

    current = start
    while current <= yesterday:
        # Skip weekends — frankfurter only publishes weekday rates.
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)

# COMMAND ----------

# DBTITLE 1,Run ingestion
def run_ingest() -> int:
    rows = []
    now = datetime.utcnow()
    skipped = 0
    fetched = 0

    for d in iter_dates_to_fetch():
        rate = fetch_rate_for_date(d)
        if rate is None:
            skipped += 1
        else:
            rows.append({
                "rate_date": d,
                "from_currency": FROM_CURRENCY,
                "to_currency": TO_CURRENCY,
                "rate": rate,
                "ingest_time": now,
            })
            fetched += 1
        time.sleep(REQUEST_SLEEP_SECONDS)

    if not rows:
        print(f"No new rates fetched (skipped={skipped}).")
        return 0

    df = spark.createDataFrame(rows)
    df.write.mode("append").saveAsTable(TARGET_TABLE)
    print(f"Inserted {fetched} new rate rows (skipped {skipped} dates without data).")
    return fetched


inserted = run_ingest()

# COMMAND ----------

# DBTITLE 1,Verify latest rate
spark.sql(
    f"SELECT rate_date, rate FROM {TARGET_TABLE} "
    f"WHERE from_currency='{FROM_CURRENCY}' AND to_currency='{TO_CURRENCY}' "
    f"ORDER BY rate_date DESC LIMIT 5"
).show()
