# Databricks notebook source
# DBTITLE 1,Ingest US For-Sale Listings (RapidAPI Zillow)
# MAGIC %md
# MAGIC # 01 — Ingest US For-Sale Listings
# MAGIC
# MAGIC Fetches active US home-purchase listings from the RapidAPI subscription
# MAGIC `zillow-com-live-data-scraper-api` and upserts them into
# MAGIC `realestate.bronze.listings_us` using `listing_id` as the merge key.
# MAGIC
# MAGIC **Behaviour:**
# MAGIC * Filter to `status_type=ForSale` only (no rentals).
# MAGIC * For each known city, page through results and map fields to the bronze
# MAGIC   schema. Compute `price_per_sqft_usd = price_usd / sqft` (None-safe).
# MAGIC * Existing rows by `listing_id` are updated for price, days_on_market, and
# MAGIC   scraped_at. New rows are inserted in full.
# MAGIC * Full API response per page is preserved in `raw_json` for downstream
# MAGIC   debugging.
# MAGIC
# MAGIC **Schedule:** Daily.

# COMMAND ----------

# DBTITLE 1,Load shared helpers
# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Catalog
spark.sql("USE CATALOG realestate")

# COMMAND ----------

# DBTITLE 1,Imports specific to this notebook
import json
import time
import traceback
from datetime import datetime, timezone

import requests

# COMMAND ----------

# DBTITLE 1,Config
PIPELINE_NAME = "01_ingest_listings_us"
TARGET_TABLE = "realestate.bronze.listings_us"

# RapidAPI host + endpoint for the Zillow scraper.
#
# Endpoint chosen from the RapidAPI test console for
# `zillow-com-live-data-scraper-api`. The provider exposes several search
# endpoints; the city-wide for-sale search currently lives at:
#
#     GET https://zillow-com-live-data-scraper-api.p.rapidapi.com/propertyExtendedSearch
#         ?location=<city, state>
#         &status_type=ForSale
#         &page=<n>
#
# If the provider renames the path (e.g. to `/byCity`, `/properties`,
# `/search`, `/searchByCity`), update `RAPIDAPI_SEARCH_PATH` below. The
# `x-rapidapi-host` header should always match the subscription's host string
# exactly as shown on the RapidAPI dashboard.
RAPIDAPI_HOST = "zillow-com-live-data-scraper-api.p.rapidapi.com"
RAPIDAPI_SEARCH_PATH = "/propertyExtendedSearch"
RAPIDAPI_SEARCH_URL = f"https://{RAPIDAPI_HOST}{RAPIDAPI_SEARCH_PATH}"

# Pull the API key once at the top of the run.
RAPIDAPI_KEY = get_secret("RAPIDAPI_KEY")

# How many result pages to walk per city before stopping. Provider typically
# returns 40 listings per page. Cap is deliberately modest so a single run
# stays within the RapidAPI free-tier quota.
MAX_PAGES_PER_CITY = 5

# Polite delay between HTTP calls so we never hammer the RapidAPI gateway.
REQUEST_DELAY_SECONDS = 0.6
REQUEST_TIMEOUT_SECONDS = 30

# Cities the daily refresh will hit. The spec also calls for picking up any
# distinct cities already in bronze.listings_us; we union those in below so
# that previously-ingested markets keep getting refreshed.
EXAMPLE_CITIES = ["Austin", "Miami", "Atlanta", "Houston"]

# Zillow's homeType field -> our normalized property_type values.
PROPERTY_TYPE_MAP = {
    "SINGLE_FAMILY": "single_family",
    "CONDO": "condo",
    "TOWNHOUSE": "townhouse",
    "MULTI_FAMILY": "multi_family",
    "MANUFACTURED": "single_family",
    "APARTMENT": "condo",
}

print(f"Target table : {TARGET_TABLE}")
print(f"Endpoint     : {RAPIDAPI_SEARCH_URL}")
print(f"Example cities: {EXAMPLE_CITIES}")

# COMMAND ----------

# DBTITLE 1,Small parsing helpers
def _safe_int(value):
    """Coerce a value to int; return None for None / blank / non-numeric."""
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _safe_float(value):
    """Coerce a value to float; return None for None / blank / non-numeric."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_str(value):
    """Trim a string, or return None if blank / not a string."""
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    cleaned = value.strip()
    return cleaned if cleaned else None


def _price_per_sqft(price_usd, sqft):
    """None-safe price_per_sqft_usd = price_usd / sqft."""
    if price_usd is None or sqft is None:
        return None
    try:
        if float(sqft) <= 0:
            return None
        return float(price_usd) / float(sqft)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _parse_listing_date(value):
    """Parse common Zillow date / epoch-ms formats into a `date` object."""
    if value is None or value == "":
        return None
    # Numeric input is treated as a Unix epoch (seconds or milliseconds).
    if isinstance(value, (int, float)):
        epoch = float(value)
        if epoch > 1e12:  # milliseconds
            epoch = epoch / 1000.0
        try:
            return datetime.fromtimestamp(epoch, tz=timezone.utc).date()
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%m/%d/%Y"):
            try:
                return datetime.strptime(text[: len(fmt) + 4], fmt).date()
            except ValueError:
                continue
    return None

# COMMAND ----------

# DBTITLE 1,Fetch one page from the Zillow search endpoint
def fetch_listings_page(location: str, page: int) -> tuple[int, dict]:
    """
    Call the RapidAPI Zillow scraper search endpoint for one (city, page).

    Returns:
        (status_code, response_json). On non-200 responses the JSON dict is
        `{"error": ..., "status_code": ...}` so the caller can still record it.
    """
    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
        "accept": "application/json",
    }
    params = {
        "location": location,
        "status_type": "ForSale",
        "page": page,
    }

    response = requests.get(
        RAPIDAPI_SEARCH_URL,
        headers=headers,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    if response.status_code != 200:
        return response.status_code, {
            "error": "non_200_response",
            "status_code": response.status_code,
            "body": response.text[:1000],
        }

    try:
        return response.status_code, response.json()
    except ValueError:
        return response.status_code, {
            "error": "non_json_body",
            "body": response.text[:1000],
        }


def extract_results(page_payload: dict) -> list:
    """
    Dig the listing array out of the response. The Zillow scraper has used
    several wrappers across versions: top-level `props`, `results`, or
    `data.results`. Try each in turn.
    """
    if not isinstance(page_payload, dict):
        return []
    for key in ("props", "results", "listings"):
        value = page_payload.get(key)
        if isinstance(value, list):
            return value
    data = page_payload.get("data")
    if isinstance(data, dict):
        for key in ("results", "props", "listings"):
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []

# COMMAND ----------

# DBTITLE 1,Map one Zillow result to a bronze row
def map_listing(item: dict, page_payload: dict, scraped_at: datetime) -> dict:
    """
    Translate a single Zillow result into the `bronze.listings_us` schema.

    `raw_json` is the *full* per-page API payload (json.dumps) so the row
    retains complete provenance for replay. The full payload is intentionally
    duplicated per row — bronze storage is cheap and downstream notebooks may
    rely on auxiliary fields (e.g. provider pagination metadata).
    """
    listing_id = _safe_str(
        item.get("zpid")
        or item.get("listingId")
        or item.get("id")
        or item.get("propertyId")
    )

    home_type_raw = (item.get("homeType") or item.get("propertyType") or "").upper()
    property_type = PROPERTY_TYPE_MAP.get(home_type_raw)

    price_usd = _safe_int(item.get("price") or item.get("listPrice"))
    sqft = _safe_int(item.get("livingArea") or item.get("sqft") or item.get("livingAreaValue"))

    return {
        "listing_id": listing_id,
        "source": "rapidapi_zillow",
        "scraped_at": scraped_at,
        "address": _safe_str(item.get("address") or item.get("streetAddress")),
        "city": _safe_str(item.get("city")),
        "state": _safe_str(item.get("state") or item.get("stateAbbreviation")),
        "zip": _safe_str(item.get("zipcode") or item.get("postalCode") or item.get("zip")),
        "lat": _safe_float(item.get("latitude") or item.get("lat")),
        "lon": _safe_float(item.get("longitude") or item.get("lon") or item.get("lng")),
        "price_usd": price_usd,
        "bedrooms": _safe_int(item.get("bedrooms") or item.get("beds")),
        "bathrooms": _safe_float(item.get("bathrooms") or item.get("baths")),
        "sqft": sqft,
        "price_per_sqft_usd": _price_per_sqft(price_usd, sqft),
        "property_type": property_type,
        "description": _safe_str(item.get("description") or item.get("homeStatus") or ""),
        "listing_url": _safe_str(item.get("detailUrl") or item.get("hdpUrl") or item.get("url")),
        "days_on_market": _safe_int(item.get("daysOnZillow") or item.get("daysOnMarket")),
        "listing_date": _parse_listing_date(
            item.get("listingDate") or item.get("datePosted") or item.get("listingDateMs")
        ),
        "raw_json": json.dumps(page_payload, ensure_ascii=False, default=str),
    }


def is_for_sale(item: dict) -> bool:
    """
    Belt-and-suspenders filter so rentals never reach bronze, even if the
    provider ignores our `status_type=ForSale` query param.
    """
    status_type = (item.get("statusType") or item.get("listingStatus") or "").upper()
    home_status = (item.get("homeStatus") or "").upper()
    listing_type = (item.get("listingType") or "").upper()

    # Reject obvious rentals first.
    rental_markers = ("FOR_RENT", "RENT", "RENTAL")
    for value in (status_type, home_status, listing_type):
        if any(marker in value for marker in rental_markers):
            return False

    # Accept anything explicitly tagged FOR_SALE / FORSALE / SALE.
    sale_markers = ("FOR_SALE", "FORSALE", "SALE")
    for value in (status_type, home_status, listing_type):
        if any(marker in value for marker in sale_markers):
            return True

    # If the provider returns no status markers at all (older response
    # shapes), trust the `status_type=ForSale` query and keep the row.
    if not any((status_type, home_status, listing_type)):
        return True

    return False

# COMMAND ----------

# DBTITLE 1,Upsert into bronze.listings_us
def upsert_listings(rows: list) -> tuple[int, int]:
    """
    MERGE the parsed rows into `realestate.bronze.listings_us`.

    On match: refresh `price_usd`, `price_per_sqft_usd`, `days_on_market`,
    and `scraped_at` (per spec). On no-match: insert the full row.

    Returns:
        (inserted_count, updated_count).
    """
    if not rows:
        return (0, 0)

    # Drop rows without a usable listing_id — they can't be merged safely.
    valid_rows = [r for r in rows if r.get("listing_id")]
    if not valid_rows:
        return (0, 0)

    # Deduplicate within this batch so MERGE doesn't see the same listing_id
    # multiple times on the source side (Delta would error otherwise).
    by_id = {}
    for row in valid_rows:
        by_id[row["listing_id"]] = row
    deduped = list(by_id.values())

    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType,
        DoubleType, LongType, IntegerType, DateType,
    )

    schema = StructType([
        StructField("listing_id", StringType(), False),
        StructField("source", StringType(), True),
        StructField("scraped_at", TimestampType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("price_usd", LongType(), True),
        StructField("bedrooms", IntegerType(), True),
        StructField("bathrooms", DoubleType(), True),
        StructField("sqft", IntegerType(), True),
        StructField("price_per_sqft_usd", DoubleType(), True),
        StructField("property_type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("listing_url", StringType(), True),
        StructField("days_on_market", IntegerType(), True),
        StructField("listing_date", DateType(), True),
        StructField("raw_json", StringType(), True),
    ])

    source_df = spark.createDataFrame(deduped, schema=schema)
    source_df.createOrReplaceTempView("_listings_us_stage")

    # How many of the staged IDs already exist in the target table? That's
    # the "updated" count for logging. Everything else is an insert.
    existing_count = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {TARGET_TABLE} AS tgt
        WHERE tgt.listing_id IN (SELECT listing_id FROM _listings_us_stage)
    """).first()["n"]

    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS tgt
        USING _listings_us_stage AS src
        ON tgt.listing_id = src.listing_id
        WHEN MATCHED THEN UPDATE SET
            tgt.price_usd          = src.price_usd,
            tgt.price_per_sqft_usd = src.price_per_sqft_usd,
            tgt.days_on_market     = src.days_on_market,
            tgt.scraped_at         = src.scraped_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    total = len(deduped)
    updated = int(existing_count or 0)
    inserted = total - updated
    return (max(inserted, 0), updated)

# COMMAND ----------

# DBTITLE 1,Determine target cities for this run
# Refresh every city we've ever ingested, plus the example list. Distinct
# (city, state) tuples are taken from bronze so we can pass the city alone if
# state is missing.
existing_locations = spark.sql(f"""
    SELECT DISTINCT city, state
    FROM {TARGET_TABLE}
    WHERE city IS NOT NULL AND TRIM(city) != ''
""").collect()

locations_to_fetch = []
seen = set()

for row in existing_locations:
    city = (row["city"] or "").strip()
    state = (row["state"] or "").strip() if row["state"] else ""
    if not city:
        continue
    location = f"{city}, {state}" if state else city
    if location.lower() in seen:
        continue
    seen.add(location.lower())
    locations_to_fetch.append(location)

for city in EXAMPLE_CITIES:
    if city.lower() in seen:
        continue
    seen.add(city.lower())
    locations_to_fetch.append(city)

print(f"Cities to fetch this run ({len(locations_to_fetch)}):")
for loc in locations_to_fetch:
    print(f"  - {loc}")

# COMMAND ----------

# DBTITLE 1,Ingest each city
run_summary = []
total_inserted = 0
total_updated = 0
total_filtered = 0

for location in locations_to_fetch:
    print(f"\n=== {location} ===")
    city_inserted = 0
    city_updated = 0
    city_filtered = 0
    city_errors = []

    for page in range(1, MAX_PAGES_PER_CITY + 1):
        try:
            time.sleep(REQUEST_DELAY_SECONDS)
            status_code, payload = fetch_listings_page(location, page)
            print(f"  page {page}: HTTP {status_code}")

            if status_code != 200:
                city_errors.append(f"page {page}: HTTP {status_code}")
                break

            results = extract_results(payload)
            if not results:
                print(f"  page {page}: no results — stopping pagination")
                break

            scraped_at = datetime.now(timezone.utc).replace(tzinfo=None)
            rows = []
            for item in results:
                if not isinstance(item, dict):
                    continue
                if not is_for_sale(item):
                    city_filtered += 1
                    continue
                rows.append(map_listing(item, payload, scraped_at))

            inserted, updated = upsert_listings(rows)
            city_inserted += inserted
            city_updated += updated
            print(f"  page {page}: parsed={len(rows)} inserted={inserted} updated={updated}")

            # If the provider gave us fewer than a typical page-worth, assume
            # we've hit the end and avoid burning more requests.
            if len(results) < 20:
                print(f"  page {page}: short page — assuming end of results")
                break

        except Exception:
            err = traceback.format_exc()
            print(f"  page {page}: FAILED\n{err.splitlines()[-1]}")
            city_errors.append(f"page {page}: {err.splitlines()[-1]}")
            break

    total_inserted += city_inserted
    total_updated += city_updated
    total_filtered += city_filtered

    run_summary.append({
        "location": location,
        "inserted": int(city_inserted),
        "updated": int(city_updated),
        "rentals_filtered": int(city_filtered),
        "errors": "; ".join(city_errors) if city_errors else "",
    })

# COMMAND ----------

# DBTITLE 1,Run summary
from pyspark.sql.types import StructType, StructField, StringType, LongType

summary_schema = StructType([
    StructField("location", StringType(), True),
    StructField("inserted", LongType(), True),
    StructField("updated", LongType(), True),
    StructField("rentals_filtered", LongType(), True),
    StructField("errors", StringType(), True),
])
summary_df = spark.createDataFrame(run_summary, schema=summary_schema)
display(summary_df.orderBy("location"))

print(f"\nTotal new listings inserted: {total_inserted}")
print(f"Total existing listings updated: {total_updated}")
print(f"Total rentals filtered out: {total_filtered}")
print(f"Cities processed: {len(run_summary)}")

if total_inserted == 0 and total_updated == 0:
    print(
        "WARNING: zero listings ingested. "
        "Check the RapidAPI dashboard for the current search endpoint path "
        f"(currently set to {RAPIDAPI_SEARCH_PATH}) and that RAPIDAPI_KEY is valid."
    )

# COMMAND ----------

# DBTITLE 1,Most recent bronze.listings_us rows
display(
    spark.sql(f"""
        SELECT
            listing_id,
            city,
            state,
            price_usd,
            bedrooms,
            bathrooms,
            sqft,
            price_per_sqft_usd,
            property_type,
            days_on_market,
            scraped_at
        FROM {TARGET_TABLE}
        ORDER BY scraped_at DESC
        LIMIT 25
    """)
)
