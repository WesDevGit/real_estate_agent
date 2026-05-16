# Databricks notebook source
# DBTITLE 1,Install scraping deps
# MAGIC %pip install beautifulsoup4 fake-useragent
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Ingest Colombia listings (Finca Raiz + Metrocuadrado)
# MAGIC %md
# MAGIC # 02 — Ingest Colombia Listings (Fincaraiz + Metrocuadrado)
# MAGIC
# MAGIC Scrapes active home-purchase listings from Finca Raiz and Metrocuadrado for
# MAGIC Bogota and Medellin into `realestate.bronze.listings_co`.
# MAGIC
# MAGIC **Sources:**
# MAGIC * `fincaraiz`     — https://www.fincaraiz.com.co/casas-y-apartamentos-en-venta/{city}/?pagina=N
# MAGIC * `metrocuadrado` — https://www.metrocuadrado.com/inmuebles/venta/casa+apartamento/{city}/?pagina=N
# MAGIC
# MAGIC **Anti-scraping rules implemented here:**
# MAGIC * Rotate `User-Agent` on every request via `fake_useragent`.
# MAGIC * Sleep `random.uniform(1.5, 3.0)` seconds between page fetches.
# MAGIC * Exponential backoff on HTTP 429 / 503 (3 retries: 5s, 15s, 45s).
# MAGIC
# MAGIC **Idempotency:** before insert, each `listing_id` is checked against the
# MAGIC existing bronze table. Already-known listings are refreshed (price /
# MAGIC scraped_at / cleared `raw_html`); new listings get a full insert with the
# MAGIC gzip+base64-encoded raw card HTML retained for debugging.
# MAGIC
# MAGIC **Schedule:** Daily.
# MAGIC
# MAGIC > **Selector caveat:** the two sites change their DOM frequently and use
# MAGIC > obfuscated CSS class names. The extraction code below targets stable
# MAGIC > `data-*` attributes and semantic HTML where possible, and tolerates
# MAGIC > empty selector matches (it skips and logs the URL rather than crashing).
# MAGIC > If the row counts in the run summary drop to zero, inspect the live
# MAGIC > HTML and update the selector blocks in `_parse_fincaraiz_card` /
# MAGIC > `_parse_metrocuadrado_card`.
# MAGIC >
# MAGIC > If either site starts returning CAPTCHAs consistently, switch to a
# MAGIC > headless-browser approach (Selenium or Playwright on a cluster with a
# MAGIC > browser driver). Not implemented preemptively.

# COMMAND ----------

# DBTITLE 1,Load shared helpers
# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports specific to this notebook
import base64
import gzip
import random
import re
import time
import traceback
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType, TimestampType
)

# COMMAND ----------

# DBTITLE 1,Use catalog
spark.sql("USE CATALOG realestate")

# COMMAND ----------

# DBTITLE 1,Config
PIPELINE_NAME = "02_ingest_listings_co"
BRONZE_TABLE = "realestate.bronze.listings_co"

# Cities supported by both scrapers. Slug is the URL-safe city name used in the
# target URLs; departamento is the Colombian state the city belongs to.
CITIES = [
    {"display": "Bogota",   "slug": "bogota",   "departamento": "Cundinamarca"},
    {"display": "Medellin", "slug": "medellin", "departamento": "Antioquia"},
]

# Polite-scraping knobs. Tuned for the two free public sites.
MAX_PAGES_PER_CITY = 20
SLEEP_RANGE_SECONDS = (1.5, 3.0)
BACKOFF_DELAYS_SECONDS = [5, 15, 45]  # 3 retries on 429 / 503
REQUEST_TIMEOUT_SECONDS = 30

# Shared User-Agent pool. fake_useragent occasionally fails to reach its CDN at
# startup; fall back to a static pool so the scraper still rotates.
try:
    _UA_POOL = UserAgent()
    _UA_POOL.random  # touch to ensure it works
    def random_user_agent() -> str:
        return _UA_POOL.random
except Exception:
    _STATIC_UA_POOL = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    def random_user_agent() -> str:
        return random.choice(_STATIC_UA_POOL)

print(f"Configured {len(CITIES)} cities x 2 sources for ingestion.")

# COMMAND ----------

# DBTITLE 1,Shared HTTP + parsing utilities
def _fetch_html(url: str) -> str:
    """
    Fetch a page with rotating UA, polite sleep, and exponential backoff on
    429 / 503. Returns the page HTML body. Raises requests.HTTPError after the
    final retry exhaustion; raises requests.RequestException for connection
    errors that cannot be retried.
    """
    last_response = None

    for attempt_idx in range(len(BACKOFF_DELAYS_SECONDS) + 1):
        headers = {
            "User-Agent": random_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
        }

        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
        last_response = response

        if response.status_code in (429, 503) and attempt_idx < len(BACKOFF_DELAYS_SECONDS):
            delay = BACKOFF_DELAYS_SECONDS[attempt_idx]
            print(f"    [retry] {response.status_code} from {url} -> sleeping {delay}s")
            time.sleep(delay)
            continue

        response.raise_for_status()
        return response.text

    # Exhausted retries — bubble up the last response's HTTPError
    last_response.raise_for_status()
    return ""  # unreachable, keeps type checkers happy


def _polite_sleep():
    time.sleep(random.uniform(*SLEEP_RANGE_SECONDS))


def _gzip_b64(html: str) -> str:
    """gzip-compress + base64-encode HTML for storage in raw_html."""
    if not html:
        return None
    compressed = gzip.compress(html.encode("utf-8"))
    return base64.b64encode(compressed).decode("ascii")


def _to_int(value) -> int:
    if value is None:
        return None
    s = re.sub(r"[^\d]", "", str(value))
    return int(s) if s else None


def _to_float(value) -> float:
    if value is None:
        return None
    # Colombian formatting often uses '.' as thousand separator and ',' as decimal.
    s = str(value).strip()
    # Strip everything but digits / separators
    s = re.sub(r"[^\d,\.]", "", s)
    if not s:
        return None
    # If both separators present, assume '.' is thousands and ',' is decimal
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # Lone comma — treat as decimal separator
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _existing_listing_ids(source: str, city_display: str) -> set:
    """
    Return the set of listing_ids already in bronze.listings_co for this
    (source, city) pair. Used for idempotent upsert decisions.
    """
    try:
        df = spark.sql(
            f"""
            SELECT listing_id
            FROM {BRONZE_TABLE}
            WHERE source = '{source}' AND city = '{city_display}'
            """
        )
        return {r["listing_id"] for r in df.collect() if r["listing_id"]}
    except Exception:
        # Table empty or first run — treat every id as new.
        return set()

# COMMAND ----------

# DBTITLE 1,Finca Raiz scraper
# Finca Raiz lists each property in a card. Stable hooks observed:
#   - <a href="/inmueble/..."> wraps the entire card (URL is the natural ID)
#   - <article data-id="..."> or <div data-listing-id="..."> on the card root
# Class names like "listingCard__..." are hashed and change frequently, so we
# fall back to text patterns when data-* hooks are missing.
def _parse_fincaraiz_card(card, base_url: str = "https://www.fincaraiz.com.co") -> dict:
    """
    Extract one listing's structured fields from a BeautifulSoup card element.
    Returns None if listing_id and listing_url cannot be derived (the card is
    probably an ad / placeholder).
    """
    # --- URL + listing_id ---
    link = card.find("a", href=re.compile(r"/inmueble/|/casa-en-venta/|/apartamento-en-venta/"))
    if link is None:
        link = card.find("a", href=True)
    if link is None or not link.get("href"):
        return None
    href = link["href"]
    listing_url = href if href.startswith("http") else f"{base_url}{href}"
    # Pull a stable slug-ish id out of the URL path.
    slug_match = re.search(r"/([\w-]+)/?(?:\?|$)", listing_url)
    listing_id = (
        card.get("data-id")
        or card.get("data-listing-id")
        or (slug_match.group(1) if slug_match else None)
    )
    if not listing_id:
        return None

    # --- Title ---
    title_el = card.find(["h1", "h2", "h3"]) or link
    title = title_el.get_text(strip=True) if title_el else None

    # --- Price (COP). Look for $ sign or "COP" near it. ---
    price_cop = None
    price_text_candidates = card.find_all(
        string=re.compile(r"\$|COP|\d[\.\,]?\d{3}")
    )
    for txt in price_text_candidates:
        ival = _to_int(txt)
        # Listing prices in COP are at least 7 digits (>= ~1M COP for any home).
        if ival and ival >= 10_000_000:
            price_cop = ival
            break

    # --- Bedrooms / bathrooms / area ---
    # Pattern: "<n> hab" / "<n> habitaciones" / "<n> baño" / "<n> m²"
    full_text = " ".join(card.stripped_strings)
    bed_m = re.search(r"(\d+)\s*(?:hab|habitaci)", full_text, flags=re.IGNORECASE)
    bath_m = re.search(r"(\d+(?:[\.,]\d+)?)\s*(?:ba[nñ])", full_text, flags=re.IGNORECASE)
    area_m = re.search(r"(\d+(?:[\.,]\d+)?)\s*m\s*(?:²|2)", full_text, flags=re.IGNORECASE)

    bedrooms = _to_int(bed_m.group(1)) if bed_m else None
    bathrooms = _to_float(bath_m.group(1)) if bath_m else None
    area_m2 = _to_float(area_m.group(1)) if area_m else None

    # --- Barrio / address ---
    barrio = None
    address = None
    loc_el = card.find(attrs={"data-testid": re.compile(r"location|address", re.IGNORECASE)})
    if loc_el:
        address = loc_el.get_text(separator=", ", strip=True)
    # Heuristic: barrio is the first segment of the address up to the first comma.
    if address:
        barrio = address.split(",")[0].strip() or None

    # --- Property type. Inferred from title text. ---
    property_type = None
    if title:
        tlow = title.lower()
        if "apartamento" in tlow or "apto" in tlow:
            property_type = "apartamento"
        elif "casa" in tlow:
            property_type = "casa"

    return {
        "listing_id": str(listing_id),
        "listing_url": listing_url,
        "title": title,
        "price_cop": price_cop,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "area_m2": area_m2,
        "barrio": barrio,
        "address": address,
        "property_type": property_type,
    }


def scrape_fincaraiz(city: str, max_pages: int = 20) -> list:
    """
    Scrape Finca Raiz listings for `city` (display name, e.g. 'Bogota').
    Returns a list of dicts in the canonical bronze schema (raw_html still raw
    HTML at this point — gzip+b64 encoding happens in the upsert step).

    Page pattern: /casas-y-apartamentos-en-venta/{slug}/?pagina={n}
    """
    city_cfg = next((c for c in CITIES if c["display"] == city), None)
    if city_cfg is None:
        raise ValueError(f"Unsupported city for fincaraiz: {city!r}")

    base = "https://www.fincaraiz.com.co"
    listings = []

    for page_num in range(1, max_pages + 1):
        url = f"{base}/casas-y-apartamentos-en-venta/{city_cfg['slug']}/?pagina={page_num}"
        try:
            html = _fetch_html(url)
        except requests.RequestException as e:
            print(f"    [fincaraiz] page {page_num} request failed: {e}")
            _polite_sleep()
            continue

        soup = BeautifulSoup(html, "html.parser")
        # Cards: prefer semantic <article>, then anchors that look like listings.
        cards = soup.find_all("article")
        if not cards:
            cards = [
                a.parent for a in soup.find_all(
                    "a", href=re.compile(r"/inmueble/|/casa-en-venta/|/apartamento-en-venta/")
                ) if a.parent is not None
            ]

        if not cards:
            print(f"    [fincaraiz] page {page_num} returned no listing cards. URL={url}")
            _polite_sleep()
            # If two consecutive empty pages, assume end of pagination.
            if page_num > 1:
                break
            else:
                _polite_sleep()
                continue

        page_count_before = len(listings)
        for card in cards:
            try:
                parsed = _parse_fincaraiz_card(card)
            except Exception:
                parsed = None
                print(f"    [fincaraiz] parse failure on {url}")
            if parsed is None:
                continue
            parsed["raw_card_html"] = str(card)
            parsed["parse_ok"] = all(
                parsed.get(k) is not None for k in ("price_cop", "bedrooms", "area_m2")
            )
            parsed["city"] = city_cfg["display"]
            parsed["departamento"] = city_cfg["departamento"]
            listings.append(parsed)

        page_added = len(listings) - page_count_before
        print(f"    [fincaraiz] page {page_num}: {page_added} listing(s) parsed")

        _polite_sleep()

    return listings

# COMMAND ----------

# DBTITLE 1,Metrocuadrado scraper
# Metrocuadrado renders cards inside <div class="card-result"> historically, but
# class names rotate. We anchor on <a href="/inmueble/..."> and the data-id
# attribute when present.
def _parse_metrocuadrado_card(card, base_url: str = "https://www.metrocuadrado.com") -> dict:
    link = card.find("a", href=re.compile(r"/inmueble/|/casa/|/apartamento/"))
    if link is None:
        link = card.find("a", href=True)
    if link is None or not link.get("href"):
        return None
    href = link["href"]
    listing_url = href if href.startswith("http") else f"{base_url}{href}"

    # listing_id from URL slug (Metrocuadrado uses a trailing numeric id often).
    id_match = re.search(r"/(\d{6,})(?:/?$|\?)", listing_url)
    slug_match = re.search(r"/([\w-]+)/?(?:\?|$)", listing_url)
    listing_id = (
        card.get("data-id")
        or card.get("data-listing-id")
        or (id_match.group(1) if id_match else None)
        or (slug_match.group(1) if slug_match else None)
    )
    if not listing_id:
        return None

    title_el = card.find(["h1", "h2", "h3"]) or link
    title = title_el.get_text(strip=True) if title_el else None

    full_text = " ".join(card.stripped_strings)

    # Price
    price_cop = None
    for txt in card.find_all(string=re.compile(r"\$|COP|\d[\.\,]?\d{3}")):
        ival = _to_int(txt)
        if ival and ival >= 10_000_000:
            price_cop = ival
            break

    bed_m = re.search(r"(\d+)\s*(?:hab|habitaci|alcoba)", full_text, flags=re.IGNORECASE)
    bath_m = re.search(r"(\d+(?:[\.,]\d+)?)\s*(?:ba[nñ])", full_text, flags=re.IGNORECASE)
    area_m = re.search(r"(\d+(?:[\.,]\d+)?)\s*m\s*(?:²|2)", full_text, flags=re.IGNORECASE)

    bedrooms = _to_int(bed_m.group(1)) if bed_m else None
    bathrooms = _to_float(bath_m.group(1)) if bath_m else None
    area_m2 = _to_float(area_m.group(1)) if area_m else None

    address = None
    barrio = None
    loc_el = card.find(attrs={"data-testid": re.compile(r"location|address", re.IGNORECASE)})
    if loc_el is None:
        loc_el = card.find(class_=re.compile(r"location|address|barrio", re.IGNORECASE))
    if loc_el:
        address = loc_el.get_text(separator=", ", strip=True)
    if address:
        barrio = address.split(",")[0].strip() or None

    property_type = None
    if title:
        tlow = title.lower()
        if "apartamento" in tlow or "apto" in tlow:
            property_type = "apartamento"
        elif "casa" in tlow:
            property_type = "casa"

    return {
        "listing_id": str(listing_id),
        "listing_url": listing_url,
        "title": title,
        "price_cop": price_cop,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "area_m2": area_m2,
        "barrio": barrio,
        "address": address,
        "property_type": property_type,
    }


def scrape_metrocuadrado(city: str, max_pages: int = 20) -> list:
    """
    Scrape Metrocuadrado listings for `city` (display name, e.g. 'Bogota').
    Page pattern: /inmuebles/venta/casa+apartamento/{slug}/?pagina={n}
    """
    city_cfg = next((c for c in CITIES if c["display"] == city), None)
    if city_cfg is None:
        raise ValueError(f"Unsupported city for metrocuadrado: {city!r}")

    base = "https://www.metrocuadrado.com"
    listings = []

    for page_num in range(1, max_pages + 1):
        url = f"{base}/inmuebles/venta/casa+apartamento/{city_cfg['slug']}/?pagina={page_num}"
        try:
            html = _fetch_html(url)
        except requests.RequestException as e:
            print(f"    [metrocuadrado] page {page_num} request failed: {e}")
            _polite_sleep()
            continue

        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all("article")
        if not cards:
            cards = soup.find_all(attrs={"data-testid": re.compile(r"card|result", re.IGNORECASE)})
        if not cards:
            cards = [
                a.parent for a in soup.find_all(
                    "a", href=re.compile(r"/inmueble/|/casa/|/apartamento/")
                ) if a.parent is not None
            ]

        if not cards:
            print(f"    [metrocuadrado] page {page_num} returned no listing cards. URL={url}")
            _polite_sleep()
            if page_num > 1:
                break
            else:
                _polite_sleep()
                continue

        page_count_before = len(listings)
        for card in cards:
            try:
                parsed = _parse_metrocuadrado_card(card)
            except Exception:
                parsed = None
                print(f"    [metrocuadrado] parse failure on {url}")
            if parsed is None:
                continue
            parsed["raw_card_html"] = str(card)
            parsed["parse_ok"] = all(
                parsed.get(k) is not None for k in ("price_cop", "bedrooms", "area_m2")
            )
            parsed["city"] = city_cfg["display"]
            parsed["departamento"] = city_cfg["departamento"]
            listings.append(parsed)

        page_added = len(listings) - page_count_before
        print(f"    [metrocuadrado] page {page_num}: {page_added} listing(s) parsed")

        _polite_sleep()

    return listings

# COMMAND ----------

# DBTITLE 1,Upsert helper
# Bronze schema mirrors realestate.bronze.listings_co exactly.
LISTINGS_CO_SCHEMA = StructType([
    StructField("listing_id",   StringType(),    True),
    StructField("source",       StringType(),    True),
    StructField("scraped_at",   TimestampType(), True),
    StructField("address",      StringType(),    True),
    StructField("city",         StringType(),    True),
    StructField("departamento", StringType(),    True),
    StructField("barrio",       StringType(),    True),
    StructField("lat",          DoubleType(),    True),
    StructField("lon",          DoubleType(),    True),
    StructField("price_cop",    LongType(),      True),
    StructField("bedrooms",     IntegerType(),   True),
    StructField("bathrooms",    DoubleType(),    True),
    StructField("area_m2",      DoubleType(),    True),
    StructField("property_type",StringType(),    True),
    StructField("description",  StringType(),    True),
    StructField("listing_url",  StringType(),    True),
    StructField("raw_html",     StringType(),    True),
])


def upsert_listings(parsed_listings: list, source: str, city_display: str) -> dict:
    """
    Idempotent upsert into bronze.listings_co.

    Splits the incoming batch into:
      * inserts        — listing_ids not yet in the bronze table; raw_html is
                         kept (gzip+b64) for debugging.
      * parse_failures — rows with missing required fields; treated as inserts
                         but retain raw_html for triage even on refresh.
      * refreshes      — listing_ids already present; raw_html is cleared
                         (NULL), only price/scraped_at-style fields are
                         refreshed via a MERGE update.

    Returns a {"inserted", "refreshed", "parse_failures", "skipped"} count dict.
    """
    if not parsed_listings:
        return {"inserted": 0, "refreshed": 0, "parse_failures": 0, "skipped": 0}

    existing_ids = _existing_listing_ids(source, city_display)

    insert_rows = []
    refresh_rows = []
    parse_failure_count = 0
    skipped = 0
    now_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    for parsed in parsed_listings:
        listing_id = parsed.get("listing_id")
        if not listing_id:
            skipped += 1
            continue

        is_known = listing_id in existing_ids
        parse_ok = parsed.get("parse_ok", False)
        raw_card_html = parsed.get("raw_card_html") or ""

        # raw_html rule: keep on first insert and on parse failure; NULL on routine refresh.
        if not is_known or not parse_ok:
            raw_html_value = _gzip_b64(raw_card_html)
            if not parse_ok:
                parse_failure_count += 1
        else:
            raw_html_value = None

        row = {
            "listing_id":   str(listing_id),
            "source":       source,
            "scraped_at":   now_ts,
            "address":      parsed.get("address"),
            "city":         city_display,
            "departamento": parsed.get("departamento"),
            "barrio":       parsed.get("barrio"),
            "lat":          None,  # geocoded later in silver build
            "lon":          None,
            "price_cop":    parsed.get("price_cop"),
            "bedrooms":     parsed.get("bedrooms"),
            "bathrooms":    parsed.get("bathrooms"),
            "area_m2":      parsed.get("area_m2"),
            "property_type":parsed.get("property_type"),
            "description":  parsed.get("title"),  # cards rarely have a separate description
            "listing_url":  parsed.get("listing_url"),
            "raw_html":     raw_html_value,
        }

        if is_known:
            refresh_rows.append(row)
        else:
            insert_rows.append(row)

    # Build dataframes; deduplicate by listing_id within batch (last write wins).
    def _dedupe(rows):
        out, seen = [], set()
        for r in reversed(rows):
            if r["listing_id"] in seen:
                continue
            seen.add(r["listing_id"])
            out.append(r)
        return list(reversed(out))

    insert_rows = _dedupe(insert_rows)
    refresh_rows = _dedupe(refresh_rows)

    if insert_rows:
        df = spark.createDataFrame([Row(**r) for r in insert_rows], schema=LISTINGS_CO_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

    if refresh_rows:
        df = spark.createDataFrame([Row(**r) for r in refresh_rows], schema=LISTINGS_CO_SCHEMA)
        df.createOrReplaceTempView("_staged_listings_co_refresh")
        # MERGE — only update mutable fields, never overwrite lat/lon (geocoded
        # downstream) or raw_html (cleared above on refresh).
        spark.sql(f"""
            MERGE INTO {BRONZE_TABLE} AS t
            USING _staged_listings_co_refresh AS s
              ON t.listing_id = s.listing_id AND t.source = s.source
            WHEN MATCHED THEN UPDATE SET
                t.scraped_at    = s.scraped_at,
                t.address       = s.address,
                t.barrio        = s.barrio,
                t.price_cop     = s.price_cop,
                t.bedrooms      = s.bedrooms,
                t.bathrooms     = s.bathrooms,
                t.area_m2       = s.area_m2,
                t.property_type = s.property_type,
                t.description   = s.description,
                t.listing_url   = s.listing_url,
                t.raw_html      = s.raw_html
        """)
        spark.catalog.dropTempView("_staged_listings_co_refresh")

    return {
        "inserted": len(insert_rows),
        "refreshed": len(refresh_rows),
        "parse_failures": parse_failure_count,
        "skipped": skipped,
    }

# COMMAND ----------

# DBTITLE 1,Run both scrapers for both cities
SCRAPERS = [
    ("fincaraiz",     scrape_fincaraiz),
    ("metrocuadrado", scrape_metrocuadrado),
]

results = []

for source_name, scrape_fn in SCRAPERS:
    for city_cfg in CITIES:
        city_display = city_cfg["display"]
        print(f"\n=== {source_name} :: {city_display} ===")
        start = datetime.now(timezone.utc)
        try:
            parsed_listings = scrape_fn(city_display, max_pages=MAX_PAGES_PER_CITY)
            counts = upsert_listings(parsed_listings, source=source_name, city_display=city_display)
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            print(
                f"  -> scraped={len(parsed_listings)} | "
                f"inserted={counts['inserted']} | refreshed={counts['refreshed']} | "
                f"parse_failures={counts['parse_failures']} | skipped={counts['skipped']} | "
                f"elapsed={elapsed:.1f}s"
            )
            results.append({
                "source": source_name,
                "city": city_display,
                "scraped": len(parsed_listings),
                "inserted": counts["inserted"],
                "refreshed": counts["refreshed"],
                "parse_failures": counts["parse_failures"],
                "skipped": counts["skipped"],
                "status": "SUCCESS",
                "error_message": "",
                "elapsed_seconds": float(elapsed),
            })
        except Exception:
            err = traceback.format_exc()
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            print(f"  ! FAILED: {err.splitlines()[-1]}")
            results.append({
                "source": source_name,
                "city": city_display,
                "scraped": 0,
                "inserted": 0,
                "refreshed": 0,
                "parse_failures": 0,
                "skipped": 0,
                "status": "FAILED",
                "error_message": err,
                "elapsed_seconds": float(elapsed),
            })

# COMMAND ----------

# DBTITLE 1,Run summary
summary_schema = StructType([
    StructField("source",          StringType(), True),
    StructField("city",            StringType(), True),
    StructField("scraped",         LongType(),   True),
    StructField("inserted",        LongType(),   True),
    StructField("refreshed",       LongType(),   True),
    StructField("parse_failures",  LongType(),   True),
    StructField("skipped",         LongType(),   True),
    StructField("status",          StringType(), True),
    StructField("error_message",   StringType(), True),
    StructField("elapsed_seconds", DoubleType(), True),
])

results_df = spark.createDataFrame(results, schema=summary_schema)
display(results_df.orderBy("source", "city"))

total_scraped = sum(r["scraped"] for r in results)
total_inserted = sum(r["inserted"] for r in results)
total_refreshed = sum(r["refreshed"] for r in results)
total_parse_failures = sum(r["parse_failures"] for r in results)
success_count = sum(1 for r in results if r["status"] == "SUCCESS")

print(f"\nScraper runs attempted: {len(results)}")
print(f"Scraper runs successful: {success_count}")
print(f"Listings scraped: {total_scraped}")
print(f"  inserted (new):       {total_inserted}")
print(f"  refreshed (existing): {total_refreshed}")
print(f"  parse failures:       {total_parse_failures}")

if success_count == 0:
    raise RuntimeError(
        "All scraper runs failed. Inspect run summary table, then "
        "check network egress and live HTML selectors."
    )

if total_scraped == 0:
    print(
        "\nWARNING: all scrapers ran without errors but parsed zero listings. "
        "This usually means the site HTML structure has changed. Inspect the "
        "live HTML of one of the target URLs and update the parse selectors in "
        "_parse_fincaraiz_card / _parse_metrocuadrado_card."
    )

# COMMAND ----------

# DBTITLE 1,Most recent bronze rows
display(
    spark.sql(f"""
        SELECT
            source,
            city,
            listing_id,
            price_cop,
            bedrooms,
            bathrooms,
            area_m2,
            property_type,
            scraped_at,
            CASE WHEN raw_html IS NULL THEN 0 ELSE length(raw_html) END AS raw_html_bytes_b64
        FROM {BRONZE_TABLE}
        ORDER BY scraped_at DESC
        LIMIT 20
    """)
)

# COMMAND ----------

# DBTITLE 1,Counts by (source, city)
display(
    spark.sql(f"""
        SELECT
            source,
            city,
            COUNT(*)                                     AS total_rows,
            COUNT(price_cop)                             AS rows_with_price,
            SUM(CASE WHEN raw_html IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_raw_html
        FROM {BRONZE_TABLE}
        GROUP BY source, city
        ORDER BY source, city
    """)
)
