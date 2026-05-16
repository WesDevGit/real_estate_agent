# Real Estate Agent — Databricks Build Guide

A step-by-step walkthrough for deploying the real estate agent into your Databricks
workspace. Every step explains what's happening, what to expect, and how to verify it worked.

---

## 0. Prerequisites

You need:

| Item | Why |
|---|---|
| Databricks workspace with **Unity Catalog enabled** | All tables live in catalog `realestate` |
| A cluster with **DBR 14.3 LTS** or newer (Photon optional) | Required for Delta CDF and Unity Catalog volume support |
| Cluster permission to **create catalogs / schemas / volumes** | First-run only |
| All 5 API keys stored in Databricks Secrets scope `realestate` | `RAPIDAPI_KEY`, `FBI_API_KEY`, `CENSUS_API_KEY`, `FRED_API_KEY`, `BLS_API_KEY` |

If you haven't stored the secrets yet, see `AGENT_BUILD_INSTRUCTIONS.md` → "Step 3 — Store
Each Key in the Scope" for the `dbutils.secrets.put(...)` snippets.

**Verify secrets are in place** before starting:

```python
scope = "realestate"
expected = ["RAPIDAPI_KEY", "FBI_API_KEY", "CENSUS_API_KEY", "FRED_API_KEY", "BLS_API_KEY"]
stored = [s.key for s in dbutils.secrets.list(scope)]
for k in expected:
    print(f"{'OK' if k in stored else 'MISSING':8s}  {k}")
```

**Expected output:** Five `OK` lines. If any show `MISSING`, store that key before continuing.

---

## 1. Import the notebooks into Databricks

1. In your workspace, create a folder (e.g., `/Workspace/Users/<you>/real_estate_agent`).
2. Upload all 28 `.py` files from `real_estate_agent/` into that folder.
   - Databricks auto-detects the `# Databricks notebook source` header and treats them as notebooks.
3. Verify the file count: you should see 28 notebooks plus `AGENT_BUILD_INSTRUCTIONS.md` and this `BUILD_GUIDE.md`.

**Why this matters:** every notebook uses `%run ./99_helpers` and `%run ./40_agent_tools`
with relative paths. They must sit in the same folder.

---

## 2. Phase 0 — Foundation

### Step 2.1 — Run `00_setup_schema.py`

**What it does:** Creates the catalog `realestate`, the three schemas (`bronze`, `silver`,
`gold`), the volume `realestate.bronze.raw` for operator-staged files, and all 28 Delta
tables with their full column schemas. Enables Change Data Feed on Silver and Gold tables.

**How to run:** Click "Run all" on the notebook.

**Expected output (last cell):**
```
== Catalog and schemas ==
realestate / bronze
realestate / silver
realestate / gold

== Tables created ==
schema   table                          columns
bronze   listings_us                    17
bronze   listings_co                    16
bronze   weather_history                13
bronze   crime_us                       11
bronze   crime_co                       10
...
silver   listings                       20
silver   neighborhood_profile           21
...
gold     neighborhood_scorecard         24
gold     market_trends                  14
gold     city_comparison                23
gold     hazard_risk                    12
gold     school_rankings                13
gold     value_opportunities            varies
gold     comparison_index               11
gold     agent_sessions                 15

Total: 16 bronze, 4 silver, 8 gold tables (gold.listing_map is a view, built in step 6.6)
```

**Validation queries:**
```sql
SHOW SCHEMAS IN realestate;
-- expect: bronze, silver, gold

SELECT COUNT(*) FROM (SHOW TABLES IN realestate.bronze);
-- expect: 16

SELECT COUNT(*) FROM (SHOW TABLES IN realestate.silver);
-- expect: 4

SELECT COUNT(*) FROM (SHOW TABLES IN realestate.gold);
-- expect: 8 (gold.listing_map view is added later)
```

**Common issues:**
- *"CREATE CATALOG denied"*: your Databricks user lacks `CREATE CATALOG` on the metastore.
  Ask an admin to create the catalog and re-run skipping the catalog DDL.
- *"VOLUME already exists"*: harmless. The notebook uses `IF NOT EXISTS`.

---

### Step 2.2 — Run `99_helpers.py`

**What it does:** Defines shared utility functions (`get_secret`, `geocode_address`,
`usd_to_cop`, `cop_to_usd`, `clean_limit`, `extract_json_object`, `none_if_nullish`).
Other notebooks load these via `%run ./99_helpers`.

**How to run:** Click "Run all".

**Expected output (last cell):**
```
Real estate helpers loaded. Secrets scope = realestate
```

**Validation:** Run a one-off cell at the bottom:
```python
print(clean_limit(99, default=10, low=1, high=50))  # expect: 50
print(clean_limit("abc"))                            # expect: 10
print(none_if_nullish("N/A"))                        # expect: None
```

---

## 3. Phase 1 — Bronze Ingestion (priority order)

Run these in the order below. **Do not skip 13 first** — Silver listings depends on the
COP/USD rate it produces.

### Step 3.1 — `13_ingest_exchange_rates.py`

**What it does:** Fetches COP→USD exchange rates from frankfurter.app (no API key needed).
First run pulls 2 years of history (skipping weekends, ~500 rows). Subsequent runs are
incremental from `max(date) + 1` to yesterday.

**How to run:** Click "Run all". First run takes ~3 minutes (one HTTP call per weekday).

**Expected output:**
```
Bootstrapping: fetching 730 days of history
Inserted 521 new rate rows (skipped 0 dates without data).

+----------+---------+
|rate_date | rate    |
+----------+---------+
|2026-05-14|3958.21  |
|2026-05-13|3961.07  |
|2026-05-12|3970.40  |
|2026-05-09|3982.13  |
|2026-05-08|3985.51  |
+----------+---------+
```

The exact rate values will differ — they're the live COP/USD rate (COP per 1 USD,
typically 3,800–4,300 as of 2026).

**Validation:**
```sql
SELECT COUNT(*) AS row_count,
       MIN(rate_date) AS earliest,
       MAX(rate_date) AS latest,
       AVG(rate) AS avg_cop_per_usd
FROM realestate.bronze.exchange_rates
WHERE from_currency = 'COP' AND to_currency = 'USD';
```
Expect ~500 rows, `earliest` ~2 years ago, `latest` yesterday, `avg_cop_per_usd` between 3000 and 5000.

---

### Step 3.2 — `01_ingest_listings_us.py`

**What it does:** Hits the RapidAPI Zillow scraper for active for-sale listings in the
default cities (Austin, Miami, Atlanta, Houston). Upserts into `bronze.listings_us`.

**How to run:** Click "Run all". Takes 2–5 minutes depending on RapidAPI response speed.

**Expected output:**
```
Target table : realestate.bronze.listings_us
Endpoint     : https://zillow-com-live-data-scraper-api.p.rapidapi.com/propertyExtendedSearch
Example cities: ['Austin', 'Miami', 'Atlanta', 'Houston']
[Austin]   page 1: 40 results
[Austin]   page 2: 40 results
...
[Houston]  page 5: 38 results
Upserted 712 listings (new: 712, updated: 0)
```

**Validation:**
```sql
SELECT city, state, COUNT(*) AS listings, AVG(price_usd)::BIGINT AS avg_price
FROM realestate.bronze.listings_us
GROUP BY city, state
ORDER BY listings DESC;
```
Expect a row per city with `listings` between 100 and 200, `avg_price` between $200K and $1M.

**Common issues:**
- *HTTP 403/401 from RapidAPI*: your key is invalid or you didn't subscribe to the
  `zillow-com-live-data-scraper-api` listing on RapidAPI. Subscribe at rapidapi.com.
- *HTTP 429 too many requests*: you've exhausted your free quota for the month
  (default ~500 requests). Wait for monthly reset or upgrade the plan.
- *Empty results*: the endpoint path may have changed. Inspect the RapidAPI test console
  for the current city-search endpoint and update `RAPIDAPI_SEARCH_PATH` in cell 4.

---

### Step 3.3 — `03_ingest_weather.py`

**What it does:** Fetches 10 years of daily weather history from Open-Meteo (no key) for
Bogotá, Medellín, and every US city present in `bronze.listings_us`.

**How to run:** Click "Run all". First run takes 5–10 minutes (~25,000 rows per city × 6 cities).

**Expected output:**
```
Bogota_CO: fetching 2016-05-15 → 2026-05-14
  inserted 3653 rows
Medellin_CO: fetching 2016-05-15 → 2026-05-14
  inserted 3653 rows
Austin_US: fetching 2016-05-15 → 2026-05-14
  inserted 3653 rows
...

Total inserted: 21918
```

**Validation:**
```sql
SELECT location_key, COUNT(*) AS days, MIN(date) AS first_date, MAX(date) AS last_date,
       SUM(CASE WHEN is_extreme_day THEN 1 ELSE 0 END) AS extreme_day_count
FROM realestate.bronze.weather_history
GROUP BY location_key
ORDER BY location_key;
```
Expect each location to have ~3,650 days of history and 50–500 `extreme_day_count`.

---

### Step 3.4 — `10_ingest_amenities.py`

**What it does:** Queries OpenStreetMap Overpass API for grocery, hospital, pharmacy,
school, restaurant, park, and transit stops within each city's bounding box. No key.

**How to run:** Click "Run all". Takes 5–15 minutes (Overpass is rate-limited; sleeps 2s between cities).

**Expected output:**
```
Fetching amenities for 6 cities

Bogota (CO)
  upserted 8431 amenities: {'grocery': 1203, 'park': 412, 'hospital': 187, ...}

Medellin (CO)
  upserted 5276 amenities: {'grocery': 692, 'park': 188, ...}

Austin (US)
  upserted 4218 amenities: {...}
...

Total amenities upserted: 38912
```

**Validation:**
```sql
SELECT country_code, city, amenity_type, COUNT(*) AS n
FROM realestate.bronze.amenities
GROUP BY country_code, city, amenity_type
ORDER BY country_code, city, amenity_type;
```
Expect at least 50 of each major amenity type per city. If any city has 0 in any type,
the Overpass bounding box may need expanding.

**Common issues:**
- *Overpass timeout (504)*: the public Overpass server is under load. The notebook
  retries with backoff up to 3 times. If it still fails, re-run after 10–30 minutes.

---

### Step 3.5 — `06_ingest_schools_us.py`

**What it does:** Fetches US K-12 school directory and grade-8 math/reading proficiency
from the Urban Institute Education Data API (no API key required). Upserts into `bronze.schools_us`.

**How to run:** Click "Run all". Takes 3–8 minutes (pagination across multiple state queries).

**Expected output:**
```
States to fetch: ['48', '12', '13', '06']  (TX, FL, GA, CA)
[2022/TX] directory page 1: 200 schools
[2022/TX] directory page 2: 200 schools
...
[2022/CA] joined 9892 schools with 8201 assessments
Upserted 18421 schools across 4 states.
```

**Validation:**
```sql
SELECT state, school_type, COUNT(*) AS schools,
       AVG(math_proficiency_pct) AS avg_math,
       AVG(reading_proficiency_pct) AS avg_reading
FROM realestate.bronze.schools_us
GROUP BY state, school_type
ORDER BY state, school_type;
```
Expect each state to have several thousand schools split across `public`/`private`/`charter`.
Some `math_proficiency_pct` rows may be null (small schools don't report).

---

### Step 3.6 — `04_ingest_crime_us.py`

**What it does:** Fetches 5 years of annual FBI UCR offense counts per agency for each US
state in `bronze.listings_us`. SpotCrime block is skipped automatically (key not stored).

**How to run:** Click "Run all". Takes 5–10 minutes (one call per agency × offense × year).

**Expected output:**
```
States: ['TX', 'FL', 'GA', 'CA']
[TX] fetching 248 agencies
[TX] year 2024, offense homicide: 187 agency-rows
...
SpotCrime: SPOTCRIME_API_KEY not in secrets. Skipping SpotCrime block.
Upserted 38712 FBI annual aggregate rows.
```

**Validation:**
```sql
SELECT state, crime_category, year, SUM(1) AS aggregate_rows
FROM realestate.bronze.crime_us
GROUP BY state, crime_category, year
ORDER BY state, crime_category, year DESC;
```
Expect rows per (state, category, year) with the most recent year having the highest
aggregate_rows (more agencies report fresh data).

**Common issues:**
- *HTTP 429 from FBI API*: rate limit is generous but if you re-run rapidly it can trip.
  Add `time.sleep(0.5)` between calls in the loop if needed.

---

### Step 3.7 — `09_ingest_demographics.py`

**What it does:** Fetches Census ACS 5-year demographics for every state's ZCTAs.
Colombia block looks for files in `/Volumes/realestate/bronze/raw/dane_censo/` — skipped if empty.

**How to run:** Click "Run all". US block takes 1–3 minutes.

**Expected output:**
```
Fetching ACS for state FIPS: ['48', '12', '13', '06']
US ACS 2022: upserted 8124 rows
CO volume not accessible (...). Skipping CO block.
Demographics ingestion complete. US rows: 8124, CO rows: 0
```

**Validation:**
```sql
SELECT state, COUNT(*) AS zctas,
       AVG(median_household_income_usd)::BIGINT AS avg_income,
       AVG(pct_homeowner) AS avg_homeowner_pct
FROM realestate.bronze.demographics_us
WHERE year = (SELECT MAX(year) FROM realestate.bronze.demographics_us)
GROUP BY state;
```
Expect avg_income $50K–$120K depending on state, avg_homeowner_pct 50–70%.

**To populate Colombia demographics:**
Download the DANE Censo 2018 aggregate tables and drop them in
`/Volumes/realestate/bronze/raw/dane_censo/`. Then re-run this notebook — the CO block
will detect them and load. The parser is a stub; you may need to map columns to the schema.

---

### Step 3.8 — `12_ingest_economic.py`

**What it does:** Fetches BLS LAUS unemployment + FRED MORTGAGE30US for the 4 US metros.
CO block is operator-staged (skipped on first run).

**How to run:** Click "Run all". Takes ~1 minute (BLS+FRED are fast).

**Expected output:**
```
[BLS] requesting 4 series for 2021-2026
[FRED] received 248 mortgage observations
US economic: upserted 240 rows for 4 metros
DANE GEIH: no files in /Volumes/realestate/bronze/raw/dane_geih/
Banco Rep: no files in /Volumes/realestate/bronze/raw/banrep/
Economic ingestion complete. US rows: 240, CO rows: 0
```

**Validation:**
```sql
SELECT metro_area, MAX(date) AS latest_date,
       AVG(unemployment_rate) AS avg_unemployment,
       AVG(mortgage_rate_30yr) AS avg_30yr_rate
FROM realestate.bronze.economic_us
WHERE date >= add_months(current_date(), -12)
GROUP BY metro_area;
```
Expect 4 rows, `avg_unemployment` 2–6%, `avg_30yr_rate` 5–8% (depends on current rates).

---

### Step 3.9 — `08_ingest_hazards.py`

**What it does:** Loads US FEMA flood zones + disaster declarations and USGS earthquake
PGA hazard. The USGS and Colombia blocks need operator-staged files in
`/Volumes/realestate/bronze/raw/`. FEMA blocks run automatically.

**How to run:** Click "Run all". Takes 5–15 minutes (FEMA flood zones return a lot of data).

**Expected output:**
```
[FEMA Flood] querying 4 state bounding boxes...
  TX: 12482 flood zone polygons
  FL: 18207 flood zone polygons
  GA: 6128 flood zone polygons
  CA: 14729 flood zone polygons
[FEMA Disasters] aggregating last 20 years by county...
  inserted 312 disaster-frequency rows
[USGS] no PGA grid staged at /Volumes/realestate/bronze/raw/usgs_pga/. Skipping.
[UNGRD] no files in /Volumes/realestate/bronze/raw/ungrd/. Skipping.

Total hazard rows: 51858
```

**Validation:**
```sql
SELECT source, hazard_type, country_code, COUNT(*) AS n,
       AVG(risk_score) AS avg_risk
FROM realestate.bronze.hazards
GROUP BY source, hazard_type, country_code
ORDER BY source, hazard_type;
```
Expect rows for `fema` (flood, plus disaster types), all `US`. Colombia hazards stay empty
until UNGRD files are staged.

---

### Step 3.10 — `02_ingest_listings_co.py`

**What it does:** Scrapes Finca Raíz and Metrocuadrado for Bogotá and Medellín listings.
Heavy installer cell at top — restarts Python.

**How to run:** Click "Run all". Takes 10–20 minutes (polite scraping with 1.5–3s sleep per page).

**Expected output (good case):**
```
[fincaraiz Bogota] page 1: 30 listings
[fincaraiz Bogota] page 2: 30 listings
...
[metrocuadrado Medellin] page 20: 28 listings

Upserted 2104 Colombian listings (new: 2104, updated: 0)
```

**Expected output (selectors broken case):**
```
[fincaraiz Bogota] page 1: 0 listings — selectors may need updating
[fincaraiz Bogota] page 2: 0 listings — selectors may need updating
...
Upserted 0 listings.
SELECTOR UPDATE REQUIRED — see notebook markdown cell for guidance.
```

**Validation:**
```sql
SELECT source, city, barrio, COUNT(*) AS n, AVG(price_cop)::BIGINT AS avg_price_cop
FROM realestate.bronze.listings_co
GROUP BY source, city, barrio
HAVING COUNT(*) > 5
ORDER BY n DESC
LIMIT 20;
```
Expect ~1,000–3,000 rows total across both sites and cities. Barrios should be recognizable
names (e.g., `El Poblado`, `Chapinero`, `Usaquén`, `Laureles`).

**Common issues:**
- *Zero listings parsed*: the sites changed their HTML. Inspect a live page in your
  browser, identify the new listing card selector, and update `_parse_fincaraiz_card`
  and/or `_parse_metrocuadrado_card`.
- *HTTP 429 / 503*: scraper has built-in backoff (3 retries: 5s, 15s, 45s). If it still
  fails, the site is actively blocking — wait an hour and retry, or switch to a Playwright
  headless browser approach (see notebook markdown).

---

### Step 3.11 — Optional: Manual file staging

These notebooks need operator-staged data and will run cleanly with empty results
until you provide files. Run them anyway so they show their "no files staged" message:

| Notebook | Files needed | Drop in |
|---|---|---|
| `05_ingest_crime_co.py` | DANE / Policía Nacional annual crime CSVs | `/Volumes/realestate/bronze/raw/crime_co/` |
| `07_ingest_schools_co.py` | ICFES Saber 11 results + MEN school registry | `/Volumes/realestate/bronze/raw/schools_co/` |
| `11_ingest_market_trends.py` | Zillow ZHVI / Redfin market tracker / DANE IPVN | `/Volumes/realestate/bronze/raw/zillow_research/`, `/redfin/`, `/dane_ipvn/` |

For each: click "Run all" — you'll see `no files staged` for each empty volume. Run them
again after dropping in the data.

**Expected output (empty case):**
```
No DANE files staged in /Volumes/realestate/bronze/raw/crime_co/. Skipping CO block.
```

---

### End-of-Phase-1 sanity check

```sql
SELECT
  (SELECT COUNT(*) FROM realestate.bronze.listings_us)        AS us_listings,
  (SELECT COUNT(*) FROM realestate.bronze.listings_co)        AS co_listings,
  (SELECT COUNT(*) FROM realestate.bronze.weather_history)    AS weather_rows,
  (SELECT COUNT(*) FROM realestate.bronze.crime_us)           AS us_crime_rows,
  (SELECT COUNT(*) FROM realestate.bronze.schools_us)         AS us_schools,
  (SELECT COUNT(*) FROM realestate.bronze.amenities)          AS amenities,
  (SELECT COUNT(*) FROM realestate.bronze.demographics_us)    AS us_demographics,
  (SELECT COUNT(*) FROM realestate.bronze.economic_us)        AS us_economic,
  (SELECT COUNT(*) FROM realestate.bronze.hazards)            AS hazards,
  (SELECT COUNT(*) FROM realestate.bronze.exchange_rates)     AS fx_rates;
```

**Expected:** every count should be > 0. If `co_listings` is 0, scrapers need fixing
but Silver/Gold will still build off the US data alone.

---

## 4. Phase 2 — Silver Layer

### Step 4.1 — `20_build_silver_listings.py`

**What it does:** Unions `bronze.listings_us` and `bronze.listings_co`, converts COP→USD
using the latest exchange rate, normalizes property types and area units, deduplicates
by `listing_id`. Writes to `silver.listings`.

**How to run:** Click "Run all". Takes < 1 minute.

**Expected output:**
```
Using COP/USD rate: 1 USD = 3,958.21 COP
silver.listings now has 2816 rows (US bronze=712, CO bronze=2104)
```

**Validation:**
```sql
SELECT country_code, COUNT(*) AS n,
       AVG(price_usd)::BIGINT AS avg_price_usd,
       AVG(price_per_sqft_usd) AS avg_psf,
       MIN(scraped_at) AS oldest_scrape
FROM realestate.silver.listings
GROUP BY country_code;
```
Expect `US` and `CO` (if CO data ingested). US avg_price_usd typically $300K–$700K;
CO avg_price_usd typically $80K–$200K (a key insight for the agent).

---

### Step 4.2 — `23_build_silver_risk.py`

**What it does:** Pivots `bronze.hazards` (long format, one row per hazard observation)
into one row per geographic area with separate columns per hazard type. Computes
weighted composite hazard score and labels (low/medium/high/very_high).

**How to run:** Click "Run all". Takes < 1 minute.

**Expected output:**
```
silver.risk_profile written: 4287 rows
```

**Validation:**
```sql
SELECT country_code, risk_label, COUNT(*) AS n,
       AVG(flood_risk_score) AS avg_flood,
       AVG(earthquake_risk_score) AS avg_eq
FROM realestate.silver.risk_profile
GROUP BY country_code, risk_label
ORDER BY country_code, risk_label;
```
Expect mostly `low`/`medium` labels with a few `high` (FEMA flood zones) and `very_high`
(coastal Florida). Colombia rows depend on UNGRD data.

---

### Step 4.3 — `21_build_silver_neighborhood.py`

**What it does:** The big one. Builds one row per (country, city, zip/municipio, barrio)
unit. Joins crime + schools + amenities + demographics + weather + hazard. Computes
`amenity_density_score` (0–100, min-max scaled globally) and `transit_access_score`.

**How to run:** Click "Run all". Takes 1–3 minutes (multiple Spark joins).

**Expected output:**
```
silver.neighborhood_profile written: 2143 rows
```

**Validation:**
```sql
SELECT country_code, COUNT(*) AS units,
       AVG(amenity_density_score) AS avg_amenity,
       AVG(transit_access_score)  AS avg_transit,
       SUM(CASE WHEN crime_rate_per_100k IS NOT NULL THEN 1 ELSE 0 END) AS with_crime,
       SUM(CASE WHEN school_score_normalized IS NOT NULL THEN 1 ELSE 0 END) AS with_school
FROM realestate.silver.neighborhood_profile
GROUP BY country_code;
```
Expect US and CO rows. `avg_amenity` and `avg_transit` should both be in the 0–100 range
(min-max ensures at least one row at 0 and one at 100).

**Common issues:**
- If `with_crime` is 0 for `CO`, the join on city name failed (DANE crime data may use
  different city spellings). Check `realestate.bronze.crime_co` for exact city values.

---

### Step 4.4 — `22_build_silver_market.py`

**What it does:** Unions US and CO market trend bronze tables. Computes rolling 3-month
and 12-month price changes, months of supply, and market temperature (`hot`/`warm`/`cool`/`cold`).

**How to run:** Click "Run all". Takes < 1 minute.

**Expected output (no market data staged yet):**
```
No bronze market_trends data to build from.
```

**Expected output (market files staged):**
```
silver.market_summary written: 42188 rows
```

**Validation:**
```sql
SELECT country_code, market_temp, COUNT(*) AS rows,
       AVG(price_trend_12mo_pct) AS avg_12mo_change
FROM realestate.silver.market_summary
WHERE market_temp IS NOT NULL
GROUP BY country_code, market_temp;
```
Expect a healthy mix of `hot`/`warm`/`cool`. `avg_12mo_change` typically -5% to +15%.

---

## 5. Phase 3 — Gold Layer

### Step 5.1 — `24_build_gold_static.py`

**What it does:** Mirrors `silver.risk_profile` to `gold.hazard_risk` (used by map widgets).
Builds `gold.school_rankings` by unioning US + CO bronze school tables with the
`school_score_normalized` calculation.

**How to run:** Click "Run all". Takes < 1 minute.

**Expected output:**
```
realestate.gold.hazard_risk written: 4287 rows
realestate.gold.school_rankings written: 18421 rows
```

**Validation:**
```sql
SELECT country_code, COUNT(*) AS schools,
       AVG(school_score_normalized) AS avg_score,
       MAX(school_score_normalized) AS max_score
FROM realestate.gold.school_rankings
WHERE school_score_normalized IS NOT NULL
GROUP BY country_code;
```
Expect `US` rows (scores 0–100 from math+reading proficiency). `CO` rows depend on ICFES
data being staged.

---

### Step 5.2 — `30_score_neighborhoods.py`

**What it does:** Computes the composite neighborhood score from `silver.neighborhood_profile`.
Weights: safety 30%, schools 25%, amenities 20%, hazard 15%, transit 10%. Inverted scores
for crime and hazard (so higher = safer). Adds country and global percentile ranks.

**How to run:** Click "Run all". Takes < 1 minute.

**Expected output:**
```
realestate.gold.neighborhood_scorecard written: 2143 rows
```

**Validation:**
```sql
SELECT country_code,
       AVG(composite_score) AS avg_composite,
       MIN(composite_score) AS min_score,
       MAX(composite_score) AS max_score
FROM realestate.gold.neighborhood_scorecard
GROUP BY country_code;
```
Expect averages near 50 (min-max keeps them centered). Min/max ranges from ~10 to ~95.

**Top 10 neighborhoods overall:**
```sql
SELECT country_code, city, zip_or_municipio, barrio, composite_score,
       global_percentile_rank
FROM realestate.gold.neighborhood_scorecard
ORDER BY composite_score DESC
LIMIT 10;
```

---

### Step 5.3 — `31_build_market_trends.py`

**What it does:** Adds 6-month and 12-month rolling averages of median price to the
`silver.market_summary` data and writes to `gold.market_trends`.

**How to run:** Click "Run all". Takes < 1 minute.

**Expected output (with market data):**
```
realestate.gold.market_trends written: 42188 rows
```

**Expected output (no market data):**
```
realestate.silver.market_summary not found.   ← acceptable; gold.market_trends stays empty
```

**Validation:**
```sql
SELECT country_code, city, date,
       median_price_usd, median_price_usd_rolling_12mo
FROM realestate.gold.market_trends
WHERE date >= add_months(current_date(), -3)
ORDER BY country_code, city, date DESC
LIMIT 20;
```
Each row should have a `median_price_usd_rolling_12mo` value (smoothed over a year).

---

### Step 5.4 — `32_detect_value_opportunities.py`

**What it does:** Finds listings priced below 85% of their area's market median. Joins
neighborhood score and risk label for context.

**How to run:** Click "Run all".

**Expected output:**
```
realestate.gold.value_opportunities written: 142 value opportunities
```

The count depends on how much listings data and market data overlap geographically.

**Validation:**
```sql
SELECT country_code, city, COUNT(*) AS opps,
       AVG(discount_pct) AS avg_discount_pct
FROM realestate.gold.value_opportunities
GROUP BY country_code, city
ORDER BY opps DESC;
```
Expect cities with lots of listings to dominate. `avg_discount_pct` typically 15–35%.

---

### Step 5.5 — `33_build_comparison_index.py`

**What it does:** Two things:
1. Builds `gold.comparison_index` — per-area normalized 0–100 metrics across US + CO
   so the agent can compare areas across countries on the same scale.
2. Builds `gold.city_comparison` — pre-computed side-by-side metrics for 11 US/CO city
   pairs, with a 3-sentence LLM-generated `narrative_context`.

**How to run:** Click "Run all". Takes 2–5 minutes (11 LLM calls for narratives).

**Expected output:**
```
realestate.gold.comparison_index written: 2143 rows
Computing pair: Miami / Bogota
Computing pair: Atlanta / Bogota
...
Computing pair: Denver / Medellin
realestate.gold.city_comparison written: 11 pairs
```

**Validation:**
```sql
SELECT us_city, co_city, us_median_price_usd, co_median_price_usd, price_ratio,
       narrative_context
FROM realestate.gold.city_comparison
ORDER BY price_ratio;
```
Expect 11 rows. `price_ratio` should be < 1.0 (CO cheaper) for most pairs. `narrative_context`
should be 3 sentences of LLM-generated prose.

**Common issues:**
- *LLM endpoint not available*: `narrative_context` will be empty. The structured numeric
  fields still populate fine.

---

### Step 5.6 — `34_build_dashboard_views.py`

**What it does:** Creates the three SQL views used by the future Lakeview dashboard:
`v_listing_map`, `v_recent_sessions`, `v_city_summary`.

**How to run:** Click "Run all". Takes < 10 seconds.

**Expected output:**
```
Created realestate.gold.v_listing_map
Created realestate.gold.v_recent_sessions
Created realestate.gold.v_city_summary
Dashboard views built.

namespace |viewName            |isTemporary
gold      |v_city_summary      |false
gold      |v_listing_map       |false
gold      |v_recent_sessions   |false
```

**Validation:**
```sql
SELECT COUNT(*) AS listings_with_scores FROM realestate.gold.v_listing_map;
SELECT * FROM realestate.gold.v_city_summary ORDER BY country_code, city;
```
`v_listing_map` should have the same count as `silver.listings`. `v_city_summary` should
have one row per (country, city).

---

## 6. Phase 4 — Agent Layer

### Step 6.1 — `40_agent_tools.py`

**What it does:** Loads the 13 tool functions into the notebook context. These are the
only functions the agent orchestrator is allowed to call.

**How to run:** Click "Run all".

**Expected output:**
```
Loaded 13 agent tools
```

**Validation cell to run after:**
```python
print(sorted(TOOL_REGISTRY.keys()))
# Then test a tool directly:
result = search_listings(country_code="CO", city="Medellin", max_price_usd=200000, limit=3)
print(f"Found {len(result)} listings")
for r in result:
    print(f"  ${r['price_usd']:,} — {r['bedrooms']}BR — {r.get('barrio_or_neighborhood', '?')}")
```
Expect 0–3 listings (depending on Bronze data) with prices under $200K.

---

### Step 6.2 — `42_session_logger.py`

**What it does:** Defines `log_session()` which inserts agent answers into
`gold.agent_sessions` after each query.

**How to run:** Click "Run all".

**Expected output:**
```
Session logger loaded.
```

No further validation needed — it'll be exercised by step 6.3.

---

### Step 6.3 — `41_realestate_agent.py` smoke test

**What it does:** The orchestrator. First runs a no-LLM smoke test that validates the
planner → executor → tool chain works without burning model calls.

**How to run:** Click "Run all" but **stop after the smoke test cell** (don't run the
example calls at the bottom yet).

**Expected output:**
```
Real estate agent configured with model: databricks-meta-llama-3-3-70b-instruct
Tools available: ['compare_cities', 'get_affordability_analysis', ..., 'search_listings']
Real estate agent loaded.
Try: agent_smoke_test()
```

Then run a cell with:
```python
agent_smoke_test()
```

**Expected output:**
```python
{
  "plan": {
    "intent": "heuristic_fallback",
    "reasoning": "LLM planner unavailable. Using broad heuristic tool selection.",
    "tool_calls": [
      {"tool": "get_neighborhood_profile", "params": {"country_code": "CO", "city": "Medellin"}},
      {"tool": "get_market_trends",        "params": {"country_code": "CO", "city": "Medellin", "months_back": 12}}
    ]
  },
  "tool_counts": {
    "get_neighborhood_profile": ["country_code", "city", "composite_score", ...],
    "get_market_trends":         ["message"]    # if no CO market data yet
  },
  "answer_preview": "Question: Find me 3 bedroom homes under $200k in Medellín\n\nEvidence summary:..."
}
```

This proves the pipeline works without an LLM. If you see an exception, debug from the
plan output.

---

### Step 6.4 — First real LLM-driven question

After the smoke test passes, run:
```python
print_realestate_answer("How does Medellín compare to Austin for an American buying a home?")
```

**Expected output (LLM-generated, will vary):**
```
PLAN:
{
  "intent": "cross_country_compare",
  "reasoning": "User wants to compare Medellín to Austin for an American buyer.",
  "tool_calls": [
    {"tool": "compare_cities", "params": {"us_city": "Austin", "co_city": "Medellin"}, "why": "main comparison"},
    {"tool": "get_market_trends", "params": {"country_code": "CO", "city": "Medellin", "months_back": 12}, "why": "current Medellín market"},
    {"tool": "get_market_trends", "params": {"country_code": "US", "city": "Austin",   "months_back": 12}, "why": "current Austin market"}
  ]
}

ANSWER:
Bottom line: Medellín is significantly cheaper than Austin (median home prices roughly
$140K vs $480K USD), but trade-offs include earthquake risk, lower walkability outside
core barrios like El Poblado, and different geographic granularity (barrio vs. zip code).

Key facts:
- Median home price: Medellín ~$140K USD, Austin ~$480K USD (price ratio 0.29x)
- Safety: Medellín composite safety score 58/100; Austin 74/100
- Schools: Medellín 52/100 (ICFES percentile); Austin 71/100 (math+reading proficiency)
- Earthquake risk: Medellín high (Andes seismic zone); Austin negligible
- Weather: Medellín much milder year-round (no extreme winters)

Caveats:
- Colombian price data is at city/barrio level, not zip-equivalent
- Exchange rate at time of comparison: 1 USD = 3,958 COP
- Colombia school scores are derived from ICFES Saber 11 percentiles

Follow-up questions you might ask:
- Which neighborhoods in Medellín have the lowest earthquake risk?
- What's the breakdown of crime types in El Poblado vs Laureles?
```

If the LLM endpoint isn't accessible from your workspace, the answer falls back to a
deterministic evidence dump.

---

### Step 6.5 — Verify session logging works

After running a few queries, check that they were logged:
```sql
SELECT timestamp, intent, user_question,
       country_filter, cities_mentioned, tool_names_used,
       evidence_record_count, latency_seconds
FROM realestate.gold.agent_sessions
ORDER BY timestamp DESC
LIMIT 10;
```

**Expected:** one row per query, with `tool_names_used` showing exactly which tools the
LLM picked. `latency_seconds` typically 3–12 seconds depending on LLM endpoint speed.

---

### Step 6.6 — Multi-turn conversation test

```python
new_session()
chat("I'm an American thinking about buying a place in Medellín")
chat("What's the safest neighborhood for families?")
chat("How do the schools there compare to what I'd find in Atlanta?")
```

**Expected behavior:** the second and third questions resolve "there" → "Medellín" without
explicit mention. The third should trigger `compare_cities` automatically. Check the
agent_sessions table to confirm `context_turns_used` is 1 and 2 for the second and third turn.

---

## 7. Phase 5 — Ongoing operation

### Daily refresh

Schedule these to run daily:
- `13_ingest_exchange_rates.py` (1 min)
- `01_ingest_listings_us.py` (5 min)
- `02_ingest_listings_co.py` (15 min)
- `03_ingest_weather.py` (incremental, < 5 min)

Then re-run the Silver and Gold pipelines after the Bronze refresh completes:
- `20_build_silver_listings.py`
- `21_build_silver_neighborhood.py`
- `22_build_silver_market.py`
- `23_build_silver_risk.py`
- `24_build_gold_static.py`
- `30_score_neighborhoods.py`
- `31_build_market_trends.py`
- `32_detect_value_opportunities.py`
- `33_build_comparison_index.py`

### Weekly refresh

- `10_ingest_amenities.py`
- `04_ingest_crime_us.py` (SpotCrime portion, if key acquired)

### Quarterly refresh

- `08_ingest_hazards.py`
- `09_ingest_demographics.py`

### Annual refresh

- `06_ingest_schools_us.py`
- `07_ingest_schools_co.py` (when new ICFES data is published)
- `11_ingest_market_trends.py` (CO IPVN annual update)
- `12_ingest_economic.py` (CO bulk files only)

---

## 8. Troubleshooting

### "Table or view not found: realestate.X.Y"

Make sure the notebook's first cells include `spark.sql("USE CATALOG realestate")` or
fully qualified names. Run `00_setup_schema.py` first.

### Agent returns "no data found" for every question

Check Phase 1 end-of-phase sanity query. If most Bronze tables are empty, the agent has
no data to draw from. Re-run the relevant ingestion notebooks.

### Agent uses heuristic fallback every time

This means the LLM planner can't reach the foundation model endpoint. Check:
1. Cluster has Databricks Runtime ML edition (or `mlflow` is `%pip install`-ed).
2. The endpoint `databricks-meta-llama-3-3-70b-instruct` is available in your workspace.
   Try a different one like `databricks-dbrx-instruct` if your workspace doesn't have Llama.

### Scrapers return 0 listings

Site HTML changed. Open a live page in your browser, inspect the listing card, identify
a stable selector (`data-*` attributes preferred), update the parser in
`02_ingest_listings_co.py`. If CAPTCHAs appear, switch to Playwright headless browser
on a cluster with browser-driver init scripts.

### "out of context" error in LLM call

The evidence got too large. `MAX_EVIDENCE_CHARS = 28000` in `41_realestate_agent.py`
should prevent this — increase the truncation threshold or lower `DEFAULT_LIMIT`.

---

## 9. Quick reference: full notebook execution order

```
00_setup_schema.py
99_helpers.py  (load helpers)
13_ingest_exchange_rates.py      ← exchange rates first (silver.listings needs them)
01_ingest_listings_us.py
02_ingest_listings_co.py
03_ingest_weather.py
04_ingest_crime_us.py
05_ingest_crime_co.py             ← bulk files
06_ingest_schools_us.py
07_ingest_schools_co.py           ← bulk files
08_ingest_hazards.py
09_ingest_demographics.py
10_ingest_amenities.py
11_ingest_market_trends.py        ← bulk files
12_ingest_economic.py
20_build_silver_listings.py
23_build_silver_risk.py           ← before neighborhood (joined in)
21_build_silver_neighborhood.py
22_build_silver_market.py
24_build_gold_static.py
30_score_neighborhoods.py
31_build_market_trends.py
32_detect_value_opportunities.py
33_build_comparison_index.py
34_build_dashboard_views.py
40_agent_tools.py
42_session_logger.py
41_realestate_agent.py            ← orchestrator; runs the agent
```
