# Real Estate Agent — Build Instructions

## Purpose

Build a Databricks-native real estate agent that helps users (primarily Americans) research
and purchase homes in the **United States** and **Colombia** (Bogotá and Medellín only for
the initial release). The agent answers natural-language questions, compares cities across
countries, and writes all results to Delta tables that are pre-wired for a future Databricks
Lakeview dashboard.

The agent does **not** handle rentals. It focuses exclusively on home purchase.

---

## Reference Implementation

Read `../osint_agent/31_osint_agent.py` and `../osint_agent/30_agent_tools.py` before
building. The real estate agent shares the same outer loop (plan → validate → execute →
synthesize) but differs in one critical way:

**The OSINT agent uses a fixed intent → fixed tool mapping.** The real estate agent does not.
Instead, the LLM planner decides which tools to call and with what parameters based on the
natural language of the question. Users do not need to say specific words or phrases to
activate a tool — the LLM infers intent, extracts parameters, and selects tools freely from
the allowed set. The executor then validates and runs exactly what the planner requested.

This means:
- "What's it like to raise a family in El Poblado?" → LLM selects `get_school_rankings`,
  `get_crime_stats`, `get_neighborhood_profile`, `get_amenity_access` without any keyword
  matching.
- "Something affordable near good transit in Bogotá" → LLM infers a price range from market
  data context, selects `search_listings` + `get_amenity_access` + `get_market_trends`.
- "How does it compare to where I'm from in Texas?" → LLM selects `compare_cities` and
  picks a representative Texas city from the supported US set.

**What is never stochastic:** the tool execution layer. Tools read only from Delta tables.
The LLM never generates listing data, prices, crime rates, or statistics. If data is not in
the tables, the tool returns an empty result and the synthesizer says so explicitly.

The primary structural differences from the OSINT agent:
- LLM planner outputs an explicit `tool_calls` array instead of a single intent
- A refinement pass re-runs sparse tools with adjusted parameters before synthesis
- A conversation context window lets follow-up questions reference prior exchanges
- A session logger persists every response for future dashboard use

---

## Directory Structure

```
real_estate_agent/
├── AGENT_BUILD_INSTRUCTIONS.md     ← this file
├── 00_setup_schema.py
├── 01_ingest_listings_us.py
├── 02_ingest_listings_co.py
├── 03_ingest_weather.py
├── 04_ingest_crime_us.py
├── 05_ingest_crime_co.py
├── 06_ingest_schools_us.py
├── 07_ingest_schools_co.py
├── 08_ingest_hazards.py
├── 09_ingest_demographics.py
├── 10_ingest_amenities.py
├── 11_ingest_market_trends.py
├── 12_ingest_economic.py
├── 13_ingest_exchange_rates.py
├── 20_build_silver_listings.py
├── 21_build_silver_neighborhood.py
├── 22_build_silver_market.py
├── 23_build_silver_risk.py
├── 24_build_gold_static.py        ← builds gold.hazard_risk + gold.school_rankings
├── 30_score_neighborhoods.py
├── 31_build_market_trends.py
├── 32_detect_value_opportunities.py
├── 33_build_comparison_index.py
├── 34_build_dashboard_views.py
├── 40_agent_tools.py
├── 41_realestate_agent.py
├── 42_session_logger.py
└── 99_helpers.py
```

---

## Medallion Architecture

```
External Sources
      │
      ▼
  BRONZE LAYER        Raw ingestion, minimal transformation, append-only
      │
      ▼
  SILVER LAYER        Cleaned, geocoded, currency-normalized, joined
      │
      ▼
   GOLD LAYER         Scored, aggregated, dashboard-ready, comparison-indexed
      │
      ▼
  AGENT LAYER         Tool functions read Gold/Silver; LLM orchestrator on top
      │
      ▼
 SESSION LOGGER       Every agent response persisted to gold.agent_sessions
```

All tables live in a single Databricks catalog. Use the catalog and schema names
established in `00_setup_schema.py`. Default: catalog = `realestate`, schema prefixes
= `bronze`, `silver`, `gold`.

---

## API Reference

### No-key APIs (use directly, no sign-up)

| API | Base URL | Used In |
|---|---|---|
| Open-Meteo Archive | `https://archive-api.open-meteo.com/v1/archive` | `03_ingest_weather.py` |
| OpenStreetMap Overpass | `https://overpass-api.de/api/interpreter` | `10_ingest_amenities.py` |
| FEMA Disaster Declarations | `https://www.fema.gov/api/open/v1/disasterDeclarationsSummaries` | `08_ingest_hazards.py` |
| USGS Earthquake Feed | `https://earthquake.usgs.gov/fdsnws/event/1/query` | `08_ingest_hazards.py` |
| frankfurter.app | `https://api.frankfurter.app/latest` | `13_ingest_exchange_rates.py` |

### Keys required (free sign-up, instant unless noted)

| API | Sign-Up URL | Env Var Name | Used In |
|---|---|---|---|
| RapidAPI Zillow Scraper | rapidapi.com — `zillow-com-live-data-scraper-api` | `RAPIDAPI_KEY` | `01_ingest_listings_us.py` |
| FBI Crime Data | api.data.gov | `FBI_API_KEY` | `04_ingest_crime_us.py` |
| US Census ACS | api.census.gov/data/key_signup.html | `CENSUS_API_KEY` | `09_ingest_demographics.py` |
| FRED | fred.stlouisfed.org/docs/api/api_key.html | `FRED_API_KEY` | `12_ingest_economic.py` |
| BLS Public Data API v2 | bls.gov/developers | `BLS_API_KEY` | `12_ingest_economic.py` |

**Note on education data:** NCES / education.data.gov (Urban Institute Education Data API)
is open and requires no key. SpotCrime is optional — FBI UCR is the primary US crime source
and works without SpotCrime.

Store all keys in Databricks Secrets under scope `realestate`. Retrieve with:
```python
dbutils.secrets.get(scope="realestate", key="RAPIDAPI_KEY")
```

### Scrapers (Colombia listings)

Two scrapers are needed. Both should be implemented as Python functions in
`02_ingest_listings_co.py` using `requests` + `BeautifulSoup4`. The scrapers must:
- Rotate a `User-Agent` header on each request (use `fake_useragent` library)
- Sleep 1.5–3 seconds between page requests (random interval)
- Handle HTTP 429 / 503 with exponential backoff (max 3 retries)
- Store raw HTML response alongside parsed fields for debugging
- Be idempotent: check `listing_id` against existing bronze table before inserting

**Finca Raíz**
- Bogotá listings: `https://www.fincaraiz.com.co/casas-y-apartamentos-en-venta/bogota/`
- Medellín listings: `https://www.fincaraiz.com.co/casas-y-apartamentos-en-venta/medellin/`
- Pagination: `?pagina={n}` appended to URL
- Key fields to extract: title, price (COP), bedrooms, bathrooms, area (m²), barrio,
  address, listing URL, listing ID (from URL slug or data attribute)

**Metrocuadrado**
- Bogotá: `https://www.metrocuadrado.com/inmuebles/venta/casa+apartamento/bogota/`
- Medellín: `https://www.metrocuadrado.com/inmuebles/venta/casa+apartamento/medellin/`
- Pagination: `?pagina={n}` or offset parameter — inspect live site at build time
- Key fields to extract: same as Finca Raíz above

Both scrapers write to `bronze.listings_co`. If site structure has changed at build time,
inspect the live HTML and adapt the selectors accordingly. Do not hardcode selectors that
are clearly dynamic CSS class names — prefer `data-*` attributes or semantic HTML where available.

---

## Python Dependencies

Add this `%pip install` cell at the top of any notebook that uses an external library not
already on the Databricks runtime. Restart Python (`dbutils.library.restartPython()`) after
install.

| Library | Used by | Purpose |
|---|---|---|
| `requests` | all ingestion notebooks | HTTP client (usually preinstalled) |
| `beautifulsoup4` | `02_ingest_listings_co.py` | HTML parsing for scrapers |
| `fake-useragent` | `02_ingest_listings_co.py` | Rotating User-Agent for scrapers |
| `mlflow` | `41_realestate_agent.py`, `33_build_comparison_index.py` | Foundation Model client (`mlflow.deployments.get_deploy_client`) |
| `openpyxl` | `05_ingest_crime_co.py`, `07_ingest_schools_co.py`, `11_ingest_market_trends.py`, `12_ingest_economic.py` | Read DANE/ICFES Excel files |
| `pyarrow` | bronze/silver/gold notebooks | Used by Spark Delta operations (preinstalled) |
| `unidecode` | `99_helpers.py` | Strip accents for Colombian city normalization |

Example install cell:
```python
%pip install beautifulsoup4 fake-useragent openpyxl unidecode
dbutils.library.restartPython()
```

The MLflow client comes with Databricks Runtime ML editions; on a standard runtime add:
```python
%pip install mlflow
```

---

## Schema Definitions

### BRONZE LAYER

#### `bronze.listings_us`
| Column | Type | Notes |
|---|---|---|
| listing_id | STRING | Source-assigned ID; primary dedup key |
| source | STRING | `rapidapi_zillow` |
| scraped_at | TIMESTAMP | Ingest time |
| address | STRING | Full street address |
| city | STRING | |
| state | STRING | Two-letter code |
| zip | STRING | 5-digit |
| lat | DOUBLE | |
| lon | DOUBLE | |
| price_usd | LONG | Asking price in USD |
| bedrooms | INT | |
| bathrooms | DOUBLE | |
| sqft | INT | Square feet |
| price_per_sqft_usd | DOUBLE | Derived: price_usd / sqft |
| property_type | STRING | `single_family`, `condo`, `townhouse`, `multi_family` |
| description | STRING | Listing text |
| listing_url | STRING | |
| days_on_market | INT | |
| listing_date | DATE | |
| raw_json | STRING | Full API response as JSON string |

Partition by: `state`, `city`

#### `bronze.listings_co`
| Column | Type | Notes |
|---|---|---|
| listing_id | STRING | URL slug or scraped ID |
| source | STRING | `fincaraiz` or `metrocuadrado` |
| scraped_at | TIMESTAMP | |
| address | STRING | |
| city | STRING | `Bogota` or `Medellin` |
| departamento | STRING | `Cundinamarca` or `Antioquia` |
| barrio | STRING | Neighborhood within city |
| lat | DOUBLE | Geocoded (see 99_helpers.py) |
| lon | DOUBLE | Geocoded |
| price_cop | LONG | Asking price in Colombian Pesos |
| bedrooms | INT | |
| bathrooms | DOUBLE | |
| area_m2 | DOUBLE | Square meters |
| property_type | STRING | `casa`, `apartamento` |
| description | STRING | |
| listing_url | STRING | |
| raw_html | STRING | gzip-compressed base64 string of the raw card HTML; only retained on first scrape or on parse failure. Cleared on subsequent successful refreshes. |

Partition by: `city`

**Storage note:** Raw HTML averages 50–500KB per listing. To avoid Bronze ballooning, the
scraper should gzip-compress the HTML (`gzip.compress(html.encode())` → base64), store it
only on initial insert and on parse failures, and set the column to `NULL` on routine
refresh updates. This keeps the field useful for debugging without exploding storage cost.

#### `bronze.weather_history`
| Column | Type | Notes |
|---|---|---|
| location_key | STRING | `{city}_{country_code}` |
| city | STRING | |
| country_code | STRING | `US` or `CO` |
| lat | DOUBLE | |
| lon | DOUBLE | |
| date | DATE | |
| temp_max_c | DOUBLE | |
| temp_min_c | DOUBLE | |
| temp_mean_c | DOUBLE | |
| precipitation_mm | DOUBLE | |
| wind_speed_max_kmh | DOUBLE | |
| weather_code | INT | WMO weather code |
| is_extreme_day | BOOLEAN | Derived: precip > 50mm OR wind > 80kmh OR temp extremes |

Partition by: `country_code`, `city`
Fetch 10 years of history for all cities: Bogotá, Medellín, and US cities that appear in listings.
Open-Meteo endpoint: `GET https://archive-api.open-meteo.com/v1/archive`
Params: `latitude`, `longitude`, `start_date`, `end_date`,
`daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max,weathercode`,
`timezone=auto`

#### `bronze.crime_us`
| Column | Type | Notes |
|---|---|---|
| incident_id | STRING | Source ID |
| source | STRING | `fbi_ucr` or `spotcrime` |
| city | STRING | |
| state | STRING | |
| zip | STRING | |
| lat | DOUBLE | |
| lon | DOUBLE | |
| crime_type | STRING | Raw category from source |
| crime_category | STRING | Normalized: `violent`, `property`, `other` |
| incident_date | DATE | |
| year | INT | |
| ingested_at | TIMESTAMP | |

FBI UCR endpoint: `https://api.usa.gov/crime/fbi/cde/`
Key endpoints:
- Offenses by state: `GET /offense/state/{state_abbr}/{offense_code}/run_start_year/{year}/run_end_year/{year}`
- Agency list: `GET /agency/byStateAbbr/{state_abbr}`
Pass `api_key` as query param.

SpotCrime endpoint: `http://api.spotcrime.com/crimes.json`
Params: `lat`, `lon`, `radius` (miles), `key`
Returns incidents within radius of a point.

#### `bronze.crime_co`
| Column | Type | Notes |
|---|---|---|
| record_id | STRING | Generated UUID |
| source | STRING | `dane` or `policia_nacional` |
| city | STRING | |
| municipio | STRING | |
| departamento | STRING | |
| crime_type | STRING | |
| crime_category | STRING | `violent`, `property`, `other` |
| count | INT | Aggregate count for period |
| period_year | INT | |
| period_month | INT | Null if annual aggregate |
| ingested_at | TIMESTAMP | |

Source: DANE and Policía Nacional publish annual Excel/CSV files.
Download URLs are subject to change — inspect `datos.gov.co` and `policia.gov.co/sijin`
at build time and store the raw files in a Databricks volume before parsing.

#### `bronze.schools_us`
| Column | Type | Notes |
|---|---|---|
| school_id | STRING | NCES `ncessch` identifier |
| school_name | STRING | |
| city | STRING | |
| state | STRING | |
| zip | STRING | |
| lat | DOUBLE | |
| lon | DOUBLE | |
| grade_levels | STRING | e.g. `KG-08`, `09-12` |
| enrollment | INT | |
| school_type | STRING | `public`, `private`, `charter` |
| title1_eligible | BOOLEAN | |
| math_proficiency_pct | DOUBLE | State assessment, if available |
| reading_proficiency_pct | DOUBLE | |
| ingested_at | TIMESTAMP | |

NCES endpoint: `https://educationdata.urban.org/api/v1/schools/ccd/directory/`
Params: `city`, `state_code`, `year` (use most recent available year)
Also call: `https://educationdata.urban.org/api/v1/schools/ccd/school-finances/` for Title I.
Assessment scores: `https://educationdata.urban.org/api/v1/schools/edfacts/assessments/`

#### `bronze.schools_co`
| Column | Type | Notes |
|---|---|---|
| school_id | STRING | DANE/MEN institution code |
| institution_name | STRING | |
| city | STRING | |
| municipio | STRING | |
| departamento | STRING | |
| lat | DOUBLE | |
| lon | DOUBLE | |
| grade_levels | STRING | |
| enrollment | INT | |
| school_type | STRING | `oficial`, `privado` |
| icfes_score | DOUBLE | Average ICFES Saber 11 score for the institution |
| icfes_percentile | DOUBLE | National percentile rank (computed) |
| icfes_year | INT | |
| ingested_at | TIMESTAMP | |

Source: ICFES publishes annual Saber 11 results as bulk CSV at `icfes.gov.co/resultados`.
Download, store in volume, then parse. Join to MEN school registry (from `datos.gov.co`)
on institution code to get lat/lon.

#### `bronze.hazards`
| Column | Type | Notes |
|---|---|---|
| hazard_id | STRING | Generated: `{source}_{type}_{geo_id}` |
| source | STRING | `fema`, `usgs`, `noaa_spc`, `ungrd` |
| country_code | STRING | |
| state_or_dept | STRING | |
| county_or_municipio | STRING | |
| zip_or_zone | STRING | |
| lat | DOUBLE | Centroid of risk zone |
| lon | DOUBLE | |
| hazard_type | STRING | `flood`, `earthquake`, `wildfire`, `tornado`, `landslide` |
| risk_level | STRING | `low`, `medium`, `high`, `very_high` |
| risk_score | DOUBLE | 0.0–1.0 |
| data_date | DATE | Currency date of the source data |
| details_json | STRING | Raw source payload |
| ingested_at | TIMESTAMP | |

FEMA flood zones: Query `https://msc.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query`
(Layer 28 = Flood Hazard Zones). Params: `geometry` (bounding box), `outFields=*`, `f=json`.
Zone A/AE/AH = high flood risk. Zone X = low. Map to risk_score accordingly.

FEMA disasters: `GET https://www.fema.gov/api/open/v1/disasterDeclarationsSummaries`
Params: `$filter=state eq '{state}'`, `$orderby=declarationDate desc`

USGS earthquake hazard: Use the USGS National Seismic Hazard Model GIS layers from
`earthquake.usgs.gov/hazards/hazmaps/` — download the 2% in 50-year PGA grid (CSV/GeoTiff)
and join to zip codes. For Colombia, use the global hazard map from the same source.

NOAA SPC tornado history: `https://www.spc.noaa.gov/gis/svrgis/` — download tornado track
shapefile, convert to CSV, aggregate by county.

UNGRD (Colombia): Download risk maps from `portal.gestiondelriesgo.gov.co`. Store raw files
in a Databricks volume. Parse to extract municipio-level risk assessments.

#### `bronze.demographics_us`
| Column | Type | Notes |
|---|---|---|
| geo_id | STRING | FIPS code |
| state | STRING | |
| county | STRING | |
| zip | STRING | ZCTA |
| city | STRING | |
| total_population | INT | |
| median_household_income_usd | INT | ACS B19013 |
| median_age | DOUBLE | ACS B01002 |
| pct_college_educated | DOUBLE | ACS B15003 |
| pct_homeowner | DOUBLE | ACS B25003 |
| median_home_value_usd | INT | ACS B25077 |
| year | INT | ACS 5-year estimate year |
| ingested_at | TIMESTAMP | |

Census ACS endpoint: `https://api.census.gov/data/{year}/acs/acs5`
Params: `get=B19013_001E,B01002_001E,...`, `for=zip code tabulation area:*`,
`in=state:{fips}`, `key={CENSUS_API_KEY}`
Use most recent 5-year ACS available.

#### `bronze.demographics_co`
| Column | Type | Notes |
|---|---|---|
| geo_id | STRING | DANE DIVIPOLA code |
| departamento | STRING | |
| municipio | STRING | |
| city | STRING | |
| total_population | INT | |
| median_household_income_cop | LONG | |
| pct_urban | DOUBLE | |
| pct_homeowner | DOUBLE | |
| year | INT | |
| ingested_at | TIMESTAMP | |

Source: DANE Censo Nacional de Población y Vivienda 2018 (most recent).
Download from `dane.gov.co/index.php/estadisticas-por-tema/demografia-y-poblacion/censo-nacional-de-poblacion-y-vivenda-2018`.
This is a one-time bulk load; schedule annual refresh for DANE updates.

#### `bronze.amenities`
| Column | Type | Notes |
|---|---|---|
| amenity_id | STRING | OSM node/way ID |
| country_code | STRING | |
| city | STRING | |
| lat | DOUBLE | |
| lon | DOUBLE | |
| amenity_type | STRING | `grocery`, `hospital`, `park`, `transit_stop`, `pharmacy`, `school`, `restaurant` |
| name | STRING | |
| address | STRING | |
| ingested_at | TIMESTAMP | |

Overpass API query for each city bounding box:
```
[out:json][timeout:60];
(
  node["amenity"~"supermarket|hospital|pharmacy|school|restaurant"](bbox);
  node["leisure"="park"](bbox);
  node["public_transport"="stop_position"](bbox);
);
out body;
```
City bounding boxes to use:
- Bogotá: `3.7,−74.3,4.9,−73.9`
- Medellín: `6.1,−75.7,6.4,−75.5`
- For US cities: derive from listing lat/lon centroids + 50-mile buffer

#### `bronze.market_trends_us`
| Column | Type | Notes |
|---|---|---|
| geo_id | STRING | Zip or metro ID |
| geo_type | STRING | `zip`, `city`, `metro` |
| zip | STRING | |
| city | STRING | |
| state | STRING | |
| date | DATE | Month-end date |
| median_list_price_usd | LONG | |
| median_sale_price_usd | LONG | |
| median_days_on_market | INT | |
| homes_sold | INT | |
| inventory_count | INT | |
| months_of_supply | DOUBLE | inventory / monthly_sales_rate |
| price_reduced_pct | DOUBLE | % of listings with price reduction |
| source | STRING | `zillow_research` or `redfin` |
| ingested_at | TIMESTAMP | |

Zillow Research bulk data: `https://www.zillow.com/research/data/`
Download: ZHVI (Zillow Home Value Index) by ZIP, median list price by ZIP.
Redfin Data Center: `https://www.redfin.com/news/data-center/`
Download: market tracker CSVs.
Both are monthly CSV files — download, unzip, load to Delta.

#### `bronze.market_trends_co`
| Column | Type | Notes |
|---|---|---|
| geo_id | STRING | DANE city code |
| city | STRING | |
| departamento | STRING | |
| date | DATE | Quarter-end date |
| ipvn_index | DOUBLE | DANE new housing price index |
| yoy_change_pct | DOUBLE | Year-over-year % change |
| new_construction_price_cop | LONG | Avg price per m² new construction |
| units_sold | INT | If available |
| source | STRING | `dane_ipvn` |
| ingested_at | TIMESTAMP | |

Source: DANE IPVN (Índice de Precios de Vivienda Nueva).
URL: `dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/indice-de-precios-de-vivienda-nueva-ipvn`
Published quarterly as Excel. Download, parse, load. Schedule quarterly refresh.

#### `bronze.economic_us`
| Column | Type | Notes |
|---|---|---|
| geo_id | STRING | BLS area code or FIPS |
| metro_area | STRING | |
| state | STRING | |
| date | DATE | Month-end |
| unemployment_rate | DOUBLE | BLS LAUS |
| job_growth_yoy_pct | DOUBLE | BLS CES |
| median_wage_usd | INT | BLS OES annual |
| mortgage_rate_30yr | DOUBLE | FRED series MORTGAGE30US |
| ingested_at | TIMESTAMP | |

BLS LAUS (local unemployment): `https://api.bls.gov/publicAPI/v2/timeseries/data/`
Series ID pattern: `LAU{fips_code}0000000000003` (unemployment rate).
BLS CES (employment): Series pattern varies by metro — see BLS series finder.
FRED mortgage rates: `https://api.stlouisfed.org/fred/series/observations`
Series: `MORTGAGE30US`. Params: `api_key`, `file_type=json`.

#### `bronze.economic_co`
| Column | Type | Notes |
|---|---|---|
| geo_id | STRING | DANE DIVIPOLA or Banco Rep region code |
| city | STRING | |
| departamento | STRING | |
| date | DATE | |
| unemployment_rate | DOUBLE | DANE |
| inflation_rate | DOUBLE | Banco de la República |
| mortgage_rate_cop | DOUBLE | Banco de la República |
| gdp_growth_pct | DOUBLE | DANE |
| ingested_at | TIMESTAMP | |

DANE unemployment: `dane.gov.co` → Gran Encuesta Integrada de Hogares (GEIH).
Published monthly as Excel.
Banco de la República API: `https://www.banrep.gov.co/es/estadisticas`
Check their open data portal for JSON/CSV feeds for mortgage rates and inflation.

#### `bronze.exchange_rates`
| Column | Type | Notes |
|---|---|---|
| date | DATE | |
| from_currency | STRING | `COP` |
| to_currency | STRING | `USD` |
| rate | DOUBLE | COP per 1 USD (or 1/rate for USD per COP) |
| ingested_at | TIMESTAMP | |

frankfurter.app: `GET https://api.frankfurter.app/latest?from=COP&to=USD`
Run daily. Store 2 years of history using:
`GET https://api.frankfurter.app/{YYYY-MM-DD}?from=COP&to=USD`

**Note on walkability:** Walk Score was evaluated but requires a public-facing domain for
attribution and is not compatible with an internal Databricks agent. Walkability and transit
access are instead derived entirely from OpenStreetMap data already ingested in
`bronze.amenities`. See `amenity_density_score` and `transit_access_score` in
`silver.neighborhood_profile`. This approach works equally for US and Colombia, making
the schema symmetric across both countries.

---

### SILVER LAYER

#### `silver.listings`
Unified listings for both countries, all prices in USD.

| Column | Type | Notes |
|---|---|---|
| listing_id | STRING | `{source}_{original_id}` |
| source | STRING | |
| country_code | STRING | `US` or `CO` |
| city | STRING | |
| state_or_dept | STRING | State abbrev (US) or departamento (CO) |
| zip_or_municipio | STRING | |
| barrio_or_neighborhood | STRING | |
| lat | DOUBLE | |
| lon | DOUBLE | |
| price_usd | LONG | Converted using bronze.exchange_rates for CO |
| price_local | LONG | Original price in local currency |
| local_currency | STRING | `USD` or `COP` |
| price_per_sqft_usd | DOUBLE | CO: convert m² to sqft first (1 m² = 10.764 sqft) |
| area_sqft | DOUBLE | Normalized to sqft |
| bedrooms | INT | |
| bathrooms | DOUBLE | |
| property_type | STRING | Normalized: `single_family`, `apartment`, `condo`, `townhouse` |
| listing_url | STRING | |
| days_on_market | INT | |
| listing_date | DATE | |
| scraped_at | TIMESTAMP | |

Build logic: join `bronze.listings_us` and `bronze.listings_co` after:
1. Geocoding any CO listings missing lat/lon (use Nominatim/OSM geocoder: `https://nominatim.openstreetmap.org/search`)
2. Converting CO price: `price_usd = price_cop / latest_exchange_rate`
   (rate is stored as COP per 1 USD, so divide to convert COP to USD)
3. Normalizing property types to common vocabulary
4. Converting m² to sqft for CO listings

Deduplicate on `listing_id`. Keep most recent `scraped_at` per `listing_id`.

#### `silver.neighborhood_profile`
One row per geographic unit (zip for US, barrio/municipio for CO).

| Column | Type | Notes |
|---|---|---|
| profile_id | STRING | `{country_code}_{city}_{zip_or_zone}` |
| country_code | STRING | |
| city | STRING | |
| zip_or_municipio | STRING | |
| barrio | STRING | null for US |
| lat_centroid | DOUBLE | |
| lon_centroid | DOUBLE | |
| crime_rate_per_100k | DOUBLE | |
| crime_trend | STRING | `rising`, `stable`, `falling` (computed from 3-yr window) |
| school_count | INT | Schools within 3 miles / 5 km |
| school_score_normalized | DOUBLE | 0–100 (see normalization section) |
| grocery_count | INT | Grocery stores within 1 mile / 2 km |
| park_count | INT | Parks within 1 mile / 2 km |
| hospital_count | INT | Within 5 miles / 8 km |
| transit_stop_count | INT | Within 0.5 mile / 1 km |
| population | INT | |
| median_income_usd | INT | Converted for CO using exchange rate |
| pct_homeowner | DOUBLE | |
| amenity_density_score | DOUBLE | 0–100; min-max scaled from OSM amenity counts; applies to US and CO equally |
| transit_access_score | DOUBLE | 0–100; derived from transit_stop_count within 0.5 mi / 1 km via OSM |
| weather_extreme_days_per_yr | DOUBLE | From bronze.weather_history |
| hazard_composite_score | DOUBLE | 0–100; higher = more risky |
| profile_updated_at | TIMESTAMP | |

#### `silver.market_summary`
One row per area per month.

| Column | Type | Notes |
|---|---|---|
| geo_id | STRING | |
| country_code | STRING | |
| city | STRING | |
| zip_or_municipio | STRING | |
| date | DATE | Month-end |
| median_price_usd | LONG | |
| price_trend_3mo_pct | DOUBLE | 3-month price change % |
| price_trend_12mo_pct | DOUBLE | 12-month price change % |
| median_days_on_market | INT | |
| inventory_count | INT | Active listings count (from bronze.listings) |
| months_of_supply | DOUBLE | |
| market_temp | STRING | `hot` (<2mo supply), `warm` (2–4), `cool` (4–6), `cold` (>6) |

#### `silver.risk_profile`
One row per geographic area.

| Column | Type | Notes |
|---|---|---|
| geo_id | STRING | |
| country_code | STRING | |
| city | STRING | |
| zip_or_municipio | STRING | |
| flood_risk_score | DOUBLE | 0–100 |
| earthquake_risk_score | DOUBLE | 0–100 |
| wildfire_risk_score | DOUBLE | 0–100; mostly US |
| tornado_risk_score | DOUBLE | 0–100; US only |
| landslide_risk_score | DOUBLE | 0–100; relevant for Medellín slopes |
| composite_hazard_score | DOUBLE | Weighted average of above |
| risk_label | STRING | `low`, `medium`, `high`, `very_high` |
| profile_updated_at | TIMESTAMP | |

Weighting for composite_hazard_score:
- Flood: 30%
- Earthquake: 30%
- Wildfire: 20%
- Tornado: 10%
- Landslide: 10%

---

### GOLD LAYER

#### `gold.listing_map`
Optimized for map widget queries. One row per active listing. Built as a view
(`realestate.gold.v_listing_map`) defined in `34_build_dashboard_views.py`, not a
materialized table.

Columns:
| Column | Type | Source |
|---|---|---|
| (all `silver.listings` columns) | | base |
| composite_score | DOUBLE | `gold.neighborhood_scorecard` |
| crime_rate_per_100k | DOUBLE | `gold.neighborhood_scorecard` |
| school_score_normalized | DOUBLE | `gold.neighborhood_scorecard` |
| risk_label | STRING | `silver.risk_profile` |
| market_temp | STRING | `silver.market_summary` (latest month) |
| area_median_price_usd | LONG | `silver.market_summary` (latest month) |

#### `gold.neighborhood_scorecard`
One row per profile_id. All `silver.neighborhood_profile` columns plus:

| Column | Type | Notes |
|---|---|---|
| composite_score | DOUBLE | 0–100 weighted composite (see weights below) |
| country_percentile_rank | DOUBLE | Rank within US or CO separately |
| global_percentile_rank | DOUBLE | Rank across all neighborhoods (US + CO combined) |

Composite score weights:
- Safety (inverted crime rate): 30%
- Schools: 25%
- Amenities (amenity_density_score): 20%
- Natural hazard (inverted): 15%
- Transit access (transit_access_score): 10%

Both amenity_density_score and transit_access_score are OSM-derived and apply equally
to US and Colombia — no country-specific fields in the composite.

#### `gold.market_trends`
All `silver.market_summary` columns plus rolling averages used for time-series charts.

| Column | Type | Notes |
|---|---|---|
| (all `silver.market_summary` columns) | | passed through |
| median_price_usd_rolling_6mo | DOUBLE | 6-month centered rolling average of `median_price_usd` |
| median_price_usd_rolling_12mo | DOUBLE | 12-month rolling average |

#### `gold.city_comparison`
Pre-computed side-by-side comparisons for US ↔ Colombia city pairs.
Rebuilt daily. One row per (us_city, co_city) pair covering supported combinations.

| Column | Type | Notes |
|---|---|---|
| comparison_id | STRING | `{us_city}_{co_city}_{date}` |
| us_city | STRING | |
| co_city | STRING | |
| comparison_date | DATE | |
| us_median_price_usd | LONG | |
| co_median_price_usd | LONG | CO price in USD |
| price_ratio | DOUBLE | co / us; <1.0 means CO is cheaper |
| us_crime_per_100k | DOUBLE | |
| co_crime_per_100k | DOUBLE | |
| us_school_score | DOUBLE | Normalized 0–100 |
| co_school_score | DOUBLE | Normalized 0–100 |
| us_composite_score | DOUBLE | |
| co_composite_score | DOUBLE | |
| us_hazard_score | DOUBLE | |
| co_hazard_score | DOUBLE | |
| us_unemployment_rate | DOUBLE | |
| co_unemployment_rate | DOUBLE | |
| us_weather_extreme_days | DOUBLE | |
| co_weather_extreme_days | DOUBLE | |
| us_amenity_density_score | DOUBLE | OSM-derived 0–100 |
| co_amenity_density_score | DOUBLE | OSM-derived 0–100; comparable to US on same scale |
| narrative_context | STRING | LLM-generated one-paragraph plain-English comparison |

#### `gold.hazard_risk`
All `silver.risk_profile` columns. Optimized for map overlay queries.

#### `gold.school_rankings`
All `bronze.schools_us` + `bronze.schools_co` columns with `school_score_normalized` (0–100).
Normalized score computation: for US, use math+reading proficiency average scaled to 0–100.
For CO, use `icfes_percentile` directly (already 0–100).

#### `gold.value_opportunities`
Active listings where `price_usd < (zip_median_price_usd * 0.85)` after controlling for
bedrooms and sqft. Include `composite_score` and `risk_label` for each listing.
Rebuild on each listings ingestion run.

#### `gold.comparison_index`
The normalization layer used by the cross-country comparison tool.
One row per city/zip/municipio.

| Column | Type | Notes |
|---|---|---|
| geo_id | STRING | |
| country_code | STRING | |
| city | STRING | |
| zip_or_municipio | STRING | |
| norm_price_usd | DOUBLE | Median price in USD |
| norm_crime_score | DOUBLE | 0–100; higher = safer (inverted crime rate) |
| norm_school_score | DOUBLE | 0–100 |
| norm_hazard_score | DOUBLE | 0–100; higher = safer (inverted hazard) |
| norm_weather_score | DOUBLE | 0–100; higher = milder weather |
| norm_affordability_score | DOUBLE | Based on price-to-income ratio |
| norm_amenity_score | DOUBLE | 0–100 |
| norm_economic_score | DOUBLE | Employment + growth composite |

Normalization method: min-max scaling across the combined US + CO dataset so scores are
comparable across countries. Recompute whenever underlying Silver tables are rebuilt.

#### `gold.agent_sessions`
Every agent invocation writes one row here via `42_session_logger.py`.

| Column | Type | Notes |
|---|---|---|
| session_id | STRING | UUID |
| timestamp | TIMESTAMP | |
| user_question | STRING | |
| intent | STRING | Planner-resolved intent label (for logging only) |
| planner_reasoning | STRING | LLM's stated reasoning for its tool selection |
| plan_json | STRING | Full validated plan including tool_calls as JSON string |
| answer_text | STRING | Final synthesized answer |
| structured_results_json | STRING | Key numeric results as JSON (for dashboard widgets) |
| country_filter | STRING | `US`, `CO`, or `BOTH` |
| cities_mentioned | STRING | Comma-separated list extracted from tool call params |
| tool_names_used | STRING | Comma-separated list of tools actually called |
| evidence_record_count | INT | Total rows returned across all tools |
| refinement_applied | BOOLEAN | True if a sparse-result refinement pass was triggered |
| context_turns_used | INT | Number of prior conversation turns passed to planner |
| latency_seconds | DOUBLE | Wall-clock time for full agent call |

---

## Notebook Specifications

### `00_setup_schema.py`

Create the catalog and all schemas if they do not exist. Always use fully qualified names
(`realestate.bronze.X`, `realestate.silver.X`, `realestate.gold.X`) for all DDL in this
notebook so first-time setup is unambiguous:

```sql
CREATE CATALOG IF NOT EXISTS realestate;
USE CATALOG realestate;
CREATE SCHEMA IF NOT EXISTS realestate.bronze;
CREATE SCHEMA IF NOT EXISTS realestate.silver;
CREATE SCHEMA IF NOT EXISTS realestate.gold;
```

Also create the volume used by raw-file ingestion notebooks:
```sql
CREATE VOLUME IF NOT EXISTS realestate.bronze.raw;
```

Create all Delta tables defined in the Schema Definitions section above using
`CREATE TABLE IF NOT EXISTS realestate.{schema}.{table}` with the schema as defined.
Apply partitioning as noted. Enable Change Data Feed on all Silver and Gold tables:
```sql
ALTER TABLE realestate.silver.listings SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true);
```

Print a summary of all created tables at the end.

**Every subsequent notebook** must run `spark.sql("USE CATALOG realestate")` (or `%sql USE
CATALOG realestate`) as its first executed cell so all unqualified `bronze.*` / `silver.*` /
`gold.*` references resolve correctly.

---

### `01_ingest_listings_us.py`

**Purpose:** Fetch active US home-purchase listings from the RapidAPI Zillow scraper.

**Steps:**
1. Retrieve `RAPIDAPI_KEY` from Databricks Secrets.
2. Identify target cities from the current distinct `city` values in `bronze.listings_us`
   (to refresh existing data) plus any new cities passed as a widget parameter.
3. For each city, call the Zillow scraper search endpoint on RapidAPI. The subscription is
   `zillow-com-live-data-scraper-api`. Required headers:
   ```
   x-rapidapi-host: zillow-com-live-data-scraper-api.p.rapidapi.com
   x-rapidapi-key:  {RAPIDAPI_KEY}
   ```
   Inspect the live RapidAPI dashboard at build time for the exact search endpoint path —
   it varies by provider. The endpoint that lookups by MLS ID is documented in the
   `keys_curls` file, but for city-wide search use the documented `/properties` or
   `/byCity` endpoint with params `location`, `status_type=ForSale`. If those paths have
   changed, use the RapidAPI test console to discover the current search endpoint.
4. Parse response. Map fields to `bronze.listings_us` schema. Compute `price_per_sqft_usd`.
5. Filter out rentals (`listing_type != 'FOR_SALE'`).
6. Upsert into `bronze.listings_us` using `listing_id` as the merge key:
   - If `listing_id` exists: update price, days_on_market, scraped_at.
   - If new: insert full row.
7. Log count of new vs. updated records.

**Schedule:** Daily.

---

### `02_ingest_listings_co.py`

**Purpose:** Scrape active home purchase listings from Finca Raíz and Metrocuadrado for
Bogotá and Medellín.

**Steps:**
1. Import `requests`, `BeautifulSoup`, `fake_useragent`, `time`, `random`.
2. Define `scrape_fincaraiz(city: str, max_pages: int = 20)` function.
3. Define `scrape_metrocuadrado(city: str, max_pages: int = 20)` function.
4. Each function must implement the retry/backoff/user-agent rotation spec from the
   API Reference section above.
5. Parse each listing card for: title, price_cop, bedrooms, bathrooms, area_m2, barrio,
   address, listing_url. Derive `listing_id` from the URL slug or a data attribute.
6. Upsert to `bronze.listings_co` using `listing_id` as merge key.
7. Run both scrapers for both cities. Log counts.

**Anti-scraping note:** If either site starts returning CAPTCHAs consistently, add a note
in the notebook output recommending switching to a headless browser approach (Selenium or
Playwright on a cluster with a browser driver). Do not implement this preemptively.

**Schedule:** Daily.

---

### `03_ingest_weather.py`

**Purpose:** Fetch 10 years of daily weather history from Open-Meteo for all supported cities.

**City coordinates to use:**
- Bogotá: lat=4.7110, lon=-74.0721
- Medellín: lat=6.2442, lon=-75.5812
- US cities: pull distinct (city, state, lat, lon) from `bronze.listings_us` centroids

**Steps:**
1. For each city, check the max `date` already in `bronze.weather_history` for that location_key.
2. Fetch from (max_date + 1 day) to yesterday. On first run, go back 10 years.
3. Call Open-Meteo archive API (no key). Parse daily arrays into rows.
4. Compute `is_extreme_day`: True if `precipitation_mm > 50` OR `wind_speed_max_kmh > 80`
   OR `temp_max_c > 38` OR `temp_min_c < -15`.
5. Append new rows to `bronze.weather_history`.

**Schedule:** Daily.

---

### `04_ingest_crime_us.py`

**Purpose:** Fetch US crime data from FBI UCR API and SpotCrime.

**Steps (FBI UCR):**
1. Retrieve `FBI_API_KEY`.
2. For each state that appears in `bronze.listings_us`, fetch annual offense counts by offense
   type for the last 5 years.
3. FBI UCR data is by agency (ORI code). Join to city via the agency list endpoint.
4. Map FBI offense codes to normalized `crime_category`: violent = Part I violent offenses
   (murder, rape, robbery, assault); property = Part I property offenses; other = remainder.
5. Insert annual aggregate rows to `bronze.crime_us` with source=`fbi_ucr`.

**Steps (SpotCrime — OPTIONAL):**
SpotCrime is not part of the initial build. Skip entirely if `SPOTCRIME_API_KEY` is not
set in the realestate secret scope. If a key is later added:
1. Retrieve `SPOTCRIME_API_KEY`.
2. For each listing zip code centroid in `bronze.listings_us`, call SpotCrime with
   lat/lon + radius=2 miles.
3. Insert incident-level rows with source=`spotcrime`.

The agent runs fine with only FBI UCR data — SpotCrime would add incident-level granularity
but is not required for any tool to function.

**Schedule:** Weekly (FBI data is annual; SpotCrime incidents can be weekly).

---

### `05_ingest_crime_co.py`

**Purpose:** Load Colombia crime statistics from DANE and Policía Nacional.

**Steps:**
1. Download the most recent annual crime statistics Excel/CSV from
   `datos.gov.co` (search: "criminalidad colombia") and
   `policia.gov.co/sijin` (SIEDCO system — look for public download links at build time).
2. Store raw files in a Databricks volume path: `/Volumes/realestate/raw/crime_co/`.
3. Parse each file. Filter to Bogotá (Cundinamarca) and Medellín (Antioquia) records only.
4. Normalize crime types to `crime_category` (violent / property / other).
5. Insert/overwrite to `bronze.crime_co` for the covered year(s).

**Schedule:** Annual (trigger manually when new data is published).

---

### `06_ingest_schools_us.py`

**Purpose:** Fetch US school data from NCES via the Urban Institute Education Data API.

**Steps:**
1. For each state in `bronze.listings_us`, call (no API key needed):
   `GET https://educationdata.urban.org/api/v1/schools/ccd/directory/`
   with params: `state_code={fips}`, `year={most_recent}`
2. Also call the assessments endpoint for math and reading proficiency:
   `GET https://educationdata.urban.org/api/v1/schools/edfacts/assessments/`
3. Join assessment data to school directory on NCES school ID.
4. Upsert to `bronze.schools_us` using `school_id` as merge key.

**Schedule:** Annual.

---

### `07_ingest_schools_co.py`

**Purpose:** Load Colombian school data from ICFES Saber 11 results and MEN registry.

**Steps:**
1. Download ICFES Saber 11 annual results from `icfes.gov.co/resultados` (CSV/Excel).
2. Download MEN school registry from `datos.gov.co` (search: "directorio establecimientos educativos").
3. Store both in `/Volumes/realestate/raw/schools_co/`.
4. Parse ICFES results: compute per-institution average score from individual student records.
5. Compute `icfes_percentile`: rank institutions by average score, compute national percentile.
6. Join ICFES to MEN registry on institution code to get location fields.
7. Filter to Bogotá and Medellín institutions only.
8. Upsert to `bronze.schools_co`.

**Schedule:** Annual.

---

### `08_ingest_hazards.py`

**Purpose:** Load natural hazard risk data for US and Colombia from multiple sources.

**Steps (FEMA Flood — US):**
1. For each bounding box covering US listing areas, query FEMA NFHL MapServer Layer 28.
2. Map flood zone to risk_score: Zone AE/A/AH/AO = 0.9, Zone X500 = 0.4, Zone X = 0.1.
3. Insert rows with `hazard_type='flood'`.

**Steps (FEMA Disasters — US):**
1. `GET https://www.fema.gov/api/open/v1/disasterDeclarationsSummaries`
2. Aggregate disaster count per county over last 20 years. Convert to 0–1 frequency score.
3. Insert rows with `hazard_type` matching the incident type.

**Steps (USGS Earthquakes — US + CO):**
1. Download the USGS National Seismic Hazard Model PGA grid from
   `earthquake.usgs.gov/hazards/hazmaps/`. Use 2% in 50 years PGA layer.
2. Spatially join PGA values to zip codes (US) and municipios (CO).
3. Normalize PGA to 0–1 risk_score. Insert with `hazard_type='earthquake'`.

**Steps (NOAA SPC Tornado — US only):**
1. Download tornado track shapefile from `spc.noaa.gov/gis/svrgis/`.
2. Aggregate tornado count per county (last 50 years). Normalize to 0–1.
3. Insert with `hazard_type='tornado'`.

**Steps (UNGRD — Colombia):**
1. Download risk maps from `portal.gestiondelriesgo.gov.co`. Store in volume.
2. Parse municipio-level flood, landslide, and earthquake risk classifications.
3. Map classifications to risk_score. Insert rows for Bogotá and Medellín municipios.

**Schedule:** Quarterly (hazard maps update infrequently).

---

### `09_ingest_demographics.py`

**Purpose:** Load US Census ACS data and Colombia DANE census data.

**Steps (US Census):**
1. Retrieve `CENSUS_API_KEY`.
2. For each state in `bronze.listings_us`, call the ACS 5-year API.
3. Variables to fetch: B19013_001E (median household income), B01002_001E (median age),
   B15003_022E+B15003_023E+B15003_024E+B15003_025E (bachelor's degree+),
   B15003_001E (total 25+ population for education denominator),
   B25003_001E (total housing units), B25003_002E (owner-occupied),
   B25077_001E (median home value).
4. Compute derived fields: `pct_college_educated`, `pct_homeowner`.
5. Upsert to `bronze.demographics_us`.

**Steps (Colombia DANE):**
1. Load the Censo 2018 microdata or aggregate tables from volume.
2. Filter to Bogotá (Cundinamarca) and Medellín (Antioquia).
3. Compute population, pct_urban, pct_homeowner at municipio level.
4. Upsert to `bronze.demographics_co`.

**Schedule:** Annual.

---

### `10_ingest_amenities.py`

**Purpose:** Fetch points of interest from OpenStreetMap Overpass API for all cities.

**Steps:**
1. For each city (Bogotá, Medellín, and US cities from bronze.listings_us):
   a. Compute bounding box from listing lat/lon extents + 10% buffer.
   b. Build Overpass QL query for: supermarket, hospital, pharmacy, school, park, transit_stop.
   c. POST to `https://overpass-api.de/api/interpreter`.
   d. Parse JSON response. Extract node lat/lon, name, amenity tag.
2. Map OSM tags to normalized `amenity_type`.
3. Upsert to `bronze.amenities` using OSM node ID as `amenity_id`.

**Schedule:** Monthly.

---

### `11_ingest_market_trends.py`

**Purpose:** Load market trend data for US (Zillow/Redfin) and Colombia (DANE IPVN).

**Steps (US):**
1. Download Zillow ZHVI by ZIP CSV from `zillow.com/research/data/` and store in volume.
2. Download Redfin market tracker CSV from `redfin.com/news/data-center/` and store in volume.
3. Parse and load to `bronze.market_trends_us`. Prefer Zillow for ZHVI; use Redfin for
   days_on_market, inventory, and price reductions.

**Steps (Colombia):**
1. Download DANE IPVN quarterly Excel from DANE website. Store in volume.
2. Parse. Filter to Bogotá and Medellín. Load to `bronze.market_trends_co`.

**Schedule:** Monthly.

---

### `12_ingest_economic.py`

**Purpose:** Load economic indicators for US (BLS/FRED) and Colombia (DANE/Banco de la República).

**Steps (BLS — US unemployment):**
1. Retrieve `BLS_API_KEY`.
2. For each metro area covering US listing cities, construct BLS LAUS series ID.
3. `POST https://api.bls.gov/publicAPI/v2/timeseries/data/` with series IDs and date range.
4. Load to `bronze.economic_us`.

**Steps (FRED — US mortgage rates):**
1. Retrieve `FRED_API_KEY`.
2. `GET https://api.stlouisfed.org/fred/series/observations?series_id=MORTGAGE30US&...`
3. Merge into `bronze.economic_us` by date.

**Steps (Colombia):**
1. Download DANE GEIH unemployment from DANE website (monthly Excel).
2. Fetch Banco de la República interest rates from their open data portal.
3. Load both to `bronze.economic_co`.

**Schedule:** Monthly.

---

### `13_ingest_exchange_rates.py`

**Purpose:** Fetch COP/USD exchange rate history and daily updates.

**Steps:**
1. Check max `date` in `bronze.exchange_rates`.
2. If table is empty, fetch 2-year history by iterating over dates in 30-day batches:
   `GET https://api.frankfurter.app/{YYYY-MM-DD}?from=COP&to=USD`
3. For daily updates: `GET https://api.frankfurter.app/latest?from=COP&to=USD`
4. Append new rows.

**Schedule:** Daily.

---

**Note:** `14_ingest_walk_scores.py` is not built. Walk Score requires a public-facing domain
for its attribution requirement and is incompatible with an internal agent. Walkability and
transit access are fully covered by `10_ingest_amenities.py` via OpenStreetMap. The
`amenity_density_score` and `transit_access_score` fields in `silver.neighborhood_profile`
are computed from OSM data in `21_build_silver_neighborhood.py`.

---

### `20_build_silver_listings.py`

**Purpose:** Join, normalize, and currency-convert all listings into `silver.listings`.

**Steps:**
1. Read `bronze.listings_us` and `bronze.listings_co`.
2. Fetch latest COP/USD rate from `bronze.exchange_rates`.
3. For CO listings: `price_usd = price_cop / exchange_rate` (since rate is COP per USD).
   Convert area: `area_sqft = area_m2 * 10.764`.
4. Geocode CO listings missing lat/lon using Nominatim (free, rate-limit to 1 req/sec):
   `GET https://nominatim.openstreetmap.org/search?q={address}&format=json&limit=1`
5. Normalize `property_type` to common vocabulary.
6. Compute `price_per_sqft_usd`.
7. Write to `silver.listings` using listing_id as merge key.

---

### `21_build_silver_neighborhood.py`

**Purpose:** Build neighborhood profiles by joining crime, schools, amenities, demographics,
weather, and hazard data per geographic unit.

**Steps:**
1. Define geographic units: US = zip code, CO = barrio (or municipio if barrio unavailable).
2. Crime rate: aggregate incidents per 100k population for trailing 12 months.
   Crime trend: compare trailing 3 years — use linear regression slope sign.
3. Schools within 3 miles (US) / 5 km (CO): count and average normalized scores.
4. Amenities within radius from `bronze.amenities`: count per type (grocery, hospital, park,
   transit_stop, pharmacy). Use radius 1 mile / 2 km for grocery/pharmacy/restaurant,
   5 miles / 8 km for hospitals, 0.5 mile / 1 km for transit stops.
5. Compute `amenity_density_score` (0–100): min-max scale the total weighted amenity count
   across all neighborhoods in the dataset. Weights: grocery=3, transit=3, park=2,
   pharmacy=1, hospital=1. Apply same formula to US and CO.
6. Compute `transit_access_score` (0–100): min-max scale `transit_stop_count` within
   0.5 mile / 1 km across all neighborhoods. Same formula for US and CO.
7. Demographics: join on zip or municipio.
8. Weather: compute average `extreme_days_per_year` from `bronze.weather_history`
   for the city covering this area.
9. Hazard: join `silver.risk_profile` on zip or municipio.
10. Compute all fields and write/merge to `silver.neighborhood_profile`.

---

### `22_build_silver_market.py`

**Purpose:** Build monthly market summary per area.

**Steps:**
1. From `bronze.market_trends_us` and `bronze.market_trends_co`, build a unified
   time series with all columns in the `silver.market_summary` schema.
2. Compute rolling 3-month and 12-month price change percentages using window functions.
3. Compute `months_of_supply = inventory_count / (homes_sold / 1)` where available.
   For CO where homes_sold is unavailable, derive a proxy from listing churn rate.
4. Set `market_temp` based on months_of_supply thresholds.
5. Write to `silver.market_summary`.

---

### `23_build_silver_risk.py`

**Purpose:** Build composite risk scores per geographic area.

**Steps:**
1. Pivot `bronze.hazards` to get one row per (geo_id, country_code) with columns per hazard_type.
2. For missing hazard types in an area, use 0.0 (no data = assume low risk but log gap).
3. Compute `composite_hazard_score` using the weighted formula defined in the schema section.
4. Assign `risk_label` from composite_hazard_score thresholds: <25=low, 25–50=medium,
   50–75=high, >75=very_high.
5. Write to `silver.risk_profile`.

---

### `24_build_gold_static.py`

**Purpose:** Build the two Gold-layer tables that don't depend on composite scoring:
`gold.hazard_risk` (mirror of silver.risk_profile, materialized for fast map queries) and
`gold.school_rankings` (unified US + CO school table with normalized scores).

**Steps (`gold.hazard_risk`):**
1. Read `silver.risk_profile`.
2. Write to `gold.hazard_risk` as a full overwrite. Schema is identical to silver.

**Steps (`gold.school_rankings`):**
1. Read `bronze.schools_us` and `bronze.schools_co`.
2. For US: compute `school_score_normalized = (math_proficiency_pct + reading_proficiency_pct) / 2`
   clamped to 0–100. Use null if both proficiency fields are missing.
3. For CO: `school_score_normalized = icfes_percentile` directly (already 0–100).
4. Union both into `gold.school_rankings` with a unified column set:
   `school_id`, `country_code`, `city`, `state_or_dept`, `zip_or_municipio`, `lat`, `lon`,
   `school_name` (alias institution_name for CO), `grade_levels`, `enrollment`,
   `school_type`, `school_score_normalized`, `score_year`.
5. Write as full overwrite.

**Schedule:** Run after `23_build_silver_risk.py` and after any school data refresh.

---

### `30_score_neighborhoods.py`

**Purpose:** Compute composite neighborhood scores and percentile ranks. Write to
`gold.neighborhood_scorecard`.

**Steps:**
1. Read `silver.neighborhood_profile`.
2. Invert crime and hazard scores (higher score = safer).
3. Compute `composite_score` using the weights defined in the gold schema section.
4. Compute `country_percentile_rank` within US rows and CO rows separately.
5. Compute `global_percentile_rank` across all rows.
6. Write to `gold.neighborhood_scorecard`.

---

### `31_build_market_trends.py`

**Purpose:** Propagate `silver.market_summary` to `gold.market_trends` with any additional
rolling averages needed for dashboard time-series charts.

**Steps:**
1. Read `silver.market_summary`.
2. Add `median_price_usd_rolling_6mo` and `median_price_usd_rolling_12mo` using window
   functions partitioned by (country_code, city, zip_or_municipio), ordered by date,
   rangeBetween for the rolling windows.
3. Write to `gold.market_trends`.

---

### `32_detect_value_opportunities.py`

**Purpose:** Identify listings priced meaningfully below their local market median.
Write to `gold.value_opportunities`.

**Steps:**
1. Join `silver.listings` to `silver.market_summary` on (country_code, city, zip_or_municipio).
2. Filter to listings where `price_usd < median_price_usd * 0.85`.
3. Further filter: `area_sqft > 500` and `bedrooms >= 1` (exclude clearly bad data).
4. Join `gold.neighborhood_scorecard` to add composite_score and risk_label.
5. Order by price discount percentage descending.
6. Write to `gold.value_opportunities` (full overwrite on each run).

---

### `33_build_comparison_index.py`

**Purpose:** Build the cross-country normalization layer and pre-compute US ↔ Colombia
city comparisons for the `gold.comparison_index` and `gold.city_comparison` tables.

**Steps:**
1. Read `gold.neighborhood_scorecard` for all geographic units.
2. For each of the 7 normalized dimensions (see gold.comparison_index schema), apply
   min-max scaling across the full combined dataset (US + CO together):
   `norm = (value - global_min) / (global_max - global_min) * 100`
3. Write to `gold.comparison_index`.
4. For `gold.city_comparison`: for each supported US/CO city pair, aggregate the
   `gold.comparison_index` metrics to city-level medians.
   City pairs to pre-compute (at minimum):
   - Bogotá vs. Miami, Atlanta, Houston, Chicago, New York, Los Angeles
   - Medellín vs. Miami, Atlanta, Houston, Austin, Denver
5. For each pair, compute `price_ratio`, `crime_ratio`, etc.
6. The `narrative_context` column: call the Databricks Foundation Model LLM once per pair
   to generate a 3-sentence plain-English summary. Prompt:
   ```
   Given these normalized comparison metrics between {us_city} and {co_city},
   write 3 sentences for an American considering buying a home in {co_city}.
   Focus on: relative cost, safety, and what makes {co_city} different from {us_city}.
   Data: {metrics_json}
   ```
7. Write to `gold.city_comparison`.

---

### `34_build_dashboard_views.py`

**Purpose:** Create optimized Delta views or pre-aggregated summary tables to support
fast Databricks SQL dashboard queries. Build now so the tables exist when dashboards
are added in a future phase.

**Create the following SQL views in the `gold` schema. All notebooks should set the
catalog context first:**

```sql
USE CATALOG realestate;

-- Fast listing map query (latest active listings with scores)
CREATE OR REPLACE VIEW realestate.gold.v_listing_map AS
SELECT l.*,
       n.composite_score,
       n.crime_rate_per_100k,
       n.school_score_normalized,
       r.risk_label,
       m.market_temp,
       m.median_price_usd AS area_median_price_usd
FROM realestate.silver.listings l
LEFT JOIN realestate.gold.neighborhood_scorecard n
       ON l.country_code = n.country_code
      AND l.zip_or_municipio = n.zip_or_municipio
LEFT JOIN realestate.silver.risk_profile r
       ON l.country_code = r.country_code
      AND l.zip_or_municipio = r.zip_or_municipio
LEFT JOIN realestate.silver.market_summary m
       ON l.country_code = m.country_code
      AND l.zip_or_municipio = m.zip_or_municipio
      AND m.date = (SELECT MAX(date) FROM realestate.silver.market_summary
                    WHERE country_code = l.country_code);

-- Agent session feed for dashboard panel
CREATE OR REPLACE VIEW realestate.gold.v_recent_sessions AS
SELECT *
FROM realestate.gold.agent_sessions
ORDER BY timestamp DESC
LIMIT 100;
```

**Rule for every notebook:** the first executed cell should be `spark.sql("USE CATALOG realestate")`
(or `%sql USE CATALOG realestate`) so all unqualified `silver.*` and `gold.*` references
resolve correctly. All schema-DDL operations in `00_setup_schema.py` must also fully qualify
with `realestate.` to be unambiguous on first run.

These views are not expected to serve traffic until dashboards are built. Create them
now so schema changes in Silver/Gold are caught early.

---

## Agent Layer

### `99_helpers.py`

Shared utilities imported by all notebooks. Include:

```python
def get_secret(key: str) -> str:
    """Retrieve from Databricks Secrets scope 'realestate'."""

def geocode_address(address: str, city: str, country_code: str) -> tuple[float, float]:
    """Nominatim geocoder. Rate-limited to 1 req/sec. Returns (lat, lon)."""

def usd_to_cop(usd: float, exchange_rates_table: str = "realestate.bronze.exchange_rates") -> float:
    """Convert USD to COP using most recent rate in the exchange table."""

def cop_to_usd(cop: float, exchange_rates_table: str = "realestate.bronze.exchange_rates") -> float:
    """Convert COP to USD using most recent rate."""

def clean_limit(value, default: int = 10, low: int = 1, high: int = 50) -> int:
    """Clamp a limit value to safe bounds."""

def extract_json_object(text: str) -> dict:
    """Extract first JSON object from an LLM response string (same as OSINT agent)."""

def none_if_nullish(value) -> any:
    """Convert LLM placeholder strings (null, none, n/a) to Python None."""
```

---

### `40_agent_tools.py`

Each function reads from Gold/Silver Delta tables. All functions return either a `List[Dict]`
or a `Dict`. They must never call external APIs directly — only Delta table reads.
Each function should handle empty results gracefully (return `[]` or `{}` with a `message` key).

```python
def search_listings(
    country_code: str,           # "US" or "CO"
    city: str,                   # e.g. "Austin" or "Medellin"
    min_price_usd: int = None,
    max_price_usd: int = None,
    bedrooms_min: int = None,
    bathrooms_min: float = None,
    property_type: str = None,   # normalized type or None for all
    limit: int = 10,
) -> List[Dict]:
    """Search silver.listings. Returns matching listings with neighborhood score and risk label."""

def get_neighborhood_profile(
    country_code: str,
    city: str,
    zip_or_municipio: str = None,
    barrio: str = None,
) -> Dict:
    """Return gold.neighborhood_scorecard row for the best-matching area."""

def get_crime_stats(
    country_code: str,
    city: str,
    zip_or_municipio: str = None,
    months_back: int = 12,
) -> Dict:
    """Aggregate crime stats for an area. Returns rate per 100k, trend, category breakdown."""

def get_school_rankings(
    country_code: str,
    city: str,
    zip_or_municipio: str = None,
    limit: int = 10,
) -> List[Dict]:
    """Return top schools near the area from gold.school_rankings."""

def get_weather_summary(
    country_code: str,
    city: str,
    years_back: int = 5,
) -> Dict:
    """Summarize weather history: avg temps, precipitation, extreme days per year."""

def get_hazard_risks(
    country_code: str,
    city: str,
    zip_or_municipio: str = None,
) -> Dict:
    """Return silver.risk_profile row. All five hazard scores plus composite and label."""

def get_market_trends(
    country_code: str,
    city: str,
    zip_or_municipio: str = None,
    months_back: int = 12,
) -> Dict:
    """Return a market summary for the area covering the last `months_back` months.
    Shape:
    {
      "city": str,
      "zip_or_municipio": str | None,
      "latest_date": date,
      "latest_median_price_usd": int,
      "price_trend_3mo_pct": float,
      "price_trend_12mo_pct": float,
      "median_days_on_market": int,
      "market_temp": str,
      "monthly_series": [{"date": date, "median_price_usd": int, "inventory_count": int}, ...]
    }
    `monthly_series` is the time-series used for charts; the rest are summary stats.
    Returns {"message": "no market data for this area"} if no rows exist."""

def get_area_demographics(
    country_code: str,
    city: str,
    zip_or_municipio: str = None,
) -> Dict:
    """Return demographics: population, median income USD, median age, pct homeowner."""

def get_nearby_amenities(
    country_code: str,
    city: str,
    zip_or_municipio: str = None,
    amenity_types: List[str] = None,   # None = all types
    limit: int = 20,
) -> List[Dict]:
    """Return bronze.amenities rows near the area centroid. Include count summary."""

def compare_cities(
    us_city: str,
    co_city: str,
) -> Dict:
    """Return gold.city_comparison row for the (us_city, co_city) pair.

    Lookup order:
    1. Look up the pre-computed row in gold.city_comparison for the most recent
       comparison_date. Return it if found.
    2. If no pre-computed row exists, compute on the fly:
       a. Filter gold.comparison_index to all rows where city == us_city and country_code == 'US',
          and all rows where city == co_city and country_code == 'CO'.
       b. For each side, compute the median of every norm_* metric across all areas
          (zip/municipio rows) in that city.
       c. Build the comparison row using the city-median values: price_ratio = co_median / us_median, etc.
       d. Skip narrative_context (only pre-computed rows have it). The synthesizer
          will produce narrative from the structured fields instead.
    3. If either city has zero rows in comparison_index, return {"message": "..."} with details.
    """

def get_affordability_analysis(
    country_code: str,
    city: str,
    annual_income_usd: float,
    down_payment_usd: float,
) -> Dict:
    """Compute max affordable price using 28% front-end DTI rule.
    Use current mortgage rate from bronze.economic_us (US) or bronze.economic_co (CO).
    Return: max_price_usd, estimated_monthly_payment, income_required_for_median,
    whether_median_is_affordable (bool)."""

def get_value_opportunities(
    country_code: str,
    city: str,
    max_price_usd: int = None,
    limit: int = 10,
) -> List[Dict]:
    """Return gold.value_opportunities filtered by city and optional price ceiling."""

def get_amenity_access(
    country_code: str,
    city: str,
    zip_or_municipio: str = None,
) -> Dict:
    """Return amenity_density_score, transit_access_score, and per-type counts
    from gold.neighborhood_scorecard (which carries all silver.neighborhood_profile
    columns). Applies equally to US and CO."""
```

---

### `41_realestate_agent.py`

**Structure:** Same outer shell as `../osint_agent/31_osint_agent.py`. Import helpers
with `%run ./99_helpers` and tools with `%run ./40_agent_tools`.

**Config:**
```python
MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
DEFAULT_LIMIT = 10
MAX_EVIDENCE_CHARS = 28000
MAX_CONTEXT_TURNS = 3        # prior Q&A turns passed to planner
MAX_REFINEMENT_PASSES = 1    # max times sparse results trigger re-planning
SUPPORTED_COUNTRIES = ["US", "CO"]
SUPPORTED_CO_CITIES = ["Bogota", "Medellin"]
```

---

#### Step 1 — Planning (`plan_question`)

The planner is called with the user's question plus an optional list of recent conversation
turns. It returns a validated plan containing an explicit list of tool calls.

**Planner system prompt:**
```
You are a planning assistant for a real estate agent covering the United States and Colombia
(Bogotá and Medellín only). Your job is to decide which tools to call and what parameters
to use, based entirely on what the user is asking.

You must return JSON only. Do not answer the user's question.

Rules:
- You may call between 1 and 5 tools. Only call tools that are genuinely relevant.
- Extract all parameters from the user's natural language. Do not require exact phrasing.
- For Colombian cities, normalize spelling: accept "Medellin", "Medellín", "medellin" → "Medellin".
  Accept "Bogota", "Bogotá", "bogota" → "Bogota".
- If the user implies a price range without stating it exactly ("affordable", "luxury",
  "mid-range"), infer a reasonable USD range from context and note your reasoning.
- If a follow-up question references a prior location ("what about schools there?"),
  extract the location from conversation history.
- If the question could involve both countries, call tools for both.
- The "intent" field is for logging only — it does not constrain which tools you pick.
```

**Planner JSON schema:**
```json
{
  "intent": "short label for logging, e.g. property_search / neighborhood_analysis / cross_country_compare / affordability_check / general",
  "reasoning": "1-2 sentences explaining what the user wants and why you chose these tools",
  "tool_calls": [
    {
      "tool": "exact_tool_function_name",
      "params": {
        "param_name": "param_value"
      },
      "why": "one phrase explaining why this tool is needed"
    }
  ]
}
```

**Available tools the planner may select from** (use exact names):
```
search_listings
get_neighborhood_profile
get_crime_stats
get_school_rankings
get_weather_summary
get_hazard_risks
get_market_trends
get_area_demographics
get_nearby_amenities
compare_cities
get_affordability_analysis
get_value_opportunities
get_amenity_access
```

**Example planner output for "What's it like raising a family in El Poblado?":**
```json
{
  "intent": "neighborhood_analysis",
  "reasoning": "User wants family suitability for El Poblado in Medellín. Schools, safety, amenities, and hazards are all relevant.",
  "tool_calls": [
    {"tool": "get_school_rankings", "params": {"country_code": "CO", "city": "Medellin", "zip_or_municipio": "El Poblado", "limit": 8}, "why": "schools are the top family concern"},
    {"tool": "get_crime_stats", "params": {"country_code": "CO", "city": "Medellin", "zip_or_municipio": "El Poblado", "months_back": 12}, "why": "safety is essential for families"},
    {"tool": "get_neighborhood_profile", "params": {"country_code": "CO", "city": "Medellin", "barrio": "El Poblado"}, "why": "overall composite profile"},
    {"tool": "get_hazard_risks", "params": {"country_code": "CO", "city": "Medellin", "zip_or_municipio": "El Poblado"}, "why": "landslide risk is elevated on Medellín slopes"}
  ]
}
```

**Example planner output for "Something affordable near good transit in Bogotá":**
```json
{
  "intent": "property_search",
  "reasoning": "User wants affordable listings with good transit access in Bogotá. 'Affordable' in Bogotá context is roughly under $120k USD based on market medians. Transit access comes from amenity data.",
  "tool_calls": [
    {"tool": "search_listings", "params": {"country_code": "CO", "city": "Bogota", "max_price_usd": 120000, "limit": 10}, "why": "find listings in affordable range"},
    {"tool": "get_amenity_access", "params": {"country_code": "CO", "city": "Bogota"}, "why": "identify transit-accessible areas"},
    {"tool": "get_market_trends", "params": {"country_code": "CO", "city": "Bogota", "months_back": 6}, "why": "confirm what affordable means in current market"}
  ]
}
```

---

#### Step 2 — Validation (`validate_plan`)

After the LLM returns a plan, validate every tool call before execution. Drop invalid calls
with a log warning rather than raising an exception so the agent degrades gracefully.

```python
ALLOWED_TOOLS = {
    "search_listings", "get_neighborhood_profile", "get_crime_stats",
    "get_school_rankings", "get_weather_summary", "get_hazard_risks",
    "get_market_trends", "get_area_demographics", "get_nearby_amenities",
    "compare_cities", "get_affordability_analysis", "get_value_opportunities",
    "get_amenity_access",
}

ALLOWED_PROPERTY_TYPES = {"single_family", "apartment", "condo", "townhouse", "casa", "apartamento"}
ALLOWED_AMENITY_TYPES  = {"grocery", "hospital", "park", "transit_stop", "pharmacy", "school", "restaurant"}

PARAM_RULES = {
    "country_code":      lambda v: v in ("US", "CO"),
    "city":              lambda v: isinstance(v, str) and len(v) > 0,
    "us_city":           lambda v: isinstance(v, str) and len(v) > 0,
    "co_city":           lambda v: v in ("Bogota", "Medellin"),
    "zip_or_municipio":  lambda v: isinstance(v, str) and 0 < len(v) <= 64,
    "barrio":            lambda v: isinstance(v, str) and 0 < len(v) <= 64,
    "limit":             lambda v: isinstance(v, int) and 1 <= v <= 50,
    "months_back":       lambda v: isinstance(v, int) and 1 <= v <= 24,
    "years_back":        lambda v: isinstance(v, int) and 1 <= v <= 10,
    "radius_miles":      lambda v: isinstance(v, (int, float)) and 0 < v <= 25,
    "min_price_usd":     lambda v: isinstance(v, (int, float)) and v >= 0,
    "max_price_usd":     lambda v: isinstance(v, (int, float)) and v > 0,
    "bedrooms_min":      lambda v: isinstance(v, int) and 0 <= v <= 10,
    "bathrooms_min":     lambda v: isinstance(v, (int, float)) and 0 <= v <= 10,
    "property_type":     lambda v: v in ALLOWED_PROPERTY_TYPES,
    "amenity_types":     lambda v: isinstance(v, list) and all(t in ALLOWED_AMENITY_TYPES for t in v),
    "annual_income_usd": lambda v: isinstance(v, (int, float)) and v > 0,
    "down_payment_usd":  lambda v: isinstance(v, (int, float)) and v >= 0,
}

# Numeric params that should be clamped (not dropped) when out of range
CLAMP_RULES = {
    "limit":         (1, 50),
    "months_back":   (1, 24),
    "years_back":    (1, 10),
    "radius_miles":  (0.1, 25),
    "bedrooms_min":  (0, 10),
    "bathrooms_min": (0.0, 10.0),
}
```

For each tool call:
1. Drop if `tool` not in `ALLOWED_TOOLS`.
2. For each param present, apply its rule if one exists. Clamp numeric params to safe bounds
   rather than dropping the entire call.
3. Normalize Colombian city names: strip accents, title-case, map to canonical spelling.
4. If `country_code` is `CO` and `city` is not in `SUPPORTED_CO_CITIES`, drop the call and
   log a warning noting the unsupported city.
5. Cap `tool_calls` at 5 after validation. If more survive validation, keep the first 5.

---

#### Step 3 — Execution (`execute_plan`)

Call each validated tool in the order the planner specified. Collect results into an evidence
dict keyed by tool name. If the same tool is called twice with different params (e.g.,
`get_market_trends` for both a US and a CO city), key them as `tool_name_1`, `tool_name_2`.

```python
evidence = {
    "question": question,
    "reasoning": plan.get("reasoning", ""),
    "tools": {}
}
for i, call in enumerate(validated_calls):
    fn = TOOL_REGISTRY[call["tool"]]   # dict mapping name → function
    result = fn(**call["params"])
    key = call["tool"] if call["tool"] not in evidence["tools"] else f"{call['tool']}_{i}"
    evidence["tools"][key] = result
```

---

#### Step 4 — Refinement Pass (`refine_if_sparse`)

After execution, check whether critical tools returned empty results. If so, make one
additional LLM call to adjust parameters and re-run only the empty tools.

A tool result is considered **sparse** if:
- It is a list and `len(result) == 0`
- It is a dict and contains a `"message"` key (tool's own empty-result signal)

**Refinement prompt** (sent only when sparse results exist):
```
The following tools returned no data for the parameters below. The user's question was:
"{question}"

Sparse tools and their parameters:
{sparse_tools_json}

Suggest adjusted parameters for each sparse tool that are more likely to return data.
You may widen price ranges by up to 30%, relax bedrooms/bathrooms constraints,
or broaden geographic scope from barrio to city level.
Return JSON in this format:
{
  "adjustments": [
    {"tool": "tool_name", "params": {adjusted params}, "change": "what you changed and why"}
  ]
}
Do not add new tools. Only adjust parameters for the listed sparse tools.
```

After refinement, re-execute only the adjusted tools. Replace the empty entries in
`evidence["tools"]` with the new results. Track every adjustment in
`evidence["refinement_log"]` (a list) so the session logger can detect whether refinement
ran. Each log entry has the shape:
```python
{"tool": "search_listings", "original_params": {...}, "adjusted_params": {...},
 "change": "widened max_price_usd from 120000 to 150000", "passes_remaining": 0}
```

Repeat the refinement loop up to `MAX_REFINEMENT_PASSES` times (default 1). After each pass,
re-check for sparse results; stop early if all critical results are populated or if the loop
hits the limit. If the final pass still returns empty results, keep the empty evidence and
let the synthesizer report the gap.

Implementation sketch:
```python
def refine_if_sparse(evidence, question, validated_calls, use_llm):
    evidence.setdefault("refinement_log", [])
    for pass_num in range(MAX_REFINEMENT_PASSES):
        sparse = _identify_sparse_tools(evidence, validated_calls)
        if not sparse:
            break
        if not use_llm:
            break
        adjustments = _llm_adjust_params(question, sparse)
        for adj in adjustments:
            new_result = TOOL_REGISTRY[adj["tool"]](**adj["params"])
            evidence["tools"][adj["tool"]] = new_result
            evidence["refinement_log"].append({
                "tool": adj["tool"],
                "original_params": sparse[adj["tool"]],
                "adjusted_params": adj["params"],
                "change": adj.get("change", ""),
                "passes_remaining": MAX_REFINEMENT_PASSES - pass_num - 1,
            })
    return evidence
```

---

#### Step 5 — Synthesis (`synthesize_answer`)

**System prompt:**
```
You are a real estate advisory assistant helping users — primarily Americans — purchase homes
in the United States and Colombia (Bogotá and Medellín).

Ground rules:
- Answer using only the evidence provided. Do not invent listings, prices, crime rates,
  school scores, or any other statistics.
- If a tool returned no data, say so plainly. Do not fill gaps with general knowledge or
  estimates presented as facts.
- You may use general knowledge only to provide context around real data
  (e.g., "Medellín's El Poblado is known as an expat-friendly area" is fine as context,
  but do not invent a crime rate for it).

When comparing US and Colombia:
- Always express Colombian prices in USD first, then note the COP equivalent.
- Use the normalized 0–100 scores to compare safety and schools across countries —
  do not compare raw numbers directly.
- Contextualize Colombian metrics in terms Americans recognize
  (e.g., "a crime rate comparable to X US city").
- Note earthquake risk for Colombian cities — it is often the most surprising factor
  for American buyers.

Format:
- Lead with a direct bottom-line answer.
- Use bullet points for supporting facts.
- End with a caveats section noting data freshness, any sparse results, and the
  COP/USD exchange rate date used.
- Suggest 1–2 natural follow-up questions the user might want to ask next.
```

**User prompt structure:**
```
Conversation context (most recent {MAX_CONTEXT_TURNS} turns):
{context_json}

Current question:
{question}

Agent reasoning for this response:
{plan_reasoning}

Tool evidence:
{truncated_evidence_json}

Write your answer following the system instructions.
```

---

#### Heuristic Fallback Planner (`_heuristic_plan`)

Used only when the LLM endpoint is unavailable or returns unparseable JSON. The goal is to
keep the agent minimally functional, not to replicate LLM flexibility. It should be broad
and conservative — calling a slightly wrong set of tools is better than returning nothing.

```python
def _heuristic_plan(question: str, context: List[Dict]) -> Dict:
    q = question.lower()

    # Extract city from question or most recent context turn
    city, country_code = _detect_city(q, context)

    # Build a broad tool set covering the most likely need
    tool_calls = []

    if city and country_code == "CO":
        tool_calls.append({"tool": "get_neighborhood_profile",
                           "params": {"country_code": "CO", "city": city}})
        tool_calls.append({"tool": "get_market_trends",
                           "params": {"country_code": "CO", "city": city, "months_back": 12}})

    if city and country_code == "US":
        tool_calls.append({"tool": "get_neighborhood_profile",
                           "params": {"country_code": "US", "city": city}})
        tool_calls.append({"tool": "get_market_trends",
                           "params": {"country_code": "US", "city": city, "months_back": 12}})

    # Add specific tools for clearly signaled topics
    if any(w in q for w in ["school", "educat", "kid", "child", "family"]):
        tool_calls.append({"tool": "get_school_rankings",
                           "params": {"country_code": country_code or "CO", "city": city or "Medellin", "limit": 8}})

    if any(w in q for w in ["safe", "crime", "danger", "secur"]):
        tool_calls.append({"tool": "get_crime_stats",
                           "params": {"country_code": country_code or "CO", "city": city or "Medellin", "months_back": 12}})

    if any(w in q for w in ["flood", "quake", "earthquake", "wildfire", "risk", "hazard", "disaster"]):
        tool_calls.append({"tool": "get_hazard_risks",
                           "params": {"country_code": country_code or "CO", "city": city or "Medellin"}})

    if any(w in q for w in ["afford", "budget", "income", "mortgage", "down payment"]):
        tool_calls.append({"tool": "get_affordability_analysis",
                           "params": {"country_code": country_code or "CO", "city": city or "Medellin",
                                      "annual_income_usd": 70000, "down_payment_usd": 20000}})

    if any(w in q for w in ["compare", "vs", "versus", "difference", "better"]):
        tool_calls.append({"tool": "compare_cities",
                           "params": {"us_city": "Miami", "co_city": "Medellin"}})

    # Always include listings if nothing more specific was triggered
    if not tool_calls:
        tool_calls = [
            {"tool": "search_listings", "params": {"country_code": "CO", "city": "Medellin", "limit": 10}},
            {"tool": "get_market_trends", "params": {"country_code": "CO", "city": "Medellin", "months_back": 12}},
        ]

    return {
        "intent": "heuristic_fallback",
        "reasoning": "LLM planner unavailable. Using broad heuristic tool selection.",
        "tool_calls": tool_calls[:5],
    }
```

`_detect_city()` should check the question for Colombian city names (including common
misspellings and accent variants) and US city names present in `bronze.listings_us`.
If no city is found in the question, scan the most recent context turn for a city mention.

---

#### Conversation Context Window

`ask_realestate()` accepts an optional `context` parameter — a list of dicts from the
current session. Pass up to `MAX_CONTEXT_TURNS` most recent turns to the planner and
synthesizer so follow-up questions resolve naturally.

```python
def ask_realestate(
    question: str,
    context: List[Dict] = None,       # [{"question": "...", "answer": "..."}, ...]
    use_llm_planner: bool = True,
    use_llm_answer: bool = True,
    show_evidence: bool = False,
) -> Dict:
    context = (context or [])[-MAX_CONTEXT_TURNS:]
    plan = plan_question(question, context, use_llm=use_llm_planner)
    validated = validate_plan(plan)
    evidence = execute_plan(validated, question)
    evidence = refine_if_sparse(evidence, question, validated, use_llm=use_llm_planner)
    answer = synthesize_answer(question, evidence, context, use_llm=use_llm_answer)
    log_session(question, plan, answer, evidence, context)
    result = {"question": question, "plan": plan, "answer": answer}
    if show_evidence:
        result["evidence"] = evidence
    return result
```

Expose a convenience wrapper for interactive notebook use that manages the context list
across multiple calls:

```python
# Session state — reset by calling new_session()
_session_context: List[Dict] = []

def new_session() -> None:
    global _session_context
    _session_context = []

def chat(question: str, **kwargs) -> None:
    """Interactive multi-turn wrapper. Maintains session context automatically."""
    global _session_context
    result = ask_realestate(question, context=_session_context, **kwargs)
    _session_context.append({"question": question, "answer": result["answer"]})
    print_realestate_answer_from_result(result)


def print_realestate_answer_from_result(result: Dict) -> None:
    """Pretty-print a pre-computed result dict (used by chat() to avoid re-running)."""
    print("PLAN:")
    print(json.dumps(result["plan"], indent=2, ensure_ascii=False, default=str))
    print("\nANSWER:")
    print(result["answer"])


def print_realestate_answer(question: str, **kwargs) -> None:
    """Run the agent and pretty-print the result (single-turn convenience)."""
    result = ask_realestate(question, **kwargs)
    print_realestate_answer_from_result(result)
```

---

#### Public functions to expose:
```python
def ask_realestate(question, context=None, use_llm_planner=True, use_llm_answer=True, show_evidence=False) -> Dict
def chat(question, **kwargs) -> None          # multi-turn interactive wrapper
def new_session() -> None                     # reset conversation context
def print_realestate_answer(question, **kwargs) -> None
def agent_smoke_test() -> Dict
```

---

### `42_session_logger.py`

**Purpose:** Persist every agent response to `gold.agent_sessions` for dashboard access.

```python
def log_session(
    question: str,
    plan: Dict,
    answer: str,
    evidence: Dict,
    context: List[Dict],
) -> str:
    """Write one row to gold.agent_sessions. Return the session_id."""
```

**Implementation:**
1. Record wall-clock start time before planning; compute `latency_seconds` at log time.
2. Generate a UUID for `session_id`.
3. Extract `country_filter`: if tool calls reference both US and CO → `BOTH`; else the
   single country_code found; else `UNKNOWN`.
4. Extract `cities_mentioned`: collect all `city` param values across all tool calls in the plan.
5. Extract `tool_names_used`: list of tool names from `evidence["tools"]` keys.
6. Extract `evidence_record_count`: sum of `len(v)` for list-valued tool results.
7. Extract `refinement_applied`: `True` if `evidence.get("refinement_log")` is a non-empty list.
8. Extract `context_turns_used`: `len(context)`.
9. Build `structured_results_json`: compact JSON of top-level numeric facts only —
   listing count, median price found, composite score, crime rate per 100k, top school score.
   Strip all long text fields (descriptions, narratives) so this column stays queryable.
10. Build `planner_reasoning`: the `reasoning` field from the plan.
11. Use `spark.createDataFrame([row_dict]).write.mode("append").saveAsTable("realestate.gold.agent_sessions")`.

---

## Cross-Country Comparison — How It Works

When an American asks "How does Medellín compare to Austin?", the agent calls `compare_cities("Austin", "Medellin")`, which returns a `gold.city_comparison` row. The synthesis prompt instructs the LLM to:

1. Lead with a bottom-line value statement in USD terms.
2. Compare crime rates using per-100k figures (not raw numbers).
3. Compare school scores on the normalized 0–100 scale.
4. Note earthquake risk for Colombian cities — this is often the most surprising difference for Americans.
5. Convert Colombian prices to USD explicitly.
6. Note that Colombia uses `departamento` + `municipio` + `barrio` geography instead of zip codes.
7. Mention that US lifestyle amenities (chain stores, English-language services) are limited
   but growing in El Poblado (Medellín) and Chapinero/Usaquén (Bogotá).

---

## Known Limitations

Document these in notebook markdown cells and in agent synthesis prompts:

1. **Colombia listing data is scraped**: freshness depends on scraper health. If Finca Raíz or
   Metrocuadrado changes HTML structure, listings will go stale until selectors are updated.
2. **Colombia crime data is aggregate**: no incident-level API exists. City-level crime rates
   are available but barrio-level granularity is limited.
3. **Colombia school ratings**: no equivalent to GreatSchools exists. Rankings are derived from
   ICFES standardized test scores and are relative, not absolute.
4. **Walkability scoring**: No third-party walkability API is used. Scores are derived from
   OpenStreetMap amenity and transit stop counts, normalized 0–100. This is consistent
   across US and Colombia but does not match the Walk Score brand that US users may recognize.
5. **Market trend granularity**: US data is zip-level; Colombia data is city-level only (DANE
   does not publish barrio-level price indices). CO listing data from scrapers provides
   a proxy for barrio-level pricing.
6. **Currency**: all price comparisons assume the exchange rate at the time of data ingestion.
   COP/USD is volatile — always note the rate used and its date in agent answers.
7. **FEMA data**: US flood zones only. Does not cover private flood risk products or
   climate-adjusted future risk.

---

## Build Order

Build notebooks in this exact order. Each stage depends on the previous.

```
1. 99_helpers.py
2. 00_setup_schema.py
3. 13_ingest_exchange_rates.py     ← needed by silver listings for COP→USD
4. 01_ingest_listings_us.py
5. 02_ingest_listings_co.py
6. 03_ingest_weather.py
7. 04_ingest_crime_us.py
8. 05_ingest_crime_co.py
9. 06_ingest_schools_us.py
10. 07_ingest_schools_co.py
11. 08_ingest_hazards.py
12. 09_ingest_demographics.py
13. 10_ingest_amenities.py
14. 11_ingest_market_trends.py
15. 12_ingest_economic.py
16. 20_build_silver_listings.py
17. 23_build_silver_risk.py        ← needed by neighborhood profile
18. 21_build_silver_neighborhood.py
19. 22_build_silver_market.py
20. 24_build_gold_static.py        ← gold.hazard_risk + gold.school_rankings
21. 30_score_neighborhoods.py
22. 31_build_market_trends.py
23. 32_detect_value_opportunities.py
24. 33_build_comparison_index.py
25. 34_build_dashboard_views.py
26. 40_agent_tools.py
27. 42_session_logger.py
28. 41_realestate_agent.py         ← last; depends on all of the above
```

---

## Example Agent Calls (add to bottom of `41_realestate_agent.py`)

```python
# Smoke test — validates planning + tool execution without LLM synthesis
agent_smoke_test()

# Single-turn calls
print_realestate_answer("Find me a 3 bedroom home under $200,000 USD in Medellín")
print_realestate_answer("How does Medellín compare to Austin for an American buying a home?")
print_realestate_answer("What are the earthquake and flood risks in Bogotá?")
print_realestate_answer("What are the best schools near El Poblado in Medellín?")
print_realestate_answer("On a $75,000 USD annual income with $30,000 down, what can I afford in Bogotá?")
print_realestate_answer("Where are the underpriced homes in Bogotá right now?")
print_realestate_answer("What is the real estate market like in Medellín compared to Miami?")

# Multi-turn conversation — context carries across calls
new_session()
chat("I'm an American thinking about buying a place in Medellín")
chat("What's the safest neighborhood for families?")     # "Medellín" resolved from context
chat("How do the schools compare to what I'd find in Atlanta?")  # cross-country from context
chat("What would a place there run me on a $90k salary?")        # affordability in same area

# Natural language flexibility examples — these should all work without magic words
print_realestate_answer("Something quiet with good transit in Bogotá, nothing too pricey")
print_realestate_answer("I have two kids and want somewhere safe near decent schools")
print_realestate_answer("Is it worth it compared to staying in the US?")
print_realestate_answer("What kind of place can I get for $150k there?")
```
