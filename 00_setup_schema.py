# Databricks notebook source
# DBTITLE 1,Real Estate Agent - Schema Setup
# MAGIC %md
# MAGIC # Real Estate Agent — Schema Setup
# MAGIC
# MAGIC Creates the `realestate` Unity Catalog, the `bronze` / `silver` / `gold` schemas,
# MAGIC the `bronze.raw` volume used by raw-file ingestion notebooks, and every Delta
# MAGIC table referenced by the rest of the agent stack. Safe to re-run — everything
# MAGIC uses `CREATE ... IF NOT EXISTS`.
# MAGIC
# MAGIC ## Layout
# MAGIC
# MAGIC **Bronze (16 raw tables)** — one per source / country:
# MAGIC * `listings_us`, `listings_co`
# MAGIC * `weather_history`
# MAGIC * `crime_us`, `crime_co`
# MAGIC * `schools_us`, `schools_co`
# MAGIC * `hazards`
# MAGIC * `demographics_us`, `demographics_co`
# MAGIC * `amenities`
# MAGIC * `market_trends_us`, `market_trends_co`
# MAGIC * `economic_us`, `economic_co`
# MAGIC * `exchange_rates`
# MAGIC
# MAGIC **Silver (4 normalized + enriched tables):**
# MAGIC * `listings` — unified US + CO listings, prices normalized to USD
# MAGIC * `neighborhood_profile` — one row per zip / barrio with safety, schools, amenities
# MAGIC * `market_summary` — one row per area per month
# MAGIC * `risk_profile` — natural-hazard composite per area
# MAGIC
# MAGIC **Gold (8 analytics tables):**
# MAGIC * `neighborhood_scorecard`, `market_trends`, `city_comparison`, `hazard_risk`,
# MAGIC   `school_rankings`, `value_opportunities`, `comparison_index`, `agent_sessions`
# MAGIC
# MAGIC Note: `gold.listing_map` is a SQL **view** (`gold.v_listing_map`) built by
# MAGIC `34_build_dashboard_views.py` — it is **not** materialized here.
# MAGIC
# MAGIC ## Change Data Feed
# MAGIC
# MAGIC All Silver and Gold tables have `delta.enableChangeDataFeed = true` so downstream
# MAGIC notebooks can incrementally consume changes. Bronze tables do not — they are
# MAGIC append / merge from external sources and we never replay them downstream via CDF.

# COMMAND ----------

# DBTITLE 1,Imports and use catalog
spark.sql("CREATE CATALOG IF NOT EXISTS realestate")
spark.sql("USE CATALOG realestate")
print("Using catalog: realestate")

# COMMAND ----------

# DBTITLE 1,Create schemas and raw volume
spark.sql("CREATE SCHEMA IF NOT EXISTS realestate.bronze COMMENT 'Raw ingested data from external sources'")
spark.sql("CREATE SCHEMA IF NOT EXISTS realestate.silver COMMENT 'Cleaned, normalized, enriched data'")
spark.sql("CREATE SCHEMA IF NOT EXISTS realestate.gold   COMMENT 'Analytics-ready tables for the agent and dashboards'")

spark.sql("CREATE VOLUME IF NOT EXISTS realestate.bronze.raw COMMENT 'Raw downloaded files (CSV / Excel / shapefiles) staged for parsing'")

print("Schemas ready: realestate.bronze, realestate.silver, realestate.gold")
print("Volume ready : realestate.bronze.raw")

# COMMAND ----------

# DBTITLE 1,Bronze: listings_us
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.listings_us (
    listing_id STRING COMMENT 'Source-assigned ID; primary dedup key',
    source STRING COMMENT 'rapidapi_zillow',
    scraped_at TIMESTAMP COMMENT 'Ingest time',
    address STRING COMMENT 'Full street address',
    city STRING,
    state STRING COMMENT 'Two-letter code',
    zip STRING COMMENT '5-digit',
    lat DOUBLE,
    lon DOUBLE,
    price_usd LONG COMMENT 'Asking price in USD',
    bedrooms INT,
    bathrooms DOUBLE,
    sqft INT COMMENT 'Square feet',
    price_per_sqft_usd DOUBLE COMMENT 'Derived: price_usd / sqft',
    property_type STRING COMMENT 'single_family, condo, townhouse, multi_family',
    description STRING COMMENT 'Listing text',
    listing_url STRING,
    days_on_market INT,
    listing_date DATE,
    raw_json STRING COMMENT 'Full API response as JSON string'
)
USING DELTA
PARTITIONED BY (state, city)
COMMENT 'Raw US home-purchase listings from RapidAPI Zillow scraper'
""")
print("  bronze.listings_us")

# COMMAND ----------

# DBTITLE 1,Bronze: listings_co
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.listings_co (
    listing_id STRING COMMENT 'URL slug or scraped ID',
    source STRING COMMENT 'fincaraiz or metrocuadrado',
    scraped_at TIMESTAMP,
    address STRING,
    city STRING COMMENT 'Bogota or Medellin',
    departamento STRING COMMENT 'Cundinamarca or Antioquia',
    barrio STRING COMMENT 'Neighborhood within city',
    lat DOUBLE COMMENT 'Geocoded (see 99_helpers.py)',
    lon DOUBLE COMMENT 'Geocoded',
    price_cop LONG COMMENT 'Asking price in Colombian Pesos',
    bedrooms INT,
    bathrooms DOUBLE,
    area_m2 DOUBLE COMMENT 'Square meters',
    property_type STRING COMMENT 'casa, apartamento',
    description STRING,
    listing_url STRING,
    raw_html STRING COMMENT 'gzip-compressed base64 of raw card HTML; retained only on first scrape or parse failure'
)
USING DELTA
PARTITIONED BY (city)
COMMENT 'Raw Colombia home-purchase listings from Fincaraiz / Metrocuadrado scrapers'
""")
print("  bronze.listings_co")

# COMMAND ----------

# DBTITLE 1,Bronze: weather_history
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.weather_history (
    location_key STRING COMMENT '{city}_{country_code}',
    city STRING,
    country_code STRING COMMENT 'US or CO',
    lat DOUBLE,
    lon DOUBLE,
    date DATE,
    temp_max_c DOUBLE,
    temp_min_c DOUBLE,
    temp_mean_c DOUBLE,
    precipitation_mm DOUBLE,
    wind_speed_max_kmh DOUBLE,
    weather_code INT COMMENT 'WMO weather code',
    is_extreme_day BOOLEAN COMMENT 'Derived: precip > 50mm OR wind > 80kmh OR temp extremes'
)
USING DELTA
PARTITIONED BY (country_code, city)
COMMENT '10 years of daily weather history per city from Open-Meteo archive API'
""")
print("  bronze.weather_history")

# COMMAND ----------

# DBTITLE 1,Bronze: crime_us and crime_co
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.crime_us (
    incident_id STRING COMMENT 'Source ID',
    source STRING COMMENT 'fbi_ucr or spotcrime',
    city STRING,
    state STRING,
    zip STRING,
    lat DOUBLE,
    lon DOUBLE,
    crime_type STRING COMMENT 'Raw category from source',
    crime_category STRING COMMENT 'Normalized: violent, property, other',
    incident_date DATE,
    year INT,
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'US crime incidents from FBI UCR and SpotCrime'
""")
print("  bronze.crime_us")

spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.crime_co (
    record_id STRING COMMENT 'Generated UUID',
    source STRING COMMENT 'dane or policia_nacional',
    city STRING,
    municipio STRING,
    departamento STRING,
    crime_type STRING,
    crime_category STRING COMMENT 'violent, property, other',
    count INT COMMENT 'Aggregate count for period',
    period_year INT,
    period_month INT COMMENT 'Null if annual aggregate',
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Colombia crime aggregates from DANE and Policia Nacional'
""")
print("  bronze.crime_co")

# COMMAND ----------

# DBTITLE 1,Bronze: schools_us and schools_co
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.schools_us (
    school_id STRING COMMENT 'NCES ncessch identifier',
    school_name STRING,
    city STRING,
    state STRING,
    zip STRING,
    lat DOUBLE,
    lon DOUBLE,
    grade_levels STRING COMMENT 'e.g. KG-08, 09-12',
    enrollment INT,
    school_type STRING COMMENT 'public, private, charter',
    title1_eligible BOOLEAN,
    math_proficiency_pct DOUBLE COMMENT 'State assessment, if available',
    reading_proficiency_pct DOUBLE,
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'US schools from NCES / Urban Institute Education Data Portal'
""")
print("  bronze.schools_us")

spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.schools_co (
    school_id STRING COMMENT 'DANE/MEN institution code',
    institution_name STRING,
    city STRING,
    municipio STRING,
    departamento STRING,
    lat DOUBLE,
    lon DOUBLE,
    grade_levels STRING,
    enrollment INT,
    school_type STRING COMMENT 'oficial, privado',
    icfes_score DOUBLE COMMENT 'Average ICFES Saber 11 score for the institution',
    icfes_percentile DOUBLE COMMENT 'National percentile rank (computed)',
    icfes_year INT,
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Colombia schools from ICFES Saber 11 + MEN registry'
""")
print("  bronze.schools_co")

# COMMAND ----------

# DBTITLE 1,Bronze: hazards
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.hazards (
    hazard_id STRING COMMENT 'Generated: {source}_{type}_{geo_id}',
    source STRING COMMENT 'fema, usgs, noaa_spc, ungrd',
    country_code STRING,
    state_or_dept STRING,
    county_or_municipio STRING,
    zip_or_zone STRING,
    lat DOUBLE COMMENT 'Centroid of risk zone',
    lon DOUBLE,
    hazard_type STRING COMMENT 'flood, earthquake, wildfire, tornado, landslide',
    risk_level STRING COMMENT 'low, medium, high, very_high',
    risk_score DOUBLE COMMENT '0.0-1.0',
    data_date DATE COMMENT 'Currency date of the source data',
    details_json STRING COMMENT 'Raw source payload',
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Natural-hazard risk zones (FEMA flood, USGS earthquake, NOAA tornado, UNGRD landslide)'
""")
print("  bronze.hazards")

# COMMAND ----------

# DBTITLE 1,Bronze: demographics_us and demographics_co
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.demographics_us (
    geo_id STRING COMMENT 'FIPS code',
    state STRING,
    county STRING,
    zip STRING COMMENT 'ZCTA',
    city STRING,
    total_population INT,
    median_household_income_usd INT COMMENT 'ACS B19013',
    median_age DOUBLE COMMENT 'ACS B01002',
    pct_college_educated DOUBLE COMMENT 'ACS B15003',
    pct_homeowner DOUBLE COMMENT 'ACS B25003',
    median_home_value_usd INT COMMENT 'ACS B25077',
    year INT COMMENT 'ACS 5-year estimate year',
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'US Census ACS 5-year demographics by ZCTA'
""")
print("  bronze.demographics_us")

spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.demographics_co (
    geo_id STRING COMMENT 'DANE DIVIPOLA code',
    departamento STRING,
    municipio STRING,
    city STRING,
    total_population INT,
    median_household_income_cop LONG,
    pct_urban DOUBLE,
    pct_homeowner DOUBLE,
    year INT,
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Colombia demographics from DANE Censo Nacional 2018'
""")
print("  bronze.demographics_co")

# COMMAND ----------

# DBTITLE 1,Bronze: amenities
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.amenities (
    amenity_id STRING COMMENT 'OSM node/way ID',
    country_code STRING,
    city STRING,
    lat DOUBLE,
    lon DOUBLE,
    amenity_type STRING COMMENT 'grocery, hospital, park, transit_stop, pharmacy, school, restaurant',
    name STRING,
    address STRING,
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'OSM amenity points-of-interest per city (Overpass API)'
""")
print("  bronze.amenities")

# COMMAND ----------

# DBTITLE 1,Bronze: market_trends_us and market_trends_co
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.market_trends_us (
    geo_id STRING COMMENT 'Zip or metro ID',
    geo_type STRING COMMENT 'zip, city, metro',
    zip STRING,
    city STRING,
    state STRING,
    date DATE COMMENT 'Month-end date',
    median_list_price_usd LONG,
    median_sale_price_usd LONG,
    median_days_on_market INT,
    homes_sold INT,
    inventory_count INT,
    months_of_supply DOUBLE COMMENT 'inventory / monthly_sales_rate',
    price_reduced_pct DOUBLE COMMENT 'pct of listings with price reduction',
    source STRING COMMENT 'zillow_research or redfin',
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'US market trend snapshots from Zillow Research and Redfin Data Center'
""")
print("  bronze.market_trends_us")

spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.market_trends_co (
    geo_id STRING COMMENT 'DANE city code',
    city STRING,
    departamento STRING,
    date DATE COMMENT 'Quarter-end date',
    ipvn_index DOUBLE COMMENT 'DANE new housing price index',
    yoy_change_pct DOUBLE COMMENT 'Year-over-year pct change',
    new_construction_price_cop LONG COMMENT 'Avg price per m2 new construction',
    units_sold INT COMMENT 'If available',
    source STRING COMMENT 'dane_ipvn',
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Colombia quarterly market trends from DANE IPVN'
""")
print("  bronze.market_trends_co")

# COMMAND ----------

# DBTITLE 1,Bronze: economic_us and economic_co
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.economic_us (
    geo_id STRING COMMENT 'BLS area code or FIPS',
    metro_area STRING,
    state STRING,
    date DATE COMMENT 'Month-end',
    unemployment_rate DOUBLE COMMENT 'BLS LAUS',
    job_growth_yoy_pct DOUBLE COMMENT 'BLS CES',
    median_wage_usd INT COMMENT 'BLS OES annual',
    mortgage_rate_30yr DOUBLE COMMENT 'FRED series MORTGAGE30US',
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'US economic indicators (BLS LAUS / CES / OES + FRED mortgage rates)'
""")
print("  bronze.economic_us")

spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.economic_co (
    geo_id STRING COMMENT 'DANE DIVIPOLA or Banco Rep region code',
    city STRING,
    departamento STRING,
    date DATE,
    unemployment_rate DOUBLE COMMENT 'DANE',
    inflation_rate DOUBLE COMMENT 'Banco de la Republica',
    mortgage_rate_pct DOUBLE COMMENT 'Banco de la Republica - Colombian mortgage rate (pct, not COP)',
    gdp_growth_pct DOUBLE COMMENT 'DANE',
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Colombia economic indicators (DANE GEIH + Banco de la Republica)'
""")
print("  bronze.economic_co")

# COMMAND ----------

# DBTITLE 1,Bronze: exchange_rates
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.bronze.exchange_rates (
    date DATE,
    from_currency STRING COMMENT 'COP',
    to_currency STRING COMMENT 'USD',
    rate DOUBLE COMMENT 'COP per 1 USD (or 1/rate for USD per COP)',
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Daily COP/USD exchange rates from frankfurter.app'
""")
print("  bronze.exchange_rates")

# COMMAND ----------

# DBTITLE 1,Silver: listings
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.silver.listings (
    listing_id STRING COMMENT '{source}_{original_id}',
    source STRING,
    country_code STRING COMMENT 'US or CO',
    city STRING,
    state_or_dept STRING COMMENT 'State abbrev (US) or departamento (CO)',
    zip_or_municipio STRING,
    barrio_or_neighborhood STRING,
    lat DOUBLE,
    lon DOUBLE,
    price_usd LONG COMMENT 'Converted using bronze.exchange_rates for CO',
    price_local LONG COMMENT 'Original price in local currency',
    local_currency STRING COMMENT 'USD or COP',
    price_per_sqft_usd DOUBLE COMMENT 'CO: convert m2 to sqft first (1 m2 = 10.764 sqft)',
    area_sqft DOUBLE COMMENT 'Normalized to sqft',
    bedrooms INT,
    bathrooms DOUBLE,
    property_type STRING COMMENT 'Normalized: single_family, apartment, condo, townhouse',
    listing_url STRING,
    days_on_market INT,
    listing_date DATE,
    scraped_at TIMESTAMP
)
USING DELTA
COMMENT 'Unified US + CO listings, prices normalized to USD, deduplicated on listing_id'
""")
print("  silver.listings")

# COMMAND ----------

# DBTITLE 1,Silver: neighborhood_profile
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.silver.neighborhood_profile (
    profile_id STRING COMMENT '{country_code}_{city}_{zip_or_zone}',
    country_code STRING,
    city STRING,
    zip_or_municipio STRING,
    barrio STRING COMMENT 'null for US',
    lat_centroid DOUBLE,
    lon_centroid DOUBLE,
    crime_rate_per_100k DOUBLE,
    crime_trend STRING COMMENT 'rising, stable, falling (3-yr window)',
    school_count INT COMMENT 'Schools within 3 miles / 5 km',
    school_score_normalized DOUBLE COMMENT '0-100',
    grocery_count INT COMMENT 'Grocery stores within 1 mile / 2 km',
    park_count INT COMMENT 'Parks within 1 mile / 2 km',
    hospital_count INT COMMENT 'Within 5 miles / 8 km',
    transit_stop_count INT COMMENT 'Within 0.5 mile / 1 km',
    population INT,
    median_income_usd INT COMMENT 'Converted for CO using exchange rate',
    pct_homeowner DOUBLE,
    amenity_density_score DOUBLE COMMENT '0-100; min-max scaled OSM amenity counts',
    transit_access_score DOUBLE COMMENT '0-100; OSM transit_stop_count within 0.5 mi / 1 km',
    weather_extreme_days_per_yr DOUBLE COMMENT 'From bronze.weather_history',
    hazard_composite_score DOUBLE COMMENT '0-100; higher = more risky',
    profile_updated_at TIMESTAMP
)
USING DELTA
COMMENT 'One row per zip (US) or barrio/municipio (CO) with safety, schools, amenities, hazards'
""")
print("  silver.neighborhood_profile")

# COMMAND ----------

# DBTITLE 1,Silver: market_summary
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.silver.market_summary (
    geo_id STRING,
    country_code STRING,
    city STRING,
    zip_or_municipio STRING,
    date DATE COMMENT 'Month-end',
    median_price_usd LONG,
    price_trend_3mo_pct DOUBLE COMMENT '3-month price change pct',
    price_trend_12mo_pct DOUBLE COMMENT '12-month price change pct',
    median_days_on_market INT,
    inventory_count INT COMMENT 'Active listings count (from bronze.listings)',
    months_of_supply DOUBLE,
    market_temp STRING COMMENT 'hot (<2mo supply), warm (2-4), cool (4-6), cold (>6)'
)
USING DELTA
COMMENT 'One row per area per month with price trends and market temperature'
""")
print("  silver.market_summary")

# COMMAND ----------

# DBTITLE 1,Silver: risk_profile
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.silver.risk_profile (
    geo_id STRING,
    country_code STRING,
    city STRING,
    zip_or_municipio STRING,
    flood_risk_score DOUBLE COMMENT '0-100',
    earthquake_risk_score DOUBLE COMMENT '0-100',
    wildfire_risk_score DOUBLE COMMENT '0-100; mostly US',
    tornado_risk_score DOUBLE COMMENT '0-100; US only',
    landslide_risk_score DOUBLE COMMENT '0-100; relevant for Medellin slopes',
    composite_hazard_score DOUBLE COMMENT 'Weighted: flood 30, eq 30, wildfire 20, tornado 10, landslide 10',
    risk_label STRING COMMENT 'low, medium, high, very_high',
    profile_updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Natural-hazard composite per area, with weighted overall risk score'
""")
print("  silver.risk_profile")

# COMMAND ----------

# DBTITLE 1,Gold: neighborhood_scorecard
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.gold.neighborhood_scorecard (
    profile_id STRING COMMENT '{country_code}_{city}_{zip_or_zone}',
    country_code STRING,
    city STRING,
    zip_or_municipio STRING,
    barrio STRING COMMENT 'null for US',
    lat_centroid DOUBLE,
    lon_centroid DOUBLE,
    crime_rate_per_100k DOUBLE,
    crime_trend STRING,
    school_count INT,
    school_score_normalized DOUBLE,
    grocery_count INT,
    park_count INT,
    hospital_count INT,
    transit_stop_count INT,
    population INT,
    median_income_usd INT,
    pct_homeowner DOUBLE,
    amenity_density_score DOUBLE,
    transit_access_score DOUBLE,
    weather_extreme_days_per_yr DOUBLE,
    hazard_composite_score DOUBLE,
    profile_updated_at TIMESTAMP,
    composite_score DOUBLE COMMENT '0-100 weighted: safety 30, schools 25, amenities 20, hazard 15, transit 10',
    country_percentile_rank DOUBLE COMMENT 'Rank within US or CO separately',
    global_percentile_rank DOUBLE COMMENT 'Rank across all neighborhoods (US + CO combined)'
)
USING DELTA
COMMENT 'Full neighborhood scorecard: silver.neighborhood_profile + composite scores and rankings'
""")
print("  gold.neighborhood_scorecard")

# COMMAND ----------

# DBTITLE 1,Gold: market_trends
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.gold.market_trends (
    geo_id STRING,
    country_code STRING,
    city STRING,
    zip_or_municipio STRING,
    date DATE COMMENT 'Month-end',
    median_price_usd LONG,
    price_trend_3mo_pct DOUBLE,
    price_trend_12mo_pct DOUBLE,
    median_days_on_market INT,
    inventory_count INT,
    months_of_supply DOUBLE,
    market_temp STRING,
    median_price_usd_rolling_6mo DOUBLE COMMENT '6-month centered rolling average of median_price_usd',
    median_price_usd_rolling_12mo DOUBLE COMMENT '12-month rolling average'
)
USING DELTA
COMMENT 'silver.market_summary plus rolling averages for time-series chart widgets'
""")
print("  gold.market_trends")

# COMMAND ----------

# DBTITLE 1,Gold: city_comparison
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.gold.city_comparison (
    comparison_id STRING COMMENT '{us_city}_{co_city}_{date}',
    us_city STRING,
    co_city STRING,
    comparison_date DATE,
    us_median_price_usd LONG,
    co_median_price_usd LONG COMMENT 'CO price in USD',
    price_ratio DOUBLE COMMENT 'co / us; <1.0 means CO is cheaper',
    us_crime_per_100k DOUBLE,
    co_crime_per_100k DOUBLE,
    us_school_score DOUBLE COMMENT 'Normalized 0-100',
    co_school_score DOUBLE COMMENT 'Normalized 0-100',
    us_composite_score DOUBLE,
    co_composite_score DOUBLE,
    us_hazard_score DOUBLE,
    co_hazard_score DOUBLE,
    us_unemployment_rate DOUBLE,
    co_unemployment_rate DOUBLE,
    us_weather_extreme_days DOUBLE,
    co_weather_extreme_days DOUBLE,
    us_amenity_density_score DOUBLE COMMENT 'OSM-derived 0-100',
    co_amenity_density_score DOUBLE COMMENT 'OSM-derived 0-100; comparable to US on same scale',
    narrative_context STRING COMMENT 'LLM-generated one-paragraph plain-English comparison'
)
USING DELTA
COMMENT 'Pre-computed side-by-side comparisons for US <-> Colombia city pairs; rebuilt daily'
""")
print("  gold.city_comparison")

# COMMAND ----------

# DBTITLE 1,Gold: hazard_risk
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.gold.hazard_risk (
    geo_id STRING,
    country_code STRING,
    city STRING,
    zip_or_municipio STRING,
    flood_risk_score DOUBLE COMMENT '0-100',
    earthquake_risk_score DOUBLE COMMENT '0-100',
    wildfire_risk_score DOUBLE COMMENT '0-100; mostly US',
    tornado_risk_score DOUBLE COMMENT '0-100; US only',
    landslide_risk_score DOUBLE COMMENT '0-100; relevant for Medellin slopes',
    composite_hazard_score DOUBLE COMMENT 'Weighted: flood 30, eq 30, wildfire 20, tornado 10, landslide 10',
    risk_label STRING COMMENT 'low, medium, high, very_high',
    profile_updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Map-overlay-optimized copy of silver.risk_profile'
""")
print("  gold.hazard_risk")

# COMMAND ----------

# DBTITLE 1,Gold: school_rankings
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.gold.school_rankings (
    school_id STRING COMMENT 'NCES ncessch (US) or DANE/MEN code (CO)',
    country_code STRING COMMENT 'US or CO',
    school_name STRING COMMENT 'school_name (US) or institution_name (CO)',
    city STRING,
    state_or_dept STRING COMMENT 'state (US) or departamento (CO)',
    zip_or_municipio STRING,
    lat DOUBLE,
    lon DOUBLE,
    grade_levels STRING,
    enrollment INT,
    school_type STRING COMMENT 'public/private/charter (US); oficial/privado (CO)',
    title1_eligible BOOLEAN COMMENT 'US only',
    math_proficiency_pct DOUBLE COMMENT 'US only',
    reading_proficiency_pct DOUBLE COMMENT 'US only',
    icfes_score DOUBLE COMMENT 'CO only',
    icfes_percentile DOUBLE COMMENT 'CO only',
    icfes_year INT COMMENT 'CO only',
    school_score_normalized DOUBLE COMMENT '0-100; US: math+reading avg scaled; CO: icfes_percentile direct',
    ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Unified US + CO school rankings on common 0-100 normalized score'
""")
print("  gold.school_rankings")

# COMMAND ----------

# DBTITLE 1,Gold: value_opportunities
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.gold.value_opportunities (
    listing_id STRING,
    source STRING,
    country_code STRING,
    city STRING,
    state_or_dept STRING,
    zip_or_municipio STRING,
    barrio_or_neighborhood STRING,
    lat DOUBLE,
    lon DOUBLE,
    price_usd LONG,
    price_local LONG,
    local_currency STRING,
    price_per_sqft_usd DOUBLE,
    area_sqft DOUBLE,
    bedrooms INT,
    bathrooms DOUBLE,
    property_type STRING,
    listing_url STRING,
    days_on_market INT,
    listing_date DATE,
    scraped_at TIMESTAMP,
    zip_median_price_usd LONG COMMENT 'Median price in the listing area (zip/municipio)',
    price_vs_area_ratio DOUBLE COMMENT 'price_usd / zip_median_price_usd; <0.85 = value opportunity',
    composite_score DOUBLE COMMENT 'From gold.neighborhood_scorecard for the listing area',
    risk_label STRING COMMENT 'From silver.risk_profile for the listing area'
)
USING DELTA
COMMENT 'Active listings priced <85pct of area median, controlled for beds and sqft'
""")
print("  gold.value_opportunities")

# COMMAND ----------

# DBTITLE 1,Gold: comparison_index
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.gold.comparison_index (
    geo_id STRING,
    country_code STRING,
    city STRING,
    zip_or_municipio STRING,
    norm_price_usd DOUBLE COMMENT 'Median price in USD',
    norm_crime_score DOUBLE COMMENT '0-100; higher = safer (inverted crime rate)',
    norm_school_score DOUBLE COMMENT '0-100',
    norm_hazard_score DOUBLE COMMENT '0-100; higher = safer (inverted hazard)',
    norm_weather_score DOUBLE COMMENT '0-100; higher = milder weather',
    norm_affordability_score DOUBLE COMMENT 'Based on price-to-income ratio',
    norm_amenity_score DOUBLE COMMENT '0-100',
    norm_economic_score DOUBLE COMMENT 'Employment + growth composite'
)
USING DELTA
COMMENT 'Normalization layer for cross-country comparison; min-max scaled across US + CO'
""")
print("  gold.comparison_index")

# COMMAND ----------

# DBTITLE 1,Gold: agent_sessions
spark.sql("""
CREATE TABLE IF NOT EXISTS realestate.gold.agent_sessions (
    session_id STRING COMMENT 'UUID',
    timestamp TIMESTAMP,
    user_question STRING,
    intent STRING COMMENT 'Planner-resolved intent label (for logging only)',
    planner_reasoning STRING COMMENT 'LLM stated reasoning for tool selection',
    plan_json STRING COMMENT 'Full validated plan including tool_calls as JSON string',
    answer_text STRING COMMENT 'Final synthesized answer',
    structured_results_json STRING COMMENT 'Key numeric results as JSON (for dashboard widgets)',
    country_filter STRING COMMENT 'US, CO, or BOTH',
    cities_mentioned STRING COMMENT 'Comma-separated list extracted from tool call params',
    tool_names_used STRING COMMENT 'Comma-separated list of tools actually called',
    evidence_record_count INT COMMENT 'Total rows returned across all tools',
    refinement_applied BOOLEAN COMMENT 'True if sparse-result refinement was triggered',
    context_turns_used INT COMMENT 'Number of prior conversation turns passed to planner',
    latency_seconds DOUBLE COMMENT 'Wall-clock time for full agent call'
)
USING DELTA
COMMENT 'One row per agent invocation; written by 42_session_logger.py'
""")
print("  gold.agent_sessions")

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed on Silver and Gold tables
# Bronze tables are append/merge from external sources and never replayed via CDF.
# Silver and Gold tables feed downstream notebooks and the agent, so we enable CDF.
SILVER_TABLES = [
    "listings",
    "neighborhood_profile",
    "market_summary",
    "risk_profile",
]

GOLD_TABLES = [
    "neighborhood_scorecard",
    "market_trends",
    "city_comparison",
    "hazard_risk",
    "school_rankings",
    "value_opportunities",
    "comparison_index",
    "agent_sessions",
]

for table in SILVER_TABLES:
    spark.sql(
        f"ALTER TABLE realestate.silver.{table} "
        f"SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true)"
    )
    print(f"  CDF on  silver.{table}")

for table in GOLD_TABLES:
    spark.sql(
        f"ALTER TABLE realestate.gold.{table} "
        f"SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true)"
    )
    print(f"  CDF on  gold.{table}")

# COMMAND ----------

# DBTITLE 1,Summary: table inventory with column counts
BRONZE_TABLES = [
    "listings_us",
    "listings_co",
    "weather_history",
    "crime_us",
    "crime_co",
    "schools_us",
    "schools_co",
    "hazards",
    "demographics_us",
    "demographics_co",
    "amenities",
    "market_trends_us",
    "market_trends_co",
    "economic_us",
    "economic_co",
    "exchange_rates",
]

rows = []
for table in BRONZE_TABLES:
    col_count = len(spark.table(f"realestate.bronze.{table}").columns)
    rows.append(("bronze", table, col_count))
for table in SILVER_TABLES:
    col_count = len(spark.table(f"realestate.silver.{table}").columns)
    rows.append(("silver", table, col_count))
for table in GOLD_TABLES:
    col_count = len(spark.table(f"realestate.gold.{table}").columns)
    rows.append(("gold", table, col_count))

# Print as a fixed-width text table
print()
print(f"{'schema':<8} {'table':<28} {'columns':>8}")
print(f"{'-' * 8} {'-' * 28} {'-' * 8}")
for schema, table, col_count in rows:
    print(f"{schema:<8} {table:<28} {col_count:>8}")
print(f"{'-' * 8} {'-' * 28} {'-' * 8}")
print(f"{'total':<8} {len(rows):<28} {sum(r[2] for r in rows):>8}")
print()
print(
    f"Created {sum(1 for r in rows if r[0] == 'bronze')} bronze, "
    f"{sum(1 for r in rows if r[0] == 'silver')} silver, "
    f"{sum(1 for r in rows if r[0] == 'gold')} gold tables."
)
print("Note: gold.listing_map is a VIEW created later by 34_build_dashboard_views.py.")
