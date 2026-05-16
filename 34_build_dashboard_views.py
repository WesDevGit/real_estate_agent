# Databricks notebook source
# DBTITLE 1,Build dashboard SQL views
# MAGIC %md
# MAGIC # Dashboard SQL Views
# MAGIC
# MAGIC Pre-wired views for the future Databricks Lakeview dashboard. These views
# MAGIC are not expected to serve traffic yet — creating them now catches Silver/Gold
# MAGIC schema drift early.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Catalog context
spark.sql("USE CATALOG realestate")

# COMMAND ----------

# DBTITLE 1,gold.v_listing_map — listings with neighborhood/risk/market context
spark.sql("""
CREATE OR REPLACE VIEW realestate.gold.v_listing_map AS
SELECT
  l.*,
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
      AND m.date = (
        SELECT MAX(date) FROM realestate.silver.market_summary mx
        WHERE mx.country_code = l.country_code
          AND mx.city = l.city
      )
""")
print("Created realestate.gold.v_listing_map")

# COMMAND ----------

# DBTITLE 1,gold.v_recent_sessions — agent answer feed for dashboard
spark.sql("""
CREATE OR REPLACE VIEW realestate.gold.v_recent_sessions AS
SELECT *
FROM realestate.gold.agent_sessions
ORDER BY timestamp DESC
LIMIT 100
""")
print("Created realestate.gold.v_recent_sessions")

# COMMAND ----------

# DBTITLE 1,gold.v_city_summary — one row per city for top-level dashboard
spark.sql("""
CREATE OR REPLACE VIEW realestate.gold.v_city_summary AS
SELECT
  n.country_code,
  n.city,
  COUNT(*)                                    AS neighborhood_count,
  AVG(n.composite_score)                      AS avg_composite_score,
  AVG(n.crime_rate_per_100k)                  AS avg_crime_per_100k,
  AVG(n.school_score_normalized)              AS avg_school_score,
  AVG(n.amenity_density_score)                AS avg_amenity_density,
  AVG(n.transit_access_score)                 AS avg_transit_access,
  AVG(n.hazard_composite_score)               AS avg_hazard_score,
  AVG(n.weather_extreme_days_per_yr)          AS avg_weather_extreme_days,
  AVG(m.median_price_usd)                     AS latest_median_price_usd
FROM realestate.gold.neighborhood_scorecard n
LEFT JOIN realestate.silver.market_summary m
       ON n.country_code = m.country_code
      AND n.city = m.city
GROUP BY n.country_code, n.city
""")
print("Created realestate.gold.v_city_summary")

# COMMAND ----------

print("Dashboard views built.")
spark.sql("SHOW VIEWS IN realestate.gold").show(truncate=False)
