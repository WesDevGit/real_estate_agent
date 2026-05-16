# Databricks notebook source
# DBTITLE 1,Build gold.comparison_index + gold.city_comparison
# MAGIC %md
# MAGIC # Build Cross-Country Comparison Layer
# MAGIC
# MAGIC * `gold.comparison_index` — per-area normalized 0–100 metrics across US + CO.
# MAGIC * `gold.city_comparison` — pre-computed US ↔ CO city pair comparisons with
# MAGIC   LLM-generated narrative.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports
import json
from datetime import date

import pyspark.sql.functions as F

try:
    from mlflow.deployments import get_deploy_client
except Exception:
    get_deploy_client = None

SCORE_SRC = "realestate.gold.neighborhood_scorecard"
RISK_SRC  = "realestate.silver.risk_profile"
MARKET_SRC = "realestate.silver.market_summary"
ECON_US  = "realestate.bronze.economic_us"
ECON_CO  = "realestate.bronze.economic_co"
WEATHER  = "realestate.bronze.weather_history"

INDEX_TARGET     = "realestate.gold.comparison_index"
COMPARISON_TARGET = "realestate.gold.city_comparison"

MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

CITY_PAIRS = [
    ("Miami",   "Bogota"),
    ("Atlanta", "Bogota"),
    ("Houston", "Bogota"),
    ("Chicago", "Bogota"),
    ("New York","Bogota"),
    ("Los Angeles","Bogota"),
    ("Miami",   "Medellin"),
    ("Atlanta", "Medellin"),
    ("Houston", "Medellin"),
    ("Austin",  "Medellin"),
    ("Denver",  "Medellin"),
]

# COMMAND ----------

# DBTITLE 1,Build gold.comparison_index (per-area min-max normalized)
def minmax(df, col, out_col, invert=False):
    stats = df.agg(F.min(col).alias("lo"), F.max(col).alias("hi")).first()
    lo, hi = stats["lo"] or 0.0, stats["hi"] or 0.0
    rng = max((hi or 0) - (lo or 0), 1.0)
    expr = (F.col(col) - F.lit(lo)) / F.lit(rng) * F.lit(100.0)
    if invert:
        expr = F.lit(100.0) - expr
    return df.withColumn(out_col, expr)


def build_index() -> int:
    if not spark.catalog.tableExists(SCORE_SRC):
        print("Need gold.neighborhood_scorecard first.")
        return 0

    df = spark.table(SCORE_SRC)
    if df.count() == 0:
        print("No neighborhood_scorecard rows.")
        return 0

    df = minmax(df, "crime_rate_per_100k", "norm_crime_score", invert=True)
    df = minmax(df, "school_score_normalized", "norm_school_score", invert=False)
    df = minmax(df, "hazard_composite_score", "norm_hazard_score", invert=True)
    df = minmax(df, "weather_extreme_days_per_yr", "norm_weather_score", invert=True)
    df = minmax(df, "amenity_density_score", "norm_amenity_score", invert=False)
    df = minmax(df, "median_income_usd", "norm_economic_score", invert=False)

    out = df.select(
        F.col("profile_id").alias("geo_id"),
        "country_code",
        "city",
        "zip_or_municipio",
        F.lit(None).cast("double").alias("norm_price_usd"),
        "norm_crime_score",
        "norm_school_score",
        "norm_hazard_score",
        "norm_weather_score",
        F.lit(None).cast("double").alias("norm_affordability_score"),
        "norm_amenity_score",
        "norm_economic_score",
    )
    out.write.mode("overwrite").saveAsTable(INDEX_TARGET)
    count = spark.table(INDEX_TARGET).count()
    print(f"{INDEX_TARGET} written: {count} rows")
    return count


build_index()

# COMMAND ----------

# DBTITLE 1,LLM client for narrative_context
def _llm_call(prompt: str) -> str:
    if get_deploy_client is None:
        return ""
    try:
        client = get_deploy_client("databricks")
        response = client.predict(
            endpoint=MODEL_ENDPOINT,
            inputs={
                "messages": [
                    {"role": "system", "content": "You are a real estate analyst. Output plain prose, no markdown."},
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": 400,
                "temperature": 0.3,
            },
        )
        choices = response.get("choices") if isinstance(response, dict) else None
        if isinstance(choices, list) and choices:
            msg = choices[0].get("message") or {}
            return str(msg.get("content") or "")
    except Exception as e:
        print(f"  LLM narrative failed: {e}")
    return ""

# COMMAND ----------

# DBTITLE 1,Build gold.city_comparison
def city_metrics(city: str, country_code: str) -> dict:
    """Median city-level metrics from gold.comparison_index + market summary."""
    out = {}
    idx = spark.sql(f"""
        SELECT
          AVG(norm_crime_score)    AS norm_crime,
          AVG(norm_school_score)   AS norm_school,
          AVG(norm_hazard_score)   AS norm_hazard,
          AVG(norm_weather_score)  AS norm_weather,
          AVG(norm_amenity_score)  AS norm_amenity
        FROM {INDEX_TARGET}
        WHERE country_code='{country_code}' AND city='{city}'
    """).first()
    out.update(idx.asDict() if idx else {})

    # Median price.
    try:
        mp = spark.sql(f"""
            SELECT median(median_price_usd) AS p
            FROM {MARKET_SRC}
            WHERE country_code='{country_code}' AND city='{city}'
              AND date = (SELECT MAX(date) FROM {MARKET_SRC} WHERE country_code='{country_code}' AND city='{city}')
        """).first()
        out["median_price_usd"] = float(mp["p"]) if mp and mp["p"] is not None else None
    except Exception:
        out["median_price_usd"] = None

    # Crime rate raw.
    try:
        cr = spark.sql(f"""
            SELECT AVG(crime_rate_per_100k) AS r
            FROM {SCORE_SRC}
            WHERE country_code='{country_code}' AND city='{city}'
        """).first()
        out["crime_per_100k"] = float(cr["r"]) if cr and cr["r"] is not None else None
    except Exception:
        out["crime_per_100k"] = None

    # Composite + hazard + amenity_density raw.
    try:
        cs = spark.sql(f"""
            SELECT AVG(composite_score) AS c, AVG(hazard_composite_score) AS h,
                   AVG(amenity_density_score) AS a, AVG(transit_access_score) AS t,
                   AVG(weather_extreme_days_per_yr) AS w
            FROM {SCORE_SRC}
            WHERE country_code='{country_code}' AND city='{city}'
        """).first()
        out["composite_score"] = float(cs["c"]) if cs and cs["c"] is not None else None
        out["hazard_score"]    = float(cs["h"]) if cs and cs["h"] is not None else None
        out["amenity_density_score"] = float(cs["a"]) if cs and cs["a"] is not None else None
        out["transit_access_score"]  = float(cs["t"]) if cs and cs["t"] is not None else None
        out["weather_extreme_days"]  = float(cs["w"]) if cs and cs["w"] is not None else None
    except Exception:
        pass

    # Unemployment rate.
    try:
        if country_code == "US":
            er = spark.sql(f"""
                SELECT AVG(unemployment_rate) AS u
                FROM {ECON_US}
                WHERE metro_area='{city}'
                  AND date = (SELECT MAX(date) FROM {ECON_US} WHERE metro_area='{city}')
            """).first()
        else:
            er = spark.sql(f"""
                SELECT AVG(unemployment_rate) AS u
                FROM {ECON_CO}
                WHERE city='{city}'
                  AND date = (SELECT MAX(date) FROM {ECON_CO} WHERE city='{city}')
            """).first()
        out["unemployment_rate"] = float(er["u"]) if er and er["u"] is not None else None
    except Exception:
        out["unemployment_rate"] = None

    return out


def build_comparison() -> int:
    today = date.today()
    rows = []
    for us_city, co_city in CITY_PAIRS:
        us = city_metrics(us_city, "US")
        co = city_metrics(co_city, "CO")

        price_ratio = None
        if us.get("median_price_usd") and co.get("median_price_usd"):
            price_ratio = co["median_price_usd"] / us["median_price_usd"]

        # Narrative.
        narrative_prompt = (
            f"Compare living in {co_city}, Colombia vs {us_city}, USA for an American buying a home. "
            f"Three sentences focused on cost, safety, and what's most different. "
            f"Data:\n{json.dumps({'us': us, 'co': co}, indent=2, default=str)}"
        )
        narrative = _llm_call(narrative_prompt)

        rows.append({
            "comparison_id": f"{us_city}_{co_city}_{today.isoformat()}",
            "us_city": us_city,
            "co_city": co_city,
            "comparison_date": today,
            "us_median_price_usd": int(us["median_price_usd"]) if us.get("median_price_usd") else None,
            "co_median_price_usd": int(co["median_price_usd"]) if co.get("median_price_usd") else None,
            "price_ratio": price_ratio,
            "us_crime_per_100k": us.get("crime_per_100k"),
            "co_crime_per_100k": co.get("crime_per_100k"),
            "us_school_score": us.get("norm_school"),
            "co_school_score": co.get("norm_school"),
            "us_composite_score": us.get("composite_score"),
            "co_composite_score": co.get("composite_score"),
            "us_hazard_score": us.get("hazard_score"),
            "co_hazard_score": co.get("hazard_score"),
            "us_unemployment_rate": us.get("unemployment_rate"),
            "co_unemployment_rate": co.get("unemployment_rate"),
            "us_weather_extreme_days": us.get("weather_extreme_days"),
            "co_weather_extreme_days": co.get("weather_extreme_days"),
            "us_amenity_density_score": us.get("amenity_density_score"),
            "co_amenity_density_score": co.get("amenity_density_score"),
            "narrative_context": narrative or None,
        })

    if not rows:
        return 0
    df = spark.createDataFrame(rows)
    df.write.mode("overwrite").saveAsTable(COMPARISON_TARGET)
    count = spark.table(COMPARISON_TARGET).count()
    print(f"{COMPARISON_TARGET} written: {count} pairs")
    return count


build_comparison()
