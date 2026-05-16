# Databricks notebook source
# DBTITLE 1,Real Estate Agent — Tool Layer
# MAGIC %md
# MAGIC # Real Estate Agent — Tool Layer
# MAGIC
# MAGIC Deterministic functions over Gold/Silver Delta tables. **No external API calls.**
# MAGIC The orchestrator notebook (`41_realestate_agent.py`) calls these from its
# MAGIC LLM-validated plan.
# MAGIC
# MAGIC All functions return either a `dict` or a `list[dict]`. Empty results return
# MAGIC `{"message": "..."}` or `[]` — never raise.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and table constants
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pyspark.sql.functions as F

SILVER_LISTINGS         = "realestate.silver.listings"
SILVER_MARKET_SUMMARY   = "realestate.silver.market_summary"
SILVER_RISK_PROFILE     = "realestate.silver.risk_profile"
SILVER_NEIGHBORHOOD     = "realestate.silver.neighborhood_profile"
BRONZE_CRIME_US         = "realestate.bronze.crime_us"
BRONZE_CRIME_CO         = "realestate.bronze.crime_co"
BRONZE_WEATHER          = "realestate.bronze.weather_history"
BRONZE_AMENITIES        = "realestate.bronze.amenities"
BRONZE_DEMOGRAPHICS_US  = "realestate.bronze.demographics_us"
BRONZE_DEMOGRAPHICS_CO  = "realestate.bronze.demographics_co"
BRONZE_ECON_US          = "realestate.bronze.economic_us"
BRONZE_ECON_CO          = "realestate.bronze.economic_co"
GOLD_NEIGHBORHOOD       = "realestate.gold.neighborhood_scorecard"
GOLD_MARKET_TRENDS      = "realestate.gold.market_trends"
GOLD_SCHOOL_RANKINGS    = "realestate.gold.school_rankings"
GOLD_VALUE_OPPS         = "realestate.gold.value_opportunities"
GOLD_CITY_COMPARISON    = "realestate.gold.city_comparison"
GOLD_COMPARISON_INDEX   = "realestate.gold.comparison_index"

DEFAULT_LIMIT = 10
MAX_LIMIT = 50

# COMMAND ----------

# DBTITLE 1,Row→dict converter (Decimal / date safe)
def _row_to_dict(row) -> Dict[str, Any]:
    out = {}
    for k, v in row.asDict(recursive=False).items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, (date, datetime)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _rows(df, n: int) -> List[Dict[str, Any]]:
    return [_row_to_dict(r) for r in df.limit(int(n)).collect()]


def _table_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False

# COMMAND ----------

# DBTITLE 1,search_listings
def search_listings(
    country_code: str,
    city: str,
    min_price_usd: Optional[int] = None,
    max_price_usd: Optional[int] = None,
    bedrooms_min: Optional[int] = None,
    bathrooms_min: Optional[float] = None,
    property_type: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    **_ignored,
) -> List[Dict[str, Any]]:
    """Active listings matching the criteria, sorted by price ascending."""
    if not _table_exists(SILVER_LISTINGS):
        return []
    limit = clean_limit(limit, DEFAULT_LIMIT, 1, MAX_LIMIT)

    df = spark.table(SILVER_LISTINGS).filter(F.col("country_code") == country_code)
    df = df.filter(F.lower(F.col("city")) == city.lower())

    if min_price_usd is not None:
        df = df.filter(F.col("price_usd") >= int(min_price_usd))
    if max_price_usd is not None:
        df = df.filter(F.col("price_usd") <= int(max_price_usd))
    if bedrooms_min is not None:
        df = df.filter(F.col("bedrooms") >= int(bedrooms_min))
    if bathrooms_min is not None:
        df = df.filter(F.col("bathrooms") >= float(bathrooms_min))
    if property_type:
        df = df.filter(F.col("property_type") == property_type)

    # Optional left-join for neighborhood score + risk label.
    if _table_exists(GOLD_NEIGHBORHOOD):
        score = spark.table(GOLD_NEIGHBORHOOD).select(
            "country_code", "zip_or_municipio",
            F.col("composite_score").alias("neighborhood_score"),
        )
        df = df.join(score, on=["country_code", "zip_or_municipio"], how="left")
    if _table_exists(SILVER_RISK_PROFILE):
        risk = spark.table(SILVER_RISK_PROFILE).select(
            "country_code", "zip_or_municipio",
            F.col("risk_label").alias("hazard_risk_label"),
        )
        df = df.join(risk, on=["country_code", "zip_or_municipio"], how="left")

    df = df.orderBy(F.col("price_usd").asc_nulls_last())
    return _rows(df, limit)

# COMMAND ----------

# DBTITLE 1,get_neighborhood_profile
def get_neighborhood_profile(
    country_code: str,
    city: str,
    zip_or_municipio: Optional[str] = None,
    barrio: Optional[str] = None,
    **_ignored,
) -> Dict[str, Any]:
    """Return the best-matching gold.neighborhood_scorecard row for the area."""
    if not _table_exists(GOLD_NEIGHBORHOOD):
        return {"message": f"{GOLD_NEIGHBORHOOD} does not exist."}

    df = spark.table(GOLD_NEIGHBORHOOD).filter(F.col("country_code") == country_code) \
                                      .filter(F.lower(F.col("city")) == city.lower())
    if zip_or_municipio:
        df = df.filter(F.col("zip_or_municipio") == zip_or_municipio)
    if barrio:
        df = df.filter(F.lower(F.col("barrio")) == barrio.lower())

    rows = df.limit(1).collect()
    if not rows:
        # Fall back to city-level aggregate.
        agg = spark.table(GOLD_NEIGHBORHOOD).filter(F.col("country_code") == country_code) \
                                            .filter(F.lower(F.col("city")) == city.lower()) \
                                            .agg(
                                                F.avg("composite_score").alias("composite_score"),
                                                F.avg("crime_rate_per_100k").alias("crime_rate_per_100k"),
                                                F.avg("school_score_normalized").alias("school_score_normalized"),
                                                F.avg("amenity_density_score").alias("amenity_density_score"),
                                                F.avg("transit_access_score").alias("transit_access_score"),
                                                F.avg("hazard_composite_score").alias("hazard_composite_score"),
                                            ).first()
        if agg is None or agg["composite_score"] is None:
            return {"message": f"No profile found for {country_code}/{city}"}
        d = _row_to_dict(agg)
        d.update({"country_code": country_code, "city": city, "aggregate": True})
        return d

    return _row_to_dict(rows[0])

# COMMAND ----------

# DBTITLE 1,get_crime_stats
def get_crime_stats(
    country_code: str,
    city: str,
    zip_or_municipio: Optional[str] = None,
    months_back: int = 12,
    **_ignored,
) -> Dict[str, Any]:
    """Aggregate crime stats for an area."""
    months_back = clean_limit(months_back, 12, 1, 24)

    if country_code == "US":
        if not _table_exists(BRONZE_CRIME_US):
            return {"message": "US crime data not yet ingested."}
        df = spark.table(BRONZE_CRIME_US).filter(F.lower(F.col("city")) == city.lower())
        agg = df.groupBy("crime_category").count().collect()
        if not agg:
            return {"message": f"No crime data for US/{city}"}
        by_cat = {r["crime_category"]: r["count"] for r in agg}
        return {
            "country_code": "US", "city": city,
            "category_counts": by_cat,
            "total_incidents": sum(by_cat.values()),
            "note": "FBI UCR rolling 5-year aggregate. Population-adjusted rates available via get_neighborhood_profile.",
        }
    else:
        if not _table_exists(BRONZE_CRIME_CO):
            return {"message": "CO crime data not yet ingested."}
        df = spark.table(BRONZE_CRIME_CO).filter(F.lower(F.col("city")) == city.lower())
        agg = df.groupBy("crime_category").agg(F.sum("count").alias("total")).collect()
        if not agg:
            return {"message": f"No crime data for CO/{city}"}
        by_cat = {r["crime_category"]: int(r["total"] or 0) for r in agg}
        return {
            "country_code": "CO", "city": city,
            "category_counts": by_cat,
            "total_incidents": sum(by_cat.values()),
            "note": "DANE/Policía Nacional aggregate counts (no per-100k rate at this granularity).",
        }

# COMMAND ----------

# DBTITLE 1,get_school_rankings
def get_school_rankings(
    country_code: str,
    city: str,
    zip_or_municipio: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    **_ignored,
) -> List[Dict[str, Any]]:
    """Top schools in the area, sorted by school_score_normalized descending."""
    if not _table_exists(GOLD_SCHOOL_RANKINGS):
        return []
    limit = clean_limit(limit, DEFAULT_LIMIT, 1, MAX_LIMIT)

    df = spark.table(GOLD_SCHOOL_RANKINGS).filter(F.col("country_code") == country_code) \
                                          .filter(F.lower(F.col("city")) == city.lower())
    if zip_or_municipio:
        df = df.filter(F.col("zip_or_municipio") == zip_or_municipio)
    df = df.orderBy(F.col("school_score_normalized").desc_nulls_last())
    return _rows(df, limit)

# COMMAND ----------

# DBTITLE 1,get_weather_summary
def get_weather_summary(
    country_code: str,
    city: str,
    years_back: int = 5,
    **_ignored,
) -> Dict[str, Any]:
    """Summarize weather history: avg temps, precipitation, extreme days/year."""
    if not _table_exists(BRONZE_WEATHER):
        return {"message": "Weather data not yet ingested."}
    years_back = clean_limit(years_back, 5, 1, 10)

    df = spark.table(BRONZE_WEATHER).filter(F.col("country_code") == country_code) \
                                    .filter(F.lower(F.col("city")) == city.lower())
    agg = df.agg(
        F.avg("temp_max_c").alias("avg_temp_max_c"),
        F.avg("temp_min_c").alias("avg_temp_min_c"),
        F.avg("precipitation_mm").alias("avg_precipitation_mm"),
        F.sum(F.col("is_extreme_day").cast("int")).alias("total_extreme_days"),
        F.countDistinct(F.year("date")).alias("years_covered"),
        F.min("date").alias("first_date"),
        F.max("date").alias("last_date"),
    ).first()
    if agg is None or agg["avg_temp_max_c"] is None:
        return {"message": f"No weather data for {country_code}/{city}"}

    d = _row_to_dict(agg)
    if d.get("total_extreme_days") and d.get("years_covered"):
        d["extreme_days_per_year"] = d["total_extreme_days"] / d["years_covered"]
    d.update({"country_code": country_code, "city": city})
    return d

# COMMAND ----------

# DBTITLE 1,get_hazard_risks
def get_hazard_risks(
    country_code: str,
    city: str,
    zip_or_municipio: Optional[str] = None,
    **_ignored,
) -> Dict[str, Any]:
    """Return silver.risk_profile row with all five hazard scores plus composite + label."""
    if not _table_exists(SILVER_RISK_PROFILE):
        return {"message": "Hazard data not yet ingested."}

    df = spark.table(SILVER_RISK_PROFILE).filter(F.col("country_code") == country_code) \
                                         .filter(F.lower(F.col("city")) == city.lower())
    if zip_or_municipio:
        df = df.filter(F.col("zip_or_municipio") == zip_or_municipio)
    rows = df.limit(1).collect()
    if not rows:
        # Aggregate at city level.
        agg = spark.table(SILVER_RISK_PROFILE).filter(F.col("country_code") == country_code) \
                                              .filter(F.lower(F.col("city")) == city.lower()) \
                                              .agg(
                                                  F.avg("flood_risk_score").alias("flood_risk_score"),
                                                  F.avg("earthquake_risk_score").alias("earthquake_risk_score"),
                                                  F.avg("wildfire_risk_score").alias("wildfire_risk_score"),
                                                  F.avg("tornado_risk_score").alias("tornado_risk_score"),
                                                  F.avg("landslide_risk_score").alias("landslide_risk_score"),
                                                  F.avg("composite_hazard_score").alias("composite_hazard_score"),
                                              ).first()
        if agg is None or agg["composite_hazard_score"] is None:
            return {"message": f"No hazard profile for {country_code}/{city}"}
        d = _row_to_dict(agg)
        d.update({"country_code": country_code, "city": city, "aggregate": True})
        return d
    return _row_to_dict(rows[0])

# COMMAND ----------

# DBTITLE 1,get_market_trends
def get_market_trends(
    country_code: str,
    city: str,
    zip_or_municipio: Optional[str] = None,
    months_back: int = 12,
    **_ignored,
) -> Dict[str, Any]:
    """Return summary stats + monthly_series for the area."""
    if not _table_exists(SILVER_MARKET_SUMMARY):
        return {"message": "Market trend data not yet built."}
    months_back = clean_limit(months_back, 12, 1, 24)

    df = spark.table(SILVER_MARKET_SUMMARY).filter(F.col("country_code") == country_code) \
                                            .filter(F.lower(F.col("city")) == city.lower())
    if zip_or_municipio:
        df = df.filter(F.col("zip_or_municipio") == zip_or_municipio)

    if df.count() == 0:
        return {"message": f"No market data for {country_code}/{city}"}

    df_sorted = df.orderBy(F.col("date").desc())
    latest = df_sorted.limit(1).collect()[0]
    series_rows = df_sorted.limit(months_back).collect()
    series_rows = list(reversed(series_rows))  # ascending order for charts

    return {
        "country_code": country_code,
        "city": city,
        "zip_or_municipio": zip_or_municipio,
        "latest_date": latest["date"].isoformat() if latest["date"] else None,
        "latest_median_price_usd": int(latest["median_price_usd"]) if latest["median_price_usd"] else None,
        "price_trend_3mo_pct":  float(latest["price_trend_3mo_pct"])  if latest["price_trend_3mo_pct"]  is not None else None,
        "price_trend_12mo_pct": float(latest["price_trend_12mo_pct"]) if latest["price_trend_12mo_pct"] is not None else None,
        "median_days_on_market": latest["median_days_on_market"],
        "market_temp": latest["market_temp"],
        "monthly_series": [
            {
                "date": r["date"].isoformat() if r["date"] else None,
                "median_price_usd": int(r["median_price_usd"]) if r["median_price_usd"] else None,
                "inventory_count": r["inventory_count"],
            }
            for r in series_rows
        ],
    }

# COMMAND ----------

# DBTITLE 1,get_area_demographics
def get_area_demographics(
    country_code: str,
    city: str,
    zip_or_municipio: Optional[str] = None,
    **_ignored,
) -> Dict[str, Any]:
    if country_code == "US":
        if not _table_exists(BRONZE_DEMOGRAPHICS_US):
            return {"message": "US demographics not yet ingested."}
        df = spark.table(BRONZE_DEMOGRAPHICS_US)
        if zip_or_municipio:
            df = df.filter(F.col("zip") == zip_or_municipio)
        agg = df.agg(
            F.sum("total_population").alias("total_population"),
            F.avg("median_household_income_usd").alias("median_income_usd"),
            F.avg("median_age").alias("median_age"),
            F.avg("pct_homeowner").alias("pct_homeowner"),
        ).first()
    else:
        if not _table_exists(BRONZE_DEMOGRAPHICS_CO):
            return {"message": "CO demographics not yet ingested."}
        df = spark.table(BRONZE_DEMOGRAPHICS_CO).filter(F.lower(F.col("city")) == city.lower())
        agg = df.agg(
            F.sum("total_population").alias("total_population"),
            F.avg("median_household_income_cop").alias("median_income_cop"),
            F.avg("pct_homeowner").alias("pct_homeowner"),
        ).first()

    if agg is None or agg.asDict().get("total_population") is None:
        return {"message": f"No demographics found for {country_code}/{city}"}
    d = _row_to_dict(agg)
    d.update({"country_code": country_code, "city": city})
    if country_code == "CO" and d.get("median_income_cop"):
        try:
            d["median_income_usd"] = cop_to_usd(d["median_income_cop"])
        except Exception:
            pass
    return d

# COMMAND ----------

# DBTITLE 1,get_nearby_amenities
def get_nearby_amenities(
    country_code: str,
    city: str,
    zip_or_municipio: Optional[str] = None,
    amenity_types: Optional[List[str]] = None,
    limit: int = 20,
    **_ignored,
) -> List[Dict[str, Any]]:
    if not _table_exists(BRONZE_AMENITIES):
        return []
    limit = clean_limit(limit, 20, 1, MAX_LIMIT)

    df = spark.table(BRONZE_AMENITIES).filter(F.col("country_code") == country_code) \
                                       .filter(F.lower(F.col("city")) == city.lower())
    if amenity_types:
        df = df.filter(F.col("amenity_type").isin(list(amenity_types)))
    return _rows(df.orderBy("amenity_type", "name"), limit)

# COMMAND ----------

# DBTITLE 1,compare_cities
def compare_cities(us_city: str, co_city: str, **_ignored) -> Dict[str, Any]:
    """Cross-country city comparison. Lookup pre-computed pair first; aggregate
    from comparison_index if missing."""
    if _table_exists(GOLD_CITY_COMPARISON):
        df = spark.table(GOLD_CITY_COMPARISON) \
                  .filter((F.lower(F.col("us_city")) == us_city.lower())
                          & (F.lower(F.col("co_city")) == co_city.lower())) \
                  .orderBy(F.col("comparison_date").desc()) \
                  .limit(1).collect()
        if df:
            return _row_to_dict(df[0])

    # Fallback: aggregate from gold.comparison_index.
    if not _table_exists(GOLD_COMPARISON_INDEX):
        return {"message": "No pre-computed comparison and gold.comparison_index missing."}

    def _city_medians(city: str, country: str) -> Dict[str, Any]:
        agg = spark.table(GOLD_COMPARISON_INDEX) \
                   .filter(F.col("country_code") == country) \
                   .filter(F.lower(F.col("city")) == city.lower()) \
                   .agg(
                       F.expr("percentile(norm_crime_score,   0.5)").alias("norm_crime"),
                       F.expr("percentile(norm_school_score,  0.5)").alias("norm_school"),
                       F.expr("percentile(norm_hazard_score,  0.5)").alias("norm_hazard"),
                       F.expr("percentile(norm_weather_score, 0.5)").alias("norm_weather"),
                       F.expr("percentile(norm_amenity_score, 0.5)").alias("norm_amenity"),
                       F.expr("percentile(norm_economic_score,0.5)").alias("norm_economic"),
                   ).first()
        return _row_to_dict(agg) if agg else {}

    us = _city_medians(us_city, "US")
    co = _city_medians(co_city, "CO")
    if not us or not co or all(v is None for v in us.values()) or all(v is None for v in co.values()):
        return {"message": f"Insufficient data to compare {us_city} vs {co_city}"}
    return {
        "us_city": us_city, "co_city": co_city,
        "comparison_date": date.today().isoformat(),
        "us_metrics": us,
        "co_metrics": co,
        "narrative_context": None,
        "aggregate": True,
    }

# COMMAND ----------

# DBTITLE 1,get_affordability_analysis
def get_affordability_analysis(
    country_code: str,
    city: str,
    annual_income_usd: float,
    down_payment_usd: float = 0.0,
    **_ignored,
) -> Dict[str, Any]:
    """Compute max affordable price using 28% front-end DTI rule.

    Uses current 30-year mortgage rate from FRED MORTGAGE30US (US) or
    Banco de la República (CO). Falls back to 7% if not available.
    """
    rate_pct = None
    if country_code == "US" and _table_exists(BRONZE_ECON_US):
        r = spark.table(BRONZE_ECON_US).filter(F.col("mortgage_rate_30yr").isNotNull()) \
                                       .orderBy(F.col("date").desc()).limit(1).collect()
        if r:
            rate_pct = float(r[0]["mortgage_rate_30yr"])
    elif country_code == "CO" and _table_exists(BRONZE_ECON_CO):
        r = spark.table(BRONZE_ECON_CO).filter(F.col("mortgage_rate_pct").isNotNull()) \
                                       .orderBy(F.col("date").desc()).limit(1).collect()
        if r:
            rate_pct = float(r[0]["mortgage_rate_pct"])

    if rate_pct is None:
        rate_pct = 7.0
        rate_note = "fallback default — no live rate available"
    else:
        rate_note = "live mortgage rate"

    monthly_rate = (rate_pct / 100.0) / 12.0
    term_months = 30 * 12
    max_monthly = (float(annual_income_usd) * 0.28) / 12.0

    # P = M / r * (1 - (1 + r) ^ -n)
    if monthly_rate <= 0:
        max_principal = max_monthly * term_months
    else:
        max_principal = max_monthly / monthly_rate * (1.0 - (1.0 + monthly_rate) ** (-term_months))

    max_price = max_principal + float(down_payment_usd or 0.0)

    # Compare to area median.
    median_price = None
    if _table_exists(SILVER_MARKET_SUMMARY):
        m = spark.table(SILVER_MARKET_SUMMARY) \
                 .filter(F.col("country_code") == country_code) \
                 .filter(F.lower(F.col("city")) == city.lower()) \
                 .orderBy(F.col("date").desc()).limit(1).collect()
        if m:
            median_price = m[0]["median_price_usd"]

    return {
        "country_code": country_code, "city": city,
        "annual_income_usd": float(annual_income_usd),
        "down_payment_usd": float(down_payment_usd or 0.0),
        "mortgage_rate_pct": rate_pct,
        "rate_note": rate_note,
        "max_price_usd": int(max_price),
        "estimated_monthly_payment": int(max_monthly),
        "median_price_usd_in_area": int(median_price) if median_price else None,
        "whether_median_is_affordable": (max_price >= median_price) if median_price else None,
    }

# COMMAND ----------

# DBTITLE 1,get_value_opportunities
def get_value_opportunities(
    country_code: str,
    city: str,
    max_price_usd: Optional[int] = None,
    limit: int = DEFAULT_LIMIT,
    **_ignored,
) -> List[Dict[str, Any]]:
    if not _table_exists(GOLD_VALUE_OPPS):
        return []
    limit = clean_limit(limit, DEFAULT_LIMIT, 1, MAX_LIMIT)

    df = spark.table(GOLD_VALUE_OPPS).filter(F.col("country_code") == country_code) \
                                     .filter(F.lower(F.col("city")) == city.lower())
    if max_price_usd is not None:
        df = df.filter(F.col("price_usd") <= int(max_price_usd))
    df = df.orderBy(F.col("discount_pct").desc_nulls_last())
    return _rows(df, limit)

# COMMAND ----------

# DBTITLE 1,get_amenity_access
def get_amenity_access(
    country_code: str,
    city: str,
    zip_or_municipio: Optional[str] = None,
    **_ignored,
) -> Dict[str, Any]:
    if not _table_exists(GOLD_NEIGHBORHOOD):
        return {"message": "Neighborhood scorecard not built yet."}

    df = spark.table(GOLD_NEIGHBORHOOD).filter(F.col("country_code") == country_code) \
                                       .filter(F.lower(F.col("city")) == city.lower())
    if zip_or_municipio:
        df = df.filter(F.col("zip_or_municipio") == zip_or_municipio)
    agg = df.agg(
        F.avg("amenity_density_score").alias("amenity_density_score"),
        F.avg("transit_access_score").alias("transit_access_score"),
        F.avg("grocery_count").alias("grocery_count"),
        F.avg("park_count").alias("park_count"),
        F.avg("hospital_count").alias("hospital_count"),
        F.avg("transit_stop_count").alias("transit_stop_count"),
    ).first()
    if agg is None or agg["amenity_density_score"] is None:
        return {"message": f"No amenity data for {country_code}/{city}"}
    d = _row_to_dict(agg)
    d.update({"country_code": country_code, "city": city})
    return d

# COMMAND ----------

# DBTITLE 1,Registry — used by the orchestrator
TOOL_REGISTRY = {
    "search_listings":          search_listings,
    "get_neighborhood_profile": get_neighborhood_profile,
    "get_crime_stats":          get_crime_stats,
    "get_school_rankings":      get_school_rankings,
    "get_weather_summary":      get_weather_summary,
    "get_hazard_risks":         get_hazard_risks,
    "get_market_trends":        get_market_trends,
    "get_area_demographics":    get_area_demographics,
    "get_nearby_amenities":     get_nearby_amenities,
    "compare_cities":           compare_cities,
    "get_affordability_analysis": get_affordability_analysis,
    "get_value_opportunities":  get_value_opportunities,
    "get_amenity_access":       get_amenity_access,
}

print(f"Loaded {len(TOOL_REGISTRY)} agent tools")
