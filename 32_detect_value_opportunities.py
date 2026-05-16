# Databricks notebook source
# DBTITLE 1,Build gold.value_opportunities
# MAGIC %md
# MAGIC # Build gold.value_opportunities
# MAGIC
# MAGIC Find active listings priced below 85% of their area's market median.
# MAGIC Filter out clearly bad data (no sqft or 0 bedrooms).

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports
import pyspark.sql.functions as F

LISTINGS = "realestate.silver.listings"
MARKET   = "realestate.silver.market_summary"
SCORE    = "realestate.gold.neighborhood_scorecard"
RISK     = "realestate.silver.risk_profile"
TARGET   = "realestate.gold.value_opportunities"

DISCOUNT_THRESHOLD = 0.85
MIN_SQFT = 500
MIN_BEDROOMS = 1

# COMMAND ----------

# DBTITLE 1,Run
def run_build() -> int:
    if not spark.catalog.tableExists(LISTINGS):
        print(f"{LISTINGS} missing.")
        return 0
    if not spark.catalog.tableExists(MARKET):
        print(f"{MARKET} missing — need silver.market_summary to compute medians.")
        return 0

    listings = spark.table(LISTINGS)
    market = spark.table(MARKET)

    # Latest market summary per area.
    latest = (
        market.groupBy("country_code", "city", "zip_or_municipio")
              .agg(F.max("date").alias("latest_date"))
    )
    market_latest = (
        market.join(latest,
                    on=["country_code", "city", "zip_or_municipio"])
              .filter(F.col("date") == F.col("latest_date"))
              .select("country_code", "city", "zip_or_municipio",
                      F.col("median_price_usd").alias("area_median_price_usd"))
    )

    joined = (
        listings.alias("l")
        .join(market_latest.alias("m"),
              on=["country_code", "city", "zip_or_municipio"],
              how="inner")
        .filter(F.col("l.price_usd").isNotNull())
        .filter(F.col("l.price_usd") < F.col("m.area_median_price_usd") * F.lit(DISCOUNT_THRESHOLD))
        .filter((F.col("l.area_sqft") > MIN_SQFT) | F.col("l.area_sqft").isNull())
        .filter(F.coalesce(F.col("l.bedrooms"), F.lit(0)) >= MIN_BEDROOMS)
        .withColumn(
            "discount_pct",
            (F.col("area_median_price_usd") - F.col("l.price_usd"))
            / F.col("area_median_price_usd") * 100.0
        )
    )

    # Optional joins for context.
    if spark.catalog.tableExists(SCORE):
        scores = spark.table(SCORE).select(
            "country_code", "zip_or_municipio",
            F.col("composite_score").alias("neighborhood_composite_score"),
        )
        joined = joined.join(scores, on=["country_code", "zip_or_municipio"], how="left")
    if spark.catalog.tableExists(RISK):
        risk = spark.table(RISK).select(
            "country_code", "zip_or_municipio",
            F.col("risk_label").alias("hazard_risk_label"),
        )
        joined = joined.join(risk, on=["country_code", "zip_or_municipio"], how="left")

    final = joined.orderBy(F.col("discount_pct").desc())
    final.write.mode("overwrite").saveAsTable(TARGET)
    count = spark.table(TARGET).count()
    print(f"{TARGET} written: {count} value opportunities")
    return count


run_build()
