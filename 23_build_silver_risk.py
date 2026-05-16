# Databricks notebook source
# DBTITLE 1,Build silver.risk_profile
# MAGIC %md
# MAGIC # Build silver.risk_profile
# MAGIC
# MAGIC Pivot `bronze.hazards` to one row per (geo_id, country_code), one column per
# MAGIC hazard type. Compute composite hazard score and risk label.
# MAGIC
# MAGIC **Weighting:** flood 30% + earthquake 30% + wildfire 20% + tornado 10% + landslide 10%.
# MAGIC **Risk labels:** <25 low, 25–50 medium, 50–75 high, >75 very_high.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports
import pyspark.sql.functions as F

SOURCE = "realestate.bronze.hazards"
TARGET = "realestate.silver.risk_profile"

# COMMAND ----------

# DBTITLE 1,Pivot and score
def run_build() -> int:
    if not spark.catalog.tableExists(SOURCE):
        print(f"{SOURCE} does not exist. Run 08_ingest_hazards.py first.")
        return 0

    base = spark.table(SOURCE)
    if base.count() == 0:
        print("No hazard rows to pivot.")
        return 0

    # bronze.hazards.risk_score is 0.0-1.0; rescale to 0-100 for silver.
    scaled = base.withColumn("risk_score_100", F.col("risk_score") * F.lit(100.0))

    # Pivot to one row per (country_code, state_or_dept, county_or_municipio, zip_or_zone).
    # For ease of joins downstream, we use county_or_municipio for CO and zip_or_zone for US.
    geo_keys = ["country_code", "state_or_dept", "county_or_municipio", "zip_or_zone"]
    pivoted = (
        scaled.groupBy(*geo_keys)
              .pivot("hazard_type", ["flood", "earthquake", "wildfire", "tornado", "landslide"])
              .agg(F.max("risk_score_100"))
    )

    # Fill missing hazard types with 0.0 (data not available -> treat as no signal).
    for h in ["flood", "earthquake", "wildfire", "tornado", "landslide"]:
        if h not in pivoted.columns:
            pivoted = pivoted.withColumn(h, F.lit(0.0))
        pivoted = pivoted.withColumn(h, F.coalesce(F.col(h), F.lit(0.0)))

    out = (
        pivoted
        .withColumn("geo_id",
                    F.when(F.col("country_code") == F.lit("US"),
                           F.coalesce(F.col("zip_or_zone"), F.col("county_or_municipio")))
                     .otherwise(F.coalesce(F.col("county_or_municipio"), F.col("zip_or_zone"))))
        .withColumn("city", F.col("county_or_municipio"))  # best available — refined downstream
        .withColumn("zip_or_municipio",
                    F.when(F.col("country_code") == F.lit("US"), F.col("zip_or_zone"))
                     .otherwise(F.col("county_or_municipio")))
        .withColumn("flood_risk_score",      F.col("flood"))
        .withColumn("earthquake_risk_score", F.col("earthquake"))
        .withColumn("wildfire_risk_score",   F.col("wildfire"))
        .withColumn("tornado_risk_score",    F.col("tornado"))
        .withColumn("landslide_risk_score",  F.col("landslide"))
        .withColumn(
            "composite_hazard_score",
            F.col("flood_risk_score")      * F.lit(0.30)
          + F.col("earthquake_risk_score") * F.lit(0.30)
          + F.col("wildfire_risk_score")   * F.lit(0.20)
          + F.col("tornado_risk_score")    * F.lit(0.10)
          + F.col("landslide_risk_score")  * F.lit(0.10)
        )
        .withColumn(
            "risk_label",
            F.when(F.col("composite_hazard_score") < 25.0, "low")
             .when(F.col("composite_hazard_score") < 50.0, "medium")
             .when(F.col("composite_hazard_score") < 75.0, "high")
             .otherwise("very_high")
        )
        .withColumn("profile_updated_at", F.current_timestamp())
        .select(
            "geo_id",
            "country_code",
            "city",
            "zip_or_municipio",
            "flood_risk_score",
            "earthquake_risk_score",
            "wildfire_risk_score",
            "tornado_risk_score",
            "landslide_risk_score",
            "composite_hazard_score",
            "risk_label",
            "profile_updated_at",
        )
    )

    out.write.mode("overwrite").saveAsTable(TARGET)
    count = spark.table(TARGET).count()
    print(f"silver.risk_profile written: {count} rows")
    return count


run_build()
