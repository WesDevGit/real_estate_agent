# Databricks notebook source
# DBTITLE 1,Build gold.neighborhood_scorecard
# MAGIC %md
# MAGIC # Build gold.neighborhood_scorecard
# MAGIC
# MAGIC Compose `silver.neighborhood_profile` into composite 0–100 score plus country
# MAGIC and global percentile rank.
# MAGIC
# MAGIC **Weights:**
# MAGIC * Safety (inverted crime): 30%
# MAGIC * Schools: 25%
# MAGIC * Amenities (amenity_density_score): 20%
# MAGIC * Hazard (inverted): 15%
# MAGIC * Transit (transit_access_score): 10%

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports
import pyspark.sql.functions as F
from pyspark.sql.window import Window

SOURCE = "realestate.silver.neighborhood_profile"
TARGET = "realestate.gold.neighborhood_scorecard"

# COMMAND ----------

# DBTITLE 1,Build composite + ranks
def run_build() -> int:
    if not spark.catalog.tableExists(SOURCE):
        print(f"{SOURCE} not found.")
        return 0

    df = spark.table(SOURCE)

    # Invert crime: min-max scale raw crime rate, then subtract from 100.
    crime_stats = df.agg(
        F.min("crime_rate_per_100k").alias("min_c"),
        F.max("crime_rate_per_100k").alias("max_c"),
    ).first()
    min_c = crime_stats["min_c"] or 0.0
    max_c = crime_stats["max_c"] or 0.0
    range_c = max(max_c - min_c, 1.0)

    df = df.withColumn(
        "safety_score",
        100.0 - F.coalesce(
            (F.col("crime_rate_per_100k") - F.lit(min_c)) / F.lit(range_c) * 100.0,
            F.lit(50.0)   # neutral if missing
        )
    )

    # Hazard score inverted (higher composite_hazard_score => more risky => lower safety).
    df = df.withColumn(
        "hazard_safety_score",
        100.0 - F.coalesce(F.col("hazard_composite_score"), F.lit(0.0))
    )

    df = df.withColumn(
        "composite_score",
        F.coalesce(F.col("safety_score"),              F.lit(50.0)) * F.lit(0.30)
      + F.coalesce(F.col("school_score_normalized"),   F.lit(50.0)) * F.lit(0.25)
      + F.coalesce(F.col("amenity_density_score"),     F.lit(50.0)) * F.lit(0.20)
      + F.coalesce(F.col("hazard_safety_score"),       F.lit(50.0)) * F.lit(0.15)
      + F.coalesce(F.col("transit_access_score"),      F.lit(50.0)) * F.lit(0.10)
    )

    # Country and global percentile ranks.
    country_w = Window.partitionBy("country_code").orderBy(F.col("composite_score").asc_nulls_last())
    global_w  = Window.orderBy(F.col("composite_score").asc_nulls_last())

    out = (
        df.withColumn("country_percentile_rank", F.percent_rank().over(country_w) * 100.0)
          .withColumn("global_percentile_rank",  F.percent_rank().over(global_w)  * 100.0)
          .drop("safety_score", "hazard_safety_score")
    )

    out.write.mode("overwrite").saveAsTable(TARGET)
    count = spark.table(TARGET).count()
    print(f"{TARGET} written: {count} rows")
    return count


run_build()
