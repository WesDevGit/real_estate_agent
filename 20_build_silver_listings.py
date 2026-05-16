# Databricks notebook source
# DBTITLE 1,Build silver.listings
# MAGIC %md
# MAGIC # Build silver.listings
# MAGIC
# MAGIC Join US + Colombia bronze listings; convert COP→USD; normalize property types
# MAGIC and area units; deduplicate by `listing_id` (most-recent `scraped_at` wins).

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and config
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

SOURCE_US = "realestate.bronze.listings_us"
SOURCE_CO = "realestate.bronze.listings_co"
TARGET    = "realestate.silver.listings"

SQFT_PER_M2 = 10.7639

# COMMAND ----------

# DBTITLE 1,Fetch latest COP→USD rate (COP per 1 USD)
def get_latest_cop_per_usd_rate() -> float:
    try:
        return float(spark.sql(
            "SELECT rate FROM realestate.bronze.exchange_rates "
            "WHERE from_currency='COP' AND to_currency='USD' "
            "ORDER BY COALESCE(rate_date, ingest_time) DESC LIMIT 1"
        ).first()["rate"])
    except Exception as e:
        raise RuntimeError(
            "No COP→USD rate available in realestate.bronze.exchange_rates. "
            "Run 13_ingest_exchange_rates.py first."
        ) from e


COP_PER_USD = get_latest_cop_per_usd_rate()
print(f"Using COP/USD rate: 1 USD = {COP_PER_USD:,.2f} COP")

# COMMAND ----------

# DBTITLE 1,Normalize property types
PROPERTY_TYPE_MAP_SQL = """
CASE
  WHEN lower(property_type) IN ('single_family','single family','sfh') THEN 'single_family'
  WHEN lower(property_type) IN ('condo','condominium','apartment','apartamento') THEN 'apartment'
  WHEN lower(property_type) IN ('townhouse','town home','town-house') THEN 'townhouse'
  WHEN lower(property_type) IN ('casa') THEN 'single_family'
  ELSE 'apartment'
END
"""

# COMMAND ----------

# DBTITLE 1,Build US side
def build_us_df() -> DataFrame:
    df = spark.table(SOURCE_US)
    return (
        df.select(
            F.concat_ws("_", F.lit("us"), F.col("listing_id")).alias("listing_id"),
            F.col("source"),
            F.lit("US").alias("country_code"),
            F.col("city"),
            F.col("state").alias("state_or_dept"),
            F.col("zip").alias("zip_or_municipio"),
            F.lit(None).cast("string").alias("barrio_or_neighborhood"),
            F.col("lat"),
            F.col("lon"),
            F.col("price_usd").cast("long").alias("price_usd"),
            F.col("price_usd").cast("long").alias("price_local"),
            F.lit("USD").alias("local_currency"),
            F.col("price_per_sqft_usd"),
            F.col("sqft").cast("double").alias("area_sqft"),
            F.col("bedrooms"),
            F.col("bathrooms"),
            F.expr(PROPERTY_TYPE_MAP_SQL).alias("property_type"),
            F.col("listing_url"),
            F.col("days_on_market"),
            F.col("listing_date"),
            F.col("scraped_at"),
        )
    )

# COMMAND ----------

# DBTITLE 1,Build CO side (convert price + area)
def build_co_df() -> DataFrame:
    df = spark.table(SOURCE_CO)
    return (
        df.select(
            F.concat_ws("_", F.lit("co"), F.col("listing_id")).alias("listing_id"),
            F.col("source"),
            F.lit("CO").alias("country_code"),
            F.col("city"),
            F.col("departamento").alias("state_or_dept"),
            F.col("city").alias("zip_or_municipio"),  # CO listings are city-level; barrio carries granularity
            F.col("barrio").alias("barrio_or_neighborhood"),
            F.col("lat"),
            F.col("lon"),
            (F.col("price_cop").cast("double") / F.lit(COP_PER_USD)).cast("long").alias("price_usd"),
            F.col("price_cop").cast("long").alias("price_local"),
            F.lit("COP").alias("local_currency"),
            ((F.col("price_cop").cast("double") / F.lit(COP_PER_USD))
                / (F.col("area_m2") * F.lit(SQFT_PER_M2))).alias("price_per_sqft_usd"),
            (F.col("area_m2") * F.lit(SQFT_PER_M2)).alias("area_sqft"),
            F.col("bedrooms"),
            F.col("bathrooms"),
            F.expr(PROPERTY_TYPE_MAP_SQL).alias("property_type"),
            F.col("listing_url"),
            F.lit(None).cast("int").alias("days_on_market"),
            F.lit(None).cast("date").alias("listing_date"),
            F.col("scraped_at"),
        )
    )

# COMMAND ----------

# DBTITLE 1,Union, dedupe by listing_id (most recent wins), write
def run_build():
    us_count = spark.table(SOURCE_US).count() if spark.catalog.tableExists(SOURCE_US) else 0
    co_count = spark.table(SOURCE_CO).count() if spark.catalog.tableExists(SOURCE_CO) else 0
    if us_count == 0 and co_count == 0:
        print("No bronze listings to process.")
        return 0

    parts = []
    if us_count > 0:
        parts.append(build_us_df())
    if co_count > 0:
        parts.append(build_co_df())
    if not parts:
        return 0

    union_df = parts[0]
    for p in parts[1:]:
        union_df = union_df.unionByName(p, allowMissingColumns=False)

    # Deduplicate: keep most recent scraped_at per listing_id.
    w = Window.partitionBy("listing_id").orderBy(F.col("scraped_at").desc())
    deduped = (
        union_df.withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
    )

    deduped.createOrReplaceTempView("_new_silver_listings")
    spark.sql(f"""
        MERGE INTO {TARGET} t
        USING _new_silver_listings s ON t.listing_id = s.listing_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    total = spark.table(TARGET).count()
    print(f"silver.listings now has {total} rows (US bronze={us_count}, CO bronze={co_count})")
    return total


run_build()
