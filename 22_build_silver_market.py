# Databricks notebook source
# DBTITLE 1,Build silver.market_summary
# MAGIC %md
# MAGIC # Build silver.market_summary
# MAGIC
# MAGIC Unify US + Colombia market trends. Compute 3-mo and 12-mo price change %,
# MAGIC months of supply, and market temperature label.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports
import pyspark.sql.functions as F
from pyspark.sql.window import Window

SOURCE_US = "realestate.bronze.market_trends_us"
SOURCE_CO = "realestate.bronze.market_trends_co"
TARGET    = "realestate.silver.market_summary"

# COMMAND ----------

# DBTITLE 1,Fetch latest COP→USD for CO price conversion
def get_latest_cop_per_usd_rate() -> float:
    try:
        return float(spark.sql(
            "SELECT rate FROM realestate.bronze.exchange_rates "
            "WHERE from_currency='COP' AND to_currency='USD' "
            "ORDER BY date DESC, ingested_at DESC LIMIT 1"
        ).first()["rate"])
    except Exception:
        return None


COP_PER_USD = get_latest_cop_per_usd_rate()
print(f"COP/USD rate: {COP_PER_USD}")

# COMMAND ----------

# DBTITLE 1,Build US side
def build_us():
    if not spark.catalog.tableExists(SOURCE_US):
        return None
    base = spark.table(SOURCE_US)
    if base.count() == 0:
        return None
    return (
        base.select(
            F.col("geo_id"),
            F.lit("US").alias("country_code"),
            F.col("city"),
            F.col("zip").alias("zip_or_municipio"),
            F.col("date"),
            F.coalesce(F.col("median_sale_price_usd"), F.col("median_list_price_usd"))
                .cast("long").alias("median_price_usd"),
            F.col("median_days_on_market"),
            F.col("inventory_count"),
            F.col("homes_sold"),
        )
        .filter(F.col("median_price_usd").isNotNull())
    )

# COMMAND ----------

# DBTITLE 1,Build CO side
def build_co():
    if not spark.catalog.tableExists(SOURCE_CO):
        return None
    base = spark.table(SOURCE_CO)
    if base.count() == 0:
        return None
    if COP_PER_USD is None:
        print("No COP/USD rate available; skipping CO market trends.")
        return None
    return (
        base.select(
            F.col("geo_id"),
            F.lit("CO").alias("country_code"),
            F.col("city"),
            F.col("city").alias("zip_or_municipio"),
            F.col("date"),
            (F.col("new_construction_price_cop").cast("double") / F.lit(COP_PER_USD))
                .cast("long").alias("median_price_usd"),
            F.lit(None).cast("int").alias("median_days_on_market"),
            F.lit(None).cast("int").alias("inventory_count"),
            F.col("units_sold").alias("homes_sold"),
        )
        .filter(F.col("median_price_usd").isNotNull())
    )

# COMMAND ----------

# DBTITLE 1,Compute rolling deltas and write
def run_build() -> int:
    parts = [df for df in [build_us(), build_co()] if df is not None]
    if not parts:
        print("No bronze market_trends data to build from.")
        return 0

    unioned = parts[0]
    for p in parts[1:]:
        unioned = unioned.unionByName(p, allowMissingColumns=False)

    # Window for trailing 3-month and 12-month change.
    w = Window.partitionBy("country_code", "city", "zip_or_municipio").orderBy("date")
    out = (
        unioned
        .withColumn("price_lag_3", F.lag("median_price_usd", 3).over(w))
        .withColumn("price_lag_12", F.lag("median_price_usd", 12).over(w))
        .withColumn(
            "price_trend_3mo_pct",
            F.when(F.col("price_lag_3") > 0,
                   100.0 * (F.col("median_price_usd") - F.col("price_lag_3")) / F.col("price_lag_3"))
        )
        .withColumn(
            "price_trend_12mo_pct",
            F.when(F.col("price_lag_12") > 0,
                   100.0 * (F.col("median_price_usd") - F.col("price_lag_12")) / F.col("price_lag_12"))
        )
        .withColumn(
            "months_of_supply",
            F.when((F.col("homes_sold").isNotNull()) & (F.col("homes_sold") > 0) & F.col("inventory_count").isNotNull(),
                   F.col("inventory_count").cast("double") / F.col("homes_sold").cast("double"))
        )
        .withColumn(
            "market_temp",
            F.when(F.col("months_of_supply") < 2.0, "hot")
             .when(F.col("months_of_supply") < 4.0, "warm")
             .when(F.col("months_of_supply") < 6.0, "cool")
             .when(F.col("months_of_supply").isNotNull(), "cold")
             .otherwise(None)
        )
        .select(
            "geo_id", "country_code", "city", "zip_or_municipio", "date",
            "median_price_usd", "price_trend_3mo_pct", "price_trend_12mo_pct",
            "median_days_on_market", "inventory_count", "months_of_supply", "market_temp",
        )
    )

    out.write.mode("overwrite").saveAsTable(TARGET)
    count = spark.table(TARGET).count()
    print(f"silver.market_summary written: {count} rows")
    return count


run_build()
