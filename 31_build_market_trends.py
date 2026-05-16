# Databricks notebook source
# DBTITLE 1,Build gold.market_trends
# MAGIC %md
# MAGIC # Build gold.market_trends
# MAGIC
# MAGIC Promote `silver.market_summary` to gold with 6-mo and 12-mo rolling averages
# MAGIC of median_price_usd per (country_code, city, zip_or_municipio).

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports
import pyspark.sql.functions as F
from pyspark.sql.window import Window

SOURCE = "realestate.silver.market_summary"
TARGET = "realestate.gold.market_trends"

# COMMAND ----------

# DBTITLE 1,Build with rolling averages
def run_build() -> int:
    if not spark.catalog.tableExists(SOURCE):
        print(f"{SOURCE} not found.")
        return 0

    df = spark.table(SOURCE)
    if df.count() == 0:
        print("No silver.market_summary rows.")
        return 0

    # Window of 6 and 12 prior rows (months), inclusive of current row.
    base_w = Window.partitionBy("country_code", "city", "zip_or_municipio") \
                   .orderBy("date")
    w6 = base_w.rowsBetween(-5, 0)
    w12 = base_w.rowsBetween(-11, 0)

    out = (
        df.withColumn("median_price_usd_rolling_6mo", F.avg("median_price_usd").over(w6))
          .withColumn("median_price_usd_rolling_12mo", F.avg("median_price_usd").over(w12))
    )
    out.write.mode("overwrite").saveAsTable(TARGET)
    count = spark.table(TARGET).count()
    print(f"{TARGET} written: {count} rows")
    return count


run_build()
