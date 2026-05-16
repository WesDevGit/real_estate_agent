# Databricks notebook source
# DBTITLE 1,Build gold.hazard_risk + gold.school_rankings
# MAGIC %md
# MAGIC # Build Gold Static Tables
# MAGIC
# MAGIC * `gold.hazard_risk` — full mirror of `silver.risk_profile`.
# MAGIC * `gold.school_rankings` — unified US + CO school table with normalized scores.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,gold.hazard_risk
def build_hazard_risk() -> int:
    src = "realestate.silver.risk_profile"
    tgt = "realestate.gold.hazard_risk"
    if not spark.catalog.tableExists(src):
        print(f"{src} does not exist — run 23_build_silver_risk.py first.")
        return 0
    df = spark.table(src)
    df.write.mode("overwrite").saveAsTable(tgt)
    count = spark.table(tgt).count()
    print(f"{tgt} written: {count} rows")
    return count


build_hazard_risk()

# COMMAND ----------

# DBTITLE 1,gold.school_rankings (unified US + CO)
def build_school_rankings() -> int:
    tgt = "realestate.gold.school_rankings"
    parts = []

    if spark.catalog.tableExists("realestate.bronze.schools_us"):
        us = (
            spark.table("realestate.bronze.schools_us")
            .select(
                F.col("school_id"),
                F.lit("US").alias("country_code"),
                F.col("city"),
                F.col("state").alias("state_or_dept"),
                F.col("zip").alias("zip_or_municipio"),
                F.col("lat"),
                F.col("lon"),
                F.col("school_name"),
                F.col("grade_levels"),
                F.col("enrollment"),
                F.col("school_type"),
                F.when(
                    F.col("math_proficiency_pct").isNotNull() | F.col("reading_proficiency_pct").isNotNull(),
                    (F.coalesce(F.col("math_proficiency_pct"), F.lit(0))
                     + F.coalesce(F.col("reading_proficiency_pct"), F.lit(0))) / 2.0
                ).alias("school_score_normalized"),
                F.lit(None).cast("int").alias("score_year"),
            )
        )
        parts.append(us)

    if spark.catalog.tableExists("realestate.bronze.schools_co"):
        co = (
            spark.table("realestate.bronze.schools_co")
            .select(
                F.col("school_id"),
                F.lit("CO").alias("country_code"),
                F.col("city"),
                F.col("departamento").alias("state_or_dept"),
                F.col("municipio").alias("zip_or_municipio"),
                F.col("lat"),
                F.col("lon"),
                F.col("institution_name").alias("school_name"),
                F.col("grade_levels"),
                F.col("enrollment"),
                F.col("school_type"),
                F.col("icfes_percentile").alias("school_score_normalized"),
                F.col("icfes_year").alias("score_year"),
            )
        )
        parts.append(co)

    if not parts:
        print("No school bronze tables — gold.school_rankings empty")
        return 0

    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    df = df.withColumn(
        "school_score_normalized",
        F.when(F.col("school_score_normalized") < 0, F.lit(0))
         .when(F.col("school_score_normalized") > 100, F.lit(100))
         .otherwise(F.col("school_score_normalized")),
    )
    df.write.mode("overwrite").saveAsTable(tgt)
    count = spark.table(tgt).count()
    print(f"{tgt} written: {count} rows")
    return count


build_school_rankings()
