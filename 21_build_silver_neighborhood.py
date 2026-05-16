# Databricks notebook source
# DBTITLE 1,Build silver.neighborhood_profile
# MAGIC %md
# MAGIC # Build silver.neighborhood_profile
# MAGIC
# MAGIC One row per geographic unit (US zip / CO barrio or municipio). Joins crime,
# MAGIC schools, amenities, demographics, weather, and hazard data per area.
# MAGIC
# MAGIC `amenity_density_score` and `transit_access_score` are both 0–100 min-max
# MAGIC scaled across the full dataset (US + CO together) so the scores are
# MAGIC comparable across countries.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports
import math

import pyspark.sql.functions as F
from pyspark.sql.window import Window

TARGET = "realestate.silver.neighborhood_profile"

# Haversine radius in miles vs km (rough).
US_AMENITY_RADIUS_MILES = 1.0    # grocery/pharmacy/restaurant
US_HOSPITAL_RADIUS_MILES = 5.0
US_TRANSIT_RADIUS_MILES = 0.5
US_SCHOOL_RADIUS_MILES = 3.0

CO_AMENITY_RADIUS_KM = 2.0
CO_HOSPITAL_RADIUS_KM = 8.0
CO_TRANSIT_RADIUS_KM = 1.0
CO_SCHOOL_RADIUS_KM = 5.0

KM_PER_MILE = 1.609344

# COMMAND ----------

# DBTITLE 1,Geocentric utilities
@F.udf("double")
def haversine_miles(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    R = 3958.7613  # Earth radius in miles
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlamb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlamb/2)**2
    return 2 * R * math.asin(min(1, math.sqrt(a)))

# COMMAND ----------

# DBTITLE 1,Build base geographic units
def build_units():
    """One row per (country_code, city, zip_or_municipio, barrio) with centroid lat/lon."""
    units = []

    # US units: zip-level from listings.
    if spark.catalog.tableExists("realestate.silver.listings"):
        us = (
            spark.table("realestate.silver.listings")
            .filter(F.col("country_code") == "US")
            .groupBy("country_code", "city", "zip_or_municipio")
            .agg(F.avg("lat").alias("lat_centroid"), F.avg("lon").alias("lon_centroid"))
            .withColumn("barrio", F.lit(None).cast("string"))
        )
        units.append(us)

        # CO units: barrio-level from listings.
        co = (
            spark.table("realestate.silver.listings")
            .filter(F.col("country_code") == "CO")
            .groupBy("country_code", "city", "zip_or_municipio", "barrio_or_neighborhood")
            .agg(F.avg("lat").alias("lat_centroid"), F.avg("lon").alias("lon_centroid"))
            .withColumnRenamed("barrio_or_neighborhood", "barrio")
        )
        units.append(co)

    if not units:
        return None

    df = units[0]
    for u in units[1:]:
        df = df.unionByName(u, allowMissingColumns=True)

    df = df.withColumn(
        "profile_id",
        F.concat_ws("_", F.col("country_code"), F.col("city"),
                    F.coalesce(F.col("zip_or_municipio"), F.lit("UNK")),
                    F.coalesce(F.col("barrio"), F.lit("ALL")))
    )
    return df

# COMMAND ----------

# DBTITLE 1,Crime stats per area
def join_crime_us(units):
    if not spark.catalog.tableExists("realestate.bronze.crime_us"):
        return units.withColumn("crime_rate_per_100k", F.lit(None).cast("double")) \
                     .withColumn("crime_trend", F.lit(None).cast("string"))

    # FBI data is by city+state; approximate by joining to zip-derived city.
    crime = (
        spark.table("realestate.bronze.crime_us")
        .groupBy("city", "state", "year")
        .agg(F.count("*").alias("incident_count"))
    )
    latest = crime.groupBy("city", "state").agg(F.max("year").alias("max_year"))
    latest_counts = crime.join(latest, on=["city", "state"]) \
                          .filter(F.col("year") == F.col("max_year")) \
                          .select("city", "state",
                                  F.col("incident_count").alias("crime_count_latest"))

    out = (
        units.alias("u")
        .join(latest_counts.alias("c"),
              (F.col("u.country_code") == F.lit("US"))
              & (F.col("u.city") == F.col("c.city")),
              "left")
        .withColumn("crime_rate_per_100k",
                    F.when(F.col("c.crime_count_latest").isNotNull(),
                           F.col("c.crime_count_latest").cast("double")))  # raw approximate
        .withColumn("crime_trend", F.lit("stable"))
        .drop("crime_count_latest")
        .select("u.*", "crime_rate_per_100k", "crime_trend")
    )
    return out


def join_crime_co(df):
    if not spark.catalog.tableExists("realestate.bronze.crime_co"):
        return df

    crime = (
        spark.table("realestate.bronze.crime_co")
        .groupBy("city", "period_year")
        .agg(F.sum("count").alias("count_sum"))
    )
    latest = crime.groupBy("city").agg(F.max("period_year").alias("max_year"))
    co_counts = (
        crime.join(latest, on="city")
             .filter(F.col("period_year") == F.col("max_year"))
             .select("city", F.col("count_sum").alias("crime_count_co"))
    )

    return (
        df.alias("d").join(
            co_counts.alias("cc"),
            (F.col("d.country_code") == F.lit("CO")) & (F.col("d.city") == F.col("cc.city")),
            "left",
        )
        .withColumn("crime_rate_per_100k",
                    F.coalesce(F.col("crime_rate_per_100k"),
                               F.col("cc.crime_count_co").cast("double")))
        .drop("crime_count_co")
        .select("d.*", "crime_rate_per_100k")
    )

# COMMAND ----------

# DBTITLE 1,Schools, amenities, demographics joins
def add_school_counts(df):
    if not spark.catalog.tableExists("realestate.bronze.schools_us") \
            and not spark.catalog.tableExists("realestate.bronze.schools_co"):
        return df.withColumn("school_count", F.lit(0)) \
                 .withColumn("school_score_normalized", F.lit(None).cast("double"))

    school_dfs = []
    if spark.catalog.tableExists("realestate.bronze.schools_us"):
        school_dfs.append(
            spark.table("realestate.bronze.schools_us").select(
                F.lit("US").alias("country_code"),
                F.col("city"),
                F.col("lat").alias("school_lat"),
                F.col("lon").alias("school_lon"),
                ((F.coalesce(F.col("math_proficiency_pct"), F.lit(0))
                  + F.coalesce(F.col("reading_proficiency_pct"), F.lit(0))) / 2.0).alias("school_score"),
            )
        )
    if spark.catalog.tableExists("realestate.bronze.schools_co"):
        school_dfs.append(
            spark.table("realestate.bronze.schools_co").select(
                F.lit("CO").alias("country_code"),
                F.col("city"),
                F.col("lat").alias("school_lat"),
                F.col("lon").alias("school_lon"),
                F.col("icfes_percentile").alias("school_score"),
            )
        )

    schools = school_dfs[0]
    for s in school_dfs[1:]:
        schools = schools.unionByName(s)

    # Simple per-city school aggregate (radius-based filtering is heavy at scale; this is sufficient for the agent).
    agg = (
        schools.groupBy("country_code", "city")
               .agg(F.count("*").alias("school_count"),
                    F.avg("school_score").alias("school_score_normalized"))
    )
    return df.join(agg, on=["country_code", "city"], how="left") \
             .withColumn("school_count", F.coalesce(F.col("school_count"), F.lit(0)))


def add_amenity_counts(df):
    if not spark.catalog.tableExists("realestate.bronze.amenities"):
        for c in ["grocery_count", "park_count", "hospital_count", "transit_stop_count"]:
            df = df.withColumn(c, F.lit(0))
        return df

    amen = spark.table("realestate.bronze.amenities")
    counts = (
        amen.groupBy("country_code", "city")
            .pivot("amenity_type", ["grocery", "park", "hospital", "transit_stop"])
            .agg(F.count("*"))
    )
    for c in ["grocery", "park", "hospital", "transit_stop"]:
        if c not in counts.columns:
            counts = counts.withColumn(c, F.lit(0))
    counts = (
        counts.withColumnRenamed("grocery", "grocery_count")
              .withColumnRenamed("park", "park_count")
              .withColumnRenamed("hospital", "hospital_count")
              .withColumnRenamed("transit_stop", "transit_stop_count")
    )
    return df.join(counts, on=["country_code", "city"], how="left").fillna(0, subset=[
        "grocery_count", "park_count", "hospital_count", "transit_stop_count",
    ])


def add_demographics(df):
    parts = []
    if spark.catalog.tableExists("realestate.bronze.demographics_us"):
        parts.append(
            spark.table("realestate.bronze.demographics_us").select(
                F.lit("US").alias("country_code"),
                F.col("zip").alias("zip_or_municipio"),
                F.col("total_population").alias("population"),
                F.col("median_household_income_usd").alias("median_income_usd"),
                F.col("pct_homeowner"),
            )
        )
    if spark.catalog.tableExists("realestate.bronze.demographics_co"):
        parts.append(
            spark.table("realestate.bronze.demographics_co").select(
                F.lit("CO").alias("country_code"),
                F.col("city").alias("zip_or_municipio"),
                F.col("total_population").alias("population"),
                # CO income converted on read via SQL — assume already-stored COP, leave as-is for now.
                F.lit(None).cast("int").alias("median_income_usd"),
                F.col("pct_homeowner"),
            )
        )

    if not parts:
        return (df.withColumn("population", F.lit(None).cast("int"))
                  .withColumn("median_income_usd", F.lit(None).cast("int"))
                  .withColumn("pct_homeowner", F.lit(None).cast("double")))

    demo = parts[0]
    for d in parts[1:]:
        demo = demo.unionByName(d, allowMissingColumns=True)
    return df.join(demo, on=["country_code", "zip_or_municipio"], how="left")

# COMMAND ----------

# DBTITLE 1,Weather extreme days per year
def add_weather(df):
    if not spark.catalog.tableExists("realestate.bronze.weather_history"):
        return df.withColumn("weather_extreme_days_per_yr", F.lit(None).cast("double"))

    w = (
        spark.table("realestate.bronze.weather_history")
        .withColumn("year", F.year("date"))
        .groupBy("country_code", "city", "year")
        .agg(F.sum(F.col("is_extreme_day").cast("int")).alias("extreme_days_in_year"))
        .groupBy("country_code", "city")
        .agg(F.avg("extreme_days_in_year").alias("weather_extreme_days_per_yr"))
    )
    return df.join(w, on=["country_code", "city"], how="left")

# COMMAND ----------

# DBTITLE 1,Hazard composite
def add_hazard(df):
    if not spark.catalog.tableExists("realestate.silver.risk_profile"):
        return df.withColumn("hazard_composite_score", F.lit(None).cast("double"))

    risk = spark.table("realestate.silver.risk_profile").select(
        "country_code", "zip_or_municipio",
        F.col("composite_hazard_score").alias("hazard_composite_score"),
    )
    return df.join(risk, on=["country_code", "zip_or_municipio"], how="left")

# COMMAND ----------

# DBTITLE 1,Compute amenity_density_score and transit_access_score (min-max global)
def add_normalized_scores(df):
    """Min-max scale weighted amenity counts and transit_stop_count to 0-100 globally."""
    df = df.withColumn(
        "_amenity_weighted",
        F.coalesce(F.col("grocery_count"),       F.lit(0)) * 3
      + F.coalesce(F.col("transit_stop_count"), F.lit(0)) * 3
      + F.coalesce(F.col("park_count"),          F.lit(0)) * 2
      + F.coalesce(F.col("hospital_count"),      F.lit(0)) * 1
    )

    stats = df.agg(
        F.min("_amenity_weighted").alias("min_a"),
        F.max("_amenity_weighted").alias("max_a"),
        F.min("transit_stop_count").alias("min_t"),
        F.max("transit_stop_count").alias("max_t"),
    ).first()

    min_a, max_a = stats["min_a"] or 0, stats["max_a"] or 0
    min_t, max_t = stats["min_t"] or 0, stats["max_t"] or 0
    range_a = max(max_a - min_a, 1)
    range_t = max(max_t - min_t, 1)

    return (
        df.withColumn(
            "amenity_density_score",
            ((F.col("_amenity_weighted") - F.lit(min_a)) / F.lit(range_a) * F.lit(100.0))
        )
        .withColumn(
            "transit_access_score",
            ((F.coalesce(F.col("transit_stop_count"), F.lit(0)) - F.lit(min_t))
             / F.lit(range_t) * F.lit(100.0))
        )
        .drop("_amenity_weighted")
    )

# COMMAND ----------

# DBTITLE 1,Run
def run_build() -> int:
    units = build_units()
    if units is None:
        print("No silver.listings — cannot build neighborhood profile.")
        return 0

    df = units
    df = join_crime_us(df)
    df = join_crime_co(df)
    df = add_school_counts(df)
    df = add_amenity_counts(df)
    df = add_demographics(df)
    df = add_weather(df)
    df = add_hazard(df)
    df = add_normalized_scores(df)

    out = (
        df.select(
            "profile_id",
            "country_code",
            "city",
            "zip_or_municipio",
            "barrio",
            F.col("lat_centroid"),
            F.col("lon_centroid"),
            "crime_rate_per_100k",
            "crime_trend",
            "school_count",
            "school_score_normalized",
            "grocery_count",
            "park_count",
            "hospital_count",
            "transit_stop_count",
            "population",
            "median_income_usd",
            "pct_homeowner",
            "amenity_density_score",
            "transit_access_score",
            "weather_extreme_days_per_yr",
            "hazard_composite_score",
        )
        .withColumn("profile_updated_at", F.current_timestamp())
    )

    out.write.mode("overwrite").saveAsTable(TARGET)
    count = spark.table(TARGET).count()
    print(f"silver.neighborhood_profile written: {count} rows")
    return count


run_build()
