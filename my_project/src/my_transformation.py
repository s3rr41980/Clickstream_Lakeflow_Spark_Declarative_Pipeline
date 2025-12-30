# DATABRICKS NOTEBOOK SOURCE
"""
LAKEFLOW SPARK DECLARATIVE PIPELINE (SDP) - CLICKSTREAM ANALYTICS
Author: Jack Goh
Date: 2024-10-01
Version: 1.0.0
Standard: Apache Spark 4.1 + Databricks Runtime 16.0+
Description: A production-grade Medallion architecture utilizing declarative
engineering to handle real-time clickstream ingestion and historical user tracking.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, current_timestamp, sha2, lower, trim, date_format
from pyspark.sql.types import StructType, StructField, StringType

# --- CONFIGURATION & PARAMETERS ---
# Dynamically fetch the landing path from the Databricks Asset Bundle (DAB) variables.
# This ensures environment-agnostic code (Dev/Stage/Prod) without manual hardcoding.
landing_path = spark.conf.get("pipeline.landing_path")

# --- 1. SCHEMA ENFORCEMENT (Data Governance) ---
# Defining a strict schema for JSON ingestion to prevent schema drift from
# breaking downstream silver/gold transformations.
bronze_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField(
            "device", StructType([StructField("os", StringType(), True), StructField("browser", StringType(), True)]), True
        ),
        StructField(
            "geo", StructType([StructField("country", StringType(), True), StructField("city", StringType(), True)]), True
        ),
        StructField("pii", StructType([StructField("email", StringType(), True)]), True),
    ]
)


# --- 2. BRONZE LAYER: Raw Streaming Ingestion ---
# Uses Auto Loader (cloudFiles) for high-frequency incremental ingestion.
# @dp.table creates a Streaming Table by default in Lakeflow SDP.
@dp.table(name="bronze_events")
def bronze_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(bronze_schema)
        # Rescued Data Column captures malformed JSON or extra fields for later debugging
        .option("rescuedDataColumn", "_rescued_data")
        .load(landing_path)
    )


# --- 3. SILVER LAYER: Data Quality & Cleansing ---
# Implements 'Expectations' (Declarative Quality Gates) and PII protection.
@dp.table(name="silver_events")
@dp.expect_or_fail("has_timestamp", "event_time IS NOT NULL")  # Hard Gate: Fail if no timestamp
@dp.expect("valid_event_id", "event_id IS NOT NULL")  # Soft Gate: Tag if ID is missing
def silver_events():
    return (
        dp.read_stream("bronze_events").select(
            "*",
            # Standardization: Cast string ISO-8601 to Spark Timestamp
            expr("to_timestamp(event_time, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")").alias("event_timestamp"),
            # Privacy: Cryptographically hash PII (Email/User ID) using SHA-256
            sha2(lower(trim(col("pii.email"))), 256).alias("email_hashed"),
            sha2(lower(trim(col("user_id"))), 256).alias("user_id_hashed"),
            # Flattening: Extracting nested JSON keys into top-level columns for performance
            col("geo.city").alias("city"),
            col("geo.country").alias("country"),
            col("device.os").alias("os"),
            col("device.browser").alias("browser"),
            current_timestamp().alias("ingestion_time"),
            # Quarantine Flag: Used for downstream reporting logic
            expr("event_id IS NULL").alias("is_quarantined"),
        )
        # Watermarking: Enables state cleanup for late-arriving data (1-hour threshold)
        .withWatermark("event_timestamp", "1 hour")
    )


# --- 4. QUARANTINE REPORT (Monitoring View) ---
# A @dp.temporary_view does not persist to the catalog. It is used here as a
# logical dashboard to identify rejected or rescued records.
@dp.temporary_view(name="quarantine_report")
def quarantine_report():
    bronze_reserves = dp.read("bronze_events").filter(col("_rescued_data").isNotNull())
    silver_fails = dp.read("silver_events").filter(col("is_quarantined") == True)
    return bronze_reserves.select(col("_rescued_data").alias("reason"), col("event_id")).union(
        silver_fails.select(expr("'Missing Event ID'").alias("reason"), col("event_id"))
    )


# --- 5. GOLD DIMENSIONS: Slowly Changing Dimensions (SCD) ---
# Preparing user data for SCD Type 2 tracking.
@dp.temporary_view(name="v_dim_users_prep")
@dp.expect_or_drop("valid_user_key", "user_id_hashed IS NOT NULL AND length(user_id_hashed) > 0")
def v_dim_users_prep():
    return dp.read_stream("silver_events").select("user_id_hashed", "email_hashed", "city", "country", "event_timestamp")


# Declarative SCD Type 2: Automatically manages 'is_current', 'start_date', and 'end_date'
# to track the history of user location and email changes.
dp.create_streaming_table("dim_users")
dp.apply_changes(
    target="dim_users",
    source="v_dim_users_prep",
    keys=["user_id_hashed"],
    sequence_by=col("event_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=["email_hashed", "city"],
)


# Materialized Views: Used for dimensions that require de-duplication across the dataset.
@dp.materialized_view(name="dim_geo")
def dim_geo():
    return dp.read("silver_events").select("city", "country").distinct()


@dp.materialized_view(name="dim_date")
def dim_date():
    return (
        dp.read("silver_events")
        .select(expr("CAST(event_timestamp AS DATE) AS date_key"))
        .distinct()
        .select("date_key", date_format("date_key", "MMMM").alias("month"), expr("YEAR(date_key)").alias("year"))
    )


# --- 6. GOLD FACT: Performance Optimized Transactions ---
# Applying 'Liquid Clustering' to optimize high-volume clickstream analysis.
# Using 'cluster_by' for initial layout and 'cluster_by_auto' for autonomous re-clustering.
@dp.table(
    name="fact_clicks",
    cluster_by=["user_id_hashed", "event_timestamp"],
    cluster_by_auto=True,  # Intelligence Engine autonomously optimizes based on query patterns
)
def fact_clicks():
    return dp.read_stream("silver_events").select(
        "event_id",
        "user_id_hashed",
        "event_type",
        "event_timestamp",
        expr("CAST(event_timestamp AS DATE) AS date_key"),
        "city",
        "country",
    )


# --- 7. GOLD AGGREGATIONS: Business Reporting ---
# Final high-level metrics updated incrementally as new events flow through.
@dp.table(name="gold_daily_geo_stats", cluster_by_auto=True)
def gold_daily_geo_stats():
    return (
        dp.read_stream("fact_clicks")
        .groupBy("date_key", "country", "city")
        .agg(expr("count(event_id)").alias("total_clicks"))
    )


# Combining dimensions and facts for the final report.
# @dp.materialized_view ensures the report is pre-computed for fast BI access.
@dp.materialized_view(name="gold_final_report")
def gold_final_report():
    return (
        dp.read("gold_daily_geo_stats")
        .alias("stats")
        .join(dp.read("dim_date").alias("date"), "date_key", "inner")
        .groupBy("date.month", "date.year", "stats.country")
        .agg(expr("sum(total_clicks)").alias("monthly_clicks"))
    )
