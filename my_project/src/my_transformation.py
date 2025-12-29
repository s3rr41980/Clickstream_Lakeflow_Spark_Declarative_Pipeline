# DATABRICKS NOTEBOOK SOURCE
import dlt
from pyspark.sql.functions import col, expr, current_timestamp, sha2, lower, trim, date_format, window
from pyspark.sql.types import StructType, StructField, StringType

# Fetch the environment-specific path from the Bundle configuration
landing_path = spark.conf.get("pipeline.landing_path")

# 1. SCHEMA ENFORCEMENT
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


# 2. BRONZE (Ingestion)
@dlt.table(name="bronze_events")
def bronze_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(bronze_schema)
        .option("rescuedDataColumn", "_rescued_data")
        .load(landing_path)
    )


# 3. SILVER (Quality Gates with TAGGING)
@dlt.table(name="silver_events")
@dlt.expect_or_fail("has_timestamp", "event_time IS NOT NULL")
@dlt.expect("valid_event_id", "event_id IS NOT NULL")
def silver_events():
    return (
        dlt.read_stream("bronze_events")
        .select(
            "*",
            expr("to_timestamp(event_time, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")").alias("event_timestamp"),
            sha2(lower(trim(col("pii.email"))), 256).alias("email_hashed"),
            sha2(lower(trim(col("user_id"))), 256).alias("user_id_hashed"),
            col("geo.city").alias("city"),
            col("geo.country").alias("country"),
            col("device.os").alias("os"),
            col("device.browser").alias("browser"),
            current_timestamp().alias("ingestion_time"),
            expr("event_id IS NULL").alias("is_quarantined"),
        )
        .withWatermark("event_timestamp", "1 hour")
    )


# 4. QUARANTINE (The "Union" View)
@dlt.view(name="quarantine_report")
def quarantine_report():
    bronze_reserves = dlt.read("bronze_events").filter(col("_rescued_data").isNotNull())
    silver_fails = dlt.read("silver_events").filter(col("is_quarantined") == True)

    return bronze_reserves.select(col("_rescued_data").alias("reason"), col("event_id")).union(
        silver_fails.select(expr("'Missing Event ID'").alias("reason"), col("event_id"))
    )


# 5. GOLD: DIMENSIONS (SCD Type 2)
@dlt.view(name="v_dim_users_prep")
@dlt.expect_or_drop("valid_user_key", "user_id_hashed IS NOT NULL AND length(user_id_hashed) > 0")
def v_dim_users_prep():
    return dlt.read_stream("silver_events").select("user_id_hashed", "email_hashed", "city", "country", "event_timestamp")


dlt.create_streaming_table("dim_users")
dlt.apply_changes(
    target="dim_users",
    source="v_dim_users_prep",
    keys=["user_id_hashed"],
    sequence_by=col("event_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=["email_hashed", "city"],
)


@dlt.table(name="dim_geo")
def dim_geo():
    return dlt.read("silver_events").select("city", "country").distinct()


@dlt.table(name="dim_date")
def dim_date():
    return (
        dlt.read("silver_events")
        .select(expr("CAST(event_timestamp AS DATE) AS date_key"))
        .distinct()
        .select("date_key", date_format("date_key", "MMMM").alias("month"), expr("YEAR(date_key)").alias("year"))
    )


# 6. GOLD: FACT TABLE
@dlt.table(name="fact_clicks", table_properties={"pipelines.clusterKeys": "user_id_hashed, event_timestamp"})
def fact_clicks():
    return dlt.read_stream("silver_events").select(
        "event_id",
        "user_id_hashed",
        "event_type",
        "event_timestamp",
        expr("CAST(event_timestamp AS DATE) AS date_key"),
        "city",
        "country",
    )


# 7. GOLD: AGGREGATIONS & REPORTING
@dlt.table(name="gold_daily_geo_stats")
def gold_daily_geo_stats():
    return (
        dlt.read_stream("fact_clicks")
        .groupBy("date_key", "country", "city")
        .agg(expr("count(event_id)").alias("total_clicks"))
    )


@dlt.table(name="gold_final_report")
def gold_final_report():
    # Use dlt.read() for the final join to allow ordering and batch reporting
    return (
        dlt.read("gold_daily_geo_stats")
        .alias("stats")
        .join(dlt.read("dim_date").alias("date"), "date_key", "inner")
        .groupBy("date.month", "date.year", "stats.country")
        .agg(expr("sum(total_clicks)").alias("monthly_clicks"))
        .orderBy("date.year", "date.month", "stats.country")
    )
