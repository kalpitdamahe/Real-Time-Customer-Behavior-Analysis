from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit, count, row_number, unix_timestamp, desc, sum as spark_sum, broadcast
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import logging
import os 

from extract_from_snowflake import read_from_snowflake_query



# Initialize logger for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# ======================
# 1. Load Event Data from InfluxDB
# ======================

influxdb_options = {
    "url": os.environ['influx_url'],
    "database": os.environ['influx_Database'],
    "username": os.environ['influx_user'],
    "password": os.environ['influx_password']
}

source_influx_query = """
    SELECT user_id, session_id, product_id, event_type, event_timestamp,
           campaign_code, payment_method_id, quantity, total_price 
    FROM customer_events 
    WHERE time > now() - 1d
"""

source_df = spark.read.format("influxdb") \
    .options(**influxdb_options) \
    .option("query", source_influx_query) \
    .load().cache()

SNOWDB, SNOWSCH = os.environ['SNOWDB'], os.environ['SNOWSCH']
S3_BUCKET, S3_OUTPATH = os.environ['S3_BUCKET'], os.environ['S3_OUTPATH']

sfOptions = {
        "sfURL" : os.environ['SNOWFLKE_ACCOUNT'],
        "sfUser" : os.environ['SNOW_USER'],
        "sfPassword" : os.environ['SNOW_PASS'],
        "sfDatabase" : os.environ['SNOW_DB'],
        "sfSchema" : os.environ['SNOW_SCH'],
        "sfWarehouse" : os.environ['SNOW_WH']
        }

# ======================
# 2. Define the SQL Queries for each dimension table to fetch the latest records
# ======================

# SQL query for dim_user to get the latest record for each user
dim_user_query = f"""
    WITH latest_user AS (
        SELECT user_id, user_name, email, region_id, updated_at,
               ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS row_num
        FROM {SNOWDB}.{SNOWSCH}.dim_user
    )
    SELECT user_id, user_name, email, region_id, updated_at
    FROM latest_user
    WHERE row_num = 1;
"""
dim_user_df = read_from_snowflake_query(dim_user_query).cache()


# SQL query for dim_product to get the latest record for each product
dim_product_query = f"""
    WITH latest_product AS (
        SELECT product_id, product_name, category_id, price, brand, updated_at,
               ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) AS row_num
        FROM {SNOWDB}.{SNOWSCH}.dim_product
    )
    SELECT product_id, product_name, category_id, price, brand, updated_at
    FROM latest_product
    WHERE row_num = 1;
"""
dim_product_df = read_from_snowflake_query(dim_product_query).cache()


# SQL query for dim_session to get the latest session data
dim_session_query = f"""
    WITH latest_session AS (
        SELECT session_id, user_id, start_time, device_type, updated_at,
               ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY updated_at DESC) AS row_num
        FROM {SNOWDB}.{SNOWSCH}.dim_session
    )
    SELECT session_id, user_id, start_time, device_type, updated_at
    FROM latest_session
    WHERE row_num = 1;
"""
dim_session_df = read_from_snowflake_query(dim_session_query).cache()


# SQL query for dim_category (no updated_at field, no filtering needed)
dim_category_query = f"""
    SELECT category_id, category_name, parent_category_id
    FROM {SNOWDB}.{SNOWSCH}.dim_category;
"""
dim_category_df = read_from_snowflake_query(dim_category_query).cache()


# SQL query for dim_region (no updated_at field, no filtering needed)
dim_region_query = f"""
    SELECT region_id, country, state, city, postal_code
    FROM {SNOWDB}.{SNOWSCH}.dim_region;
"""
dim_region_df = read_from_snowflake_query(dim_region_query).cache()

# Optimization: Broadcast smaller tables like dim_category and dim_region for joins
latest_dim_category_df = broadcast(dim_category_df)
latest_dim_region_df = broadcast(dim_region_df)

# Multi-level join with parent category and region enrichment
product_category_join = dim_product_df.join(latest_dim_category_df, "category_id", "left") \
    .select("product_id", "product_name", "category_name", "parent_category_id", "price", "brand")

parent_category_df = latest_dim_category_df.alias("parent").join(
    latest_dim_category_df.alias("child"), 
    col("child.parent_category_id") == col("parent.category_id"), "left"
).select("child.category_id", "parent.category_name as parent_category_name")

product_enriched_df = product_category_join.join(parent_category_df, "category_id", "left")


# Aggregation: Calculate total cart value for each session
session_aggregated_df = source_df.groupBy("session_id").agg(
    spark_sum("total_price").alias("total_session_value"),
    spark_sum(when(col("event_type") == "add_to_cart", col("quantity")).otherwise(0)).alias("total_cart_items")
)


# Enrich the source_df with user, session, and product data
enriched_df = source_df.join(dim_user_df, "user_id", "left") \
    .join(dim_session_df, "session_id", "left") \
    .join(product_enriched_df, "product_id", "left") \
    .select("user_id", "user_name", "email", "region_id", "product_id", "product_name", 
            "category_name", "parent_category_name", "price", "brand", "session_id", 
            "start_time", "device_type", "campaign_code", "payment_method_id")


# Derived column: Calculate discounted price and engagement score
enriched_df = enriched_df.withColumn("discounted_price", 
                                     when(col("campaign_code").isNotNull(), col("price") * 0.9)
                                     .otherwise(col("price"))) \
    .withColumn("engagement_score", 
                expr("CASE WHEN event_type = 'view' THEN 1 "
                     "WHEN event_type = 'add_to_cart' THEN 2 "
                     "WHEN event_type = 'purchase' THEN 3 ELSE 0 END"))

# Payment method mapping (broadcast the mapping table if using a DataFrame)
payment_method_mapping = {
    1: "Credit Card",
    2: "PayPal",
    3: "Bank Transfer"
}

enriched_df = enriched_df.withColumn("payment_method", 
                                     when(col("payment_method_id").isNotNull(), 
                                          expr(f"CASE payment_method_id " +
                                               " ".join([f"WHEN {k} THEN '{v}'" for k, v in payment_method_mapping.items()]) +
                                               " ELSE 'Unknown' END"))
                                     .otherwise(lit("Unknown")))

# Joining with aggregated session data
final_df = enriched_df.join(session_aggregated_df, "session_id", "left")

# Optimization: Repartitioning the final DataFrame for balanced writing
final_df = final_df.repartition(200, "session_id")  # Repartition based on session_id for balanced partitions
final_df.write.parquet(f'{S3_BUCKET}/{S3_OUTPATH}/cust_data')

# Write final data to flat table in Snowflake with optimized repartition
final_df.write.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", f"{SNOWDB}.{SNOWSCH}.flat_customer_events") \
    .mode("Append") \
    .save()




