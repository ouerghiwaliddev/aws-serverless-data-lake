"""
silver_to_gold.py
------------------
AWS Glue PySpark ETL Job — Silver → Gold zone.

Reads clean Parquet from Silver, computes business KPI aggregations,
and writes results to the Gold zone as partitioned Parquet for QuickSight / Athena.

KPIs produced:
  1. revenue_by_product   — daily revenue & units sold per product category
  2. revenue_by_region    — daily revenue & transaction count per store region
  3. payment_method_split — daily breakdown by payment method

Glue job parameters:
  --silver_path   s3://datalake-silver-<account>/transactions/
  --gold_path     s3://datalake-gold-<account>/kpis/
  --job_name      silver-to-gold
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from common.spark_utils import write_parquet_partitioned

# ---------------------------------------------------------------------------
# Glue bootstrap
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "silver_path", "gold_path"],
)

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

silver_path = args["silver_path"]
gold_path   = args["gold_path"]

print(f"📥 Reading Silver from : {silver_path}")
print(f"📤 Writing Gold to     : {gold_path}")

# ---------------------------------------------------------------------------
# 1. Read Silver (partitioned Parquet)
# ---------------------------------------------------------------------------
silver_df = spark.read.parquet(silver_path)

# Filter out returns for revenue KPIs (returns reduce revenue)
sales_df = silver_df.filter(F.col("is_return") == False)

total_rows = sales_df.count()
print(f"   Silver sales records: {total_rows:,}")

# ---------------------------------------------------------------------------
# 2. KPI 1 — Daily revenue by product category
# ---------------------------------------------------------------------------
revenue_by_product = (
    sales_df
    .groupBy("year", "month", "day", "category")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("transaction_id").alias("transaction_count"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
    )
    .withColumn("kpi_type", F.lit("revenue_by_product"))
)

write_parquet_partitioned(
    df=revenue_by_product,
    output_path=f"{gold_path}revenue_by_product/",
    partition_cols=["year", "month", "day"],
    mode="overwrite",
)

print("✅ KPI 1 written: revenue_by_product")
revenue_by_product.orderBy(F.desc("total_revenue")).show(5)

# ---------------------------------------------------------------------------
# 3. KPI 2 — Daily revenue by region
# ---------------------------------------------------------------------------
revenue_by_region = (
    sales_df
    .groupBy("year", "month", "day", "region")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.countDistinct("transaction_id").alias("transaction_count"),
        F.countDistinct("store_id").alias("active_stores"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
    )
    .withColumn("kpi_type", F.lit("revenue_by_region"))
)

write_parquet_partitioned(
    df=revenue_by_region,
    output_path=f"{gold_path}revenue_by_region/",
    partition_cols=["year", "month", "day"],
    mode="overwrite",
)

print("✅ KPI 2 written: revenue_by_region")
revenue_by_region.orderBy(F.desc("total_revenue")).show(5)

# ---------------------------------------------------------------------------
# 4. KPI 3 — Daily payment method breakdown
# ---------------------------------------------------------------------------
payment_split = (
    sales_df
    .groupBy("year", "month", "day", "payment_method")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("transaction_id").alias("transaction_count"),
    )
    .withColumn("kpi_type", F.lit("payment_method_split"))
)

write_parquet_partitioned(
    df=payment_split,
    output_path=f"{gold_path}payment_method_split/",
    partition_cols=["year", "month", "day"],
    mode="overwrite",
)

print("✅ KPI 3 written: payment_method_split")
payment_split.orderBy(F.desc("transaction_count")).show(5)

# ---------------------------------------------------------------------------
# 5. KPI 4 — Top 10 products by revenue (current day)
# ---------------------------------------------------------------------------
top_products = (
    sales_df
    .groupBy("year", "month", "day", "product_id", "product_name", "category")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.sum("quantity").alias("units_sold"),
    )
    .withColumn(
        "revenue_rank",
        F.rank().over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.partitionBy("year", "month", "day")
            .orderBy(F.desc("total_revenue"))
        ),
    )
    .filter(F.col("revenue_rank") <= 10)
    .withColumn("kpi_type", F.lit("top_products"))
)

write_parquet_partitioned(
    df=top_products,
    output_path=f"{gold_path}top_products/",
    partition_cols=["year", "month", "day"],
    mode="overwrite",
)

print("✅ KPI 4 written: top_products")
top_products.orderBy("year", "month", "day", "revenue_rank").show(10)

job.commit()
print("\n🏆 silver_to_gold job complete. All KPIs in Gold zone.")
