"""
spark_utils.py
--------------
Shared helper functions for AWS Glue PySpark ETL jobs.
Import this module inside your Glue job scripts.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def cast_transaction_schema(df: DataFrame) -> DataFrame:
    """
    Enforce correct data types on the raw Bronze transaction DataFrame.
    Kinesis Firehose delivers everything as strings inside JSON — this fixes that.
    """
    return (
        df
        .withColumn("transaction_id", F.col("transaction_id").cast(StringType()))
        .withColumn("timestamp",       F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("store_id",        F.col("store_id").cast(StringType()))
        .withColumn("region",          F.col("region").cast(StringType()))
        .withColumn("product_id",      F.col("product_id").cast(StringType()))
        .withColumn("product_name",    F.col("product_name").cast(StringType()))
        .withColumn("category",        F.col("category").cast(StringType()))
        .withColumn("quantity",        F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",      F.col("unit_price").cast(DoubleType()))
        .withColumn("discount_pct",    F.col("discount_pct").cast(DoubleType()))
        .withColumn("total_amount",    F.col("total_amount").cast(DoubleType()))
        .withColumn("payment_method",  F.col("payment_method").cast(StringType()))
        .withColumn("customer_id",     F.col("customer_id").cast(StringType()))
        .withColumn("is_return",       F.col("is_return").cast("boolean"))
    )


# ---------------------------------------------------------------------------
# Partition helpers
# ---------------------------------------------------------------------------

def add_date_partitions(df: DataFrame, ts_col: str = "timestamp") -> DataFrame:
    """
    Add year / month / day partition columns derived from a timestamp column.
    These columns are used as Hive-style partition keys when writing to S3.
    """
    return (
        df
        .withColumn("year",  F.year(F.col(ts_col)).cast(StringType()))
        .withColumn("month", F.lpad(F.month(F.col(ts_col)).cast(StringType()), 2, "0"))
        .withColumn("day",   F.lpad(F.dayofmonth(F.col(ts_col)).cast(StringType()), 2, "0"))
    )


# ---------------------------------------------------------------------------
# Data quality helpers
# ---------------------------------------------------------------------------

def drop_nulls_in_critical_columns(df: DataFrame) -> DataFrame:
    """Remove rows where any critical business column is NULL."""
    critical = ["transaction_id", "timestamp", "store_id", "product_id", "total_amount"]
    return df.dropna(subset=critical)


def remove_duplicates(df: DataFrame, key_col: str = "transaction_id") -> DataFrame:
    """Deduplicate by transaction_id, keeping the latest record."""
    return df.dropDuplicates([key_col])


def filter_valid_amounts(df: DataFrame) -> DataFrame:
    """Remove rows with negative or zero total_amount (data anomalies)."""
    return df.filter(F.col("total_amount") > 0)


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def write_parquet_partitioned(
    df: DataFrame,
    output_path: str,
    partition_cols: list = None,
    mode: str = "append",
) -> None:
    """
    Write a DataFrame to S3 as partitioned Parquet.

    Args:
        df:             Spark DataFrame to write.
        output_path:    S3 URI, e.g. s3://datalake-silver-123456789/transactions/
        partition_cols: List of column names to partition by (default: year/month/day).
        mode:           Spark write mode ('append' or 'overwrite').
    """
    if partition_cols is None:
        partition_cols = ["year", "month", "day"]

    (
        df.write
        .mode(mode)
        .partitionBy(*partition_cols)
        .parquet(output_path)
    )
    print(f"✅ Written to {output_path}  (mode={mode}, partitions={partition_cols})")
