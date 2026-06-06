"""
bronze_to_silver.py
--------------------
AWS Glue PySpark ETL Job — Bronze → Silver zone.

Reads raw JSON from the Bronze S3 bucket, applies data quality rules,
casts schema, deduplicates, and writes partitioned Parquet to Silver.

Glue job parameters (set in AWS Console or CloudFormation):
  --bronze_path   s3://datalake-bronze-<account>/transactions/
  --silver_path   s3://datalake-silver-<account>/transactions/
  --job_name      bronze-to-silver (injected automatically by Glue)
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Import shared helpers (upload spark_utils.py to S3 and reference via --extra-py-files)
from common.spark_utils import (
    add_date_partitions,
    cast_transaction_schema,
    drop_nulls_in_critical_columns,
    filter_valid_amounts,
    remove_duplicates,
    write_parquet_partitioned,
)

# ---------------------------------------------------------------------------
# Glue bootstrap
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bronze_path", "silver_path"],
)

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

bronze_path = args["bronze_path"]
silver_path = args["silver_path"]

print(f"📥 Reading Bronze from : {bronze_path}")
print(f"📤 Writing Silver to   : {silver_path}")

# ---------------------------------------------------------------------------
# 1. Read raw JSON from Bronze
# ---------------------------------------------------------------------------
raw_df = (
    spark.read
    .option("multiLine", "false")       # one JSON object per line (Firehose default)
    .json(bronze_path)
)

raw_count = raw_df.count()
print(f"   Raw records read   : {raw_count:,}")

# ---------------------------------------------------------------------------
# 2. Data quality — drop nulls, invalid amounts
# ---------------------------------------------------------------------------
clean_df = drop_nulls_in_critical_columns(raw_df)
clean_df = filter_valid_amounts(clean_df)

after_quality = clean_df.count()
print(f"   After quality check: {after_quality:,}  (dropped {raw_count - after_quality:,})")

# ---------------------------------------------------------------------------
# 3. Deduplicate by transaction_id
# ---------------------------------------------------------------------------
deduped_df = remove_duplicates(clean_df, key_col="transaction_id")

after_dedup = deduped_df.count()
print(f"   After dedup        : {after_dedup:,}  (dropped {after_quality - after_dedup:,} duplicates)")

# ---------------------------------------------------------------------------
# 4. Cast schema to correct types
# ---------------------------------------------------------------------------
typed_df = cast_transaction_schema(deduped_df)

# ---------------------------------------------------------------------------
# 5. Add Hive partition columns (year / month / day)
# ---------------------------------------------------------------------------
partitioned_df = add_date_partitions(typed_df, ts_col="timestamp")

# ---------------------------------------------------------------------------
# 6. Write to Silver as partitioned Parquet
# ---------------------------------------------------------------------------
write_parquet_partitioned(
    df=partitioned_df,
    output_path=silver_path,
    partition_cols=["year", "month", "day"],
    mode="append",
)

# ---------------------------------------------------------------------------
# 7. Print sample for validation
# ---------------------------------------------------------------------------
print("\n📊 Sample Silver records:")
partitioned_df.select(
    "transaction_id", "timestamp", "store_id", "category",
    "total_amount", "year", "month", "day"
).show(5, truncate=False)

job.commit()
print("✅ bronze_to_silver job complete.")
