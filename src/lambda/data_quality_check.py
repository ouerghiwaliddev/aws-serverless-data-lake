"""
data_quality_check.py
----------------------
AWS Lambda function — optional data quality gate triggered by EventBridge
before the Glue ETL jobs run.

Checks:
  1. Bronze bucket has new files since last execution
  2. Sample 100 records and validate schema / value ranges
  3. Publishes a CloudWatch custom metric: DataLake/QualityScore
  4. Sends SNS alert if quality score drops below threshold

Environment variables (set in Lambda console or CloudFormation):
  BRONZE_BUCKET       datalake-bronze-<account-id>
  SILVER_BUCKET       datalake-silver-<account-id>
  SNS_TOPIC_ARN       arn:aws:sns:us-east-1:<account>:datalake-alerts
  QUALITY_THRESHOLD   0.95   (fail if >5% records are bad)
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone

import boto3

s3        = boto3.client("s3")
sns       = boto3.client("sns")
cw        = boto3.client("cloudwatch")
glue      = boto3.client("glue")

BRONZE_BUCKET      = os.environ["BRONZE_BUCKET"]
SNS_TOPIC_ARN      = os.environ["SNS_TOPIC_ARN"]
QUALITY_THRESHOLD  = float(os.environ.get("QUALITY_THRESHOLD", "0.95"))

REQUIRED_FIELDS = [
    "transaction_id", "timestamp", "store_id", "product_id",
    "total_amount", "quantity", "category",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def list_new_objects(bucket: str, prefix: str, since_hours: int = 24) -> list:
    """Return S3 keys modified within the last `since_hours` hours."""
    cutoff  = datetime.now(tz=timezone.utc) - timedelta(hours=since_hours)
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["LastModified"] >= cutoff:
                keys.append(obj["Key"])
    return keys


def sample_records(bucket: str, key: str, sample_size: int = 10) -> list:
    """Download a file and return up to sample_size parsed JSON records."""
    response = s3.get_object(Bucket=bucket, Key=key)
    body     = response["Body"].read().decode("utf-8")
    records  = []
    for line in body.strip().splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pass
        if len(records) >= sample_size:
            break
    return records


def validate_record(record: dict) -> tuple[bool, list]:
    """
    Validate a single transaction record.
    Returns (is_valid, list_of_errors).
    """
    errors = []

    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None:
            errors.append(f"Missing field: {field}")

    # Business rules
    if record.get("total_amount", -1) <= 0:
        errors.append(f"Invalid total_amount: {record.get('total_amount')}")

    if record.get("quantity", -1) <= 0:
        errors.append(f"Invalid quantity: {record.get('quantity')}")

    if record.get("discount_pct", -1) < 0 or record.get("discount_pct", 2) > 1:
        errors.append(f"Discount out of range: {record.get('discount_pct')}")

    return len(errors) == 0, errors


def publish_quality_metric(score: float, record_count: int) -> None:
    """Push a custom CloudWatch metric for the quality score."""
    cw.put_metric_data(
        Namespace="DataLake",
        MetricData=[
            {
                "MetricName": "QualityScore",
                "Value":      score,
                "Unit":       "None",
                "Dimensions": [{"Name": "Zone", "Value": "Bronze"}],
            },
            {
                "MetricName": "SampledRecords",
                "Value":      record_count,
                "Unit":       "Count",
                "Dimensions": [{"Name": "Zone", "Value": "Bronze"}],
            },
        ],
    )


def send_alert(subject: str, message: str) -> None:
    """Send an SNS alert."""
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message,
    )


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    run_date = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")
    prefix   = f"transactions/{run_date[:4]}/{run_date[5:7]}/{run_date[8:10]}/"

    print(f"🔍 Checking Bronze bucket: s3://{BRONZE_BUCKET}/{prefix}")

    # 1. Find new files
    new_keys = list_new_objects(BRONZE_BUCKET, prefix, since_hours=25)
    if not new_keys:
        msg = f"⚠️ No new files found in Bronze for prefix: {prefix}"
        print(msg)
        send_alert("[DataLake] WARNING: No new Bronze data", msg)
        return {"status": "NO_DATA", "prefix": prefix}

    print(f"   Found {len(new_keys)} new file(s). Sampling...")

    # 2. Sample records across files
    sample_keys   = random.sample(new_keys, min(5, len(new_keys)))
    all_records   = []
    for key in sample_keys:
        all_records.extend(sample_records(BRONZE_BUCKET, key, sample_size=20))

    # 3. Validate records
    valid_count = 0
    error_log   = []
    for rec in all_records:
        is_valid, errors = validate_record(rec)
        if is_valid:
            valid_count += 1
        else:
            error_log.append({"record_id": rec.get("transaction_id", "unknown"), "errors": errors})

    total        = len(all_records)
    quality_score = valid_count / total if total > 0 else 0.0

    print(f"   Sampled {total} records | Valid: {valid_count} | Score: {quality_score:.2%}")

    # 4. Publish CloudWatch metric
    publish_quality_metric(quality_score, total)

    # 5. Alert if quality is below threshold
    if quality_score < QUALITY_THRESHOLD:
        alert_msg = (
            f"❌ Data quality below threshold!\n"
            f"Score: {quality_score:.2%} (threshold: {QUALITY_THRESHOLD:.0%})\n"
            f"Sampled: {total} records from {len(sample_keys)} files\n"
            f"Errors (first 5):\n{json.dumps(error_log[:5], indent=2)}"
        )
        send_alert("[DataLake] ALERT: Low data quality — ETL paused", alert_msg)
        print(alert_msg)
        return {"status": "QUALITY_FAIL", "score": quality_score, "errors": error_log[:5]}

    print(f"✅ Quality check passed ({quality_score:.2%}). ETL can proceed.")
    return {
        "status":        "OK",
        "quality_score": quality_score,
        "files_checked": len(new_keys),
        "records_sampled": total,
    }
