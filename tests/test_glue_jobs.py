"""
test_glue_jobs.py
-----------------
Unit tests for the shared Spark utility functions.
Run locally with a PySpark installation:
    pip install pyspark pytest
    pytest tests/test_glue_jobs.py -v
"""

import sys
import unittest

sys.path.insert(0, "src/glue_jobs")

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from common.spark_utils import (
        add_date_partitions,
        cast_transaction_schema,
        drop_nulls_in_critical_columns,
        filter_valid_amounts,
        remove_duplicates,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

import unittest


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark not installed — skipping Spark tests")
class TestSparkUtils(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("test_spark_utils")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _make_df(self, rows: list, schema=None):
        return self.spark.createDataFrame(rows, schema=schema)

    # ------------------------------------------------------------------
    # drop_nulls_in_critical_columns
    # ------------------------------------------------------------------
    def test_drop_nulls_removes_bad_rows(self):
        data = [
            ("tx-1", "2026-05-16T10:00:00Z", "store-001", "P001", 99.9),
            (None,   "2026-05-16T10:01:00Z", "store-002", "P002", 49.9),  # bad
        ]
        schema = ["transaction_id", "timestamp", "store_id", "product_id", "total_amount"]
        df  = self._make_df(data, schema=schema)
        out = drop_nulls_in_critical_columns(df)
        self.assertEqual(out.count(), 1)

    # ------------------------------------------------------------------
    # remove_duplicates
    # ------------------------------------------------------------------
    def test_remove_duplicates_keeps_one(self):
        data = [
            ("tx-1", "store-001", 99.9),
            ("tx-1", "store-001", 99.9),   # duplicate
            ("tx-2", "store-002", 49.9),
        ]
        schema = ["transaction_id", "store_id", "total_amount"]
        df  = self._make_df(data, schema=schema)
        out = remove_duplicates(df, key_col="transaction_id")
        self.assertEqual(out.count(), 2)

    # ------------------------------------------------------------------
    # filter_valid_amounts
    # ------------------------------------------------------------------
    def test_filter_valid_amounts_removes_zero_and_negative(self):
        data = [
            ("tx-1", 99.9),
            ("tx-2",  0.0),   # invalid
            ("tx-3", -5.0),   # invalid
            ("tx-4", 10.0),
        ]
        schema = ["transaction_id", "total_amount"]
        df  = self._make_df(data, schema=schema)
        out = filter_valid_amounts(df)
        self.assertEqual(out.count(), 2)

    # ------------------------------------------------------------------
    # add_date_partitions
    # ------------------------------------------------------------------
    def test_add_date_partitions_creates_columns(self):
        data = [("tx-1", "2026-05-16T14:30:00")]
        schema = ["transaction_id", "timestamp"]
        df = self._make_df(data, schema=schema)
        df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
        out = add_date_partitions(df, ts_col="timestamp")
        cols = out.columns
        self.assertIn("year",  cols)
        self.assertIn("month", cols)
        self.assertIn("day",   cols)

        row = out.first()
        self.assertEqual(row["year"],  "2026")
        self.assertEqual(row["month"], "05")
        self.assertEqual(row["day"],   "16")


if __name__ == "__main__":
    unittest.main()
