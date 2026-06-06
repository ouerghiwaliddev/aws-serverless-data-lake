"""
test_producer.py
----------------
Unit tests for the Kinesis Firehose transaction producer.
Run with: pytest tests/test_producer.py -v
"""

import json
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, "src/producer")
from producer import generate_transaction, send_batch, PRODUCTS, STORES


class TestGenerateTransaction(unittest.TestCase):

    def setUp(self):
        self.tx = generate_transaction()

    def test_required_fields_present(self):
        required = [
            "transaction_id", "timestamp", "store_id", "region",
            "product_id", "product_name", "category", "quantity",
            "unit_price", "discount_pct", "total_amount",
            "payment_method", "customer_id", "is_return",
        ]
        for field in required:
            self.assertIn(field, self.tx, f"Missing field: {field}")

    def test_store_id_valid(self):
        self.assertIn(self.tx["store_id"], STORES)

    def test_quantity_positive(self):
        self.assertGreater(self.tx["quantity"], 0)

    def test_total_amount_positive(self):
        self.assertGreater(self.tx["total_amount"], 0)

    def test_discount_in_range(self):
        self.assertGreaterEqual(self.tx["discount_pct"], 0)
        self.assertLessEqual(self.tx["discount_pct"], 0.20)

    def test_is_return_is_bool(self):
        self.assertIsInstance(self.tx["is_return"], bool)

    def test_total_amount_matches_unit_price_and_quantity(self):
        expected = round(self.tx["unit_price"] * self.tx["quantity"], 2)
        self.assertAlmostEqual(self.tx["total_amount"], expected, places=1)


class TestSendBatch(unittest.TestCase):

    @patch("producer.boto3.client")
    def test_send_batch_success(self, mock_boto):
        mock_client = MagicMock()
        mock_client.put_record_batch.return_value = {"FailedPutCount": 0}

        records = [generate_transaction() for _ in range(10)]
        sent    = send_batch(mock_client, "test-stream", records)

        self.assertEqual(sent, 10)
        mock_client.put_record_batch.assert_called_once()

    @patch("producer.boto3.client")
    def test_send_batch_partial_failure(self, mock_boto):
        mock_client = MagicMock()
        mock_client.put_record_batch.return_value = {"FailedPutCount": 3}

        records = [generate_transaction() for _ in range(10)]
        sent    = send_batch(mock_client, "test-stream", records)

        self.assertEqual(sent, 7)   # 10 - 3 failed

    def test_record_serializable_to_json(self):
        """Firehose requires JSON-serializable records."""
        tx = generate_transaction()
        try:
            json.dumps(tx)
        except (TypeError, ValueError) as e:
            self.fail(f"Transaction not JSON serializable: {e}")


if __name__ == "__main__":
    unittest.main()
