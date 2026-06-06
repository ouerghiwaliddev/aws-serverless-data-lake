"""
producer.py
-----------
Simulates retail transaction events and sends them to Kinesis Data Firehose.
Usage:
    python producer.py --kinesis-stream <delivery-stream-name> \
                       --region <aws-region> \
                       --records-per-second <int>
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STORES = [f"store-{i:03d}" for i in range(1, 51)]          # 50 stores
PRODUCTS = [
    {"id": "P001", "name": "Laptop Pro 15",       "category": "Electronics", "price": 1299.99},
    {"id": "P002", "name": "Wireless Headphones",  "category": "Electronics", "price":  149.99},
    {"id": "P003", "name": "Running Shoes",         "category": "Footwear",    "price":   89.99},
    {"id": "P004", "name": "Coffee Maker Deluxe",  "category": "Appliances",  "price":   79.99},
    {"id": "P005", "name": "Yoga Mat Premium",      "category": "Sports",      "price":   34.99},
    {"id": "P006", "name": "Desk Lamp LED",         "category": "Furniture",   "price":   24.99},
    {"id": "P007", "name": "Water Bottle 1L",       "category": "Sports",      "price":   19.99},
    {"id": "P008", "name": "Notebook Set",          "category": "Stationery",  "price":    9.99},
    {"id": "P009", "name": "Bluetooth Speaker",     "category": "Electronics", "price":   59.99},
    {"id": "P010", "name": "Winter Jacket",         "category": "Clothing",    "price":  199.99},
]
PAYMENT_METHODS = ["credit_card", "debit_card", "cash", "mobile_pay"]
REGIONS = {
    "store-001": "North", "store-002": "North", "store-003": "South",
    "store-004": "East",  "store-005": "West",  "store-006": "North",
}


def generate_transaction() -> dict:
    """Generate a single realistic retail transaction."""
    store_id  = random.choice(STORES)
    product   = random.choice(PRODUCTS)
    quantity  = random.randint(1, 5)
    discount  = round(random.uniform(0, 0.20), 2)          # 0-20% discount
    unit_price = product["price"] * (1 - discount)
    total      = round(unit_price * quantity, 2)

    return {
        "transaction_id":  str(uuid.uuid4()),
        "timestamp":       datetime.utcnow().isoformat() + "Z",
        "store_id":        store_id,
        "region":          REGIONS.get(store_id, "Central"),
        "product_id":      product["id"],
        "product_name":    product["name"],
        "category":        product["category"],
        "quantity":        quantity,
        "unit_price":      round(unit_price, 2),
        "discount_pct":    discount,
        "total_amount":    total,
        "payment_method":  random.choice(PAYMENT_METHODS),
        "customer_id":     f"cust-{random.randint(1000, 9999)}",
        "is_return":       random.random() < 0.02,          # 2% return rate
    }


def send_batch(client, stream_name: str, records: list) -> int:
    """Send up to 500 records to Firehose in one PutRecordBatch call."""
    firehose_records = [
        {"Data": (json.dumps(r) + "\n").encode("utf-8")}
        for r in records
    ]
    try:
        response = client.put_record_batch(
            DeliveryStreamName=stream_name,
            Records=firehose_records,
        )
        failed = response.get("FailedPutCount", 0)
        if failed:
            print(f"  ⚠️  {failed} records failed to deliver")
        return len(records) - failed
    except ClientError as exc:
        print(f"  ❌ Firehose error: {exc}")
        return 0


def main():
    parser = argparse.ArgumentParser(description="Retail transaction producer for Kinesis Firehose")
    parser.add_argument("--kinesis-stream",      required=True, help="Firehose delivery stream name")
    parser.add_argument("--region",              default="us-east-1")
    parser.add_argument("--records-per-second",  type=int, default=10)
    parser.add_argument("--duration-seconds",    type=int, default=300,
                        help="How long to run (0 = forever)")
    args = parser.parse_args()

    client = boto3.client("firehose", region_name=args.region)
    batch_size = min(args.records_per_second, 500)
    sleep_time = 1.0                                        # send every second

    print(f"🚀 Starting producer → stream: {args.kinesis_stream}")
    print(f"   Rate : {args.records_per_second} records/s  |  Batch: {batch_size}")

    total_sent = 0
    start = time.time()

    try:
        while True:
            elapsed = time.time() - start
            if args.duration_seconds and elapsed >= args.duration_seconds:
                break

            batch = [generate_transaction() for _ in range(batch_size)]
            sent  = send_batch(client, args.kinesis_stream, batch)
            total_sent += sent
            print(f"  ✅ Sent {sent:>4} records  |  Total: {total_sent:>8,}  |  Elapsed: {elapsed:>6.1f}s")
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n⛔ Interrupted by user")

    print(f"\n📊 Done. Total records sent: {total_sent:,}")


if __name__ == "__main__":
    main()
