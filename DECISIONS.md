# Architecture Decisions & Tradeoffs

This document records the key design decisions made during the project,
the alternatives considered, and the reasoning behind each choice.

---

## 1. Kinesis Data Firehose vs. Kinesis Data Streams

| Factor       | **Firehose (chosen)**          | Data Streams                     |
|--------------|-------------------------------|----------------------------------|
| Management   | Fully managed, zero consumers  | Must write consumer (Lambda/app) |
| Latency      | 60s buffer (acceptable)        | Sub-second                       |
| Cost         | $0.03/GB                       | $0.015/shard/hr + $0.08/M GET    |
| S3 delivery  | Built-in                       | Manual Lambda required           |

**Decision:** Firehose. Our use case is analytics (not real-time alerting), so 60-second
buffering is acceptable. Firehose delivers directly to S3 with built-in compression
and retry logic — no extra Lambda consumer needed.

---

## 2. Medallion Architecture (Bronze / Silver / Gold) vs. Single-Zone

**Why three zones?**

- **Bronze (raw):** Immutable audit trail. If a bug corrupts Silver, we can re-run ETL from Bronze.
- **Silver (curated):** Deduplicated, typed, partitioned — safe for data scientists.
- **Gold (KPIs):** Pre-aggregated for BI tools. QuickSight queries Gold, not Silver → 100x faster.

**Alternative considered:** Single S3 bucket with raw + processed folders.
**Rejected because:** No isolation between raw and processed data. One bad ETL job
could overwrite raw data permanently.

---

## 3. AWS Glue vs. EMR for ETL

| Factor       | **Glue (chosen)**               | EMR                             |
|--------------|--------------------------------|---------------------------------|
| Setup        | Serverless, no cluster spin-up  | Cluster management required     |
| Cost         | Pay per DPU-hour               | Pay per instance-hour (idle too)|
| Scale        | Auto-scales                    | Manual scaling                  |
| Integration  | Native Data Catalog integration | Requires extra config           |

**Decision:** Glue. For batch ETL running once per day on moderate data (100 GB/day),
Glue is significantly cheaper and requires no cluster lifecycle management.
EMR would be justified at petabyte scale or for streaming Spark jobs.

---

## 4. Amazon Athena vs. Redshift for Analytics

| Factor       | **Athena (chosen)**             | Redshift                        |
|--------------|--------------------------------|---------------------------------|
| Infrastructure | Serverless                   | Cluster or Serverless           |
| Cost         | $5/TB scanned                  | $0.25/hr (dc2.large) minimum    |
| Maintenance  | None                           | Vacuum, analyze, cluster resize |
| Use case     | Ad-hoc queries on S3           | High-concurrency BI workloads   |

**Decision:** Athena. Our workload is ad-hoc analytics on S3 data, not high-concurrency
dashboard queries. With proper partitioning, Athena scans only a fraction of the data
(< 1 GB per query vs. 100 GB total), keeping costs under $1/month.

---

## 5. Lake Formation vs. Raw IAM for Access Control

**Why Lake Formation?**

- Enables **column-level security** (e.g., hide `customer_id` from analysts).
- Enables **row-level security** (e.g., regional managers see only their region's data).
- Centralizes permissions in one place instead of scattered S3 bucket policies.

**Alternative:** S3 bucket policies + IAM roles only.
**Rejected because:** S3 policies cannot filter at column or row level. Every IAM role
would see the full dataset.

---

## 6. Partitioning Strategy: year / month / day

**Why this granularity?**

- Most Athena queries filter by date range (daily, weekly, monthly reports).
- `year=2026/month=05/day=16` maps directly to Kinesis Firehose's dynamic prefixes.
- Finer granularity (by hour) would create too many small files.

**Parquet columnar format benefits:**
- 70% smaller than JSON (compression + encoding).
- Predicate pushdown: Athena skips columns not in SELECT.
- Compatible with QuickSight SPICE import.

---

## 7. EventBridge Scheduler vs. Glue Triggers for Orchestration

**Decision:** EventBridge Scheduler.

- More flexible cron expressions.
- Can trigger any AWS service (not just Glue).
- Decoupled from Glue — if Glue changes, the schedule stays.
- Glue Triggers only work within Glue workflows; EventBridge works cross-service.

---

## 8. S3 Intelligent-Tiering for Bronze

Bronze data is accessed frequently in the first 30 days (re-runs, debugging),
then rarely after that. S3 Intelligent-Tiering automatically moves objects to
cheaper tiers (Infrequent Access → Archive) based on access patterns —
without requiring manual lifecycle rule tuning.

**Cost saving:** ~60% reduction on Bronze storage after 90 days.
