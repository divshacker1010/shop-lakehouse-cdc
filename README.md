# Shop Lakehouse — CDC Streaming Pipeline

A fully-Dockerized, local sandbox for learning **change-data-capture (CDC) streaming pipelines** and the **lakehouse architecture**. It wires together Postgres, Debezium, Kafka, Schema Registry, Spark Structured Streaming, Delta Lake, MinIO, Hive Metastore, and Trino into one working end-to-end system, built up in deliberate, numbered steps so each concept can be inspected in isolation before the next one is layered on.

A synthetic "shop" workload (customers placing orders) continuously writes to Postgres. Every insert/update is captured via logical replication, streamed through Kafka as Avro, decoded and merged into Delta tables in an S3-compatible object store, and made queryable with plain SQL through Trino.

## Table of Contents

- [What You'll Learn](#what-youll-learn)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [The Spark Jobs, Explained](#the-spark-jobs-explained)
- [Querying with Trino](#querying-with-trino)
- [Data Model](#data-model)
- [Ports Reference](#ports-reference)
- [Credentials & Security Notes](#credentials--security-notes)
- [Troubleshooting](#troubleshooting)

## What You'll Learn

This project is a hands-on walkthrough of the concepts below — each one is exercised by a specific script or config file, referenced in parentheses:

- **Change Data Capture (CDC)** from a relational database using logical replication (`postgres` WAL + Debezium's `pgoutput` plugin, [connectors/postgres-source.json](connectors/postgres-source.json)).
- **Schema-driven event streaming**: CDC events are serialized as Avro and registered in Confluent Schema Registry, rather than shipped as raw/untyped JSON.
- **Decoding the Confluent Avro wire format** by hand in Spark — both the built-in (`from_avro`) path and a manual `fastavro` + schema-registry-lookup path ([spark-jobs/02](spark-jobs/02_avro_decode_orders.py), [spark-jobs/03](spark-jobs/03_generic_avro_decode.py)).
- **Spark Structured Streaming** with `foreachBatch`, dynamic topic discovery (`subscribePattern`), and per-batch schema refresh on decode failure ([spark-jobs/05](spark-jobs/05_native_avro_per_topic.py), [spark-jobs/06](spark-jobs/06_delta_write.py)).
- **Lakehouse table design**: an append-only raw event log per table, plus a deduplicated "current state" table built with a Delta `MERGE` keyed on Postgres LSN ordering ([spark-jobs/06](spark-jobs/06_delta_write.py), [spark-jobs/07](spark-jobs/07_backfill_current_tables.py)).
- **Delta Lake liquid clustering** (`CLUSTER BY`), declared at table-creation time rather than retrofitted, plus `OPTIMIZE` for file compaction ([spark-jobs/08](spark-jobs/08_optimize_tables.py), [spark-jobs/08a](spark-jobs/08a_enable_clustering.py)).
- **Lakehouse federation**: MinIO as S3-compatible object storage, Hive Metastore as the shared catalog, and Trino as a SQL query engine sitting on top of the same Delta tables Spark wrote — all without copying data.

## Architecture

```
                         ┌─────────────────┐
  workload-generator ───▶│   Postgres 16    │  (logical replication, wal_level=logical)
  (synthetic inserts/    │   db: shop       │
   updates every 3-8s)   └────────┬─────────┘
                                  │  Debezium (pgoutput, publication + replication slot)
                                  ▼
                         ┌─────────────────┐        ┌──────────────────┐
                         │  Kafka Connect   │───────▶│  Schema Registry  │
                         │  (Debezium PG    │  Avro  │  (schema per      │
                         │   connector)     │ schema │   topic)          │
                         └────────┬─────────┘        └──────────────────┘
                                  │  produces to
                                  ▼
                    topics: shop.public.customers
                            shop.public.orders
                            shop.public.order_items
                                  │
                                  ▼  Spark Structured Streaming (foreachBatch, 30s trigger)
                         ┌─────────────────┐
                         │  Spark 3.5.1     │  decode Avro → write raw event log (append)
                         │                  │              → MERGE into *_current table
                         └────────┬─────────┘
                                  │  writes Delta tables to
                                  ▼
                         ┌─────────────────┐        ┌──────────────────┐
                         │  MinIO (S3 API)  │◀──────▶│  Hive Metastore   │
                         │  bucket:         │ catalog│  (Postgres-backed)│
                         │  shop-lakehouse  │        └──────────────────┘
                         └────────┬─────────┘                 ▲
                                  │                            │ catalog lookups
                                  ▼                            │
                         ┌─────────────────────────────────────┘
                         │        Trino (delta_lake connector)
                         │        SQL queries over the same Delta files
                         └─────────────────────────────────────
```

`kafka-ui` sits alongside this stack purely as an observability window — inspect topics, messages, consumer groups, and registered connectors without any CLI.

## Tech Stack

| Service | Image | Role |
|---|---|---|
| `postgres` | `postgres:16` | Source-of-truth OLTP database (`shop` db), logical replication enabled |
| `hive-metastore-db` | `postgres:16` | Backing store for Hive Metastore's own catalog metadata |
| `kafka` | `apache/kafka:3.8.0` | Event backbone (KRaft mode, no ZooKeeper) |
| `schema-registry` | `confluentinc/cp-schema-registry:7.6.1` | Avro schema storage/versioning for Kafka topics |
| `kafka-connect` | built from [kafka-connect/Dockerfile](kafka-connect/Dockerfile) (`cp-kafka-connect:7.6.1` + Debezium Postgres connector + Avro converter) | Runs the CDC connector |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui:v1.1.0` | Web UI for Kafka topics/connect/schema registry |
| `workload-generator` | built from [workload-generator/Dockerfile](workload-generator/Dockerfile) | Python script generating a steady trickle of fake customers/orders |
| `spark` | built from [spark/Dockerfile](spark/Dockerfile) (`apache/spark:3.5.1`) | Runs the streaming job continuously (default command: `06_delta_write.py`) |
| `spark-batch` / `spark-ondemand` | same image, `profiles: ["batch"]` | For one-off/backfill jobs run via `docker compose run` |
| `minio` | `cgr.dev/chainguard/minio:latest`, built from [minio/Dockerfile](minio/Dockerfile) (source build — see Dockerfile comments) | S3-compatible object storage for the lakehouse |
| `hive-metastore` | built from [hive-metastore/Dockerfile](hive-metastore/Dockerfile) (`apache/hive:3.1.3`) | Shared table catalog for Spark and Trino |
| `trino` | `trinodb/trino:latest` | Distributed SQL query engine over the Delta tables |

## Repository Structure

```
.
├── docker-compose.yml           # Orchestrates every service listed above
├── connectors/
│   └── postgres-source.json     # Debezium connector config (register via Kafka Connect REST API)
├── workload-generator/
│   ├── generator.py             # Generates fake customers/orders/order_items continuously
│   ├── requirements.txt
│   └── Dockerfile
├── spark/
│   └── Dockerfile               # Spark image + Python deps (requests, fastavro)
├── spark-jobs/                  # Mounted into the spark container at /opt/spark-jobs
│   ├── 01_connectivity_check.py     # Step 1: prove raw bytes are arriving from Kafka
│   ├── 02_avro_decode_orders.py     # Step 2: decode one known topic with from_avro
│   ├── 03_generic_avro_decode.py    # Step 3: generic decode via fastavro + schema-id lookup
│   ├── 05_native_avro_per_topic.py  # Step 4: per-topic decode inside foreachBatch, no hardcoded topic list
│   ├── 06_delta_write.py            # Step 5: write raw + MERGE into current-state Delta tables (default job)
│   ├── 07_backfill_current_tables.py# One-off: (re)build *_current tables from the raw event log
│   ├── 08_optimize_tables.py        # Runs OPTIMIZE (applies declared liquid clustering)
│   └── 08a_enable_clustering.py     # One-off: ALTER TABLE ... CLUSTER BY on existing tables
├── hive-metastore/
│   └── Dockerfile               # Adds the Postgres JDBC driver to the standalone metastore image
├── minio/
│   └── Dockerfile               # Source-built MinIO (see file for why)
├── trino/
│   └── catalog/
│       └── delta.properties     # Trino's delta_lake catalog config (metastore + S3 endpoint)
└── kafka-connect/
    └── Dockerfile               # Adds Debezium Postgres connector + Avro converter plugins
```

> `kafka-data-backup/` (raw Kafka broker log segments) and `spark-jobs/__pycache__/` exist locally but are excluded via `.gitignore` — see [Credentials & Security Notes](#credentials--security-notes).

## Prerequisites

- Docker and Docker Compose (v2 CLI syntax, `docker compose ...`)
- ~4 GB of free RAM for the container set (Hive Metastore + Spark are the heaviest)
- `curl`, for registering the Kafka Connect connector
- A Postgres client (`psql`, TablePlus, etc.) if you want to inspect/seed the source database directly — optional, the workload generator handles inserts on its own

## Quick Start

### 1. Start the core services

```bash
docker compose up -d postgres hive-metastore-db kafka schema-registry kafka-connect kafka-ui minio hive-metastore trino
```

Give it a minute — `hive-metastore`'s healthcheck has a 180s `start_period` because schema initialization is slow under emulation (e.g. running the amd64 image on an Apple Silicon host). Check status with:

```bash
docker compose ps
```

### 2. Create the source schema

No init SQL is checked into the repo — connect to Postgres and create the three tables the pipeline expects (matches the columns [workload-generator/generator.py](workload-generator/generator.py) writes to and [connectors/postgres-source.json](connectors/postgres-source.json) captures):

```sql
-- psql -h localhost -p 5433 -U postgres -d shop
CREATE TABLE customers (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL
);

CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    status      TEXT NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id           SERIAL PRIMARY KEY,
    order_id     INTEGER NOT NULL REFERENCES orders(id),
    product_name TEXT NOT NULL,
    quantity     INTEGER NOT NULL,
    unit_price   NUMERIC(10,2) NOT NULL
);
```

### 3. Register the Debezium connector

Kafka Connect doesn't auto-load connector configs — POST it to the REST API once the `kafka-connect` container is healthy:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/postgres-source.json \
  http://localhost:8083/connectors
```

Verify it's running:

```bash
curl -s http://localhost:8083/connectors/shop-postgres-source/status | jq
```

### 4. Start generating traffic

```bash
docker compose up -d workload-generator
```

This inserts/updates customers, orders, and order_items roughly every 3-8 seconds — enough to watch individual events flow through Kafka UI without a firehose.

### 5. Start the streaming job

```bash
docker compose up -d spark
```

This runs [spark-jobs/06_delta_write.py](spark-jobs/06_delta_write.py) continuously (30s micro-batch trigger), writing to `s3a://shop-lakehouse/` in MinIO and registering tables in Hive Metastore. Watch its progress:

```bash
docker compose logs -f spark
```

Spark's own UI is at [http://localhost:4040](http://localhost:4040) while a job is running.

### 6. Explore

- **Kafka UI** — [http://localhost:8080](http://localhost:8080) — browse topics, messages, the registered connector, and consumer groups.
- **Schema Registry** — `curl http://localhost:8081/subjects` — see registered Avro schemas.
- **MinIO Console** — [http://localhost:9091](http://localhost:9091) (login `minioadmin` / `minioadmin123`) — browse the `shop-lakehouse` bucket's Delta files directly.
- **Trino** — see [Querying with Trino](#querying-with-trino) below.

## The Spark Jobs, Explained

The jobs in `spark-jobs/` are numbered as a learning progression, not a pipeline you run all at once — `06_delta_write.py` is the only one that runs by default (as the `spark` service). The rest are meant to be run ad hoc (see [Running Ad-Hoc Jobs](#running-ad-hoc-jobs)) to see one concept at a time before they're combined:

| Job | Purpose |
|---|---|
| `01_connectivity_check.py` | Confirms raw bytes are arriving from Kafka topics matching `shop\.public\..*` — no decoding, just proves the plumbing works. |
| `02_avro_decode_orders.py` | Decodes a single hardcoded topic (`shop.public.orders`) using Spark's built-in `from_avro`, fetching the schema from Schema Registry once at startup. |
| `03_generic_avro_decode.py` | Decodes *any* topic generically using `fastavro`, reading the schema ID out of each message's Confluent wire-format header and caching schemas per executor. |
| `05_native_avro_per_topic.py` | Combines both ideas: dynamically discovers which topics appear in each micro-batch and decodes each with `from_avro` and its own fetched schema — no hardcoded topic list. |
| `06_delta_write.py` | The real pipeline. For each topic in a batch: decodes Avro, appends to a raw Delta event-log table, and `MERGE`s a deduplicated view into a `*_current` table (using Postgres LSN to resolve ordering, deletes on `op = 'd'`). Bootstraps new `*_current` tables with liquid clustering declared at creation time. |
| `07_backfill_current_tables.py` | Rebuilds all `*_current` tables from scratch by reading the full raw event log — useful after schema changes or to backfill history the streaming job missed. Discovers tables dynamically from the Hive Metastore rather than a hardcoded list. |
| `08_optimize_tables.py` | Runs `OPTIMIZE` on every `*_current` table, which applies the liquid clustering declared at creation — no need to restate clustering columns. |
| `08a_enable_clustering.py` | Retrofits `CLUSTER BY` onto existing tables via `ALTER TABLE` — kept as a one-off/reference, since the in-code comments note this path proved less reliable than declaring clustering at `CREATE TABLE` time. |
| `09_test.py` | Scratch/debug script for inspecting clustering state via `DESCRIBE DETAIL` — not part of the pipeline. |

## Running Ad-Hoc Jobs

The `spark-batch` and `spark-ondemand` services (both under the `batch` Compose profile) exist for one-off jobs so you don't have to stop the streaming job in `spark`:

```bash
# Run the backfill job (spark-batch's default command)
docker compose --profile batch up spark-batch

# Run any other job on demand, e.g. the connectivity check
docker compose run --rm spark-ondemand /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1 \
  /opt/spark-jobs/01_connectivity_check.py
```

For jobs touching Delta tables (writing or reading), include the Delta + S3 packages and Hive Metastore conf, mirroring the `spark` service's command in [docker-compose.yml](docker-compose.yml).

## Querying with Trino

```bash
docker exec -it shop-trino trino --catalog delta
```

```sql
SHOW SCHEMAS;
SHOW TABLES FROM shop;

SELECT * FROM shop.customers_current LIMIT 10;
SELECT status, count(*) FROM shop.orders_current GROUP BY status;

-- Raw event log vs. current-state table
SELECT op, count(*) FROM shop.orders GROUP BY op;
```

## Data Model

Three CDC-tracked tables flow through the pipeline, each producing **two** Delta tables:

- `<table>` — append-only raw event log: every insert/update/delete Debezium captured, plus `topic`/`partition`/`offset`/`op`/`source_lsn`/`snapshot_marker` audit columns.
- `<table>_current` — deduplicated current state (one row per entity, deletes applied), clustered by its natural filter key:

| Table | Clustered by |
|---|---|
| `customers_current` | `id` |
| `orders_current` | `customer_id` |
| `order_items_current` | `order_id` |

## Ports Reference

| Port (host) | Service | Notes |
|---|---|---|
| 5433 | Postgres | mapped from container's 5432 |
| 9092 | Kafka | external listener |
| 8081 | Schema Registry | |
| 8083 | Kafka Connect | REST API for registering/inspecting connectors |
| 8080 | Kafka UI | |
| 4040 | Spark UI | only while the `spark` service is actively running a job |
| 9090 | MinIO S3 API | mapped from container's 9000 |
| 9091 | MinIO Console | web UI |
| 9083 | Hive Metastore | Thrift |
| 8082 | Trino | mapped from container's 8080 |

## Credentials & Security Notes

Everything below is a **local-only placeholder credential**, scoped to an isolated Docker network (`shop-network`) that isn't exposed beyond your machine. They exist so the project runs immediately after `docker compose up` with no setup step for secrets management — appropriate for a local learning sandbox, **not** appropriate to reuse anywhere real.

| Credential | Value | Where it's used |
|---|---|---|
| Postgres (`shop` db) | `postgres` / `postgres` | [docker-compose.yml](docker-compose.yml), [connectors/postgres-source.json](connectors/postgres-source.json) |
| Hive Metastore DB | `hive` / `hivepassword` | [docker-compose.yml](docker-compose.yml) |
| MinIO root user | `minioadmin` / `minioadmin123` | [docker-compose.yml](docker-compose.yml), [trino/catalog/delta.properties](trino/catalog/delta.properties) |

If you fork this to point at anything beyond your own laptop, replace all three with real secrets pulled from environment variables or a secrets manager — never commit real credentials in their place.

Also excluded from version control (see [.gitignore](.gitignore)):

- **`kafka-data-backup/`** — raw Kafka broker log segments (~18MB). This is regenerated runtime state, not source code; it's excluded to keep the repo small and reviewable, not because it contains secrets (the data in it is synthetic, generated by `workload-generator`).
- **`spark-jobs/__pycache__/`** — compiled Python bytecode.

No API keys, tokens, or private key files exist anywhere in this repository.

## Troubleshooting

- **`hive-metastore` healthcheck taking minutes** — expected on the first run, especially under amd64-on-arm64 emulation; the healthcheck allows up to 180s before the first check even runs.
- **Hive Metastore fails with "already exists" on restart** — the compose file sets `IS_RESUME: "true"` so the entrypoint doesn't rerun `-initSchema` against an already-initialized volume. Only unset this if you've also wiped the `hive-metastore-db-data` volume.
- **Spark can't reach the metastore / S3** — confirm `hive-metastore` and `minio` are both healthy (`docker compose ps`) before starting `spark`; it depends on `kafka` and `hive-metastore` being healthy but not on `minio` explicitly, so a slow MinIO start can surface as an obscure S3A error.
- **Connector registration returns a 409/500** — the connector may already be registered from a previous run; check `curl http://localhost:8083/connectors` first, or `DELETE http://localhost:8083/connectors/shop-postgres-source` before re-POSTing.
- **`network default` naming** — the compose file pins the network name to `shop-network` explicitly, because Compose's auto-generated default (`<project>_default`) contains an underscore that `java.net.URI` rejects when Hive canonicalizes the metastore hostname.
