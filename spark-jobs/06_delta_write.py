import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, row_number

SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
LAKEHOUSE_BUCKET = "shop-lakehouse"

# Which column each _current table should be liquid-clustered by. This
# is genuine domain knowledge (which column is the natural filter key
# per table) -- not mechanically derivable from the data itself, same
# reasoning as why 08_optimize_tables.py needed an equivalent map.
CLUSTER_COLUMNS = {
    "customers_current": "id",
    "orders_current": "customer_id",
    "order_items_current": "order_id",
}


def schema_to_ddl(spark_schema) -> str:
    """Build a CREATE TABLE column list from an ALREADY-INFERRED Spark
    schema (derived from the real Avro schema via from_avro), rather
    than hand-writing DDL that could drift out of sync with reality."""
    return ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in spark_schema.fields)

spark = SparkSession.builder.appName("delta-write-per-topic").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"catalogImplementation = {spark.conf.get('spark.sql.catalogImplementation', 'NOT SET')}")
print(f"hive.metastore.uris (SQLConf) = {spark.conf.get('hive.metastore.uris', 'NOT SET')}")
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
print(f"hive.metastore.uris (HadoopConf) = {hadoop_conf.get('hive.metastore.uris', 'NOT SET')}")
print(f"hive.metastore.version = {spark.conf.get('spark.sql.hive.metastore.version', 'NOT SET')}")


_schema_cache = {}


def get_latest_schema_str(topic: str) -> str:
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions/latest")
    resp.raise_for_status()
    return resp.json()["schema"]


def get_schema_cached(topic: str) -> str:
    if topic not in _schema_cache:
        _schema_cache[topic] = get_latest_schema_str(topic)
    return _schema_cache[topic]


_known_databases = set()
_registered_tables = set()


def ensure_database(db_name: str):
    if db_name not in _known_databases:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        _known_databases.add(db_name)


raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribePattern", "shop\\.public\\..*")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "1000")
    .load()
)

kafka_meta = raw.select("topic", "partition", "offset", "value")


def process_batch(batch_df, batch_id):
    batch_df.persist()
    topics_in_batch = [row.topic for row in batch_df.select("topic").distinct().collect()]

    for topic_name in topics_in_batch:
        table_batch = batch_df.filter(
            (col("topic") == topic_name) & col("value").isNotNull()
        )

        def decode(schema_str):
            avro_payload = expr("substring(value, 6, length(value) - 5)")
            return table_batch.select(
                "topic", "partition", "offset", avro_payload.alias("avro_bytes")
            ).select(
                "topic",
                "partition",
                "offset",
                from_avro(col("avro_bytes"), schema_str).alias("data"),
            )

        schema_str = get_schema_cached(topic_name)
        try:
            decoded = decode(schema_str)
        except Exception as e:
            print(f"decode failed for {topic_name}, refreshing schema and retrying: {e}")
            _schema_cache[topic_name] = get_latest_schema_str(topic_name)
            decoded = decode(_schema_cache[topic_name])

        # Narrow peek, used ONLY to decide where to write -- not persisted.
        sample = decoded.select(
            col("data.source.db").alias("source_db"),
            col("data.source.table").alias("source_table"),
        ).first()
        if sample is None:
            continue
        db_name = sample["source_db"]
        table_name = sample["source_table"]
        ensure_database(db_name)

        # This is the actual persisted column set -- source_db/source_table
        # deliberately excluded, since they were only needed for the
        # naming decision above, not as data worth keeping in the table.
        flat = decoded.select(
            "topic",
            "partition",
            "offset",
            col("data.op").alias("op"),
            col("data.after.*"),
            col("data.source.lsn").alias("source_lsn"),
            col("data.source.snapshot").alias("snapshot_marker"),
        )

        delta_path = f"s3a://{LAKEHOUSE_BUCKET}/{table_name}/"

        (
            flat.write.format("delta")
            .option("txnAppId", f"ingest-{table_name}")
            .option("txnVersion", batch_id)
            .mode("append")
            .save(delta_path)
        )

        # Register as an EXTERNAL table (LOCATION explicit) -- DROP TABLE
        # later would only remove the catalog entry, never the actual
        # files. Only actually calls the metastore ONCE per table for
        # this job's lifetime -- was previously a Thrift round-trip on
        # every single batch, which is expensive when the metastore
        # itself is slow (e.g. running under emulation).
        full_table_name = f"{db_name}.{table_name}"
        if full_table_name not in _registered_tables:
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {full_table_name} "
                f"USING DELTA LOCATION '{delta_path}'"
            )
            _registered_tables.add(full_table_name)

        # Columns that persist in the CURRENT-STATE table: business fields
        # plus source_lsn (required -- future merges compare against it).
        # topic/partition/offset/op/snapshot_marker are raw-table-only
        # audit fields, not meaningful once we're representing "current
        # state" rather than an event history.
        current_target_cols = [
            c for c in flat.columns
            if c not in ("topic", "partition", "offset", "op", "snapshot_marker")
        ]

        # --- Current-state table: deduplicated, one row per real entity ---
        # Dedupe WITHIN this batch first -- MERGE requires at most one
        # source row per matched key, and a batch could contain e.g. an
        # insert and an update for the same id in the same 30s window.
        # 'op' stays in this DataFrame -- needed by the merge CONDITIONS
        # below -- even though it won't be part of what's actually stored.
        window = Window.partitionBy("id").orderBy(col("source_lsn").desc())
        deduped = (
            flat.withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        current_path = f"s3a://{LAKEHOUSE_BUCKET}/{table_name}_current/"
        current_table_name = f"{db_name}.{table_name}_current"

        if not DeltaTable.isDeltaTable(spark, current_path):
            # Bootstrap: nothing to merge into yet. Explicitly CREATE the
            # table WITH liquid clustering declared at creation -- this is
            # the code path documented as most reliable for clustering
            # (retrofitting via ALTER TABLE on an existing table proved
            # unreliable). DDL is generated from the real schema, not
            # hand-written, so it can't drift from what we actually decode.
            bootstrap_df = deduped.filter(col("op") != "d").select(*current_target_cols)
            cluster_col = CLUSTER_COLUMNS.get(f"{table_name}_current")
            if cluster_col is not None:
                ddl_cols = schema_to_ddl(bootstrap_df.schema)
                spark.sql(
                    f"CREATE TABLE {current_table_name} ({ddl_cols}) "
                    f"USING DELTA LOCATION '{current_path}' "
                    f"CLUSTER BY ({cluster_col})"
                )
                bootstrap_df.write.format("delta").mode("append").save(current_path)
            else:
                bootstrap_df.write.format("delta").save(current_path)
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {current_table_name} "
                    f"USING DELTA LOCATION '{current_path}'"
                )
            _registered_tables.add(current_table_name)
        else:
            target = DeltaTable.forPath(spark, current_path)
            (
                target.alias("t")
                .merge(deduped.alias("s"), "t.id = s.id")
                .whenMatchedDelete(condition="s.op = 'd'")
                .whenMatchedUpdate(
                    condition="s.source_lsn > t.source_lsn",
                    set={c: f"s.{c}" for c in current_target_cols},
                )
                .whenNotMatchedInsert(
                    condition="s.op != 'd'",
                    values={c: f"s.{c}" for c in current_target_cols},
                )
                .execute()
            )

        print(f"batch {batch_id}: wrote {table_name} to {delta_path}")

    batch_df.unpersist()


query = (
    kafka_meta.writeStream.foreachBatch(process_batch)
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/tmp/spark-checkpoints/delta-write-per-topic")
    .start()
)

query.awaitTermination()