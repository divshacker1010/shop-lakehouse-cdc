import requests
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr

SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

spark = SparkSession.builder.appName("native-avro-per-topic").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


_schema_cache = {}


def get_latest_schema_str(topic: str) -> str:
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions/latest")
    resp.raise_for_status()
    return resp.json()["schema"]


def get_schema_cached(topic: str) -> str:
    if topic not in _schema_cache:
        _schema_cache[topic] = get_latest_schema_str(topic)
    return _schema_cache[topic]


raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribePattern", "shop\\.public\\..*")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "1000")  # cap per-batch pull, bound memory under load spikes
    .load()
)

# keep raw bytes as-is here -- no UDF, no JSON. Decoding happens per-topic
# inside foreachBatch, using each topic's freshly-fetched Avro schema.
kafka_meta = raw.select("topic", "partition", "offset", "value")


def process_batch(batch_df, batch_id):
    batch_df.persist()
    # Discover which topics actually showed up in THIS micro-batch, rather
    # than looping over a hardcoded list. This is what makes the pipeline
    # genuinely scale: add a new table to Debezium's table.include.list,
    # its topic starts matching subscribePattern, and it shows up here
    # automatically -- no code change needed.
    # Small driver-side collect() -- cheap, since it's just distinct topic
    # names (a handful of short strings), not row data.
    topics_in_batch = [row.topic for row in batch_df.select("topic").distinct().collect()]

    for topic_name in topics_in_batch:
        table_batch = batch_df.filter(col("topic") == topic_name)

        def decode_and_show(schema_str):
            avro_payload = expr("substring(value, 6, length(value) - 5)")
            decoded = table_batch.select(
                "topic", "partition", "offset", avro_payload.alias("avro_bytes")
            ).select(
                "topic",
                "partition",
                "offset",
                from_avro(col("avro_bytes"), schema_str).alias("data"),
            )
            flat = decoded.select(
                "topic",
                "partition",
                "offset",
                col("data.op").alias("op"),
                col("data.after.*"),
                col("data.source.lsn").alias("source_lsn"),
                col("data.source.snapshot").alias("snapshot_marker"),
            )
            print(f"--- batch {batch_id} : {topic_name} ---")
            flat.show(truncate=False)  # action -- actually triggers decoding here

        schema_str = get_schema_cached(topic_name)
        try:
            decode_and_show(schema_str)
        except Exception as e:
            # Decode failed -- likely the cached schema is stale (topic's
            # schema evolved since we last fetched it). Refresh and retry
            # once. If it fails again, let it raise -- a second consecutive
            # failure means something other than staleness.
            print(f"decode failed for {topic_name}, refreshing schema and retrying: {e}")
            _schema_cache[topic_name] = get_latest_schema_str(topic_name)
            decode_and_show(_schema_cache[topic_name])
    batch_df.unpersist()


query = (
    kafka_meta.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/spark-checkpoints/native-avro-per-topic")
    .start()
)

query.awaitTermination()