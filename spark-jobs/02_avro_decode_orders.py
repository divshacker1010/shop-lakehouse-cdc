import requests
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr

SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC = "shop.public.orders"

spark = SparkSession.builder.appName("avro-decode-orders").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Fetch the current Avro value schema from Schema Registry ONCE at startup.
# NOTE: if the schema evolves later, this job needs a restart to see the new
# version -- a production job would instead look up the schema ID embedded
# in each individual message. More on that once this works.
resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{TOPIC}-value/versions/latest")
resp.raise_for_status()
value_schema_str = resp.json()["schema"]

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Confluent wire format: [magic byte: 1 byte][schema id: 4 bytes][avro payload]
# substring is 1-indexed in Spark SQL -- start at byte 6 to skip the first 5.
avro_payload = expr("substring(value, 6, length(value) - 5)")

decoded = raw.select(
    col("topic"),
    col("partition"),
    col("offset"),
    from_avro(avro_payload, value_schema_str).alias("data"),
)

# Print the ACTUAL inferred struct shape before we guess any field paths.
# This runs immediately at driver startup -- doesn't require the stream running.
decoded.printSchema()

# Confirmed via printSchema(): Spark's from_avro maps nullable unions
# directly to nullable fields -- no wrapper struct, unlike the JSON
# encoding we saw earlier by hand. So field paths are direct: data.after.id, etc.
flat = decoded.select(
    "topic", "partition", "offset",
    col("data.op").alias("op"),
    col("data.after.id").alias("id"),
    col("data.after.customer_id").alias("customer_id"),
    col("data.after.status").alias("status"),
    col("data.before.status").alias("previous_status"),
    col("data.source.lsn").alias("source_lsn"),
    col("data.source.snapshot").alias("snapshot_marker"),
)

query = (
    flat.writeStream.format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/spark-checkpoints/avro-decode-orders")
    .start()
)

query.awaitTermination()