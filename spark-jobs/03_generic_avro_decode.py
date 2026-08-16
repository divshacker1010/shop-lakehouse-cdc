import io
import json
import struct

import fastavro
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

spark = SparkSession.builder.appName("generic-avro-decode").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Cache of schema_id -> parsed fastavro schema. Lives in each executor
# process's memory. First message with a given schema ID pays the HTTP
# round-trip to Schema Registry; every subsequent message with that same
# ID (the overwhelming majority, in practice) is a free in-memory hit.
# NOTE: in a real multi-executor cluster, each executor has its OWN
# separate cache (not shared cluster-wide) -- expected, not a bug, just
# means the "first hit per schema ID" cost is paid once per executor.
_schema_cache = {}


def get_schema(schema_id: int):
    if schema_id not in _schema_cache:
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}")
        resp.raise_for_status()
        schema_str = resp.json()["schema"]
        _schema_cache[schema_id] = fastavro.parse_schema(json.loads(schema_str))
    return _schema_cache[schema_id]


def decode_avro(raw_bytes):
    if raw_bytes is None:
        return None
    # Confluent wire format: byte 0 = magic byte, bytes 1-4 = schema id (big-endian)
    schema_id = struct.unpack(">I", raw_bytes[1:5])[0]
    schema = get_schema(schema_id)
    payload = io.BytesIO(raw_bytes[5:])
    record = fastavro.schemaless_reader(payload, schema)
    # default=str handles non-JSON-native types Avro can produce (e.g. bytes)
    return json.dumps(record, default=str)


decode_avro_udf = udf(decode_avro, StringType())

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribePattern", "shop\\.public\\..*")
    .option("startingOffsets", "earliest")
    .load()
)

decoded = raw.select(
    col("topic"),
    col("partition"),
    col("offset"),
    decode_avro_udf(col("value")).alias("json_value"),
)

query = (
    decoded.writeStream.format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/spark-checkpoints/generic-avro-decode")
    .start()
)

query.awaitTermination()