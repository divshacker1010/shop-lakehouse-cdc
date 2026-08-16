from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("kafka-connectivity-check").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribePattern", "shop\\.public\\..*")
    .option("startingOffsets", "earliest")
    .load()
)

# key/value are raw bytes at this point -- no Avro decoding yet.
# Just proving messages are actually arriving, and showing which
# partition/offset each one came from.
preview = raw.selectExpr(
    "CAST(key AS STRING) as key_str",
    "length(value) as value_byte_len",
    "topic",
    "partition",
    "offset",
)

query = (
    preview.writeStream.format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/spark-checkpoints/connectivity-check")
    .start()
)

query.awaitTermination()