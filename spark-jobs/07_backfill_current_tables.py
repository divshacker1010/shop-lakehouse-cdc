from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

LAKEHOUSE_BUCKET = "shop-lakehouse"

CLUSTER_COLUMNS = {
    "customers_current": "id",
    "orders_current": "customer_id",
    "order_items_current": "order_id",
}


def schema_to_ddl(spark_schema) -> str:
    return ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in spark_schema.fields)


spark = SparkSession.builder.appName("backfill-current-tables").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def backfill_table(db_name: str, table_name: str):
    raw_path = f"s3a://{LAKEHOUSE_BUCKET}/{table_name}/"
    current_path = f"s3a://{LAKEHOUSE_BUCKET}/{table_name}_current/"
    current_table_name = f"{db_name}.{table_name}_current"

    print(f"backfilling {current_table_name} from {raw_path} ...")
    raw = spark.read.format("delta").load(raw_path)

    current_target_cols = [
        c for c in raw.columns
        if c not in ("topic", "partition", "offset", "op", "snapshot_marker")
    ]

    window = Window.partitionBy("id").orderBy(col("source_lsn").desc())
    deduped = (
        raw.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    if not DeltaTable.isDeltaTable(spark, current_path):
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
        print(f"  bootstrapped {current_table_name} fresh (clustered by {cluster_col})")
        return

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
    print(f"  merged historical data into {current_table_name}")


# Discover tables dynamically from the metastore -- any table NOT already
# ending in "_current" is treated as a raw table needing a current-state
# counterpart, in whichever databases actually exist.
for db in spark.catalog.listDatabases():
    if db.name == "default":
        continue
    for tbl in spark.catalog.listTables(db.name):
        if not tbl.name.endswith("_current"):
            backfill_table(db.name, tbl.name)

print("backfill complete.")