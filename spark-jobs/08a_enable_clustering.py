from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("enable-clustering").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Natural clustering key per table. Only tables actually intended for
# clustering need an entry here -- see the note in the project docs
# about which tables (raw event log vs. current-state) are included
# and why.
CLUSTER_COLUMNS = {
    "customers_current": "id",
    "orders_current": "customer_id",
    "order_items_current": "order_id",
}

for db in spark.catalog.listDatabases():
    if db.name == "default":
        continue
    for tbl in spark.catalog.listTables(db.name):
        cluster_col = CLUSTER_COLUMNS.get(tbl.name)
        if cluster_col is None:
            continue
        full_name = f"{db.name}.{tbl.name}"
        print(f"enabling clustering on {full_name} by ({cluster_col})")
        spark.sql(f"ALTER TABLE {full_name} CLUSTER BY ({cluster_col})")

print("clustering setup complete -- run this once, not on a schedule.")