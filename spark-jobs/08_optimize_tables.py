from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("optimize-tables").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Liquid clustering is enabled (declared at CREATE TABLE time, confirmed
# via DESCRIBE DETAIL). A bare OPTIMIZE automatically applies the
# already-declared clustering columns -- no need to restate them here,
# unlike the Z-order fallback this replaced.
for db in spark.catalog.listDatabases():
    if db.name == "default":
        continue
    for tbl in spark.catalog.listTables(db.name):
        if not tbl.name.endswith("_current"):
            continue
        full_name = f"{db.name}.{tbl.name}"
        print(f"running OPTIMIZE on {full_name} ...")
        spark.sql(f"OPTIMIZE {full_name}")
        print(f"  done: {full_name}")

print("optimize run complete.")


print("optimize run complete.")
