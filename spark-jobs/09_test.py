from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("clustering_test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# table_name = "shop.orders_current"

# result = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
# print(f"clusteringColumns = {result['clusteringColumns']}")


for table in ["customers_current", "orders_current", "order_items_current"]:
    row = spark.sql(f"DESCRIBE DETAIL shop.{table}").first()
    print(row)
# for table in ["customers_current", "orders_current", "order_items_current"]:
#     spark.sql(f"ALTER TABLE shop.{table} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")