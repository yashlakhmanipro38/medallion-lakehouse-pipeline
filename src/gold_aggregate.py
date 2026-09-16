"""
Gold layer aggregation.

Builds business-facing, dimensionally-modeled marts on top of silver:
a daily revenue-by-region fact table used directly by BI dashboards.
"""

from pyspark.sql import SparkSession, functions as F

SILVER_ORDERS = "lakehouse.silver.orders"
SILVER_CUSTOMERS = "lakehouse.silver.customers"
GOLD_TABLE = "lakehouse.gold.daily_revenue_by_region"


def get_spark() -> SparkSession:
  return (
    SparkSession.builder.appName("gold-aggregate")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
  )


def aggregate(spark: SparkSession) -> None:
  orders = spark.table(SILVER_ORDERS)
  customers = spark.table(SILVER_CUSTOMERS)

  gold = (
    orders.join(customers, orders.customer_key == customers.customer_key, "left")
    .groupBy(F.col("order_date"), F.col("region"))
    .agg(
      F.sum("order_amount").alias("total_revenue"),
      F.countDistinct("order_id").alias("order_count"),
      F.countDistinct("orders.customer_key").alias("distinct_customers"),
    )
    .withColumn("avg_order_value", F.col("total_revenue") / F.col("order_count"))
  )

  (
    gold.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_date")
    .saveAsTable(GOLD_TABLE)
  )

  print(f"Wrote gold aggregate to {GOLD_TABLE}")


if __name__ == "__main__":
  spark = get_spark()
  aggregate(spark)
  spark.stop()
"""
Gold layer aggregation.

Builds business-facing, dimensionally-modeled marts on top of silver:
a daily revenue-by-region fact table used directly by BI dashboards.
"""

from pyspark.sql import SparkSession, functions as F

SILVER_ORDERS = "lakehouse.silver.orders"
SILVER_CUSTOMERS = "lakehouse.silver.customers"
GOLD_TABLE = "lakehouse.gold.daily_revenue_by_region"


def get_spark() -> SparkSession:
  return (
    SparkSession.builder.appName("gold-aggregate")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
  )


def aggregate(spark: SparkSession) -> None:
  orders = spark.table(SILVER_ORDERS)
  customers = spark.table(SILVER_CUSTOMERS)

gold = (
  orders.join(customers, orders.customer_key == customers.customer_key, "left")
  .groupBy(F.col("order_date"), F.col("region"))
  .agg(
    F.sum("order_amount").alias("total_revenue"),
    F.countDistinct("order_id").alias("order_count"),
    F.countDistinct("orders.customer_key").alias("distinct_customers"),
  )
  .withColumn("avg_order_value", F.col("total_revenue") / F.col("order_count"))
)

(
  gold.write.format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .partitionBy("order_date")
  .saveAsTable(GOLD_TABLE)
)

print(f"Wrote gold aggregate to {GOLD_TABLE}")


if __name__ == "__main__":
  spark = get_spark()
  aggregate(spark)
  spark.stop()
  
