"""
Silver layer transformation.

Cleans and conforms bronze data: dedupes on business key, enforces types,
drops nulls in required fields, and standardizes column names. Silver is
query-ready but still grain-per-source (one row per order line).
"""

from pyspark.sql import SparkSession, Window, functions as F

BRONZE_TABLE = "lakehouse.bronze.orders"
SILVER_TABLE = "lakehouse.silver.orders"


def get_spark() -> SparkSession:
  return (
    SparkSession.builder.appName("silver-transform")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
  )


def transform(spark: SparkSession) -> None:
  bronze = spark.table(BRONZE_TABLE)

dedup_window = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())

silver = (
  bronze.withColumn("_rn", F.row_number().over(dedup_window))
  .filter(F.col("_rn") == 1)
  .drop("_rn", "_source_file")
  .withColumn("order_date", F.to_date("order_date"))
  .withColumn("order_amount", F.col("order_amount").cast("decimal(12,2)"))
  .filter(F.col("order_id").isNotNull() & F.col("order_amount").isNotNull())
  .withColumnRenamed("customer_id", "customer_key")
)

(
  silver.write.format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable(SILVER_TABLE)
)

print(f"Wrote {silver.count()} deduped rows to {SILVER_TABLE}")


if __name__ == "__main__":
  spark = get_spark()
  transform(spark)
  spark.stop()
  
