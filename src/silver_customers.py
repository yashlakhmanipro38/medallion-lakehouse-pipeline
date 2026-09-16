"""
Silver layer for the customers dimension: dedupes on business key and
standardizes text fields.
"""

from pyspark.sql import SparkSession, functions as F

BRONZE_TABLE = "lakehouse.bronze.customers"
SILVER_TABLE = "lakehouse.silver.customers"


def get_spark() -> SparkSession:
  return (
    SparkSession.builder.appName("silver-customers")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
  )


def transform(spark: SparkSession) -> None:
  bronze = spark.table(BRONZE_TABLE)

  silver = (
    bronze.dropDuplicates(["customer_key"])
    .withColumn("customer_name", F.trim(F.initcap("customer_name")))
    .withColumn("region", F.trim(F.col("region")))
    .drop("_source_file", "_ingested_at")
  )

  (
    silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
  )

  print(f"Wrote {silver.count()} rows to {SILVER_TABLE}")


if __name__ == "__main__":
  spark = get_spark()
  transform(spark)
  spark.stop()
