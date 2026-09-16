"""
Bronze layer ingestion.

Reads raw source files (CSV/JSON) as-is into a Delta table, preserving the
original schema and adding minimal ingestion metadata (source file, load
timestamp). No business transformations happen here by design -- bronze is
a durable, replayable copy of the source.
"""

from pyspark.sql import SparkSession, functions as F

RAW_PATH = "data/raw/orders"
BRONZE_TABLE = "lakehouse.bronze.orders"


def get_spark() -> SparkSession:
  return (
    SparkSession.builder.appName("bronze-ingest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
  )


def ingest(spark: SparkSession) -> None:
  df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(RAW_PATH)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
  )

(
  df.write.format("delta")
  .mode("append")
  .option("mergeSchema", "true")
  .saveAsTable(BRONZE_TABLE)
)

print(f"Ingested {df.count()} rows into {BRONZE_TABLE}")


if __name__ == "__main__":
  spark = get_spark()
  ingest(spark)
  spark.stop()
  
