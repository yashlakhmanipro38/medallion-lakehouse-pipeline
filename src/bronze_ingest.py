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

RAW_CUSTOMERS_PATH = "data/raw/customers"
BRONZE_CUSTOMERS_TABLE = "lakehouse.bronze.customers"


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


def _ingest_csv(spark: SparkSession, raw_path: str, bronze_table: str) -> None:
  df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
  )

(
  df.write.format("delta")
  .mode("append")
  .option("mergeSchema", "true")
  .saveAsTable(bronze_table)
)

print(f"Ingested {df.count()} rows into {bronze_table}")


def ingest(spark: SparkSession) -> None:
  """Ingests the orders source. Kept as the default entrypoint so the
  Airflow DAG's bronze_ingest_orders task can call this directly."""
  _ingest_csv(spark, RAW_PATH, BRONZE_TABLE)


def ingest_customers(spark: SparkSession) -> None:
  _ingest_csv(spark, RAW_CUSTOMERS_PATH, BRONZE_CUSTOMERS_TABLE)


if __name__ == "__main__":
  spark = get_spark()
  ingest(spark)
  ingest_customers(spark)
  spark.stop()
