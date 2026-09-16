"""
Structured Streaming ingestion into bronze.

Demonstrates the streaming counterpart to bronze_ingest.py: continuously
reads new JSON event files landing in a directory (simulating a Kafka/
event-hub source) and appends them to the bronze Delta table using
checkpointed, exactly-once micro-batches.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
StructType,
StructField,
StringType,
DoubleType,
TimestampType,
)

EVENTS_PATH = "data/streaming/events"
CHECKPOINT_PATH = "data/_checkpoints/bronze_events"
BRONZE_EVENTS_TABLE = "lakehouse.bronze.order_events"

EVENT_SCHEMA = StructType(
  [
    StructField("order_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_amount", DoubleType()),
    StructField("event_time", TimestampType()),
  ]
)


def get_spark() -> SparkSession:
  return (
    SparkSession.builder.appName("streaming-bronze-ingest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
  )


def run(spark: SparkSession) -> None:
  stream_df = (
    spark.readStream.schema(EVENT_SCHEMA)
    .option("maxFilesPerTrigger", 10)
    .json(EVENTS_PATH)
  )

query = (
  stream_df.writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", CHECKPOINT_PATH)
  .trigger(processingTime="30 seconds")
  .toTable(BRONZE_EVENTS_TABLE)
)

query.awaitTermination()


if __name__ == "__main__":
  spark = get_spark()
  run(spark)
  
