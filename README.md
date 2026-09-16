# Medallion Lakehouse Pipeline

A batch and streaming data pipeline implementing the medallion architecture (bronze, silver, gold) on Delta Lake, orchestrated with Airflow. Built to demonstrate the same architecture pattern used in production lakehouse platforms: durable raw ingestion, conformed and deduplicated silver tables, and dimensionally-modeled gold marts ready for BI.

## Architecture

Raw CSV/JSON plus streaming JSON (simulated events) land in BRONZE as a raw, append-only copy: bronze.orders, bronze.customers, bronze.order_events. Bronze feeds SILVER, which is deduped, typed, and clean: silver.orders, silver.customers. Silver feeds GOLD, the business marts layer: gold.daily_revenue_by_region.

Bronze (src/bronze_ingest.py, src/streaming_ingest.py) lands raw data as-is from batch CSV drops and a simulated streaming event source, with ingestion metadata (_source_file, _ingested_at) attached. No transformation happens here.

Silver (src/silver_transform.py, src/silver_customers.py) deduplicates on business key, enforces types, filters nulls, and standardizes columns. Query-ready.

Gold (src/gold_aggregate.py) is a daily revenue-by-region fact table, joined and aggregated from silver, partitioned by date for BI consumption.

Orchestration (notebooks/orchestration_dag.py) is an Airflow DAG showing how these jobs are scheduled and chained in production, with retries and a fan-in from both silver tables before gold runs.

## Why this shape

This mirrors the lakehouse patterns used day to day in production: append-only bronze for replayability, silver as the single conformed source of truth, and narrow purpose-built gold marts rather than one giant denormalized table. The same structure scales from a laptop demo (this repo) to a multi-hundred-table production migration.

## Running locally

Install dependencies with pip install -r requirements.txt. Then generate synthetic sample data with python src/generate_sample_data.py (no real or company data is used anywhere). Then run the pipeline stages in order: python src/bronze_ingest.py, python src/silver_customers.py, python src/silver_transform.py, python src/gold_aggregate.py.

Streaming ingestion (src/streaming_ingest.py) expects JSON event files to land incrementally in data/streaming/events/, and can point at any directory-watching or Kafka-backed source in a real deployment.

## Stack

Python, PySpark, Delta Lake, Spark Structured Streaming, Apache Airflow.

## Notes

All data in this repo is synthetically generated (src/generate_sample_data.py). No proprietary or employer data is used anywhere in this project.
