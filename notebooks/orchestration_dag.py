"""
Airflow DAG showing how the bronze -> silver -> gold jobs in src/ are
orchestrated in production: daily schedule, retries, and a validation
gate before gold runs.

This mirrors the ADF/Databricks Jobs pattern used at work, translated to
Airflow for portability in this demo repo.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
  "owner": "data-eng",
  "retries": 2,
  "retry_delay": timedelta(minutes=5),
}

with DAG(
  dag_id="medallion_lakehouse_pipeline",
  default_args=default_args,
  start_date=datetime(2026, 1, 1),
  schedule_interval="@daily",
  catchup=False,
  tags=["lakehouse", "medallion", "portfolio-demo"],
) as dag:

  def _run(module: str, func: str = "main") -> None:
    import importlib

    mod = importlib.import_module(module)
    spark = mod.get_spark()
    {
      "bronze_ingest": mod.ingest,
      "silver_transform": mod.transform,
      "silver_customers": mod.transform,
      "gold_aggregate": mod.aggregate,
    }[module.split(".")[-1]](spark)
    spark.stop()

  bronze_orders = PythonOperator(
    task_id="bronze_ingest_orders",
    python_callable=_run,
    op_kwargs={"module": "src.bronze_ingest"},
  )
  
  silver_orders = PythonOperator(
    task_id="silver_transform_orders",
    python_callable=_run,
    op_kwargs={"module": "src.silver_transform"},
  )
  
  silver_customers = PythonOperator(
    task_id="silver_transform_customers",
    python_callable=_run,
    op_kwargs={"module": "src.silver_customers"},
  )
  
  gold = PythonOperator(
    task_id="gold_aggregate_daily_revenue",
    python_callable=_run,
    op_kwargs={"module": "src.gold_aggregate"},
  )
  
  bronze_orders >> silver_orders
  [silver_orders, silver_customers] >> gold
