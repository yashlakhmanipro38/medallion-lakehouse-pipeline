"""
Generates a small synthetic orders/customers dataset so the pipeline can be
run end-to-end without any external data source. Not representative of any
real company's data -- purely for demoing the pipeline shape.
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

RAW_DIR = Path("data/raw/orders")
REGIONS = ["North", "South", "East", "West"]


def generate_customers(path: Path, n: int = 200) -> None:
  path.mkdir(parents=True, exist_ok=True)
  with open(path / "customers.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["customer_key", "customer_name", "region"])
    for i in range(1, n + 1):
      writer.writerow([f"C{i:05d}", f"Customer {i}", random.choice(REGIONS)])


def generate_orders(path: Path, n: int = 5000, n_customers: int = 200) -> None:
  path.mkdir(parents=True, exist_ok=True)
  start = date(2026, 1, 1)
  with open(path / "orders_2026_01.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer_id", "order_date", "order_amount"])
    for i in range(1, n + 1):
      order_date = start + timedelta(days=random.randint(0, 30))
      writer.writerow(
      [
      f"O{i:06d}",
      f"C{random.randint(1, n_customers):05d}",
      order_date.isoformat(),
      round(random.uniform(15, 850), 2),
      ]
      )


if __name__ == "__main__":
  generate_customers(Path("data/raw/customers"))
  generate_orders(RAW_DIR)
  print("Sample data generated under data/raw/")
  
