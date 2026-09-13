"""
Seed pipeline_run_metrics with synthetic historical rows so the anomaly
detection logic (quality_monitor DAG) has a baseline to compare against.

This does NOT go through Airflow -- it connects to Postgres directly and
inserts rows as if `financial_pipeline_dynamic` had been running daily for
the past N days. One day is intentionally seeded with a rejection_rate spike
so we have a known case to verify `detect_anomaly` actually catches it.

Usage:
    ./venv/bin/python src/seed_historical_metrics.py
"""

import random
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

# Connects from the host machine to the Postgres container's exposed port.
# Adjust the port/user/password/db here if your docker-compose differs.
DB_URL = "postgresql://airflow:airflow@postgres:5432/financial_pipeline"
DAYS_OF_HISTORY = 14
ANOMALY_DAY_OFFSET = 3  # 3 days ago will have an injected spike
FILE_NAME = "transactions_001.csv"
DAG_ID = "financial_pipeline_dynamic"

# Normal-day baseline: small, stable rejection rate
NORMAL_TOTAL_ROWS = 500
NORMAL_REJECTION_RATE_MEAN = 0.02   # 2%
NORMAL_REJECTION_RATE_STD = 0.005   # +/- 0.5%

# Anomaly-day: rejection rate spikes far above normal
ANOMALY_REJECTION_RATE = 0.22       # 22%


def make_row(days_ago: int, is_anomaly: bool):
    run_date = datetime.now(timezone.utc) - timedelta(days=days_ago)
    run_id = f"seed__{run_date.strftime('%Y-%m-%dT%H:%M:%S')}"

    total_rows = NORMAL_TOTAL_ROWS

    if is_anomaly:
        rejection_rate = ANOMALY_REJECTION_RATE
    else:
        rejection_rate = max(
            0.0,
            random.gauss(NORMAL_REJECTION_RATE_MEAN, NORMAL_REJECTION_RATE_STD),
        )

    rejected_rows = round(total_rows * rejection_rate)
    # Split rejected rows across the two failure reasons roughly evenly
    missing_amount = rejected_rows // 2
    negative_amount = rejected_rows - missing_amount

    return {
        "run_id": run_id,
        "dag_id": DAG_ID,
        "execution_date": run_date,
        "file_name": FILE_NAME,
        "total_rows": total_rows,
        "missing_amount_count": missing_amount,
        "missing_customer_id_count": 0,
        "negative_amount_count": negative_amount,
        "rejection_rate": round(rejected_rows / total_rows, 4),
    }


def main():
    engine = create_engine(DB_URL)

    rows = []
    for days_ago in range(DAYS_OF_HISTORY, 0, -1):
        is_anomaly = days_ago == ANOMALY_DAY_OFFSET
        rows.append(make_row(days_ago, is_anomaly))

    insert_sql = text(
        """
        INSERT INTO pipeline_run_metrics (
            run_id, dag_id, execution_date, file_name, total_rows,
            missing_amount_count, missing_customer_id_count,
            negative_amount_count, rejection_rate
        )
        VALUES (
            :run_id, :dag_id, :execution_date, :file_name, :total_rows,
            :missing_amount_count, :missing_customer_id_count,
            :negative_amount_count, :rejection_rate
        )
        ON CONFLICT (run_id, file_name) DO NOTHING
        """
    )

    with engine.begin() as conn:
        for row in rows:
            conn.execute(insert_sql, row)

    print(f"Seeded {len(rows)} historical rows into pipeline_run_metrics.")
    print(f"Anomaly injected at {ANOMALY_DAY_OFFSET} days ago "
          f"(rejection_rate={ANOMALY_REJECTION_RATE}).")


if __name__ == "__main__":
    main()
