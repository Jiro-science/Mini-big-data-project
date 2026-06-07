from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

from include.ingest import ingest_day, validate_silver
from include.paths import report_json
from include.team_marie_spark import run_daily

DEFAULT_ARGS = {
    "owner": "team_marie",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="team_marie",
    description="Pipeline KPI retail — team marie",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone"],
) as dag:

    wait_csv = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/data/incoming/transactions_{{ ds }}.csv",
        poke_interval=10,
        timeout=300,
        mode="poke",
    )

    @task
    def ingest(ds=None):
        ingest_day(ds)

    @task
    def validate(ds=None):
        validate_silver(ds)

    @task
    def compute_kpis(ds=None):
        return run_daily(ds)

    @task
    def publish(ds=None):
        path = report_json(ds)
        print(f"[publish] rapport disponible : {path}")
        return str(path)

    ingest_task   = ingest()
    validate_task = validate()
    compute_task  = compute_kpis()
    publish_task  = publish()

    wait_csv >> ingest_task >> validate_task >> compute_task >> publish_task