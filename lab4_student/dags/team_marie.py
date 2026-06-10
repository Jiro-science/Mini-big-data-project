from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.sensors.filesystem import FileSensor

from include.ingest import ingest_day, validate_silver
from include.paths import report_json
from include.team_marie_spark import run_daily


def on_failure_alert(context):
    task_id = context["task_instance"].task_id
    ds = context["ds"]
    print(
        f"[ALERT] Tâche '{task_id}' a échoué pour la date {ds}. Vérifier le pipeline immédiatement."
    )


DEFAULT_ARGS = {
    "owner": "team_marie",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure_alert,
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
        mode="reschedule",
    )

    @task_group(group_id="ingestion")
    def ingestion_group():
        @task
        def ingest(ds=None):
            ingest_day(ds)

        ingest()

    @task_group(group_id="quality")
    def quality_group():
        @task
        def check_row_count(ds=None):
            import duckdb

            silver_path = f"/opt/airflow/data/raw/dt={ds}"
            con = duckdb.connect()
            count = con.execute(
                f"SELECT COUNT(*) FROM parquet_scan('{silver_path}/*.parquet')"
            ).fetchone()[0]
            print(f"[check_row_count] {count} lignes détectées pour {ds}")
            if count < 10:
                raise ValueError(
                    f"Trop peu de lignes : {count} (minimum attendu : 10). Pipeline interrompu."
                )
            return count

        @task
        def validate(ds=None):
            validate_silver(ds)

        check_row_count() >> validate()

    @task_group(group_id="analytics")
    def analytics_group():
        @task
        def compute_kpis(ds=None):
            return run_daily(ds)

        @task
        def publish(ds=None):
            path = report_json(ds)
            print(f"[publish] rapport disponible : {path}")
            return str(path)

        @task
        def export_summary(ds=None):
            import json
            from pathlib import Path
            from datetime import datetime, timedelta

            reports_dir = Path("data/reports")
            logical_date = datetime.strptime(ds, "%Y-%m-%d")

            total_revenue = 0.0
            total_transactions = 0
            days_processed = 0

            for i in range(7):
                day = logical_date - timedelta(days=i)
                report_path = reports_dir / f"dashboard_{day.strftime('%Y-%m-%d')}.json"
                if report_path.exists():
                    with open(report_path) as f:
                        data = json.load(f)
                    total_revenue += data.get("total_revenue", 0)
                    total_transactions += data.get("total_transactions", 0)
                    days_processed += 1

            summary = {
                "week_ending": ds,
                "total_revenue_week": round(total_revenue, 2),
                "total_transactions_week": total_transactions,
                "days_processed": days_processed,
                "generated_at": datetime.utcnow().isoformat(),
            }

            output_path = reports_dir / f"weekly_summary_{ds}.json"
            with open(output_path, "w") as f:
                json.dump(summary, f, indent=2)

            print(f"[export_summary] résumé hebdomadaire généré : {output_path}")
            print(f"[export_summary] contenu : {json.dumps(summary, indent=2)}")

            # Retourne le summary complet → visible dans XCom depuis l'UI
            return summary

        compute_kpis() >> publish() >> export_summary()

    wait_csv >> ingestion_group() >> quality_group() >> analytics_group()
