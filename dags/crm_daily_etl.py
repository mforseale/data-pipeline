"""Daily analytical snapshots of a historical CRM experiment."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task


@dag(dag_id="crm_daily_etl", schedule="0 10 * * *",
     start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Tomsk"), catchup=False,
     max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
     tags=["portfolio", "postgres", "spark", "etl"],
     description="PostgreSQL -> Spark -> atomic analytical snapshot -> report")
def crm_daily_etl():
    @task()
    def extract_source_bound():
        from pipeline.job import extract_bound
        context = get_current_context()
        requested = (context["dag_run"].conf or {}).get("snapshot_date")
        snapshot_date = requested or pendulum.instance(context["data_interval_end"]).in_timezone("Asia/Tomsk").to_date_string()
        return extract_bound(snapshot_date)

    @task()
    def spark_transform_and_publish(metadata):
        from pipeline.job import run_batch
        return run_batch(metadata)

    @task()
    def check_warehouse(metadata):
        from pipeline.job import validate_snapshot
        validate_snapshot(metadata["snapshot_date"])
        return metadata

    @task()
    def build_report(metadata):
        from pipeline.report import build_report
        return build_report(metadata["snapshot_date"])

    build_report(check_warehouse(spark_transform_and_publish(extract_source_bound())))


dag = crm_daily_etl()
