"""
DAG: p6_transform_load_dag
Schedule: Triggered externally (by p6_extract_dag or p6_full_pipeline_dag)
Purpose: Read raw JSON from MinIO, compute EV and resource metrics,
         then write to Oracle DB (MERGE) and MinIO processed zone (Parquet).

Task flow:
  transform_cost_metrics  →  load_cost_metrics_oracle  ┐
                          →  upload_cost_metrics_minio  ├─ done
  transform_resource_metrics → load_resource_metrics_oracle ┘
                             → upload_resource_metrics_minio
  load_projects_oracle
  load_wbs_oracle
  load_activities_oracle
  load_resources_oracle
  load_assignments_oracle
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _eac_method() -> str:
    return Variable.get("eac_method", default_var="BAC_CPI")


# ------------------------------------------------------------------
# Transform tasks
# ------------------------------------------------------------------

def transform_cost_metrics(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    from src.transformers.cost_calculator import calculate_project_ev_metrics

    run_date = context["ds"]
    minio = MinIOLoader()
    projects = minio.download_raw("projects", run_date=run_date)
    metrics = calculate_project_ev_metrics(projects, eac_method=_eac_method(), run_date=run_date)
    minio.upload_processed("cost_metrics", metrics, run_date=run_date)
    context["ti"].xcom_push(key="cost_metric_count", value=len(metrics))


def transform_resource_metrics(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    from src.transformers.resource_calculator import (
        calculate_assignment_metrics,
        calculate_resource_summary,
    )

    run_date = context["ds"]
    minio = MinIOLoader()
    assignments = minio.download_raw("resource_assignments", run_date=run_date)
    assignment_metrics = calculate_assignment_metrics(assignments, run_date=run_date)
    resource_summary = calculate_resource_summary(assignment_metrics, run_date=run_date)

    minio.upload_processed("assignment_metrics", assignment_metrics, run_date=run_date)
    minio.upload_processed("resource_metrics", resource_summary, run_date=run_date)
    context["ti"].xcom_push(key="resource_metric_count", value=len(resource_summary))


# ------------------------------------------------------------------
# Load to Oracle
# ------------------------------------------------------------------

def _oracle_loader():
    from src.loaders.oracle_loader import OracleLoader
    loader = OracleLoader()
    loader.connect()
    return loader


def load_projects_oracle(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    minio = MinIOLoader()
    records = minio.download_raw("projects", run_date=run_date)
    with _oracle_loader() as loader:
        n = loader.upsert_projects(records)
    context["ti"].xcom_push(key="projects_loaded", value=n)


def load_wbs_oracle(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    minio = MinIOLoader()
    records = minio.download_raw("wbs", run_date=run_date)
    with _oracle_loader() as loader:
        loader.upsert_wbs(records)


def load_activities_oracle(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    minio = MinIOLoader()
    records = minio.download_raw("activities", run_date=run_date)
    with _oracle_loader() as loader:
        loader.upsert_activities(records)


def load_resources_oracle(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    minio = MinIOLoader()
    records = minio.download_raw("resources", run_date=run_date)
    with _oracle_loader() as loader:
        loader.upsert_resources(records)


def load_assignments_oracle(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    from src.transformers.resource_calculator import calculate_assignment_metrics

    run_date = context["ds"]
    minio = MinIOLoader()
    raw = minio.download_raw("resource_assignments", run_date=run_date)
    metrics = calculate_assignment_metrics(raw, run_date=run_date)
    with _oracle_loader() as loader:
        loader.upsert_resource_assignments(metrics)


def load_cost_metrics_oracle(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    minio = MinIOLoader()
    df = minio.download_processed("cost_metrics", run_date=run_date)
    records = df.to_dict(orient="records")
    with _oracle_loader() as loader:
        loader.upsert_cost_metrics(records)


def load_resource_metrics_oracle(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    minio = MinIOLoader()
    df = minio.download_processed("resource_metrics", run_date=run_date)
    records = df.to_dict(orient="records")
    with _oracle_loader() as loader:
        loader.upsert_resource_metrics(records)


# ------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------

with DAG(
    dag_id="p6_transform_load_dag",
    description="Transform P6 data and load to Oracle DB + MinIO processed zone",
    schedule_interval=None,  # triggered externally
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["p6", "transform", "load", "oracle", "minio"],
    max_active_runs=1,
) as dag:

    # Raw entity loads to Oracle (run in parallel)
    with TaskGroup("load_raw_to_oracle") as tg_raw_load:
        t_load_projects = PythonOperator(task_id="load_projects", python_callable=load_projects_oracle)
        t_load_wbs = PythonOperator(task_id="load_wbs", python_callable=load_wbs_oracle)
        t_load_activities = PythonOperator(task_id="load_activities", python_callable=load_activities_oracle)
        t_load_resources = PythonOperator(task_id="load_resources", python_callable=load_resources_oracle)
        t_load_assignments = PythonOperator(task_id="load_assignments", python_callable=load_assignments_oracle)

    # Transform metrics
    with TaskGroup("transform_metrics") as tg_transform:
        t_transform_cost = PythonOperator(task_id="transform_cost_metrics", python_callable=transform_cost_metrics)
        t_transform_resource = PythonOperator(task_id="transform_resource_metrics", python_callable=transform_resource_metrics)

    # Load computed metrics to Oracle
    with TaskGroup("load_metrics_to_oracle") as tg_metrics_load:
        t_load_cost_metrics = PythonOperator(task_id="load_cost_metrics", python_callable=load_cost_metrics_oracle)
        t_load_resource_metrics = PythonOperator(task_id="load_resource_metrics", python_callable=load_resource_metrics_oracle)

    # Flow: raw loads run first, then transforms, then metric loads
    tg_raw_load >> tg_transform >> tg_metrics_load
