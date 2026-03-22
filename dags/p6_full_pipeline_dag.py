"""
DAG: p6_full_pipeline_dag
Schedule: Daily at 02:00 UTC
Purpose: End-to-end pipeline combining extract, transform, and load in a single DAG.

Task Groups:
  [extract]  → projects, wbs, activities, resources, roles, assignments → MinIO raw
  [transform] → cost metrics, resource metrics
  [load_oracle] → upsert all tables (parallel)
  [load_minio_processed] → upload parquet files (parallel)
"""
from __future__ import annotations

from datetime import datetime, timedelta

import yaml
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


def _cfg() -> dict:
    config_path = Variable.get("pipeline_config_path", default_var="/opt/airflow/config/config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def _opc_config() -> dict:
    cfg = _cfg()
    return {
        "base_url": Variable.get("opc_base_url"),
        "token_url": Variable.get("opc_token_url"),
        "client_id": Variable.get("opc_client_id"),
        "client_secret": Variable.get("opc_client_secret"),
        "scope": Variable.get("opc_scope", default_var=""),
        "timeout": cfg["opc"]["request_timeout_seconds"],
        "max_retries": cfg["opc"]["max_retries"],
        "retry_wait": cfg["opc"]["retry_wait_seconds"],
        "page_size": cfg["opc"]["page_size"],
        "fields": cfg["opc"],
    }


# ======================================================================
# EXTRACT
# ======================================================================

def extract_and_store_projects(**context) -> None:
    from src.p6_client import OPCClient
    from src.extractors.project_extractor import ProjectExtractor
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    cfg = _cfg()
    status_filter = cfg["pipeline"].get("project_filter_status", ["Active"])

    with OPCClient(_opc_config()) as client:
        projects = ProjectExtractor(client, status_filter).extract_projects()

    MinIOLoader().upload_raw("projects", projects, run_date=run_date)
    context["ti"].xcom_push(key="project_ids", value=[p["ObjectId"] for p in projects])


def extract_and_store_wbs(**context) -> None:
    from src.p6_client import OPCClient
    from src.extractors.project_extractor import ProjectExtractor
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    pids = context["ti"].xcom_pull(task_ids="extract.extract_projects", key="project_ids")
    with OPCClient(_opc_config()) as client:
        wbs = ProjectExtractor(client).extract_wbs(pids)
    MinIOLoader().upload_raw("wbs", wbs, run_date=run_date)


def extract_and_store_activities(**context) -> None:
    from src.p6_client import OPCClient
    from src.extractors.project_extractor import ProjectExtractor
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    pids = context["ti"].xcom_pull(task_ids="extract.extract_projects", key="project_ids")
    with OPCClient(_opc_config()) as client:
        activities = ProjectExtractor(client).extract_activities(pids)
    MinIOLoader().upload_raw("activities", activities, run_date=run_date)


def extract_and_store_resources(**context) -> None:
    from src.p6_client import OPCClient
    from src.extractors.resource_extractor import ResourceExtractor
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    pids = context["ti"].xcom_pull(task_ids="extract.extract_projects", key="project_ids")
    with OPCClient(_opc_config()) as client:
        extractor = ResourceExtractor(client)
        resources = extractor.extract_resources()
        roles = extractor.extract_roles()
        assignments = extractor.extract_assignments(pids)

    minio = MinIOLoader()
    minio.upload_raw("resources", resources, run_date=run_date)
    minio.upload_raw("roles", roles, run_date=run_date)
    minio.upload_raw("resource_assignments", assignments, run_date=run_date)


# ======================================================================
# TRANSFORM
# ======================================================================

def transform_cost(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    from src.transformers.cost_calculator import calculate_project_ev_metrics

    run_date = context["ds"]
    eac_method = Variable.get("eac_method", default_var="BAC_CPI")
    minio = MinIOLoader()
    projects = minio.download_raw("projects", run_date=run_date)
    metrics = calculate_project_ev_metrics(projects, eac_method=eac_method, run_date=run_date)
    minio.upload_processed("cost_metrics", metrics, run_date=run_date)
    context["ti"].xcom_push(key="cost_metrics", value=metrics)


def transform_resources(**context) -> None:
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
    context["ti"].xcom_push(key="resource_summary_count", value=len(resource_summary))


# ======================================================================
# LOAD → ORACLE
# ======================================================================

def _ora():
    from src.loaders.oracle_loader import OracleLoader
    loader = OracleLoader()
    loader.connect()
    return loader


def load_oracle_projects(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    run_date = context["ds"]
    records = MinIOLoader().download_raw("projects", run_date=run_date)
    with _ora() as loader:
        loader.upsert_projects(records)


def load_oracle_wbs(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    run_date = context["ds"]
    with _ora() as loader:
        loader.upsert_wbs(MinIOLoader().download_raw("wbs", run_date=run_date))


def load_oracle_activities(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    run_date = context["ds"]
    with _ora() as loader:
        loader.upsert_activities(MinIOLoader().download_raw("activities", run_date=run_date))


def load_oracle_resources(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    run_date = context["ds"]
    with _ora() as loader:
        loader.upsert_resources(MinIOLoader().download_raw("resources", run_date=run_date))


def load_oracle_assignments(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    from src.transformers.resource_calculator import calculate_assignment_metrics
    run_date = context["ds"]
    raw = MinIOLoader().download_raw("resource_assignments", run_date=run_date)
    metrics = calculate_assignment_metrics(raw, run_date=run_date)
    with _ora() as loader:
        loader.upsert_resource_assignments(metrics)


def load_oracle_cost_metrics(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    run_date = context["ds"]
    df = MinIOLoader().download_processed("cost_metrics", run_date=run_date)
    with _ora() as loader:
        loader.upsert_cost_metrics(df.to_dict(orient="records"))


def load_oracle_resource_metrics(**context) -> None:
    from src.loaders.minio_loader import MinIOLoader
    run_date = context["ds"]
    df = MinIOLoader().download_processed("resource_metrics", run_date=run_date)
    with _ora() as loader:
        loader.upsert_resource_metrics(df.to_dict(orient="records"))


# ======================================================================
# DAG DEFINITION
# ======================================================================

with DAG(
    dag_id="p6_full_pipeline_dag",
    description="Full P6 pipeline: Extract OPC → Transform EV/Resource → Load Oracle + MinIO",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["p6", "pipeline", "oracle", "minio", "opc"],
    max_active_runs=1,
    doc_md="""
## P6 Full Pipeline DAG

**Flow:** `Extract` → `Transform` → `Load Oracle` (parallel with `Load MinIO Processed`)

### Connections required (Airflow UI → Admin → Connections)
- `oracle_p6_conn`: Oracle DB for P6 target tables
- `minio_s3_conn`: MinIO S3-compatible storage

### Variables required (Airflow UI → Admin → Variables)
- `opc_base_url`, `opc_token_url`, `opc_client_id`, `opc_client_secret`
- `pipeline_config_path` (default: `/opt/airflow/config/config.yaml`)
- `eac_method` (default: `BAC_CPI`)
""",
) as dag:

    with TaskGroup("extract") as tg_extract:
        t_proj = PythonOperator(task_id="extract_projects", python_callable=extract_and_store_projects)
        t_wbs = PythonOperator(task_id="extract_wbs", python_callable=extract_and_store_wbs)
        t_act = PythonOperator(task_id="extract_activities", python_callable=extract_and_store_activities)
        t_res = PythonOperator(task_id="extract_resources", python_callable=extract_and_store_resources)
        t_proj >> [t_wbs, t_act, t_res]

    with TaskGroup("transform") as tg_transform:
        t_cost = PythonOperator(task_id="transform_cost", python_callable=transform_cost)
        t_res_metrics = PythonOperator(task_id="transform_resources", python_callable=transform_resources)

    with TaskGroup("load_oracle") as tg_oracle:
        t_ora_proj = PythonOperator(task_id="load_projects", python_callable=load_oracle_projects)
        t_ora_wbs = PythonOperator(task_id="load_wbs", python_callable=load_oracle_wbs)
        t_ora_act = PythonOperator(task_id="load_activities", python_callable=load_oracle_activities)
        t_ora_res = PythonOperator(task_id="load_resources", python_callable=load_oracle_resources)
        t_ora_assign = PythonOperator(task_id="load_assignments", python_callable=load_oracle_assignments)
        t_ora_cost_met = PythonOperator(task_id="load_cost_metrics", python_callable=load_oracle_cost_metrics)
        t_ora_res_met = PythonOperator(task_id="load_resource_metrics", python_callable=load_oracle_resource_metrics)

    tg_extract >> tg_transform >> tg_oracle
