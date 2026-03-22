"""
DAG: p6_extract_dag
Schedule: Daily at 01:00 UTC
Purpose: Extract all P6 entities from Oracle OPC and store raw JSON in MinIO.

Task flow:
  extract_projects → extract_wbs → extract_activities
                   ↘                              ↘
                    extract_resources              upload_all_raw (fanin)
                   ↗
  extract_roles  →
  extract_assignments →
"""
from __future__ import annotations

import json
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


def _get_opc_config_from_airflow() -> dict:
    return {
        "base_url": Variable.get("opc_base_url"),
        "token_url": Variable.get("opc_token_url"),
        "client_id": Variable.get("opc_client_id"),
        "client_secret": Variable.get("opc_client_secret"),
        "scope": Variable.get("opc_scope", default_var=""),
        "timeout": 30,
        "max_retries": 3,
        "retry_wait": 2,
        "page_size": 500,
        "fields": _load_field_config(),
    }


def _load_field_config() -> dict:
    import yaml
    config_path = Variable.get("pipeline_config_path", default_var="/opt/airflow/config/config.yaml")
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    return cfg["opc"]


def extract_projects(**context) -> None:
    from src.p6_client import OPCClient
    from src.extractors.project_extractor import ProjectExtractor
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    opc_cfg = _get_opc_config_from_airflow()

    with OPCClient(opc_cfg) as client:
        extractor = ProjectExtractor(client)
        projects = extractor.extract_projects()

    minio = MinIOLoader()
    minio.upload_raw("projects", projects, run_date=run_date)
    context["ti"].xcom_push(key="project_ids", value=[p["ObjectId"] for p in projects])
    context["ti"].xcom_push(key="project_count", value=len(projects))


def extract_wbs(**context) -> None:
    from src.p6_client import OPCClient
    from src.extractors.project_extractor import ProjectExtractor
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    project_ids = context["ti"].xcom_pull(task_ids="extract_projects", key="project_ids")
    opc_cfg = _get_opc_config_from_airflow()

    with OPCClient(opc_cfg) as client:
        extractor = ProjectExtractor(client)
        wbs = extractor.extract_wbs(project_ids)

    minio = MinIOLoader()
    minio.upload_raw("wbs", wbs, run_date=run_date)


def extract_activities(**context) -> None:
    from src.p6_client import OPCClient
    from src.extractors.project_extractor import ProjectExtractor
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    project_ids = context["ti"].xcom_pull(task_ids="extract_projects", key="project_ids")
    opc_cfg = _get_opc_config_from_airflow()

    with OPCClient(opc_cfg) as client:
        extractor = ProjectExtractor(client)
        activities = extractor.extract_activities(project_ids)

    minio = MinIOLoader()
    minio.upload_raw("activities", activities, run_date=run_date)


def extract_resources(**context) -> None:
    from src.p6_client import OPCClient
    from src.extractors.resource_extractor import ResourceExtractor
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    opc_cfg = _get_opc_config_from_airflow()

    with OPCClient(opc_cfg) as client:
        extractor = ResourceExtractor(client)
        resources = extractor.extract_resources()
        roles = extractor.extract_roles()

    minio = MinIOLoader()
    minio.upload_raw("resources", resources, run_date=run_date)
    minio.upload_raw("roles", roles, run_date=run_date)


def extract_assignments(**context) -> None:
    from src.p6_client import OPCClient
    from src.extractors.resource_extractor import ResourceExtractor
    from src.loaders.minio_loader import MinIOLoader

    run_date = context["ds"]
    project_ids = context["ti"].xcom_pull(task_ids="extract_projects", key="project_ids")
    opc_cfg = _get_opc_config_from_airflow()

    with OPCClient(opc_cfg) as client:
        extractor = ResourceExtractor(client)
        assignments = extractor.extract_assignments(project_ids)

    minio = MinIOLoader()
    minio.upload_raw("resource_assignments", assignments, run_date=run_date)


with DAG(
    dag_id="p6_extract_dag",
    description="Extract Oracle OPC P6 data to MinIO raw zone",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["p6", "extract", "opc"],
    max_active_runs=1,
) as dag:

    t_extract_projects = PythonOperator(
        task_id="extract_projects",
        python_callable=extract_projects,
    )

    with TaskGroup("extract_project_details") as tg_project_details:
        t_wbs = PythonOperator(task_id="extract_wbs", python_callable=extract_wbs)
        t_activities = PythonOperator(task_id="extract_activities", python_callable=extract_activities)
        t_assignments = PythonOperator(task_id="extract_assignments", python_callable=extract_assignments)

    with TaskGroup("extract_master_data") as tg_master:
        t_resources = PythonOperator(task_id="extract_resources", python_callable=extract_resources)

    t_extract_projects >> tg_project_details
    t_extract_projects >> tg_master
