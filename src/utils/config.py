import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()

_DEFAULT_CONFIG_PATH = Path(__file__).parents[2] / "config" / "config.yaml"


@lru_cache(maxsize=1)
def load_config(config_path: str | None = None) -> dict[str, Any]:
    path = Path(config_path) if config_path else _DEFAULT_CONFIG_PATH
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_opc_config() -> dict[str, Any]:
    cfg = load_config()
    return {
        "base_url": os.environ["OPC_BASE_URL"],
        "token_url": os.environ["OPC_TOKEN_URL"],
        "client_id": os.environ["OPC_CLIENT_ID"],
        "client_secret": os.environ["OPC_CLIENT_SECRET"],
        "scope": os.environ.get("OPC_SCOPE", ""),
        "timeout": cfg["opc"]["request_timeout_seconds"],
        "max_retries": cfg["opc"]["max_retries"],
        "retry_wait": cfg["opc"]["retry_wait_seconds"],
        "page_size": cfg["opc"]["page_size"],
        "fields": cfg["opc"],
    }


def get_oracle_config() -> dict[str, Any]:
    cfg = load_config()["oracle"]
    return {
        "host": os.environ["ORACLE_HOST"],
        "port": int(os.environ.get("ORACLE_PORT", 1521)),
        "service_name": os.environ["ORACLE_SERVICE"],
        "user": os.environ["ORACLE_USER"],
        "password": os.environ["ORACLE_PASSWORD"],
        "schema": os.environ.get("ORACLE_SCHEMA", cfg["schema"]),
        "pool_min": cfg["pool_min"],
        "pool_max": cfg["pool_max"],
        "pool_increment": cfg["pool_increment"],
        "tables": cfg["tables"],
    }


def get_minio_config() -> dict[str, Any]:
    cfg = load_config()["minio"]
    return {
        "endpoint": os.environ["MINIO_ENDPOINT"],
        "access_key": os.environ["MINIO_ACCESS_KEY"],
        "secret_key": os.environ["MINIO_SECRET_KEY"],
        "secure": os.environ.get("MINIO_SECURE", "false").lower() == "true",
        "raw_bucket": os.environ.get("MINIO_RAW_BUCKET", cfg["raw_bucket"]),
        "processed_bucket": os.environ.get("MINIO_PROCESSED_BUCKET", cfg["processed_bucket"]),
        "file_format": cfg["file_format"],
    }
