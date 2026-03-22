"""
MinIO loader: stores raw JSON extracts and processed Parquet files.

Bucket layout:
  p6-raw/       YYYY/MM/DD/<entity>.json       (original API responses)
  p6-processed/ YYYY/MM/DD/<entity>.parquet    (post-transform DataFrames)
"""
from __future__ import annotations

import io
import json
from datetime import date
from typing import Any

import pandas as pd
from minio import Minio
from minio.error import S3Error

from src.utils.config import get_minio_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MinIOLoader:
    def __init__(self, config: dict[str, Any] | None = None):
        self._cfg = config or get_minio_config()
        self._client = Minio(
            endpoint=self._cfg["endpoint"],
            access_key=self._cfg["access_key"],
            secret_key=self._cfg["secret_key"],
            secure=self._cfg["secure"],
        )
        self._ensure_buckets()

    def _ensure_buckets(self) -> None:
        for bucket in (self._cfg["raw_bucket"], self._cfg["processed_bucket"]):
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket)
                logger.info("Created bucket", extra={"bucket": bucket})

    def _date_prefix(self, run_date: date | str | None = None) -> str:
        d = run_date or date.today()
        if isinstance(d, str):
            d = date.fromisoformat(d)
        return f"{d.year:04d}/{d.month:02d}/{d.day:02d}"

    # ------------------------------------------------------------------
    # Raw zone: JSON
    # ------------------------------------------------------------------

    def upload_raw(
        self,
        entity: str,
        records: list[dict[str, Any]],
        run_date: date | str | None = None,
    ) -> str:
        """Upload raw API response records as a JSON file. Returns the object path."""
        prefix = self._date_prefix(run_date)
        object_name = f"{prefix}/{entity}.json"
        payload = json.dumps(records, default=str).encode("utf-8")
        bucket = self._cfg["raw_bucket"]
        self._client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        logger.info("Uploaded raw file", extra={"bucket": bucket, "object": object_name, "records": len(records)})
        return f"{bucket}/{object_name}"

    def download_raw(self, entity: str, run_date: date | str | None = None) -> list[dict[str, Any]]:
        """Download raw JSON records for a given entity and date."""
        prefix = self._date_prefix(run_date)
        object_name = f"{prefix}/{entity}.json"
        response = self._client.get_object(self._cfg["raw_bucket"], object_name)
        try:
            data = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
        return data

    # ------------------------------------------------------------------
    # Processed zone: Parquet
    # ------------------------------------------------------------------

    def upload_processed(
        self,
        entity: str,
        records: list[dict[str, Any]],
        run_date: date | str | None = None,
    ) -> str:
        """Convert records to Parquet and upload. Returns the object path."""
        prefix = self._date_prefix(run_date)
        object_name = f"{prefix}/{entity}.parquet"
        df = pd.DataFrame(records)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        size = buf.getbuffer().nbytes
        bucket = self._cfg["processed_bucket"]
        self._client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=buf,
            length=size,
            content_type="application/octet-stream",
        )
        logger.info(
            "Uploaded processed file",
            extra={"bucket": bucket, "object": object_name, "rows": len(df)},
        )
        return f"{bucket}/{object_name}"

    def download_processed(self, entity: str, run_date: date | str | None = None) -> pd.DataFrame:
        """Download and return a processed Parquet file as a DataFrame."""
        prefix = self._date_prefix(run_date)
        object_name = f"{prefix}/{entity}.parquet"
        response = self._client.get_object(self._cfg["processed_bucket"], object_name)
        try:
            buf = io.BytesIO(response.read())
        finally:
            response.close()
            response.release_conn()
        return pd.read_parquet(buf, engine="pyarrow")

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        return [obj.object_name for obj in self._client.list_objects(bucket, prefix=prefix, recursive=True)]
