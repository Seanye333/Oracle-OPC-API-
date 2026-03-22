"""Unit tests for Oracle and MinIO loaders — all external calls mocked."""
import io
import json
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from src.loaders.minio_loader import MinIOLoader
from src.loaders.oracle_loader import OracleLoader

# ======================================================================
# MinIO Loader Tests
# ======================================================================

MOCK_MINIO_CONFIG = {
    "endpoint": "localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "secure": False,
    "raw_bucket": "p6-raw",
    "processed_bucket": "p6-processed",
    "file_format": {"raw": "json", "processed": "parquet"},
}


@pytest.fixture
def mock_minio_client():
    with patch("src.loaders.minio_loader.Minio") as mock_cls:
        instance = MagicMock()
        mock_cls.return_value = instance
        instance.bucket_exists.return_value = True
        yield instance


class TestMinIOLoader:

    def test_upload_raw_calls_put_object(self, mock_minio_client):
        loader = MinIOLoader(config=MOCK_MINIO_CONFIG)
        records = [{"ObjectId": 1, "Name": "Test Project"}]
        loader.upload_raw("projects", records, run_date="2026-01-15")
        assert mock_minio_client.put_object.called
        call_kwargs = mock_minio_client.put_object.call_args
        assert call_kwargs.kwargs["bucket_name"] == "p6-raw"
        assert "2026/01/15/projects.json" in call_kwargs.kwargs["object_name"]

    def test_upload_raw_json_content(self, mock_minio_client):
        loader = MinIOLoader(config=MOCK_MINIO_CONFIG)
        records = [{"ObjectId": 42, "Id": "PRJ-042"}]
        captured_data = {}

        def capture_put(**kwargs):
            captured_data["data"] = kwargs["data"].read()

        mock_minio_client.put_object.side_effect = capture_put
        loader.upload_raw("projects", records, run_date="2026-01-15")
        parsed = json.loads(captured_data["data"])
        assert parsed[0]["ObjectId"] == 42

    def test_upload_processed_creates_parquet(self, mock_minio_client):
        loader = MinIOLoader(config=MOCK_MINIO_CONFIG)
        records = [{"PROJECT_OBJECT_ID": 1, "CPI": 0.95, "SPI": 1.02}]
        loader.upload_processed("cost_metrics", records, run_date="2026-01-15")
        assert mock_minio_client.put_object.called
        call_kwargs = mock_minio_client.put_object.call_args.kwargs
        assert call_kwargs["bucket_name"] == "p6-processed"
        assert "cost_metrics.parquet" in call_kwargs["object_name"]

    def test_download_raw_returns_records(self, mock_minio_client):
        records = [{"ObjectId": 1, "Name": "Alpha"}]
        response_mock = MagicMock()
        response_mock.read.return_value = json.dumps(records).encode()
        mock_minio_client.get_object.return_value = response_mock

        loader = MinIOLoader(config=MOCK_MINIO_CONFIG)
        result = loader.download_raw("projects", run_date="2026-01-15")
        assert result == records

    def test_download_processed_returns_dataframe(self, mock_minio_client):
        df_original = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        buf = io.BytesIO()
        df_original.to_parquet(buf, index=False)
        buf.seek(0)

        response_mock = MagicMock()
        response_mock.read.return_value = buf.read()
        mock_minio_client.get_object.return_value = response_mock

        loader = MinIOLoader(config=MOCK_MINIO_CONFIG)
        result = loader.download_processed("cost_metrics", run_date="2026-01-15")
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["A", "B"]
        assert len(result) == 2

    def test_ensure_buckets_creates_missing_bucket(self, mock_minio_client):
        mock_minio_client.bucket_exists.return_value = False
        loader = MinIOLoader(config=MOCK_MINIO_CONFIG)
        # Should call make_bucket for both buckets
        assert mock_minio_client.make_bucket.call_count == 2

    def test_date_prefix_format(self, mock_minio_client):
        loader = MinIOLoader(config=MOCK_MINIO_CONFIG)
        prefix = loader._date_prefix("2026-03-01")
        assert prefix == "2026/03/01"

    def test_upload_raw_returns_object_path(self, mock_minio_client):
        loader = MinIOLoader(config=MOCK_MINIO_CONFIG)
        path = loader.upload_raw("activities", [{"id": 1}], run_date="2026-01-15")
        assert "p6-raw" in path
        assert "activities.json" in path


# ======================================================================
# Oracle Loader Tests
# ======================================================================

MOCK_ORACLE_CONFIG = {
    "host": "localhost",
    "port": 1521,
    "service_name": "ORCL",
    "user": "p6_user",
    "password": "secret",
    "schema": "P6_DATA",
    "pool_min": 1,
    "pool_max": 2,
    "pool_increment": 1,
    "tables": {},
}


@pytest.fixture
def mock_oracle_pool():
    with patch("src.loaders.oracle_loader.oracledb") as mock_db:
        pool = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        mock_db.create_pool.return_value = pool
        pool.acquire.return_value = conn
        conn.cursor.return_value = cursor
        yield pool, conn, cursor, mock_db


class TestOracleLoader:

    def test_connect_creates_pool(self, mock_oracle_pool):
        pool, conn, cursor, mock_db = mock_oracle_pool
        loader = OracleLoader(config=MOCK_ORACLE_CONFIG)
        loader.connect()
        assert mock_db.create_pool.called

    def test_upsert_projects_calls_executemany(self, mock_oracle_pool):
        pool, conn, cursor, mock_db = mock_oracle_pool
        loader = OracleLoader(config=MOCK_ORACLE_CONFIG)
        loader.connect()
        records = [{"ObjectId": 1, "Id": "P1", "Name": "Test"}]
        loader.upsert_projects(records)
        assert cursor.executemany.called

    def test_upsert_empty_records_skips_db(self, mock_oracle_pool):
        pool, conn, cursor, mock_db = mock_oracle_pool
        loader = OracleLoader(config=MOCK_ORACLE_CONFIG)
        loader.connect()
        result = loader.upsert_projects([])
        assert result == 0
        assert not cursor.executemany.called

    def test_upsert_commits_on_success(self, mock_oracle_pool):
        pool, conn, cursor, mock_db = mock_oracle_pool
        loader = OracleLoader(config=MOCK_ORACLE_CONFIG)
        loader.connect()
        loader.upsert_wbs([{"ObjectId": 10}])
        conn.commit.assert_called()

    def test_upsert_rollback_on_error(self, mock_oracle_pool):
        pool, conn, cursor, mock_db = mock_oracle_pool
        cursor.executemany.side_effect = Exception("DB error")
        loader = OracleLoader(config=MOCK_ORACLE_CONFIG)
        loader.connect()
        with pytest.raises(Exception, match="DB error"):
            loader.upsert_cost_metrics([{"PROJECT_OBJECT_ID": 1}])
        conn.rollback.assert_called()

    def test_close_releases_pool(self, mock_oracle_pool):
        pool, conn, cursor, mock_db = mock_oracle_pool
        loader = OracleLoader(config=MOCK_ORACLE_CONFIG)
        loader.connect()
        loader.close()
        pool.close.assert_called_once()

    def test_context_manager_auto_close(self, mock_oracle_pool):
        pool, conn, cursor, mock_db = mock_oracle_pool
        with OracleLoader(config=MOCK_ORACLE_CONFIG) as loader:
            assert loader._pool is not None
        pool.close.assert_called_once()
