"""
Oracle Database loader using python-oracledb thin mode.
All writes use MERGE (upsert) via PL/SQL procedures defined in sql/upsert_procedures.sql.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

import oracledb

from src.utils.config import get_oracle_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class OracleLoader:
    def __init__(self, config: dict[str, Any] | None = None):
        self._cfg = config or get_oracle_config()
        self._pool: oracledb.ConnectionPool | None = None

    def _get_dsn(self) -> str:
        return (
            f"{self._cfg['host']}:{self._cfg['port']}/"
            f"{self._cfg['service_name']}"
        )

    def connect(self) -> None:
        """Create connection pool. Call once before using the loader."""
        self._pool = oracledb.create_pool(
            user=self._cfg["user"],
            password=self._cfg["password"],
            dsn=self._get_dsn(),
            min=self._cfg["pool_min"],
            max=self._cfg["pool_max"],
            increment=self._cfg["pool_increment"],
        )
        logger.info("Oracle connection pool created", extra={"dsn": self._get_dsn()})

    def close(self) -> None:
        if self._pool:
            self._pool.close()
            logger.info("Oracle connection pool closed")

    @contextmanager
    def _get_connection(self) -> Generator[oracledb.Connection, None, None]:
        if not self._pool:
            self.connect()
        conn = self._pool.acquire()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.release(conn)

    def _bulk_execute(
        self,
        conn: oracledb.Connection,
        procedure: str,
        records: list[dict[str, Any]],
        batch_size: int = 500,
    ) -> int:
        """Call a stored procedure for each record via executemany."""
        if not records:
            return 0
        cursor = conn.cursor()
        keys = list(records[0].keys())
        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            data = [[r.get(k) for k in keys] for r in batch]
            cursor.executemany(procedure, data)
            total += len(batch)
        cursor.close()
        logger.info("Upserted records", extra={"procedure": procedure, "count": total})
        return total

    # ------------------------------------------------------------------
    # Public upsert methods
    # ------------------------------------------------------------------

    def upsert_projects(self, records: list[dict[str, Any]]) -> int:
        with self._get_connection() as conn:
            return self._bulk_execute(
                conn,
                "BEGIN PKG_P6_PIPELINE.UPSERT_PROJECT(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12); END;",
                records,
            )

    def upsert_wbs(self, records: list[dict[str, Any]]) -> int:
        with self._get_connection() as conn:
            return self._bulk_execute(
                conn,
                "BEGIN PKG_P6_PIPELINE.UPSERT_WBS(:1,:2,:3,:4,:5,:6,:7,:8); END;",
                records,
            )

    def upsert_activities(self, records: list[dict[str, Any]]) -> int:
        with self._get_connection() as conn:
            return self._bulk_execute(
                conn,
                "BEGIN PKG_P6_PIPELINE.UPSERT_ACTIVITY(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17); END;",
                records,
            )

    def upsert_resources(self, records: list[dict[str, Any]]) -> int:
        with self._get_connection() as conn:
            return self._bulk_execute(
                conn,
                "BEGIN PKG_P6_PIPELINE.UPSERT_RESOURCE(:1,:2,:3,:4,:5,:6,:7,:8,:9); END;",
                records,
            )

    def upsert_resource_assignments(self, records: list[dict[str, Any]]) -> int:
        with self._get_connection() as conn:
            return self._bulk_execute(
                conn,
                "BEGIN PKG_P6_PIPELINE.UPSERT_RESOURCE_ASSIGNMENT(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19); END;",
                records,
            )

    def upsert_cost_metrics(self, records: list[dict[str, Any]]) -> int:
        with self._get_connection() as conn:
            return self._bulk_execute(
                conn,
                "BEGIN PKG_P6_PIPELINE.UPSERT_COST_METRIC(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19); END;",
                records,
            )

    def upsert_resource_metrics(self, records: list[dict[str, Any]]) -> int:
        with self._get_connection() as conn:
            return self._bulk_execute(
                conn,
                "BEGIN PKG_P6_PIPELINE.UPSERT_RESOURCE_METRIC(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12); END;",
                records,
            )

    def __enter__(self) -> "OracleLoader":
        self.connect()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
