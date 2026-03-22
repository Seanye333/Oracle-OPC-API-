"""
Oracle Primavera Cloud (OPC) REST API v2 Client
Handles OAuth2 client_credentials, pagination, and retries.
"""
from __future__ import annotations

from typing import Any, Generator

import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.utils.config import get_opc_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class OPCClient:
    """Thread-safe OPC REST API v2 client with auto token refresh and pagination."""

    def __init__(self, config: dict[str, Any] | None = None):
        self._cfg = config or get_opc_config()
        self._base_url = self._cfg["base_url"].rstrip("/")
        self._timeout = self._cfg["timeout"]
        self._page_size = self._cfg["page_size"]
        self._session = self._create_session()

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _create_session(self) -> OAuth2Session:
        client = BackendApplicationClient(client_id=self._cfg["client_id"])
        session = OAuth2Session(client=client)
        self._fetch_token(session)
        return session

    def _fetch_token(self, session: OAuth2Session) -> None:
        token = session.fetch_token(
            token_url=self._cfg["token_url"],
            client_id=self._cfg["client_id"],
            client_secret=self._cfg["client_secret"],
            scope=self._cfg["scope"] or None,
        )
        logger.info("OPC token acquired", extra={"expires_at": token.get("expires_at")})

    def _ensure_token(self) -> None:
        if self._session.token.get("expires_in", 0) < 60:
            self._fetch_token(self._session)

    # ------------------------------------------------------------------
    # Core HTTP
    # ------------------------------------------------------------------

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    )
    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self._ensure_token()
        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        response = self._session.get(url, params=params, timeout=self._timeout)
        response.raise_for_status()
        return response.json()

    def _paginate(
        self, endpoint: str, fields: list[str], extra_filter: str | None = None
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Yield pages of records from a paginated OPC endpoint."""
        offset = 0
        field_str = ",".join(fields)
        while True:
            params: dict[str, Any] = {
                "fields": field_str,
                "limit": self._page_size,
                "offset": offset,
            }
            if extra_filter:
                params["filter"] = extra_filter
            data = self._get(endpoint, params=params)
            records = data if isinstance(data, list) else data.get("items", [])
            if not records:
                break
            yield records
            if len(records) < self._page_size:
                break
            offset += self._page_size

    def _get_all(self, endpoint: str, fields: list[str], extra_filter: str | None = None) -> list[dict[str, Any]]:
        """Collect all paginated records into a single list."""
        result: list[dict[str, Any]] = []
        for page in self._paginate(endpoint, fields, extra_filter):
            result.extend(page)
        logger.info("Fetched records", extra={"endpoint": endpoint, "count": len(result)})
        return result

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def get_projects(self, status_filter: list[str] | None = None) -> list[dict[str, Any]]:
        fields = self._cfg["fields"]["project_fields"]
        f = None
        if status_filter:
            statuses = ",".join(f'"{s}"' for s in status_filter)
            f = f"Status IN ({statuses})"
        return self._get_all("projects", fields, extra_filter=f)

    def get_wbs(self, project_object_id: int) -> list[dict[str, Any]]:
        fields = self._cfg["fields"]["wbs_fields"]
        return self._get_all("wbs", fields, extra_filter=f"ProjectObjectId = {project_object_id}")

    def get_activities(self, project_object_id: int) -> list[dict[str, Any]]:
        fields = self._cfg["fields"]["activity_fields"]
        return self._get_all(
            "activities", fields, extra_filter=f"ProjectObjectId = {project_object_id}"
        )

    def get_resource_assignments(self, project_object_id: int) -> list[dict[str, Any]]:
        fields = self._cfg["fields"]["resource_assignment_fields"]
        return self._get_all(
            "activityresourceassignments",
            fields,
            extra_filter=f"ProjectObjectId = {project_object_id}",
        )

    def get_resources(self) -> list[dict[str, Any]]:
        fields = self._cfg["fields"]["resource_fields"]
        return self._get_all("resources", fields)

    def get_roles(self) -> list[dict[str, Any]]:
        role_fields = ["ObjectId", "Id", "Name", "PricePerUnit", "UnitOfMeasure", "IsActive"]
        return self._get_all("roles", role_fields)

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "OPCClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
