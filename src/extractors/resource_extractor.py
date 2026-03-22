"""Extract resource master data and assignments from OPC."""
from __future__ import annotations

from typing import Any

from src.p6_client import OPCClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ResourceExtractor:
    def __init__(self, client: OPCClient):
        self._client = client

    def extract_resources(self) -> list[dict[str, Any]]:
        resources = self._client.get_resources()
        logger.info("Resources extracted", extra={"count": len(resources)})
        return resources

    def extract_roles(self) -> list[dict[str, Any]]:
        roles = self._client.get_roles()
        logger.info("Roles extracted", extra={"count": len(roles)})
        return roles

    def extract_assignments(self, project_ids: list[int]) -> list[dict[str, Any]]:
        all_assignments: list[dict[str, Any]] = []
        for pid in project_ids:
            assignments = self._client.get_resource_assignments(pid)
            all_assignments.extend(assignments)
            logger.debug("Assignments fetched", extra={"project_id": pid, "count": len(assignments)})
        logger.info("All assignments extracted", extra={"total": len(all_assignments)})
        return all_assignments

    def extract_all(self, project_ids: list[int]) -> dict[str, list[dict[str, Any]]]:
        return {
            "resources": self.extract_resources(),
            "roles": self.extract_roles(),
            "resource_assignments": self.extract_assignments(project_ids),
        }
