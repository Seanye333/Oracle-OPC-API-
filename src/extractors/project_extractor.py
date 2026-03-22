"""Extract project list, WBS hierarchy, and activities from OPC."""
from __future__ import annotations

from typing import Any

from src.p6_client import OPCClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ProjectExtractor:
    def __init__(self, client: OPCClient, status_filter: list[str] | None = None):
        self._client = client
        self._status_filter = status_filter or ["Active", "What-if"]

    def extract_projects(self) -> list[dict[str, Any]]:
        logger.info("Extracting projects", extra={"status_filter": self._status_filter})
        projects = self._client.get_projects(self._status_filter)
        logger.info("Projects extracted", extra={"count": len(projects)})
        return projects

    def extract_wbs(self, project_ids: list[int]) -> list[dict[str, Any]]:
        all_wbs: list[dict[str, Any]] = []
        for pid in project_ids:
            wbs = self._client.get_wbs(pid)
            all_wbs.extend(wbs)
            logger.debug("WBS fetched", extra={"project_id": pid, "wbs_count": len(wbs)})
        logger.info("WBS extraction complete", extra={"total": len(all_wbs)})
        return all_wbs

    def extract_activities(self, project_ids: list[int]) -> list[dict[str, Any]]:
        all_activities: list[dict[str, Any]] = []
        for pid in project_ids:
            acts = self._client.get_activities(pid)
            all_activities.extend(acts)
            logger.debug("Activities fetched", extra={"project_id": pid, "count": len(acts)})
        logger.info("Activities extraction complete", extra={"total": len(all_activities)})
        return all_activities

    def extract_all(self) -> dict[str, list[dict[str, Any]]]:
        projects = self.extract_projects()
        project_ids = [p["ObjectId"] for p in projects]
        wbs = self.extract_wbs(project_ids)
        activities = self.extract_activities(project_ids)
        return {
            "projects": projects,
            "wbs": wbs,
            "activities": activities,
        }
