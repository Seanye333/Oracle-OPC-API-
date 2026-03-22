"""Extract earned value / cost data for each project from OPC."""
from __future__ import annotations

from typing import Any

from src.p6_client import OPCClient
from src.utils.logger import get_logger

logger = get_logger(__name__)

# EV spreadsheet fields available in OPC for period-level data
_EV_SPREAD_FIELDS = [
    "ProjectObjectId",
    "StartDate",
    "FinishDate",
    "PlannedValue",
    "EarnedValue",
    "ActualCost",
    "BudgetAtCompletion",
    "PlannedDuration",
    "ActualDuration",
    "RemainingDuration",
]


class CostExtractor:
    def __init__(self, client: OPCClient):
        self._client = client

    def extract_project_cost(self, projects: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return project-level cost/EV fields already embedded in project records."""
        cost_fields = [
            "ObjectId", "Id", "Name", "DataDate",
            "TotalBudgetedCost", "ActualTotalCost",
            "EarnedValueCost", "PlannedValueCost",
            "CostPerformanceIndex", "SchedulePerformanceIndex",
            "EstimateAtCompletion", "BudgetAtCompletion",
            "SummaryBudget",
        ]
        records = []
        for p in projects:
            record = {k: p.get(k) for k in cost_fields}
            records.append(record)
        logger.info("Project cost records extracted", extra={"count": len(records)})
        return records

    def extract_activity_cost(self, activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter activity records to cost-relevant fields."""
        cost_fields = [
            "ObjectId", "ProjectObjectId", "WBSObjectId", "Id", "Name",
            "PlannedTotalCost", "ActualTotalCost", "BudgetAtCompletion",
            "EarnedValueCost", "PlannedDuration", "RemainingDuration",
            "PercentComplete", "PlannedStartDate", "PlannedFinishDate",
            "ActualStartDate", "ActualFinishDate", "Status",
        ]
        records = []
        for a in activities:
            record = {k: a.get(k) for k in cost_fields}
            records.append(record)
        logger.info("Activity cost records extracted", extra={"count": len(records)})
        return records
