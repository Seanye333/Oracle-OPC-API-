"""
Resource utilization calculations for P6 resource assignment data.

Metrics computed per assignment:
  utilization_pct  = (ActualUnits / PlannedUnits) * 100
  unit_variance    = PlannedUnits - ActualUnits
  cost_variance    = PlannedCost - ActualCost
  over_allocated   = ActualUnits > PlannedUnits

Summary metrics per resource (across all projects):
  total_planned_units
  total_actual_units
  total_remaining_units
  overall_utilization_pct
  total_planned_cost
  total_actual_cost
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    return numerator / denominator if denominator else default


def calculate_assignment_metrics(
    assignments: list[dict[str, Any]],
    run_date: str | None = None,
) -> list[dict[str, Any]]:
    """Compute per-assignment utilization metrics."""
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    metrics = []

    for a in assignments:
        planned = float(a.get("PlannedUnits") or 0)
        actual = float(a.get("ActualUnits") or 0)
        remaining = float(a.get("RemainingUnits") or 0)
        planned_cost = float(a.get("PlannedCost") or 0)
        actual_cost = float(a.get("ActualCost") or 0)
        remaining_cost = float(a.get("RemainingCost") or 0)

        utilization_pct = _safe_div(actual, planned) * 100
        unit_variance = planned - actual
        cost_variance = planned_cost - actual_cost
        over_allocated = actual > planned

        metrics.append(
            {
                "ASSIGNMENT_OBJECT_ID": a.get("ObjectId"),
                "ACTIVITY_OBJECT_ID": a.get("ActivityObjectId"),
                "PROJECT_OBJECT_ID": a.get("ProjectObjectId"),
                "RESOURCE_OBJECT_ID": a.get("ResourceObjectId"),
                "ROLE_OBJECT_ID": a.get("RoleObjectId"),
                "RUN_DATE": run_date,
                "PLANNED_UNITS": round(planned, 4),
                "ACTUAL_UNITS": round(actual, 4),
                "REMAINING_UNITS": round(remaining, 4),
                "PLANNED_COST": round(planned_cost, 4),
                "ACTUAL_COST": round(actual_cost, 4),
                "REMAINING_COST": round(remaining_cost, 4),
                "UTILIZATION_PCT": round(utilization_pct, 4),
                "UNIT_VARIANCE": round(unit_variance, 4),
                "COST_VARIANCE": round(cost_variance, 4),
                "OVER_ALLOCATED": 1 if over_allocated else 0,
                "PLANNED_START": a.get("PlannedStartDate"),
                "PLANNED_FINISH": a.get("PlannedFinishDate"),
                "ACTUAL_START": a.get("ActualStartDate"),
                "ACTUAL_FINISH": a.get("ActualFinishDate"),
            }
        )

    return metrics


def calculate_resource_summary(
    assignment_metrics: list[dict[str, Any]],
    run_date: str | None = None,
) -> list[dict[str, Any]]:
    """Aggregate assignment metrics to produce per-resource summary."""
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    agg: dict[int, dict[str, Any]] = defaultdict(
        lambda: {
            "total_planned_units": 0.0,
            "total_actual_units": 0.0,
            "total_remaining_units": 0.0,
            "total_planned_cost": 0.0,
            "total_actual_cost": 0.0,
            "total_remaining_cost": 0.0,
            "assignment_count": 0,
            "over_allocated_count": 0,
        }
    )

    for m in assignment_metrics:
        rid = m["RESOURCE_OBJECT_ID"]
        agg[rid]["total_planned_units"] += m["PLANNED_UNITS"]
        agg[rid]["total_actual_units"] += m["ACTUAL_UNITS"]
        agg[rid]["total_remaining_units"] += m["REMAINING_UNITS"]
        agg[rid]["total_planned_cost"] += m["PLANNED_COST"]
        agg[rid]["total_actual_cost"] += m["ACTUAL_COST"]
        agg[rid]["total_remaining_cost"] += m["REMAINING_COST"]
        agg[rid]["assignment_count"] += 1
        agg[rid]["over_allocated_count"] += m["OVER_ALLOCATED"]

    summaries = []
    for rid, data in agg.items():
        utilization = _safe_div(data["total_actual_units"], data["total_planned_units"]) * 100
        summaries.append(
            {
                "RESOURCE_OBJECT_ID": rid,
                "RUN_DATE": run_date,
                "TOTAL_PLANNED_UNITS": round(data["total_planned_units"], 4),
                "TOTAL_ACTUAL_UNITS": round(data["total_actual_units"], 4),
                "TOTAL_REMAINING_UNITS": round(data["total_remaining_units"], 4),
                "TOTAL_PLANNED_COST": round(data["total_planned_cost"], 4),
                "TOTAL_ACTUAL_COST": round(data["total_actual_cost"], 4),
                "TOTAL_REMAINING_COST": round(data["total_remaining_cost"], 4),
                "OVERALL_UTILIZATION_PCT": round(utilization, 4),
                "ASSIGNMENT_COUNT": data["assignment_count"],
                "OVER_ALLOCATED_COUNT": data["over_allocated_count"],
            }
        )

    return summaries
