"""
Earned Value (EV) calculations for P6 project cost data.

Metrics computed:
  CPI   = EV / AC             (Cost Performance Index)
  SPI   = EV / PV             (Schedule Performance Index)
  CV    = EV - AC             (Cost Variance)
  SV    = EV - PV             (Schedule Variance)
  EAC   = BAC / CPI  (BAC_CPI method)  or  AC + (BAC - EV)  (AC_PLUS_REMAINING)
  VAC   = BAC - EAC           (Variance at Completion)
  TCPI  = (BAC - EV) / (BAC - AC)  (To-Complete Performance Index)
  CV%   = CV / BAC * 100
  SV%   = SV / BAC * 100
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if denominator == 0:
        return default
    return numerator / denominator


def calculate_project_ev_metrics(
    projects: list[dict[str, Any]],
    eac_method: str = "BAC_CPI",
    run_date: str | None = None,
) -> list[dict[str, Any]]:
    """
    Compute earned value metrics for each project.

    Args:
        projects:   List of project dicts with EV fields from OPC.
        eac_method: "BAC_CPI" or "AC_PLUS_REMAINING".
        run_date:   ISO date string for the pipeline run (defaults to today UTC).

    Returns:
        List of metric dicts ready for the P6_COST_METRICS table.
    """
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    metrics = []

    for p in projects:
        pv = float(p.get("PlannedValueCost") or 0)
        ev = float(p.get("EarnedValueCost") or 0)
        ac = float(p.get("ActualTotalCost") or 0)
        bac = float(p.get("BudgetAtCompletion") or p.get("TotalBudgetedCost") or 0)

        cpi = _safe_div(ev, ac)
        spi = _safe_div(ev, pv)
        cv = ev - ac
        sv = ev - pv

        if eac_method == "AC_PLUS_REMAINING":
            eac = ac + (bac - ev)
        else:
            eac = _safe_div(bac, cpi) if cpi else bac

        vac = bac - eac
        tcpi = _safe_div(bac - ev, bac - ac)
        cv_pct = _safe_div(cv, bac) * 100 if bac else 0.0
        sv_pct = _safe_div(sv, bac) * 100 if bac else 0.0

        metrics.append(
            {
                "PROJECT_OBJECT_ID": p.get("ObjectId"),
                "PROJECT_ID": p.get("Id"),
                "PROJECT_NAME": p.get("Name"),
                "DATA_DATE": p.get("DataDate"),
                "RUN_DATE": run_date,
                "PLANNED_VALUE": round(pv, 4),
                "EARNED_VALUE": round(ev, 4),
                "ACTUAL_COST": round(ac, 4),
                "BUDGET_AT_COMPLETION": round(bac, 4),
                "CPI": round(cpi, 6),
                "SPI": round(spi, 6),
                "COST_VARIANCE": round(cv, 4),
                "SCHEDULE_VARIANCE": round(sv, 4),
                "EAC": round(eac, 4),
                "VAC": round(vac, 4),
                "TCPI": round(tcpi, 6),
                "CV_PCT": round(cv_pct, 4),
                "SV_PCT": round(sv_pct, 4),
                "EAC_METHOD": eac_method,
            }
        )

    return metrics


def calculate_activity_ev_metrics(
    activities: list[dict[str, Any]],
    run_date: str | None = None,
) -> list[dict[str, Any]]:
    """Compute EV metrics at the activity level."""
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    metrics = []

    for a in activities:
        pv = float(a.get("PlannedTotalCost") or 0)
        ac = float(a.get("ActualTotalCost") or 0)
        ev = float(a.get("EarnedValueCost") or 0)
        bac = float(a.get("BudgetAtCompletion") or 0)

        cpi = _safe_div(ev, ac)
        cv = ev - ac

        metrics.append(
            {
                "ACTIVITY_OBJECT_ID": a.get("ObjectId"),
                "PROJECT_OBJECT_ID": a.get("ProjectObjectId"),
                "WBS_OBJECT_ID": a.get("WBSObjectId"),
                "ACTIVITY_ID": a.get("Id"),
                "ACTIVITY_NAME": a.get("Name"),
                "RUN_DATE": run_date,
                "PLANNED_VALUE": round(pv, 4),
                "EARNED_VALUE": round(ev, 4),
                "ACTUAL_COST": round(ac, 4),
                "BUDGET_AT_COMPLETION": round(bac, 4),
                "CPI": round(cpi, 6),
                "COST_VARIANCE": round(cv, 4),
                "PERCENT_COMPLETE": float(a.get("PercentComplete") or 0),
            }
        )

    return metrics
