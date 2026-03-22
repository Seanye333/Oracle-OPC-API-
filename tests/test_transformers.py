"""Unit tests for cost and resource calculators."""
import pytest

from src.transformers.cost_calculator import (
    calculate_project_ev_metrics,
    calculate_activity_ev_metrics,
)
from src.transformers.resource_calculator import (
    calculate_assignment_metrics,
    calculate_resource_summary,
)

RUN_DATE = "2026-01-15"


# ======================================================================
# Cost Calculator
# ======================================================================

class TestCostCalculator:

    def _make_project(self, pv, ev, ac, bac, obj_id=1):
        return {
            "ObjectId": obj_id,
            "Id": f"PRJ-{obj_id:03d}",
            "Name": f"Test Project {obj_id}",
            "DataDate": "2026-01-10",
            "PlannedValueCost": pv,
            "EarnedValueCost": ev,
            "ActualTotalCost": ac,
            "BudgetAtCompletion": bac,
            "TotalBudgetedCost": bac,
        }

    def test_cpi_calculation(self):
        project = self._make_project(pv=1000, ev=800, ac=1000, bac=2000)
        metrics = calculate_project_ev_metrics([project], run_date=RUN_DATE)
        assert len(metrics) == 1
        m = metrics[0]
        assert m["CPI"] == pytest.approx(0.8, rel=1e-4)

    def test_spi_calculation(self):
        project = self._make_project(pv=1000, ev=800, ac=900, bac=2000)
        m = calculate_project_ev_metrics([project], run_date=RUN_DATE)[0]
        assert m["SPI"] == pytest.approx(0.8, rel=1e-4)

    def test_cost_variance(self):
        project = self._make_project(pv=1000, ev=900, ac=1000, bac=2000)
        m = calculate_project_ev_metrics([project], run_date=RUN_DATE)[0]
        assert m["COST_VARIANCE"] == pytest.approx(-100.0, rel=1e-4)

    def test_schedule_variance(self):
        project = self._make_project(pv=1000, ev=800, ac=900, bac=2000)
        m = calculate_project_ev_metrics([project], run_date=RUN_DATE)[0]
        assert m["SCHEDULE_VARIANCE"] == pytest.approx(-200.0, rel=1e-4)

    def test_eac_bac_cpi_method(self):
        # BAC=2000, EV=1000, AC=1000 → CPI=1.0 → EAC=2000/1.0=2000
        project = self._make_project(pv=1000, ev=1000, ac=1000, bac=2000)
        m = calculate_project_ev_metrics([project], eac_method="BAC_CPI", run_date=RUN_DATE)[0]
        assert m["EAC"] == pytest.approx(2000.0, rel=1e-4)

    def test_eac_ac_plus_remaining_method(self):
        # AC=1000, BAC=2000, EV=800 → EAC = 1000 + (2000 - 800) = 2200
        project = self._make_project(pv=1000, ev=800, ac=1000, bac=2000)
        m = calculate_project_ev_metrics([project], eac_method="AC_PLUS_REMAINING", run_date=RUN_DATE)[0]
        assert m["EAC"] == pytest.approx(2200.0, rel=1e-4)

    def test_tcpi_calculation(self):
        # TCPI = (BAC - EV) / (BAC - AC) = (2000-1000)/(2000-1200) = 1000/800 = 1.25
        project = self._make_project(pv=1000, ev=1000, ac=1200, bac=2000)
        m = calculate_project_ev_metrics([project], run_date=RUN_DATE)[0]
        assert m["TCPI"] == pytest.approx(1.25, rel=1e-4)

    def test_zero_ac_cpi_defaults_to_zero(self):
        project = self._make_project(pv=1000, ev=500, ac=0, bac=2000)
        m = calculate_project_ev_metrics([project], run_date=RUN_DATE)[0]
        assert m["CPI"] == 0.0

    def test_zero_pv_spi_defaults_to_zero(self):
        project = self._make_project(pv=0, ev=500, ac=400, bac=2000)
        m = calculate_project_ev_metrics([project], run_date=RUN_DATE)[0]
        assert m["SPI"] == 0.0

    def test_run_date_set_correctly(self):
        project = self._make_project(pv=1000, ev=1000, ac=1000, bac=2000)
        m = calculate_project_ev_metrics([project], run_date=RUN_DATE)[0]
        assert m["RUN_DATE"] == RUN_DATE

    def test_multiple_projects(self):
        projects = [
            self._make_project(1000, 800, 900, 2000, obj_id=1),
            self._make_project(500, 500, 500, 1000, obj_id=2),
        ]
        metrics = calculate_project_ev_metrics(projects, run_date=RUN_DATE)
        assert len(metrics) == 2
        assert metrics[0]["PROJECT_OBJECT_ID"] == 1
        assert metrics[1]["PROJECT_OBJECT_ID"] == 2

    def test_activity_ev_metrics(self):
        activity = {
            "ObjectId": 100,
            "ProjectObjectId": 1,
            "WBSObjectId": 10,
            "Id": "A-001",
            "Name": "Design",
            "PlannedTotalCost": 500,
            "ActualTotalCost": 600,
            "EarnedValueCost": 450,
            "BudgetAtCompletion": 500,
            "PercentComplete": 90,
        }
        m = calculate_activity_ev_metrics([activity], run_date=RUN_DATE)[0]
        assert m["CPI"] == pytest.approx(0.75, rel=1e-4)
        assert m["COST_VARIANCE"] == pytest.approx(-150.0, rel=1e-4)


# ======================================================================
# Resource Calculator
# ======================================================================

class TestResourceCalculator:

    def _make_assignment(self, obj_id, planned, actual, project_id=1, resource_id=10):
        return {
            "ObjectId": obj_id,
            "ActivityObjectId": 100,
            "ProjectObjectId": project_id,
            "ResourceObjectId": resource_id,
            "RoleObjectId": None,
            "PlannedUnits": planned,
            "ActualUnits": actual,
            "RemainingUnits": max(planned - actual, 0),
            "PlannedCost": planned * 100,
            "ActualCost": actual * 100,
            "RemainingCost": max(planned - actual, 0) * 100,
            "PlannedStartDate": "2026-01-01",
            "PlannedFinishDate": "2026-03-01",
            "ActualStartDate": "2026-01-02",
            "ActualFinishDate": None,
        }

    def test_utilization_100_percent(self):
        a = self._make_assignment(1, planned=10, actual=10)
        m = calculate_assignment_metrics([a], run_date=RUN_DATE)[0]
        assert m["UTILIZATION_PCT"] == pytest.approx(100.0, rel=1e-4)

    def test_utilization_50_percent(self):
        a = self._make_assignment(1, planned=10, actual=5)
        m = calculate_assignment_metrics([a], run_date=RUN_DATE)[0]
        assert m["UTILIZATION_PCT"] == pytest.approx(50.0, rel=1e-4)

    def test_over_allocation_flag(self):
        a = self._make_assignment(1, planned=8, actual=10)
        m = calculate_assignment_metrics([a], run_date=RUN_DATE)[0]
        assert m["OVER_ALLOCATED"] == 1

    def test_not_over_allocated(self):
        a = self._make_assignment(1, planned=10, actual=8)
        m = calculate_assignment_metrics([a], run_date=RUN_DATE)[0]
        assert m["OVER_ALLOCATED"] == 0

    def test_unit_variance(self):
        a = self._make_assignment(1, planned=10, actual=7)
        m = calculate_assignment_metrics([a], run_date=RUN_DATE)[0]
        assert m["UNIT_VARIANCE"] == pytest.approx(3.0, rel=1e-4)

    def test_zero_planned_utilization_defaults_to_zero(self):
        a = self._make_assignment(1, planned=0, actual=5)
        m = calculate_assignment_metrics([a], run_date=RUN_DATE)[0]
        assert m["UTILIZATION_PCT"] == 0.0

    def test_resource_summary_aggregates_correctly(self):
        assignments = [
            self._make_assignment(1, planned=10, actual=8, resource_id=10),
            self._make_assignment(2, planned=20, actual=15, resource_id=10),
        ]
        assignment_metrics = calculate_assignment_metrics(assignments, run_date=RUN_DATE)
        summary = calculate_resource_summary(assignment_metrics, run_date=RUN_DATE)

        assert len(summary) == 1
        s = summary[0]
        assert s["RESOURCE_OBJECT_ID"] == 10
        assert s["TOTAL_PLANNED_UNITS"] == pytest.approx(30.0, rel=1e-4)
        assert s["TOTAL_ACTUAL_UNITS"] == pytest.approx(23.0, rel=1e-4)
        assert s["ASSIGNMENT_COUNT"] == 2

    def test_resource_summary_multiple_resources(self):
        assignments = [
            self._make_assignment(1, planned=10, actual=10, resource_id=1),
            self._make_assignment(2, planned=10, actual=5, resource_id=2),
        ]
        metrics = calculate_assignment_metrics(assignments, run_date=RUN_DATE)
        summary = calculate_resource_summary(metrics, run_date=RUN_DATE)
        assert len(summary) == 2

    def test_over_allocated_count_in_summary(self):
        assignments = [
            self._make_assignment(1, planned=8, actual=10, resource_id=5),
            self._make_assignment(2, planned=8, actual=6, resource_id=5),
        ]
        metrics = calculate_assignment_metrics(assignments, run_date=RUN_DATE)
        summary = calculate_resource_summary(metrics, run_date=RUN_DATE)
        assert summary[0]["OVER_ALLOCATED_COUNT"] == 1
