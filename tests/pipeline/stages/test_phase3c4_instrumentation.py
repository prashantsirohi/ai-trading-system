from __future__ import annotations

from pathlib import Path

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult, compute_stage_input_hash
from ai_trading_system.pipeline.stages.investigator import InvestigatorStage
from ai_trading_system.platform.telemetry import PerformanceCollector


class _Service:
    def run(self, context: StageContext) -> StageResult:
        return StageResult(artifacts=[StageArtifact("legacy", str(context.project_root / "legacy.csv"), row_count=3)])

    def run_routed_shadow(self, context: StageContext) -> StageResult:
        return StageResult(artifacts=[StageArtifact("routed", str(context.project_root / "routed.csv"), row_count=2)])


def test_investigator_stage_emits_operation_metrics_without_changing_result(tmp_path: Path) -> None:
    collector = PerformanceCollector(run_id="run", as_of="2026-07-15")
    context = StageContext(
        project_root=tmp_path, db_path=tmp_path / "db.duckdb", run_id="run",
        run_date="2026-07-15", stage_name="investigator", attempt_number=1,
        params={"opportunity_scan_routing_mode": "shadow"}, performance=collector,
    )
    result = InvestigatorStage(_Service()).run(context)
    assert [item.artifact_type for item in result.artifacts] == ["legacy", "routed"]
    assert [item.operation_name for item in collector.metrics] == [
        "investigator.evaluate_full", "investigator.evaluate_position_monitor",
    ]


def test_performance_params_do_not_change_stage_input_hash() -> None:
    base = compute_stage_input_hash(stage_name="rank", run_date="2026-07-15", params={"exchange": "NSE"}, artifacts={})
    instrumented = compute_stage_input_hash(
        stage_name="rank", run_date="2026-07-15",
        params={"exchange": "NSE", "performance_instrumentation_enabled": True, "performance_fail_pipeline": False}, artifacts={},
    )
    assert instrumented == base
