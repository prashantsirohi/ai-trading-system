"""Stock investigator stage."""

from __future__ import annotations

import time

from ai_trading_system.domains.investigator import InvestigatorService
from ai_trading_system.pipeline.contracts import StageContext, StageResult


class InvestigatorStage:
    """Post-rank decision context stage."""

    name = "investigator"

    def __init__(self, service: InvestigatorService | None = None):
        self.service = service or InvestigatorService()

    def run(self, context: StageContext) -> StageResult:
        if context.params.get("smoke"):
            raise RuntimeError("Smoke mode is disabled because synthetic investigator artifacts are not allowed.")
        started = time.perf_counter_ns()
        legacy = self.service.run(context)
        _record(context, "investigator.evaluate_full", started, legacy)
        if str(context.params.get("opportunity_scan_routing_mode", "off")).lower() == "off":
            return legacy
        started = time.perf_counter_ns()
        routed = self.service.run_routed_shadow(context)
        _record(context, "investigator.evaluate_position_monitor", started, routed)
        return StageResult(
            artifacts=[*legacy.artifacts, *routed.artifacts],
            metadata={**legacy.metadata, "phase3b_routed_shadow": routed.metadata},
        )


def _record(context: StageContext, operation: str, started_ns: int, result: StageResult) -> None:
    if context.performance is None:
        return
    rows = sum(artifact.row_count or 0 for artifact in result.artifacts)
    context.performance.record_duration(
        stage_name="investigator", operation_name=operation,
        duration_ms=(time.perf_counter_ns() - started_ns) / 1_000_000.0,
        rows_out=rows,
    )
