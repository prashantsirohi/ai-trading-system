"""Feature computation stage."""

from __future__ import annotations

from typing import Callable, Dict, Optional

from run.stages.base import StageContext, StageResult
from services.features import FeaturesOrchestrationService


class FeaturesStage:
    """Thin wrapper around feature-stage orchestration services."""

    name = "features"

    def __init__(self, operation: Optional[Callable[[StageContext], Dict]] = None):
        self.operation = operation
        self.service = FeaturesOrchestrationService(operation=operation)

    def run(self, context: StageContext) -> StageResult:
        if context.params.get("smoke"):
            raise RuntimeError("Smoke mode is disabled because synthetic feature artifacts have been removed.")
        return self.service.run(context, record_snapshot=self._record_snapshot)

    def _run_default(self, context: StageContext) -> Dict:
        return self.service.run_default(context, record_snapshot=self._record_snapshot)

    def _record_snapshot(self, context: StageContext) -> tuple[int, int, int]:
        return self.service.record_snapshot(context)
