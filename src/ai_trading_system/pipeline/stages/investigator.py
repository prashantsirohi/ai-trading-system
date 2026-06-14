"""Stock investigator stage."""

from __future__ import annotations

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
        return self.service.run(context)
