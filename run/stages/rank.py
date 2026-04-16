"""Ranking stage with explicit artifact outputs."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

import pandas as pd

from run.stages.base import StageContext, StageResult
from services.rank import (
    RankOrchestrationService,
    augment_dashboard_payload_with_ml,
    build_dashboard_payload,
    summarize_task_statuses,
)


class RankStage:
    """Thin wrapper around rank-stage orchestration services."""

    name = "rank"

    def __init__(
        self,
        operation: Optional[Callable[[StageContext], Dict[str, pd.DataFrame]]] = None,
        ml_overlay_builder: Optional[Callable[[StageContext, pd.DataFrame], Dict[str, Any]]] = None,
    ):
        self.operation = operation
        self.ml_overlay_builder = ml_overlay_builder
        self.service = RankOrchestrationService(
            operation=operation,
            ml_overlay_builder=ml_overlay_builder,
        )

    def run(self, context: StageContext) -> StageResult:
        if context.params.get("smoke"):
            raise RuntimeError("Smoke mode is disabled because synthetic ranking artifacts have been removed.")
        return self.service.run(context, dashboard_payload_builder=self._build_dashboard_payload)

    def _run_default(self, context: StageContext) -> Dict[str, pd.DataFrame]:
        return self.service.run_default(context, dashboard_payload_builder=self._build_dashboard_payload)

    def _apply_ml_overlay(
        self,
        *,
        context: StageContext,
        outputs: Dict[str, pd.DataFrame],
        stage_metadata: Dict[str, Any],
        dashboard_payload: Optional[Dict[str, object]],
    ) -> tuple[Dict[str, pd.DataFrame], Dict[str, Any], Optional[Dict[str, object]], Dict[int, Dict[str, Any]]]:
        return self.service.apply_ml_overlay(
            context=context,
            outputs=outputs,
            stage_metadata=stage_metadata,
            dashboard_payload=dashboard_payload,
        )

    def _build_dashboard_payload(
        self,
        context: StageContext,
        ranked_df: pd.DataFrame,
        breakout_df: pd.DataFrame,
        pattern_df: pd.DataFrame,
        stock_scan_df: pd.DataFrame,
        sector_dashboard_df: pd.DataFrame,
        warnings: list[str],
        trust_summary: Dict[str, Any] | None = None,
        task_status: Dict[str, Any] | None = None,
    ) -> Dict[str, object]:
        return build_dashboard_payload(
            context=context,
            ranked_df=ranked_df,
            breakout_df=breakout_df,
            pattern_df=pattern_df,
            stock_scan_df=stock_scan_df,
            sector_dashboard_df=sector_dashboard_df,
            warnings=warnings,
            trust_summary=trust_summary,
            task_status=task_status,
        )

    def _summarize_task_statuses(self, task_status: Dict[str, Any]) -> Dict[str, int]:
        return summarize_task_statuses(task_status)

    def _augment_dashboard_payload_with_ml(
        self,
        dashboard_payload: Optional[Dict[str, object]],
        *,
        ml_status: str,
        ml_mode: str,
        ml_overlay_df: pd.DataFrame,
    ) -> Optional[Dict[str, object]]:
        return augment_dashboard_payload_with_ml(
            dashboard_payload,
            ml_status=ml_status,
            ml_mode=ml_mode,
            ml_overlay_df=ml_overlay_df,
        )
