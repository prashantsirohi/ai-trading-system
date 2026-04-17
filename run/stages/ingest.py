"""Ingest stage wrapper for resilient pipeline orchestration."""

from __future__ import annotations

from typing import Callable, Dict, Optional

import pandas as pd

from run.stages.base import StageContext, StageResult
from services.ingest import IngestOrchestrationService


def classify_freshness_status(target_end_date: str, latest_available_date: str | None) -> str:
    if latest_available_date is None:
        return "stale"
    if str(latest_available_date) == str(target_end_date):
        return "fresh"
    return "delayed"


class IngestStage:
    """Thin wrapper around ingest orchestration services."""

    name = "ingest"

    def __init__(self, operation: Optional[Callable[[StageContext], Dict]] = None):
        self.operation = operation
        self.service = IngestOrchestrationService(operation=operation)

    def run(self, context: StageContext) -> StageResult:
        if context.params.get("smoke"):
            raise RuntimeError("Smoke mode is disabled because synthetic ingest artifacts have been removed.")
        return self.service.run(context)

    def _run_default(self, context: StageContext) -> Dict:
        return self.service.run_default(context)

    def _run_bhavcopy_validation(self, context: StageContext, ingest_payload: Dict) -> Dict:
        return self.service.run_bhavcopy_validation(context, ingest_payload)

    def _resolve_validation_scope_symbols(self, ingest_payload: Dict, catalog_df: pd.DataFrame) -> set[str]:
        return self.service.resolve_validation_scope_symbols(ingest_payload, catalog_df)

    def _load_catalog_close_frame(self, context: StageContext, validation_date: str) -> pd.DataFrame:
        return self.service.load_catalog_close_frame(context, validation_date)

    def _load_reference_close_frame(
        self,
        *,
        context: StageContext,
        validation_date: str,
        symbol_ids: list[str],
    ) -> tuple[pd.DataFrame, str]:
        return self.service.load_reference_close_frame(
            context=context,
            validation_date=validation_date,
            symbol_ids=symbol_ids,
        )

    def _load_bhavcopy_close_frame(self, context: StageContext, validation_date: str) -> tuple[pd.DataFrame, str]:
        return self.service.load_bhavcopy_close_frame(context, validation_date)

    def _load_yfinance_close_frame(self, *, validation_date: str, symbol_ids: list[str]) -> tuple[pd.DataFrame, str]:
        return self.service.load_yfinance_close_frame(validation_date=validation_date, symbol_ids=symbol_ids)

    def _run_delivery_collection(self, context: StageContext, ingest_payload: Dict) -> Dict:
        return self.service.run_delivery_collection(context, ingest_payload)
