"""Legacy weekly stock-stage adapter."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence

from ai_trading_system.domains.opportunities.compatibility import adapt_legacy_weekly_stage, map_legacy_stage
from ai_trading_system.domains.opportunities.contracts import StageSnapshot, StageStatus, StageTransitionReason, WeinsteinStage
from ai_trading_system.domains.opportunities.orchestration.contracts import AdaptedRecord, AdapterResult, AdapterWarning, LEGACY_STAGE_CONFIDENCE_VERSION, RejectedSourceRow, SourceDescriptor

from .common import as_date, as_datetime, as_float, confidence_band, first, normalize_exchange, normalize_symbol, row_identity


def adapt_stock_stage_rows(
    rows: Sequence[Mapping[str, Any]], *, source: SourceDescriptor, as_of: datetime,
    prior_locked_stages: Mapping[tuple[str, str], WeinsteinStage] | None = None,
) -> AdapterResult[AdaptedRecord[StageSnapshot]]:
    records: list[AdaptedRecord[StageSnapshot]] = []
    warnings: list[AdapterWarning] = []
    rejected: list[RejectedSourceRow] = []
    prior = prior_locked_stages or {}
    for row in rows:
        identity = row_identity(row)
        symbol = normalize_symbol(first(row, "symbol_id", "symbol", "ticker"))
        exchange = normalize_exchange(first(row, "exchange", "exchange_code"))
        if not symbol:
            rejected.append(RejectedSourceRow(source.artifact_type, identity, "missing stock-stage symbol", ("symbol_id",)))
            continue
        label = first(row, "effective_stage", "provisional_stage", "weekly_stage_label", "stage_label", "stage")
        confidence_raw = as_float(first(row, "stage_confidence_score", "weekly_stage_confidence", "stage_confidence", "confidence"))
        if confidence_raw is None:
            confidence_raw = 0.0
        try:
            adapted = adapt_legacy_weekly_stage(label, confidence_raw) if confidence_raw <= 1 else None
            mapped = adapted.stage if adapted else map_legacy_stage(label).value
            confidence = adapted.confidence_score if adapted else confidence_raw
            adapter_warnings = adapted.warnings if adapted else map_legacy_stage(label).warnings
        except ValueError as exc:
            rejected.append(RejectedSourceRow(source.artifact_type, identity, str(exc), ("stage_confidence",)))
            continue
        warnings.extend(AdapterWarning(source.artifact_type, identity, "legacy_stock_stage", message) for message in adapter_warnings)
        week_end = as_date(first(row, "source_week_end", "week_end_date", "trade_date"), as_of.date())
        week_start = as_date(first(row, "source_week_start"), week_end - timedelta(days=week_end.weekday()))
        explicit_status = str(first(row, "stage_status", "lock_status") or "").strip().lower()
        locked_at_value = first(row, "stage_locked_at", "locked_at", "created_at", "as_of")
        locked = mapped is not WeinsteinStage.UNKNOWN and (
            explicit_status == "locked" or (explicit_status != "provisional" and week_end < as_of.date())
        )
        if locked and locked_at_value is None:
            rejected.append(RejectedSourceRow(source.artifact_type, identity, "locked stage lacks a source lock/creation timestamp", ("stage_locked_at", "created_at")))
            continue
        locked_at = as_datetime(locked_at_value, as_of) if locked_at_value is not None else (as_of if locked else None)
        previous = map_legacy_stage(first(row, "previous_locked_stage", "previous_stage")).value if first(row, "previous_locked_stage", "previous_stage") else prior.get((exchange, symbol))
        explicit_provisional = map_legacy_stage(first(row, "provisional_stage")).value if first(row, "provisional_stage") else None
        explicit_locked = map_legacy_stage(first(row, "locked_stage")).value if first(row, "locked_stage") else None
        provisional = explicit_provisional if explicit_provisional is not None else (WeinsteinStage.UNKNOWN if locked else mapped)
        locked_stage = explicit_locked if explicit_locked is not None else (mapped if locked else (prior.get((exchange, symbol)) or WeinsteinStage.UNKNOWN))
        status = StageStatus.UNKNOWN if mapped is WeinsteinStage.UNKNOWN else StageStatus.LOCKED if locked else StageStatus.PROVISIONAL
        snapshot = StageSnapshot(
            provisional_stage=provisional,
            locked_stage=locked_stage,
            effective_stage=provisional if provisional is not WeinsteinStage.UNKNOWN else locked_stage,
            stage_status=status,
            confidence_score=confidence,
            confidence_band=confidence_band(confidence, unknown=status is StageStatus.UNKNOWN),
            confidence_components=None,
            stage_as_of=as_of,
            stage_locked_at=locked_at,
            source_week_start=week_start,
            source_week_end=week_end,
            previous_locked_stage=previous,
            weeks_in_locked_stage=int(as_float(first(row, "weeks_in_locked_stage", "bars_in_stage")) or 0) if locked else 0,
            provisional_persistence_days=int(as_float(first(row, "provisional_persistence_days")) or 0),
            transition_reason=StageTransitionReason.UNKNOWN,
            classifier_version=str(first(row, "classifier_version", "stage_classifier_version", "model_version") or "weekly-stage-unknown"),
            confidence_formula_version=LEGACY_STAGE_CONFIDENCE_VERSION,
        )
        records.append(AdaptedRecord(exchange, symbol, snapshot, identity, source))
    return AdapterResult(tuple(records), tuple(warnings), tuple(rejected), source)
