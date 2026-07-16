"""Sector structural-stage and rotation adapter."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence

from ai_trading_system.domains.opportunities.compatibility import map_legacy_stage
from ai_trading_system.domains.opportunities.contracts import SectorStageSnapshot, StageSnapshot, StageStatus, StageTransitionReason, WeinsteinStage
from ai_trading_system.domains.opportunities.orchestration.contracts import AdaptedRecord, AdapterResult, AdapterWarning, LEGACY_STAGE_CONFIDENCE_VERSION, RejectedSourceRow, SourceDescriptor

from .common import as_date, as_datetime, as_float, confidence_band, first, normalize_exchange, normalize_symbol, row_identity


def adapt_sector_stage_rows(rows: Sequence[Mapping[str, Any]], *, source: SourceDescriptor, as_of: datetime) -> AdapterResult[AdaptedRecord[SectorStageSnapshot]]:
    records: list[AdaptedRecord[SectorStageSnapshot]] = []
    warnings: list[AdapterWarning] = []
    rejected: list[RejectedSourceRow] = []
    for row in rows:
        identity = row_identity(row)
        symbol = normalize_symbol(first(row, "symbol_id", "symbol", "ticker"))
        exchange = normalize_exchange(first(row, "exchange", "exchange_code"))
        sector = str(first(row, "sector_name", "sector", "Sector") or "").strip()
        if not sector:
            rejected.append(RejectedSourceRow(source.artifact_type, identity, "missing sector mapping", ("sector",)))
            continue
        raw_stage = first(row, "effective_stage", "sector_stage", "sector_stage_label", "weekly_sector_stage")
        mapped = map_legacy_stage(raw_stage).value if raw_stage is not None else WeinsteinStage.UNKNOWN
        confidence = as_float(first(row, "stage_confidence_score", "sector_stage_confidence", "stage_confidence")) or 0.0
        if confidence <= 1 and raw_stage is not None:
            confidence *= 100
        if mapped is WeinsteinStage.UNKNOWN:
            confidence = 0.0
        explicit = str(first(row, "sector_stage_status", "stage_status") or "").lower()
        week_end = as_date(first(row, "source_week_end", "week_end_date"), as_of.date())
        week_start = as_date(first(row, "source_week_start"), week_end - timedelta(days=week_end.weekday()))
        locked = mapped is not WeinsteinStage.UNKNOWN and (
            explicit == "locked" or (explicit != "provisional" and week_end < as_of.date())
        )
        locked_at_raw = first(row, "stage_locked_at", "sector_stage_locked_at", "created_at", "as_of")
        if locked and locked_at_raw is None:
            rejected.append(RejectedSourceRow(source.artifact_type, identity, "locked sector stage lacks a source lock/creation timestamp", ("sector_stage_locked_at", "created_at")))
            continue
        if mapped is WeinsteinStage.UNKNOWN:
            warnings.append(AdapterWarning(source.artifact_type, identity, "sector_structural_stage_unavailable", "sector RS/rotation does not imply a Weinstein stage"))
        status = StageStatus.UNKNOWN if mapped is WeinsteinStage.UNKNOWN else StageStatus.LOCKED if locked else StageStatus.PROVISIONAL
        provisional = WeinsteinStage.UNKNOWN if locked else mapped
        locked_stage = mapped if locked else WeinsteinStage.UNKNOWN
        stage = StageSnapshot(
            provisional_stage=provisional,
            locked_stage=locked_stage,
            effective_stage=provisional if provisional is not WeinsteinStage.UNKNOWN else locked_stage,
            stage_status=status,
            confidence_score=confidence,
            confidence_band=confidence_band(confidence, unknown=mapped is WeinsteinStage.UNKNOWN),
            confidence_components=None,
            stage_as_of=as_of,
            stage_locked_at=as_datetime(locked_at_raw, as_of) if locked_at_raw is not None else (as_of if locked else None),
            source_week_start=week_start,
            source_week_end=week_end,
            previous_locked_stage=None,
            weeks_in_locked_stage=0,
            provisional_persistence_days=0,
            transition_reason=StageTransitionReason.UNKNOWN,
            classifier_version=str(first(row, "aggregation_rule_version", "sector_stage_classifier_version") or "sector-stage-unavailable-v1"),
            confidence_formula_version=LEGACY_STAGE_CONFIDENCE_VERSION,
        )
        sector_id = str(first(row, "sector_id") or sector).strip().upper().replace(" ", "_")
        snapshot = SectorStageSnapshot(
            sector_id=sector_id,
            sector_name=sector,
            stage_snapshot=stage,
            sector_relative_strength_state=str(first(row, "sector_relative_strength_state", "RS_rank_pct", "rs_state") or "unknown"),
            sector_rotation_state=str(first(row, "sector_rotation_state", "Quadrant", "quadrant") or "unknown"),
        )
        records.append(AdaptedRecord(exchange, symbol or sector_id, snapshot, identity, source))
    return AdapterResult(tuple(records), tuple(warnings), tuple(rejected), source)
