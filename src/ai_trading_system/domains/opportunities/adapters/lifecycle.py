"""Existing lifecycle, follow-through, and tracker-progress adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

from ai_trading_system.domains.opportunities.compatibility import map_candidate_tracker_progress, map_legacy_followthrough, map_stage1_lifecycle
from ai_trading_system.domains.opportunities.contracts import CandidateState, FollowthroughStatus, ProgressSnapshot
from ai_trading_system.domains.opportunities.orchestration.contracts import AdaptedRecord, AdapterResult, AdapterWarning, RejectedSourceRow, SourceDescriptor

from .common import first, normalize_exchange, normalize_symbol, row_identity


@dataclass(frozen=True, slots=True)
class LifecycleEvidence:
    lifecycle_state: CandidateState | None
    followthrough_status: FollowthroughStatus
    progress: ProgressSnapshot | None


def adapt_lifecycle_rows(rows: Sequence[Mapping[str, Any]], *, source: SourceDescriptor, as_of: datetime) -> AdapterResult[AdaptedRecord[LifecycleEvidence]]:
    records: list[AdaptedRecord[LifecycleEvidence]] = []
    warnings: list[AdapterWarning] = []
    rejected: list[RejectedSourceRow] = []
    for row in rows:
        identity = row_identity(row)
        symbol = normalize_symbol(first(row, "symbol_id", "symbol", "ticker"))
        if not symbol:
            rejected.append(RejectedSourceRow(source.artifact_type, identity, "missing lifecycle symbol", ("symbol_id",)))
            continue
        lifecycle = map_stage1_lifecycle(
            first(row, "stage1_lifecycle_state", "lifecycle_state", "state"),
            pattern_promotion_state=first(row, "pattern_promotion_state", "promotion_state", "followthrough_status"),
        )
        followthrough = map_legacy_followthrough(first(row, "followthrough_status", "pattern_promotion_state") or "UNKNOWN")
        tracker_raw = first(row, "candidate_health", "health_status", "progress_status")
        progress_result = map_candidate_tracker_progress(tracker_raw) if tracker_raw is not None else None
        for message in (*lifecycle.warnings, *followthrough.warnings, *((progress_result.warnings) if progress_result else ())):
            warnings.append(AdapterWarning(source.artifact_type, identity, "legacy_lifecycle", message))
        progress = None
        if progress_result is not None:
            progress = ProgressSnapshot(
                status=progress_result.value,
                observed_at=as_of,
                rank_velocity_improved=None,
                evidence_score_improved=None,
                base_contraction_improved=None,
                volume_dry_up_improved=None,
                weekly_ma_slope_improved=None,
                distance_to_pivot_narrowed=None,
                relative_strength_improved=None,
                sector_alignment_improved=None,
                notes=("mapped from candidate-tracker health; lifecycle unchanged",),
            )
        records.append(AdaptedRecord(
            normalize_exchange(first(row, "exchange")), symbol,
            LifecycleEvidence(lifecycle.value, followthrough.value, progress), identity, source,
        ))
    return AdapterResult(tuple(records), tuple(warnings), tuple(rejected), source)
