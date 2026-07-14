"""Ranking artifact adapter."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from ai_trading_system.domains.opportunities.contracts import OpportunitySnapshot, ProgressStatus
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    AdaptedRecord,
    AdapterResult,
    AdapterWarning,
    RejectedSourceRow,
    SourceDescriptor,
)

from .common import as_float, first, normalize_exchange, normalize_symbol, row_identity


FACTOR_FIELDS = (
    "relative_strength_score", "rs_score", "volume_intensity_score", "volume_score",
    "trend_persistence_score", "trend_score", "proximity_to_high_score", "delivery_score",
    "sector_strength_score", "sector_score", "momentum_score", "quality_score",
)


def adapt_ranking_rows(
    rows: Sequence[Mapping[str, Any]], *, source: SourceDescriptor, as_of: datetime,
    prior_rank_positions: Mapping[tuple[str, str], int] | None = None,
) -> AdapterResult[AdaptedRecord[OpportunitySnapshot]]:
    records: list[AdaptedRecord[OpportunitySnapshot]] = []
    warnings: list[AdapterWarning] = []
    rejected: list[RejectedSourceRow] = []
    total = len(rows)
    prior = prior_rank_positions or {}
    for index, row in enumerate(rows, start=1):
        identity = row_identity(row)
        symbol = normalize_symbol(first(row, "symbol_id", "symbol", "ticker"))
        exchange = normalize_exchange(first(row, "exchange", "exchange_code"))
        score = as_float(first(row, "composite_score", "opportunity_score", "score"))
        if not symbol or score is None or not 0 <= score <= 100:
            rejected.append(RejectedSourceRow(source.artifact_type, identity, "missing or invalid ranking identity/score", ("symbol_id", "composite_score")))
            continue
        rank = as_float(first(row, "rank_position", "rank", "active_rank"))
        rank_position = int(rank) if rank is not None and rank >= 1 else index
        if rank is None:
            warnings.append(AdapterWarning(source.artifact_type, identity, "derived_rank_position", "rank position derived from deterministic artifact order"))
        percentile = as_float(first(row, "rank_percentile", "active_rank_pctile", "percentile"))
        if percentile is None:
            percentile = 100.0 if total <= 1 else 100.0 * (total - rank_position) / (total - 1)
            percentile = min(100.0, max(0.0, percentile))
            warnings.append(AdapterWarning(source.artifact_type, identity, "derived_rank_percentile", "rank percentile derived from position and row count"))
        previous_position = prior.get((exchange, symbol))
        velocity = float(rank_position - previous_position) if previous_position is not None else None
        velocity_state = ProgressStatus.UNKNOWN
        if velocity is not None:
            velocity_state = ProgressStatus.IMPROVING if velocity < 0 else ProgressStatus.DETERIORATING if velocity > 0 else ProgressStatus.STABLE
        else:
            warnings.append(AdapterWarning(source.artifact_type, identity, "missing_rank_velocity", "no legitimate prior rank observation"))
        factors = {
            name: value for name in FACTOR_FIELDS
            if (value := as_float(row.get(name))) is not None and 0 <= value <= 100
        }
        snapshot = OpportunitySnapshot(
            opportunity_score=score,
            rank_position=rank_position,
            rank_percentile=percentile,
            rank_velocity=velocity,
            rank_velocity_state=velocity_state,
            factor_scores=factors,
            rank_model_version=str(first(row, "rank_model_version", "model_version", "scoring_version") or "rank-model-unknown"),
            ranked_at=as_of,
        )
        records.append(AdaptedRecord(exchange, symbol, snapshot, identity, source))
    return AdapterResult(tuple(records), tuple(warnings), tuple(rejected), source)
