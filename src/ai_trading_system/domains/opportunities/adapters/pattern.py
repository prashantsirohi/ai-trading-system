"""Pattern artifact adapter."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from ai_trading_system.domains.opportunities.orchestration.contracts import AdaptedRecord, AdapterResult, PatternEvidence, RejectedSourceRow, SourceDescriptor

from .common import as_bool, as_float, first, normalize_exchange, normalize_symbol, row_identity


def adapt_pattern_rows(rows: Sequence[Mapping[str, Any]], *, source: SourceDescriptor, as_of: datetime) -> AdapterResult[AdaptedRecord[PatternEvidence]]:
    del as_of
    records: list[AdaptedRecord[PatternEvidence]] = []
    rejected: list[RejectedSourceRow] = []
    for row in rows:
        identity = row_identity(row)
        symbol = normalize_symbol(first(row, "symbol_id", "symbol", "ticker"))
        if not symbol:
            rejected.append(RejectedSourceRow(source.artifact_type, identity, "missing pattern symbol", ("symbol_id",)))
            continue
        state = str(first(row, "pattern_state", "state") or "unknown").strip().upper()
        explicit = as_bool(first(row, "qualified", "pattern_qualified"))
        qualified = explicit if explicit is not None else state in {"READY", "TRIGGERED", "CONFIRMED", "QUALIFIED"}
        failed = state in {"FAILED", "INVALID", "INVALIDATED"} or explicit is False
        event = PatternEvidence(
            family=str(first(row, "pattern_family", "family", "setup_family") or "unknown"),
            state=state,
            score=as_float(first(row, "pattern_score", "score")),
            setup_quality=as_float(first(row, "setup_quality", "setup_quality_score", "pattern_rank")),
            qualified=qualified,
            failed=failed,
            metadata={"signal_id": str(first(row, "signal_id") or "")},
        )
        records.append(AdaptedRecord(normalize_exchange(first(row, "exchange")), symbol, event, identity, source))
    return AdapterResult(tuple(records), (), tuple(rejected), source)
