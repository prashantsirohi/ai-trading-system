"""Breakout artifact adapter."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from ai_trading_system.domains.opportunities.orchestration.contracts import AdaptedRecord, AdapterResult, BreakoutEvidence, RejectedSourceRow, SourceDescriptor

from .common import as_bool, as_datetime, as_float, first, normalize_exchange, normalize_symbol, row_identity


def adapt_breakout_rows(rows: Sequence[Mapping[str, Any]], *, source: SourceDescriptor, as_of: datetime) -> AdapterResult[AdaptedRecord[BreakoutEvidence]]:
    records: list[AdaptedRecord[BreakoutEvidence]] = []
    rejected: list[RejectedSourceRow] = []
    for row in rows:
        identity = row_identity(row)
        symbol = normalize_symbol(first(row, "symbol_id", "symbol", "ticker"))
        if not symbol:
            rejected.append(RejectedSourceRow(source.artifact_type, identity, "missing breakout symbol", ("symbol_id",)))
            continue
        state = str(first(row, "breakout_state", "state") or "unknown").strip().upper()
        score = as_float(first(row, "breakout_score", "score"))
        explicit = as_bool(first(row, "qualified", "breakout_qualified"))
        qualified = explicit if explicit is not None else state in {"QUALIFIED", "READY", "TRIGGERED", "CONFIRMED"}
        failed = state in {"FAILED", "INVALIDATED", "FAILED_3D"} or explicit is False
        event = BreakoutEvidence(
            qualified=qualified,
            failed=failed,
            score=score,
            tier=str(first(row, "candidate_tier", "tier") or "").strip().upper() or None,
            state=state,
            trigger_price=as_float(first(row, "trigger_price", "breakout_level")),
            pivot_price=as_float(first(row, "pivot", "pivot_price")),
            occurred_at=as_datetime(first(row, "breakout_date", "triggered_at"), as_of) if first(row, "breakout_date", "triggered_at") else None,
            metadata={"setup_family": str(first(row, "setup_family", "breakout_type") or "")},
        )
        records.append(AdaptedRecord(normalize_exchange(first(row, "exchange")), symbol, event, identity, source))
    return AdapterResult(tuple(records), (), tuple(rejected), source)
