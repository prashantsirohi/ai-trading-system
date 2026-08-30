"""Research-only multi-label technical evidence for opportunity episodes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from ai_trading_system.domains.opportunities.contracts import InvestigatorContext

from .contracts import (
    INVESTIGATOR_PRIMARY_TRIGGER,
    TECHNICAL_EVIDENCE_NEAR_HIGH_RATIO,
    TECHNICAL_EVIDENCE_POLICY_VERSION,
    SymbolTechnicalEvidence,
    TechnicalEvidenceState,
)


@dataclass(frozen=True, slots=True)
class TechnicalMarketMetrics:
    price: float | None
    sma20: float | None
    high_52w: float | None
    observed_sessions: int
    price_basis: str = "ADJUSTED_CLOSE"
    missing_reasons: tuple[str, ...] = ()


def classify_symbol_technical_evidence(
    *,
    symbol_id: str,
    exchange: str,
    as_of: datetime,
    observed_session: date,
    context: InvestigatorContext | None,
    market_metrics: TechnicalMarketMetrics | None = None,
    previous: SymbolTechnicalEvidence | None = None,
) -> SymbolTechnicalEvidence:
    """Classify labels without granting admission or lifecycle authority."""

    missing: list[str] = []
    if context is None:
        missing.append("investigator_context_unavailable")
        weekly = TechnicalEvidenceState.UNKNOWN
    else:
        trigger = str(context.trigger_reason or "").strip().upper()
        weekly = (
            TechnicalEvidenceState.UNKNOWN
            if trigger in {"", "UNKNOWN"}
            else (
                TechnicalEvidenceState.MET
                if trigger == INVESTIGATOR_PRIMARY_TRIGGER
                else TechnicalEvidenceState.NOT_MET
            )
        )
        if weekly is TechnicalEvidenceState.UNKNOWN:
            missing.append("investigator_trigger_reason_unavailable")

    if market_metrics is not None:
        missing.extend(market_metrics.missing_reasons)
        price = _positive(market_metrics.price, "price", missing)
        sma20 = _positive(market_metrics.sma20, "sma20", missing)
        high_52w = _positive(market_metrics.high_52w, "high_52w", missing)
    elif context is not None:
        price = _positive(context.price, "price", missing)
        sma20 = _positive(context.sma20, "sma20", missing)
        high_52w = _positive(context.high_52w, "high_52w", missing)
    else:
        price = sma20 = high_52w = None
    distance = (
        round(((price / high_52w) - 1.0) * 100.0, 6)
        if price is not None and high_52w is not None
        else None
    )

    near_high = _comparison_state(
        price,
        high_52w,
        lambda current, reference: (
            current >= reference * TECHNICAL_EVIDENCE_NEAR_HIGH_RATIO
        ),
    )
    above_sma20 = _comparison_state(
        price,
        sma20,
        lambda current, reference: current >= reference,
    )
    entry_confirmed = _and_state(near_high, above_sma20)
    sma20_break = _break_state(previous, above_sma20)

    return SymbolTechnicalEvidence(
        symbol_id=symbol_id.strip().upper(),
        exchange=exchange.strip().upper(),
        as_of=as_of,
        observed_session=observed_session,
        price=price,
        sma20=sma20,
        high_52w=high_52w,
        distance_from_52w_high_pct=distance,
        weekly_gainer=weekly,
        near_52w_high_10=near_high,
        above_sma20=above_sma20,
        entry_confirmed=entry_confirmed,
        sma20_break=sma20_break,
        missing_reasons=tuple(sorted(set(missing))),
        price_basis=(
            market_metrics.price_basis
            if market_metrics is not None
            else "INVESTIGATOR_CONTEXT"
        ),
        source_run_id=(context.source_run_id if context is not None else "UNKNOWN"),
        source_artifact_hashes=(
            context.source_artifact_hashes if context is not None else ()
        ),
        policy_version=TECHNICAL_EVIDENCE_POLICY_VERSION,
    )


def _positive(value: float | None, name: str, missing: list[str]) -> float | None:
    if value is None or float(value) <= 0:
        missing.append(f"{name}_unavailable")
        return None
    return float(value)


def _comparison_state(
    left: float | None,
    right: float | None,
    predicate,
) -> TechnicalEvidenceState:
    if left is None or right is None:
        return TechnicalEvidenceState.UNKNOWN
    return (
        TechnicalEvidenceState.MET
        if predicate(left, right)
        else TechnicalEvidenceState.NOT_MET
    )


def _and_state(
    left: TechnicalEvidenceState,
    right: TechnicalEvidenceState,
) -> TechnicalEvidenceState:
    if TechnicalEvidenceState.NOT_MET in {left, right}:
        return TechnicalEvidenceState.NOT_MET
    if left is TechnicalEvidenceState.MET and right is TechnicalEvidenceState.MET:
        return TechnicalEvidenceState.MET
    return TechnicalEvidenceState.UNKNOWN


def _break_state(
    previous: SymbolTechnicalEvidence | None,
    current_above: TechnicalEvidenceState,
) -> TechnicalEvidenceState:
    if previous is None:
        return TechnicalEvidenceState.NOT_APPLICABLE
    previous_above = previous.above_sma20
    if TechnicalEvidenceState.UNKNOWN in {previous_above, current_above}:
        return TechnicalEvidenceState.UNKNOWN
    if previous_above is TechnicalEvidenceState.MET:
        return (
            TechnicalEvidenceState.MET
            if current_above is TechnicalEvidenceState.NOT_MET
            else TechnicalEvidenceState.NOT_MET
        )
    return TechnicalEvidenceState.NOT_MET
