"""Pure opportunity progress evaluation."""

from __future__ import annotations

from datetime import datetime

from ai_trading_system.domains.opportunities.contracts import ProgressSnapshot, ProgressStatus, WeinsteinStage

from .contracts import OpportunitySourceBundle


def evaluate_progress(current: OpportunitySourceBundle, prior: OpportunitySourceBundle | None, *, observed_at: datetime | None = None) -> ProgressSnapshot:
    at = observed_at or current.as_of
    if prior is None:
        return ProgressSnapshot(ProgressStatus.UNKNOWN, at, notes=("no prior comparable observation",))
    signals: dict[str, bool | None] = {
        "rank_velocity_improved": _compare_lower(
            current.opportunity.rank_position if current.opportunity else None,
            prior.opportunity.rank_position if prior.opportunity else None,
        ),
        "evidence_score_improved": _compare_higher(
            current.evidence.evidence_score if current.evidence else None,
            prior.evidence.evidence_score if prior.evidence else None,
        ),
        "relative_strength_improved": _factor_improved(current, prior, ("relative_strength_score", "rs_score")),
        "sector_alignment_improved": (
            current.evidence.sector_alignment > prior.evidence.sector_alignment
            if current.evidence and prior.evidence and current.evidence.sector_alignment is not None and prior.evidence.sector_alignment is not None else None
        ),
    }
    hard_negative = bool(
        current.stock_stage and current.stock_stage.effective_stage in {
            WeinsteinStage.TRANSITION_2_TO_3, WeinsteinStage.STAGE_3,
            WeinsteinStage.TRANSITION_3_TO_4, WeinsteinStage.STAGE_4,
        }
    ) or any(event.failed for event in current.breakout_events)
    comparable = [value for value in signals.values() if value is not None]
    positives = sum(value is True for value in comparable)
    negatives = sum(value is False for value in comparable)
    if hard_negative or negatives >= 2:
        status = ProgressStatus.DETERIORATING
    elif positives >= 2:
        status = ProgressStatus.IMPROVING
    elif comparable:
        status = ProgressStatus.STABLE
    else:
        status = ProgressStatus.UNKNOWN
    return ProgressSnapshot(
        status=status,
        observed_at=at,
        rank_velocity_improved=signals["rank_velocity_improved"],
        evidence_score_improved=signals["evidence_score_improved"],
        relative_strength_improved=signals["relative_strength_improved"],
        sector_alignment_improved=signals["sector_alignment_improved"],
        notes=(("hard structural deterioration",) if hard_negative else ()),
    )


def _factor_improved(current: OpportunitySourceBundle, prior: OpportunitySourceBundle, names: tuple[str, ...]) -> bool | None:
    if not current.opportunity or not prior.opportunity:
        return None
    for name in names:
        left = current.opportunity.factor_scores.get(name)
        right = prior.opportunity.factor_scores.get(name)
        if left is not None and right is not None:
            return _compare_higher(left, right)
    return None


def _compare_higher(current: float | int | None, prior: float | int | None) -> bool | None:
    if current is None or prior is None or current == prior:
        return None
    return current > prior


def _compare_lower(current: float | int | None, prior: float | int | None) -> bool | None:
    if current is None or prior is None or current == prior:
        return None
    return current < prior
