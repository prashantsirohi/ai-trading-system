"""Candidate snapshot assembly without persistence side effects."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import Any

from ai_trading_system.domains.opportunities.contracts import (
    ActionEligibility,
    CandidateAction,
    CandidateSnapshot,
    CandidateState,
    FollowthroughStatus,
    InvestigatorContext,
    SectorStageSnapshot,
    StageConfidenceBand,
    StageSnapshot,
    StageStatus,
    StageTransitionReason,
    WeinsteinStage,
)
from .contracts import LEGACY_STAGE_CONFIDENCE_VERSION, OpportunitySourceBundle


def unknown_stage(
    bundle: OpportunitySourceBundle, *, classifier_version: str
) -> StageSnapshot:
    week_end = bundle.as_of.date()
    week_start = week_end - timedelta(days=week_end.weekday())
    return StageSnapshot(
        provisional_stage=WeinsteinStage.UNKNOWN,
        locked_stage=WeinsteinStage.UNKNOWN,
        effective_stage=WeinsteinStage.UNKNOWN,
        stage_status=StageStatus.UNKNOWN,
        confidence_score=0.0,
        confidence_band=StageConfidenceBand.UNKNOWN,
        confidence_components=None,
        stage_as_of=bundle.as_of,
        stage_locked_at=None,
        source_week_start=week_start,
        source_week_end=week_end,
        previous_locked_stage=None,
        weeks_in_locked_stage=0,
        provisional_persistence_days=0,
        transition_reason=StageTransitionReason.UNKNOWN,
        classifier_version=classifier_version,
        confidence_formula_version=LEGACY_STAGE_CONFIDENCE_VERSION,
    )


def assemble_candidate_snapshot(
    *,
    candidate_id: str,
    setup_id: str,
    bundle: OpportunitySourceBundle,
    lifecycle_state: CandidateState,
    days_in_state: int,
    days_without_progress: int,
    active_position: bool,
) -> CandidateSnapshot | None:
    if bundle.opportunity is None or bundle.evidence is None:
        return None
    stock = bundle.stock_stage or unknown_stage(
        bundle, classifier_version="stock-stage-unavailable-v1"
    )
    sector = bundle.sector_stage or SectorStageSnapshot(
        sector_id=bundle.sector_name.upper().replace(" ", "_") or "UNKNOWN",
        sector_name=bundle.sector_name or "unknown",
        stage_snapshot=unknown_stage(
            bundle, classifier_version="sector-stage-unavailable-v1"
        ),
        sector_relative_strength_state="unknown",
        sector_rotation_state="unknown",
    )
    followthrough = _compatible_followthrough(
        lifecycle_state, bundle.followthrough_status
    )
    return CandidateSnapshot(
        candidate_id=candidate_id,
        setup_id=setup_id,
        symbol_id=bundle.symbol_id,
        exchange=bundle.exchange,
        as_of=bundle.as_of,
        opportunity=bundle.opportunity,
        evidence=bundle.evidence,
        lifecycle_state=lifecycle_state,
        followthrough_status=followthrough,
        stock_stage=stock,
        sector_stage=sector,
        market_regime=bundle.market_regime,
        sector_regime=bundle.sector_regime,
        days_in_state=max(days_in_state, 0),
        days_without_progress=max(days_without_progress, 0),
        active_position=active_position,
        latest_action=CandidateAction.WATCH,
        eligibility=ActionEligibility.UNKNOWN,
        investigator_context=_investigator_context(bundle),
    )


def _compatible_followthrough(
    state: CandidateState, supplied: FollowthroughStatus
) -> FollowthroughStatus:
    pending = {
        FollowthroughStatus.PENDING_1D,
        FollowthroughStatus.PENDING_3D,
        FollowthroughStatus.PENDING_5D,
    }
    if state is CandidateState.PENDING_FOLLOWTHROUGH:
        return supplied if supplied in pending else FollowthroughStatus.PENDING_3D
    if state is CandidateState.CONFIRMED:
        return FollowthroughStatus.CONFIRMED
    return FollowthroughStatus.UNKNOWN if supplied in pending else supplied


def _investigator_context(bundle: OpportunitySourceBundle) -> InvestigatorContext:
    base = bundle.investigator_context or InvestigatorContext()
    patterns = tuple(
        sorted(
            (
                {
                    "family": item.family,
                    "state": item.state,
                    "score": item.score,
                    "setup_quality": item.setup_quality,
                    "qualified": item.qualified,
                    "failed": item.failed,
                    "metadata": dict(item.metadata),
                }
                for item in bundle.pattern_events
            ),
            key=lambda item: (
                not bool(item.get("qualified")),
                bool(item.get("failed")),
                -_number(item.get("score")),
                -_number(item.get("setup_quality")),
                str(item.get("family") or ""),
            ),
        )
    )
    breakouts = tuple(
        sorted(
            (
                {
                    "qualified": item.qualified,
                    "failed": item.failed,
                    "score": item.score,
                    "tier": item.tier,
                    "state": item.state,
                    "trigger_price": item.trigger_price,
                    "pivot_price": item.pivot_price,
                    "occurred_at": item.occurred_at,
                    "metadata": dict(item.metadata),
                }
                for item in bundle.breakout_events
            ),
            key=lambda item: (
                not bool(item.get("qualified")),
                bool(item.get("failed")),
                _tier_rank(item.get("tier")),
                -_number(item.get("score")),
                str(item.get("state") or ""),
            ),
        )
    )
    primary_pattern = patterns[0] if patterns else {}
    primary_breakout = breakouts[0] if breakouts else {}
    breakout_metadata = primary_breakout.get("metadata")
    breakout_metadata = breakout_metadata if isinstance(breakout_metadata, dict) else {}
    stock = bundle.stock_stage
    stage_label = _known(
        base.stage_label,
        stock.effective_stage.value.upper() if stock is not None else "UNKNOWN",
    )
    stage_confidence = (
        base.stage_confidence
        if base.stage_confidence is not None
        else (stock.confidence_score if stock is not None else None)
    )
    pattern_family = _known(primary_pattern.get("family"), base.pattern_family)
    pattern_state = _known(primary_pattern.get("state"), base.pattern_state)
    setup_bucket = _setup_bucket(
        primary_pattern.get("setup_quality"), base.setup_quality_bucket
    )
    breakout_type = _known(breakout_metadata.get("setup_family"), base.breakout_type)
    candidate_tier = _known(primary_breakout.get("tier"), base.candidate_tier)
    qualified = (
        bool(primary_breakout.get("qualified"))
        if primary_breakout
        else base.qualified_breakout
    )
    sector_rs = (
        bundle.sector_stage.sector_relative_strength_state
        if bundle.sector_stage is not None
        else "UNKNOWN"
    )
    values = {
        "stage_label": stage_label,
        "stage_confidence": stage_confidence,
        "pattern_family": pattern_family,
        "pattern_state": pattern_state,
        "pattern_score": (
            _optional_number(primary_pattern.get("score"))
            if primary_pattern
            else base.pattern_score
        ),
        "setup_quality_score": (
            _optional_number(primary_pattern.get("setup_quality"))
            if primary_pattern
            else base.setup_quality_score
        ),
        "setup_quality_bucket": setup_bucket,
        "breakout_type": breakout_type,
        "candidate_tier": candidate_tier,
        "breakout_tier": candidate_tier,
        "qualified_breakout": qualified,
        "confirmed_regime": _known(bundle.market_regime),
        "raw_regime": _known(bundle.raw_market_regime),
        "regime_confidence": bundle.regime_confidence,
        "breadth_velocity_bucket": _known(bundle.breadth_velocity_bucket),
        "breadth_velocity_quantile": _known(bundle.breadth_velocity_quantile),
        "regime_score_chg_5d": bundle.regime_score_chg_5d,
        "sector_relative_strength_bucket": _sector_rs_bucket(sector_rs),
        "sector_leadership": _known(
            bundle.sector_stage.sector_rotation_state
            if bundle.sector_stage is not None
            else None,
            base.sector_leadership,
        ),
    }
    required = (
        "stage_label",
        "stage_confidence",
        "pattern_family",
        "pattern_state",
        "setup_quality_bucket",
        "breakout_type",
        "candidate_tier",
        "qualified_breakout",
        "confirmed_regime",
        "breadth_velocity_bucket",
        "sector_relative_strength_bucket",
    )
    missing = tuple(
        name
        for name in required
        if values[name] is None or str(values[name]).upper() == "UNKNOWN"
    )
    evaluation_states = dict(base.evaluation_states)
    evaluation_states.update(
        {
            "stage": _context_state(stage_label),
            "pattern_attempted": (
                "KNOWN" if patterns else evaluation_states.get("pattern_attempted", "UNKNOWN")
            ),
            "pattern": _context_state(pattern_family),
            "setup_quality": _context_state(values["setup_quality_bucket"]),
            "breakout": _context_state(breakout_type),
            "regime": _context_state(values["confirmed_regime"]),
            "breadth": _context_state(values["breadth_velocity_bucket"]),
            "sector": _context_state(values["sector_relative_strength_bucket"]),
            "lineage": "KNOWN" if bundle.source_lineage else "UNKNOWN",
        }
    )
    source_lineage = tuple(
        {
            "artifact_type": item.artifact_type,
            "run_id": item.run_id,
            "stage_attempt": item.stage_attempt,
            "observed_at": bundle.as_of,
            "artifact_hash": item.artifact_hash,
        }
        for item in sorted(
            bundle.source_lineage,
            key=lambda item: (
                item.stage_name,
                item.artifact_type,
                item.stage_attempt,
                item.artifact_hash,
            ),
        )
    )
    return replace(
        base,
        **values,
        context_as_of=bundle.as_of,
        source_run_id=base.source_run_id
        if base.source_run_id != "UNKNOWN"
        else (bundle.source_lineage[0].run_id if bundle.source_lineage else "UNKNOWN"),
        source_artifact_hashes=tuple(
            sorted({item.artifact_hash for item in bundle.source_lineage})
        ),
        classifier_versions=tuple(
            sorted(
                {
                    *base.classifier_versions,
                    *((stock.classifier_version,) if stock is not None else ()),
                }
            )
        ),
        missing_fields=missing,
        pattern_events=patterns,
        breakout_events=breakouts,
        source_lineage=source_lineage or base.source_lineage,
        evaluation_states=evaluation_states,
    )


def _known(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip().upper().replace(" ", "_").replace("-", "_")
        if text not in {"", "UNKNOWN", "NAN", "<NA>"}:
            return text
    return "UNKNOWN"


def _number(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return -1.0
    return parsed if parsed == parsed else -1.0


def _optional_number(value: Any) -> float | None:
    parsed = _number(value)
    return parsed if parsed >= 0 else None


def _context_state(value: Any) -> str:
    text = _known(value)
    if text == "NONE":
        return "NONE"
    return "KNOWN" if text != "UNKNOWN" else "UNKNOWN"


def _setup_bucket(value: Any, fallback: Any) -> str:
    direct = _known(value)
    if direct in {"HIGH", "MEDIUM", "LOW"}:
        return direct
    parsed = _number(value)
    if parsed >= 0:
        return "HIGH" if parsed >= 70 else "MEDIUM" if parsed >= 45 else "LOW"
    return _known(fallback)


def _tier_rank(value: Any) -> int:
    return {"A": 0, "B": 1, "C": 2, "D": 3}.get(_known(value), 4)


def _sector_rs_bucket(value: Any) -> str:
    parsed = _number(value)
    if parsed >= 0:
        return "HIGH" if parsed >= 75 else "MID" if parsed >= 25 else "LOW"
    text = _known(value)
    if text in {"HIGH", "LEADING", "STRONG", "IMPROVING"}:
        return "HIGH"
    if text in {"MID", "MIDDLE", "NEUTRAL"}:
        return "MID"
    if text in {"LOW", "LAGGING", "WEAK", "WEAKENING"}:
        return "LOW"
    return "UNKNOWN"
