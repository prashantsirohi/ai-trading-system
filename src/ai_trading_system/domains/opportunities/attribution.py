"""Versioned, P&L-independent structural-stage attribution rules."""

from __future__ import annotations

from dataclasses import dataclass

from .contracts import (
    DecisionContextSnapshot,
    OutcomeAttribution,
    RegimeShockEvidence,
    StageAttributionConfig,
    StageForwardObservation,
    StageStatus,
    WeinsteinStage,
)


@dataclass(frozen=True, slots=True)
class AttributionEvaluation:
    """Pure attribution result before it is attached to a candidate record."""

    category: OutcomeAttribution
    confidence: float
    supporting_evidence: tuple[str, ...]
    rule_version: str


def evaluate_stage_attribution(
    *,
    decision_context: DecisionContextSnapshot,
    observations: tuple[StageForwardObservation, ...] = (),
    same_week_locked_stage: WeinsteinStage | None = None,
    shock_evidence: RegimeShockEvidence | None = None,
    evidence_complete: bool = True,
    config: StageAttributionConfig | None = None,
) -> AttributionEvaluation:
    """Classify structural outcomes without using trade return or later revisions."""
    cfg = config or StageAttributionConfig()

    if (
        decision_context.decision_stage_status is StageStatus.PROVISIONAL
        and same_week_locked_stage is not None
        and same_week_locked_stage is not decision_context.decision_stage
    ):
        return AttributionEvaluation(
            category=OutcomeAttribution.PROVISIONAL_STAGE_NONCONFIRMATION,
            confidence=95.0,
            supporting_evidence=(
                f"provisional={decision_context.decision_stage.value}",
                f"same_week_locked={same_week_locked_stage.value}",
            ),
            rule_version=cfg.rule_version,
        )

    if shock_evidence is not None and shock_evidence.shock_confirmed:
        return AttributionEvaluation(
            category=OutcomeAttribution.EXOGENOUS_REGIME_SHOCK,
            confidence=90.0,
            supporting_evidence=shock_evidence.reasons,
            rule_version=cfg.rule_version,
        )

    ordered = tuple(sorted((item for item in observations if item.complete), key=lambda item: item.week_number_after_decision))
    locked_stage2_decision = (
        decision_context.decision_stage_status is StageStatus.LOCKED
        and decision_context.decision_locked_stage is WeinsteinStage.STAGE_2
    )
    if locked_stage2_decision:
        within_window = tuple(
            item
            for item in ordered
            if cfg.lookforward_min_weeks <= item.week_number_after_decision <= cfg.lookforward_max_weeks
        )
        run = _qualifying_persistent_run(within_window, cfg.opposite_structure_persistence_weeks)
        if run:
            evidence = tuple(
                f"week_{item.week_number_after_decision}:negative_ma+below_30w+negative_rs+opposite_structure"
                for item in run
            )
            return AttributionEvaluation(
                category=OutcomeAttribution.STAGE_CLASSIFICATION_ERROR,
                confidence=90.0,
                supporting_evidence=evidence,
                rule_version=cfg.rule_version,
            )

        later = tuple(item for item in ordered if item.week_number_after_decision > cfg.minimum_valid_hold_weeks)
        later_run = _qualifying_persistent_run(later, cfg.opposite_structure_persistence_weeks)
        if later_run:
            return AttributionEvaluation(
                category=OutcomeAttribution.STAGE_TRANSITION_AFTER_VALID_ENTRY,
                confidence=85.0,
                supporting_evidence=(
                    f"locked Stage 2 held through week {cfg.minimum_valid_hold_weeks}",
                    f"opposite structure began week {later_run[0].week_number_after_decision}",
                ),
                rule_version=cfg.rule_version,
            )

    if not evidence_complete or not observations:
        return AttributionEvaluation(
            category=OutcomeAttribution.UNDETERMINED,
            confidence=25.0,
            supporting_evidence=("insufficient forward structural evidence",),
            rule_version=cfg.rule_version,
        )
    return AttributionEvaluation(
        category=OutcomeAttribution.VALID_SIGNAL_NORMAL_FAILURE,
        confidence=70.0,
        supporting_evidence=("no configured structural-error or shock rule was satisfied",),
        rule_version=cfg.rule_version,
    )


def _qualifying_persistent_run(
    observations: tuple[StageForwardObservation, ...],
    required: int,
) -> tuple[StageForwardObservation, ...]:
    current: list[StageForwardObservation] = []
    previous_week: int | None = None
    for item in observations:
        qualifies = (
            item.ma30w_slope_negative
            and item.close_below_30w_ma
            and item.relative_strength_slope_negative
            and item.opposite_structure_confirmed
        )
        if not qualifies:
            current = []
            previous_week = None
            continue
        if previous_week is None or item.week_number_after_decision == previous_week + 1:
            current.append(item)
        else:
            current = [item]
        previous_week = item.week_number_after_decision
        if len(current) >= required:
            return tuple(current[-required:])
    return ()
