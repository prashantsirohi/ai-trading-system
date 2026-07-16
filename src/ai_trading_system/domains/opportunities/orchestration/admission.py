"""Deterministic evaluate-all admission and setup-family classification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from ai_trading_system.domains.opportunities.contracts import (
    EvidenceVerdict,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.registry.identity import stable_digest
from ai_trading_system.domains.opportunities.serialization import to_json

from . import contracts as admission_contracts
from .contracts import (
    ADMISSION_IDENTITY_RULE_VERSION,
    AdmissionEvaluation,
    AdmissionReason,
    AdmissionRuleEvaluation,
    OpportunityShadowConfig,
    OpportunitySourceBundle,
    SetupFamily,
)


RuleResult = tuple[bool, Mapping[str, Any], Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class AdmissionRuleDefinition:
    rule: AdmissionReason
    setup_family: SetupFamily
    supporting_text: str
    evaluate: Callable[[OpportunitySourceBundle, OpportunityShadowConfig], RuleResult]


def _qualified_breakout(bundle: OpportunitySourceBundle, config: OpportunityShadowConfig) -> RuleResult:
    event = next(
        (item for item in bundle.breakout_events if item.qualified and not item.failed),
        None,
    )
    score = event.score if event else None
    tier = event.tier if event else None
    passed = bool(
        event
        and score is not None
        and score >= config.breakout_admission_score
        and (tier in config.breakout_admission_tiers or tier is None)
    )
    return passed, {"score": score, "tier": tier}, {
        "score_min": config.breakout_admission_score,
        "tiers": list(config.breakout_admission_tiers),
    }


def _stage_transition(bundle: OpportunitySourceBundle, config: OpportunityShadowConfig) -> RuleResult:
    stage = bundle.stock_stage
    provisional = stage.provisional_stage.value if stage else None
    confidence = stage.confidence_score if stage else None
    passed = bool(
        stage
        and stage.provisional_stage is WeinsteinStage.TRANSITION_1_TO_2
        and confidence is not None
        and confidence >= config.early_trigger_stage_confidence_threshold
    )
    return passed, {"provisional_stage": provisional, "confidence": confidence}, {
        "provisional_stage": WeinsteinStage.TRANSITION_1_TO_2.value,
        "confidence_min": config.early_trigger_stage_confidence_threshold,
    }


def _early_accumulation(bundle: OpportunitySourceBundle, config: OpportunityShadowConfig) -> RuleResult:
    hint = bundle.lifecycle_hint.value if bundle.lifecycle_hint else None
    score = bundle.evidence.accumulation_score if bundle.evidence else None
    passed = bool(
        (bundle.lifecycle_hint and hint == "early_accumulation")
        or (score is not None and score >= config.accumulation_admission_score)
    )
    return passed, {"lifecycle_hint": hint, "accumulation_score": score}, {
        "lifecycle_hint": "early_accumulation",
        "accumulation_score_min": config.accumulation_admission_score,
    }


def _investigator_promotion(bundle: OpportunitySourceBundle, config: OpportunityShadowConfig) -> RuleResult:
    evidence = bundle.evidence
    score = evidence.evidence_score if evidence else None
    verdict = evidence.investigator_verdict.value if evidence else None
    accepted = (
        EvidenceVerdict.HIGH_CONVICTION,
        EvidenceVerdict.MEDIUM_CONVICTION,
    )
    passed = bool(
        evidence
        and score is not None
        and score >= config.investigator_admission_score
        and evidence.investigator_verdict in accepted
    )
    return passed, {"evidence_score": score, "verdict": verdict}, {
        "evidence_score_min": config.investigator_admission_score,
        "verdicts": [item.value for item in accepted],
    }


def _qualified_pattern(bundle: OpportunitySourceBundle, config: OpportunityShadowConfig) -> RuleResult:
    event = next(
        (item for item in bundle.pattern_events if item.qualified and not item.failed),
        None,
    )
    score = event.score if event else None
    passed = bool(
        event
        and score is not None
        and score >= config.pattern_admission_score
    )
    return passed, {"score": score}, {"score_min": config.pattern_admission_score}


def _rank_velocity(bundle: OpportunitySourceBundle, config: OpportunityShadowConfig) -> RuleResult:
    opportunity = bundle.opportunity
    velocity = opportunity.rank_velocity if opportunity else None
    percentile = opportunity.rank_percentile if opportunity else None
    passed = bool(
        opportunity
        and velocity is not None
        and velocity <= config.rank_velocity_floor
        and percentile is not None
        and percentile >= config.rank_velocity_percentile_floor
    )
    return passed, {"rank_velocity": velocity, "rank_percentile": percentile}, {
        "rank_velocity_max": config.rank_velocity_floor,
        "rank_percentile_min": config.rank_velocity_percentile_floor,
    }


def _rank_threshold(bundle: OpportunitySourceBundle, config: OpportunityShadowConfig) -> RuleResult:
    percentile = bundle.opportunity.rank_percentile if bundle.opportunity else None
    passed = bool(
        bundle.opportunity
        and percentile is not None
        and percentile >= config.rank_admission_percentile
    )
    return passed, {"rank_percentile": percentile}, {
        "rank_percentile_min": config.rank_admission_percentile
    }


_RULES_BY_NAME: dict[str, AdmissionRuleDefinition] = {
    item.rule.value: item
    for item in (
        AdmissionRuleDefinition(AdmissionReason.QUALIFIED_BREAKOUT, SetupFamily.BREAKOUT, "qualified breakout", _qualified_breakout),
        AdmissionRuleDefinition(AdmissionReason.STAGE_TRANSITION, SetupFamily.STAGE_1_TO_2_TRANSITION, "high-confidence Stage 1 to 2 transition", _stage_transition),
        AdmissionRuleDefinition(AdmissionReason.EARLY_ACCUMULATION, SetupFamily.EARLY_ACCUMULATION, "strong early accumulation", _early_accumulation),
        AdmissionRuleDefinition(AdmissionReason.INVESTIGATOR_PROMOTION, SetupFamily.BASE_BUILDING, "positive Investigator promotion", _investigator_promotion),
        AdmissionRuleDefinition(AdmissionReason.QUALIFIED_PATTERN, SetupFamily.BASE_BUILDING, "qualified high-quality pattern", _qualified_pattern),
        AdmissionRuleDefinition(AdmissionReason.RANK_VELOCITY, SetupFamily.MOMENTUM_LEADER, "material rank improvement", _rank_velocity),
        AdmissionRuleDefinition(AdmissionReason.RANK_THRESHOLD, SetupFamily.MOMENTUM_LEADER, "top rank percentile", _rank_threshold),
    )
}
assert set(_RULES_BY_NAME) == set(admission_contracts.ADMISSION_RULE_PRECEDENCE)


def evaluate_all_admission_rules(
    bundle: OpportunitySourceBundle, config: OpportunityShadowConfig
) -> tuple[AdmissionRuleEvaluation, ...]:
    source_ids = tuple(sorted(bundle.source_row_identities))
    results = []
    for name in admission_contracts.ADMISSION_RULE_PRECEDENCE:
        definition = _RULES_BY_NAME[name]
        passed, observed, threshold = definition.evaluate(bundle, config)
        results.append(
            AdmissionRuleEvaluation(
                definition.rule,
                definition.setup_family,
                passed,
                observed,
                threshold,
                source_ids,
            )
        )
    return tuple(results)


def choose_by_precedence(
    evaluations: tuple[AdmissionRuleEvaluation, ...],
) -> AdmissionRuleEvaluation | None:
    by_rule = {item.rule.value: item for item in evaluations}
    return next(
        (
            by_rule[name]
            for name in admission_contracts.ADMISSION_RULE_PRECEDENCE
            if by_rule[name].passed
        ),
        None,
    )


def satisfied_rules_json(evaluation: AdmissionEvaluation) -> str:
    return to_json(evaluation.satisfied_rules)


def rule_evaluations_json(evaluation: AdmissionEvaluation) -> str:
    return to_json(evaluation.rule_evaluations)


def evaluate_admission(
    bundle: OpportunitySourceBundle,
    config: OpportunityShadowConfig,
    policy_snapshot_id: str | None = None,
) -> AdmissionEvaluation:
    stage = (
        bundle.stock_stage.effective_stage
        if bundle.stock_stage
        else WeinsteinStage.UNKNOWN
    )
    if stage in {
        WeinsteinStage.TRANSITION_2_TO_3,
        WeinsteinStage.STAGE_3,
        WeinsteinStage.TRANSITION_3_TO_4,
        WeinsteinStage.STAGE_4,
    }:
        return AdmissionEvaluation(
            False,
            None,
            None,
            (),
            ("stage_3_or_4_blocks_new_long_admission",),
            (),
            None,
        )

    evaluations = evaluate_all_admission_rules(bundle, config)
    primary = choose_by_precedence(evaluations)
    satisfied = tuple(item.rule for item in evaluations if item.passed)
    if primary is None:
        return AdmissionEvaluation(
            False,
            None,
            None,
            (),
            ("no admission rule passed",),
            (),
            None,
            satisfied_rules=satisfied,
            rule_evaluations=evaluations,
        )
    identity = stable_digest(
        {
            "exchange": bundle.exchange,
            "symbol_id": bundle.symbol_id,
            "reason": primary.rule.value,
            "setup_family": primary.setup_family.value,
            "as_of": bundle.as_of.isoformat(),
            "source_rows": sorted(bundle.source_row_identities),
            "artifact_hashes": sorted(
                source.artifact_hash for source in bundle.source_lineage
            ),
            "rule_version": ADMISSION_IDENTITY_RULE_VERSION,
            "policy_snapshot_id": policy_snapshot_id,
        }
    )
    supporting = (_RULES_BY_NAME[primary.rule.value].supporting_text,)
    return AdmissionEvaluation(
        True,
        primary.rule,
        primary.setup_family,
        supporting,
        (),
        (),
        identity,
        satisfied_rules=satisfied,
        rule_evaluations=evaluations,
    )
