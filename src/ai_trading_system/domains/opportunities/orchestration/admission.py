"""Deterministic candidate admission and setup-family classification."""

from __future__ import annotations

from ai_trading_system.domains.opportunities.contracts import EvidenceVerdict, WeinsteinStage
from ai_trading_system.domains.opportunities.registry.identity import stable_digest

from .contracts import AdmissionEvaluation, AdmissionReason, OpportunityShadowConfig, OpportunitySourceBundle, SetupFamily


def evaluate_admission(
    bundle: OpportunitySourceBundle,
    config: OpportunityShadowConfig,
    policy_snapshot_id: str | None = None,
) -> AdmissionEvaluation:
    blockers: list[str] = []
    supporting: list[str] = []
    warnings: list[str] = []
    stage = bundle.stock_stage.effective_stage if bundle.stock_stage else WeinsteinStage.UNKNOWN
    if stage in {WeinsteinStage.TRANSITION_2_TO_3, WeinsteinStage.STAGE_3, WeinsteinStage.TRANSITION_3_TO_4, WeinsteinStage.STAGE_4}:
        return AdmissionEvaluation(False, None, None, (), ("stage_3_or_4_blocks_new_long_admission",), (), None)

    reason: AdmissionReason | None = None
    family: SetupFamily | None = None
    opportunity = bundle.opportunity
    evidence = bundle.evidence
    qualified_breakout = next((event for event in bundle.breakout_events if event.qualified and not event.failed), None)
    qualified_pattern = next((event for event in bundle.pattern_events if event.qualified and not event.failed), None)

    if qualified_breakout and qualified_breakout.score is not None and qualified_breakout.score >= config.breakout_admission_score and (
        qualified_breakout.tier in config.breakout_admission_tiers or qualified_breakout.tier is None
    ):
        reason, family = AdmissionReason.QUALIFIED_BREAKOUT, SetupFamily.BREAKOUT
        supporting.append("qualified breakout")
    elif bundle.stock_stage and bundle.stock_stage.provisional_stage is WeinsteinStage.TRANSITION_1_TO_2 and bundle.stock_stage.confidence_score >= config.early_trigger_stage_confidence_threshold:
        reason, family = AdmissionReason.STAGE_TRANSITION, SetupFamily.STAGE_1_TO_2_TRANSITION
        supporting.append("high-confidence Stage 1 to 2 transition")
    elif bundle.lifecycle_hint and bundle.lifecycle_hint.value == "early_accumulation" or (
        evidence and evidence.accumulation_score is not None and evidence.accumulation_score >= config.accumulation_admission_score
    ):
        reason, family = AdmissionReason.EARLY_ACCUMULATION, SetupFamily.EARLY_ACCUMULATION
        supporting.append("strong early accumulation")
    elif evidence and evidence.evidence_score >= config.investigator_admission_score and evidence.investigator_verdict in {
        EvidenceVerdict.HIGH_CONVICTION, EvidenceVerdict.MEDIUM_CONVICTION,
    }:
        reason, family = AdmissionReason.INVESTIGATOR_PROMOTION, SetupFamily.BASE_BUILDING
        supporting.append("positive Investigator promotion")
    elif qualified_pattern and qualified_pattern.score is not None and qualified_pattern.score >= config.pattern_admission_score:
        reason, family = AdmissionReason.QUALIFIED_PATTERN, SetupFamily.BASE_BUILDING
        supporting.append("qualified high-quality pattern")
    elif opportunity and opportunity.rank_velocity is not None and opportunity.rank_velocity <= config.rank_velocity_floor and opportunity.rank_percentile >= config.rank_velocity_percentile_floor:
        reason, family = AdmissionReason.RANK_VELOCITY, SetupFamily.MOMENTUM_LEADER
        supporting.append("material rank improvement")
    elif opportunity and opportunity.rank_percentile >= config.rank_admission_percentile:
        reason, family = AdmissionReason.RANK_THRESHOLD, SetupFamily.MOMENTUM_LEADER
        supporting.append("top rank percentile")
    else:
        blockers.append("no admission rule passed")

    if reason is None or family is None:
        return AdmissionEvaluation(False, None, None, tuple(supporting), tuple(blockers), tuple(warnings), None)
    identity = stable_digest({
        "exchange": bundle.exchange,
        "symbol_id": bundle.symbol_id,
        "reason": reason.value,
        "setup_family": family.value,
        "as_of": bundle.as_of.isoformat(),
        "source_rows": sorted(bundle.source_row_identities),
        "artifact_hashes": sorted(source.artifact_hash for source in bundle.source_lineage),
        "rule_version": "admission-rules-v1",
        # ADR-0006 A3: bind admission identity to the exact policy content used.
        # Pre-A3 open episodes still match by setup family, so identity drift
        # cannot open duplicate episodes.
        "policy_snapshot_id": policy_snapshot_id,
    })
    return AdmissionEvaluation(True, reason, family, tuple(supporting), (), tuple(warnings), identity)
