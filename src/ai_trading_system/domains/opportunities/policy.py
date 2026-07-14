"""Pure structural-stage guards; these helpers never dispatch orders."""

from __future__ import annotations

from .contracts import (
    ActionEligibility,
    CandidateState,
    RiskLevel,
    SectorStageSnapshot,
    StageSnapshot,
    StageStatus,
    StructuralGuardConfig,
    StructuralGuardResult,
    WeinsteinStage,
)


def evaluate_early_entry_stage_guard(
    *,
    stock_stage: StageSnapshot,
    sector_stage: SectorStageSnapshot,
    lifecycle_state: CandidateState,
    evidence_score: float,
    extension_risk: RiskLevel,
    market_regime: str,
    portfolio_blockers: tuple[str, ...] = (),
    config: StructuralGuardConfig | None = None,
) -> StructuralGuardResult:
    """Evaluate the conservative provisional Stage-1-to-2 pilot guard."""
    cfg = config or StructuralGuardConfig()
    blockers: list[str] = []
    reasons: list[str] = []
    warnings: list[str] = ["provisional stock stage; normal entry requires a locked Stage 2"]
    sector_snapshot = sector_stage.stage_snapshot

    if stock_stage.stage_status is not StageStatus.PROVISIONAL:
        blockers.append("stock stage must be provisional for early-entry mode")
    if stock_stage.provisional_stage is not WeinsteinStage.TRANSITION_1_TO_2:
        blockers.append("stock provisional stage must be transition_1_to_2")
    if stock_stage.confidence_score < cfg.early_stock_confidence_min:
        blockers.append(f"stock stage confidence must be >= {cfg.early_stock_confidence_min:g}")
    if sector_snapshot.stage_status is not StageStatus.LOCKED:
        blockers.append("sector stage must be locked; both stock and sector provisional is blocked")
    if sector_snapshot.locked_stage is not WeinsteinStage.STAGE_2:
        blockers.append("sector locked stage must be stage_2_advancing")
    if sector_stage.sector_relative_strength_state.strip().lower() != "improving":
        blockers.append("sector relative strength must be improving")
    if lifecycle_state is not CandidateState.READY:
        blockers.append("candidate lifecycle must be ready")
    if float(evidence_score) < cfg.evidence_score_min:
        blockers.append(f"evidence score must be >= {cfg.evidence_score_min:g}")
    if extension_risk is not RiskLevel.LOW:
        blockers.append("extension risk must be low")
    if market_regime.strip().lower() not in cfg.allowed_market_regimes:
        blockers.append("market regime is not allowed for early entry")
    blockers.extend(str(item) for item in portfolio_blockers if str(item).strip())

    passed = not blockers
    if passed:
        reasons.extend(("high-confidence provisional stock transition", "locked Stage 2 sector confirmation"))
    return StructuralGuardResult(
        passed=passed,
        eligibility=ActionEligibility.CONDITIONALLY_ELIGIBLE if passed else ActionEligibility.BLOCKED,
        reasons=tuple(reasons),
        blockers=tuple(blockers),
        warnings=tuple(warnings),
        recommended_max_size_multiplier=cfg.pilot_size_multiplier if passed else 0.0,
        rule_version=cfg.rule_version,
    )

def evaluate_normal_entry_stage_guard(
    *,
    stock_stage: StageSnapshot,
    sector_stage: SectorStageSnapshot,
    sector_regime: str,
    config: StructuralGuardConfig | None = None,
) -> StructuralGuardResult:
    """Evaluate locked structural eligibility for a normal new long entry."""
    cfg = config or StructuralGuardConfig()
    blockers: list[str] = []
    reasons: list[str] = []
    sector_snapshot = sector_stage.stage_snapshot

    if stock_stage.stage_status is not StageStatus.LOCKED:
        blockers.append("normal entry requires a locked stock stage")
    if stock_stage.locked_stage is not WeinsteinStage.STAGE_2:
        blockers.append("normal entry requires locked stock Stage 2")
    if stock_stage.confidence_score < cfg.normal_stock_confidence_min:
        blockers.append(f"stock stage confidence must be >= {cfg.normal_stock_confidence_min:g}")
    if sector_snapshot.stage_status is not StageStatus.LOCKED:
        blockers.append("normal entry requires a locked sector stage")
    if sector_snapshot.locked_stage not in {
        WeinsteinStage.TRANSITION_1_TO_2,
        WeinsteinStage.STAGE_2,
    }:
        blockers.append("sector locked stage must be transition_1_to_2 or Stage 2")
    if sector_regime.strip().lower() in cfg.blocked_sector_regimes:
        blockers.append("sector regime is risk_off")

    passed = not blockers
    if passed:
        reasons.extend(("locked stock Stage 2", "sector structure allows normal entry"))
    return StructuralGuardResult(
        passed=passed,
        eligibility=ActionEligibility.ELIGIBLE if passed else ActionEligibility.BLOCKED,
        reasons=tuple(reasons),
        blockers=tuple(blockers),
        warnings=(),
        recommended_max_size_multiplier=1.0 if passed else 0.0,
        rule_version=cfg.rule_version,
    )
