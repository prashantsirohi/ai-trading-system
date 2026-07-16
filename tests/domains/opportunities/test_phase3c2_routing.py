from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from ai_trading_system.domains.investigator.service import InvestigatorService
from ai_trading_system.domains.opportunities.contracts import (
    CandidateState,
    StageStatus,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    BreakoutEvidence,
    OpportunityShadowConfig,
)
from ai_trading_system.domains.opportunities.orchestration.service import (
    _attach_routing,
)
from ai_trading_system.domains.opportunities.orchestration.transitions import (
    evaluate_transition,
)
from ai_trading_system.domains.opportunities.routing import (
    ManualScanOverride,
    REASON_MINIMUM_TIER,
    RoutingConflictCode,
    SCAN_ROUTING_POLICY_VERSION,
    ScanRoutingDecision,
    ScanReason,
    ScanTier,
    decide_scan_route,
    validate_scan_routing_row,
)
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext

NOW = datetime(2026, 7, 14, tzinfo=timezone.utc)


def test_reason_minimum_tier_mapping_is_policy_v2() -> None:
    assert SCAN_ROUTING_POLICY_VERSION == "scan-routing-policy-v2"
    assert (
        REASON_MINIMUM_TIER[ScanReason.FULL_UNIVERSE_STRUCTURAL] is ScanTier.STAGE_ONLY
    )
    assert REASON_MINIMUM_TIER[ScanReason.STAGE_1_DISCOVERY] is ScanTier.LIGHT_PATTERN
    assert (
        REASON_MINIMUM_TIER[ScanReason.STAGE_TRANSITION_DISCOVERY]
        is ScanTier.LIGHT_PATTERN
    )
    assert REASON_MINIMUM_TIER[ScanReason.RANK_SELECTED] is ScanTier.FULL_INVESTIGATOR
    assert REASON_MINIMUM_TIER[ScanReason.STAGE_PROMOTED] is ScanTier.FULL_INVESTIGATOR
    assert (
        REASON_MINIMUM_TIER[ScanReason.TRIGGERED_CANDIDATE]
        is ScanTier.FULL_INVESTIGATOR
    )
    assert (
        REASON_MINIMUM_TIER[ScanReason.PENDING_FOLLOWTHROUGH]
        is ScanTier.FULL_INVESTIGATOR
    )
    assert REASON_MINIMUM_TIER[ScanReason.ACTIVE_POSITION] is ScanTier.POSITION_MONITOR
    assert REASON_MINIMUM_TIER[ScanReason.RECENT_EXIT] is ScanTier.POSITION_MONITOR


def test_winning_reason_is_deterministic_and_permutation_independent() -> None:
    first = decide_scan_route(
        symbol_id="abc",
        rank_selected=True,
        stage_promoted=True,
        triggered=True,
        pending_followthrough=True,
        stock_stage=WeinsteinStage.STAGE_2,
    )
    second = decide_scan_route(
        symbol_id="abc",
        pending_followthrough=True,
        triggered=True,
        stage_promoted=True,
        rank_selected=True,
        stock_stage=WeinsteinStage.STAGE_2,
    )
    assert first.scan_tier is ScanTier.FULL_INVESTIGATOR
    assert first.effective_scan_tier is ScanTier.FULL_INVESTIGATOR
    assert first.winning_reason is ScanReason.PENDING_FOLLOWTHROUGH
    assert first.reasons == second.reasons
    assert first.winning_reason is second.winning_reason
    assert first.routing_input_hash == second.routing_input_hash
    assert first.routing_decision_id == second.routing_decision_id


def test_active_recent_and_followthrough_cannot_be_downgraded() -> None:
    expired = ManualScanOverride(
        requested_tier=ScanTier.STAGE_ONLY,
        reviewer="risk",
        expires_at=NOW - timedelta(days=1),
    )
    ignored = decide_scan_route(
        symbol_id="AAA",
        active_position=True,
        manual_overrides=(expired,),
        decided_at=NOW,
    )
    assert ignored.scan_tier is ScanTier.POSITION_MONITOR
    assert not ignored.validation_conflicts

    active = decide_scan_route(
        symbol_id="AAA",
        active_position=True,
        manual_overrides=(
            ManualScanOverride(ScanTier.STAGE_ONLY, "risk", NOW + timedelta(days=1)),
        ),
        decided_at=NOW,
    )
    assert active.scan_tier is ScanTier.POSITION_MONITOR
    assert active.effective_scan_tier is ScanTier.POSITION_MONITOR
    assert {conflict.code for conflict in active.validation_conflicts} >= {
        RoutingConflictCode.INVALID_MANUAL_OVERRIDE,
        RoutingConflictCode.ACTIVE_POSITION_DEMOTION,
    }

    followthrough = decide_scan_route(
        symbol_id="BBB",
        pending_followthrough=True,
        manual_overrides=(
            ManualScanOverride(ScanTier.LIGHT_PATTERN, "risk", NOW + timedelta(days=1)),
        ),
        decided_at=NOW,
    )
    assert followthrough.scan_tier is ScanTier.FULL_INVESTIGATOR
    assert followthrough.effective_scan_tier is ScanTier.FULL_INVESTIGATOR
    assert RoutingConflictCode.FOLLOWTHROUGH_DEMOTION in {
        conflict.code for conflict in followthrough.validation_conflicts
    }


def test_manual_override_can_only_elevate() -> None:
    decision = decide_scan_route(
        symbol_id="AAA",
        stock_stage=WeinsteinStage.STAGE_1,
        manual_overrides=(
            ManualScanOverride(
                ScanTier.FULL_INVESTIGATOR,
                "lead",
                NOW + timedelta(days=1),
                "event risk",
            ),
        ),
        decided_at=NOW,
    )
    assert decision.scan_tier is ScanTier.FULL_INVESTIGATOR
    assert decision.effective_scan_tier is ScanTier.FULL_INVESTIGATOR
    assert decision.winning_reason is ScanReason.MANUAL_OVERRIDE
    assert ScanReason.MANUAL_OVERRIDE in decision.reasons

    invalid = decide_scan_route(
        symbol_id="AAA",
        manual_overrides=(
            ManualScanOverride(ScanTier.FULL_INVESTIGATOR, "", NOW + timedelta(days=1)),
        ),
        decided_at=NOW,
    )
    assert invalid.scan_tier is ScanTier.STAGE_ONLY
    assert invalid.effective_scan_tier is ScanTier.STAGE_ONLY
    assert RoutingConflictCode.INVALID_MANUAL_OVERRIDE in {
        conflict.code for conflict in invalid.validation_conflicts
    }


@pytest.mark.parametrize(
    ("stage", "severity"),
    (
        (WeinsteinStage.STAGE_3, "HIGH"),
        (WeinsteinStage.TRANSITION_3_TO_4, "CRITICAL"),
        (WeinsteinStage.STAGE_4, "CRITICAL"),
    ),
)
def test_active_structural_stages_route_to_position_monitor_with_risk(
    stage: WeinsteinStage, severity: str
) -> None:
    decision = decide_scan_route(
        symbol_id="AAA",
        active_position=True,
        stock_stage=stage,
    )
    assert decision.effective_scan_tier is ScanTier.POSITION_MONITOR
    assert decision.winning_reason is ScanReason.ACTIVE_POSITION
    assert set(decision.all_selection_reasons) == {
        ScanReason.ACTIVE_POSITION,
        ScanReason.FULL_UNIVERSE_STRUCTURAL,
    }
    assert decision.new_long_structural_blocked is True
    assert decision.structural_long_blocked is True
    assert decision.active_position_structural_risk is True
    assert decision.structural_risk_severity == severity


def test_active_stage4_rank_selected_keeps_rank_but_active_wins() -> None:
    decision = decide_scan_route(
        symbol_id="AAA",
        active_position=True,
        rank_selected=True,
        rank_position=12,
        stock_stage=WeinsteinStage.STAGE_4,
    )
    replay = decide_scan_route(
        symbol_id="AAA",
        rank_selected=True,
        rank_position=12,
        active_position=True,
        stock_stage=WeinsteinStage.STAGE_4,
    )
    assert decision.effective_scan_tier is ScanTier.POSITION_MONITOR
    assert decision.winning_reason is ScanReason.ACTIVE_POSITION
    assert ScanReason.RANK_SELECTED in decision.all_selection_reasons
    assert ScanReason.ACTIVE_POSITION in decision.all_selection_reasons
    assert decision.new_long_structural_blocked is True
    assert decision.active_position_structural_risk is True
    assert decision.structural_risk_severity == "CRITICAL"
    assert decision.routing_input_hash == replay.routing_input_hash
    assert decision.routing_decision_id == replay.routing_decision_id


def test_structural_policy_splits_new_long_block_from_active_risk() -> None:
    stage3 = decide_scan_route(symbol_id="AAA", stock_stage=WeinsteinStage.STAGE_3)
    assert stage3.new_long_structural_blocked is True
    assert stage3.active_position_structural_risk is False

    active_stage3 = decide_scan_route(
        symbol_id="AAA", active_position=True, stock_stage=WeinsteinStage.STAGE_3
    )
    assert active_stage3.scan_tier is ScanTier.POSITION_MONITOR
    assert active_stage3.effective_scan_tier is ScanTier.POSITION_MONITOR
    assert active_stage3.new_long_structural_blocked is True
    assert active_stage3.active_position_structural_risk is True
    assert active_stage3.structural_risk_severity == "HIGH"

    active_stage4 = decide_scan_route(
        symbol_id="AAA",
        active_position=True,
        stock_stage=WeinsteinStage.TRANSITION_3_TO_4,
    )
    assert active_stage4.structural_risk_severity == "CRITICAL"


def test_invalid_active_position_full_investigator_construction_raises() -> None:
    with pytest.raises(ValueError, match="EFFECTIVE_TIER_TOO_LOW"):
        ScanRoutingDecision(
            symbol_id="AAA",
            exchange="NSE",
            scan_tier=ScanTier.FULL_INVESTIGATOR,
            effective_scan_tier=ScanTier.FULL_INVESTIGATOR,
            winning_reason=ScanReason.ACTIVE_POSITION,
            reasons=(ScanReason.ACTIVE_POSITION,),
            all_selection_reasons=(ScanReason.ACTIVE_POSITION,),
            rank_selected=False,
            stage_selected=False,
            position_selected=True,
            recent_exit_selected=False,
            followthrough_selected=False,
            rank_position=None,
            stock_stage=WeinsteinStage.STAGE_2,
            sector_stage=WeinsteinStage.UNKNOWN,
            active_position=True,
            recently_exited=False,
            structural_long_blocked=False,
            market_data_available=True,
        )


def test_routing_row_validation_rejects_unknowns_and_downgrades() -> None:
    conflicts = validate_scan_routing_row(
        {
            "exchange": "NSE",
            "symbol_id": "AAA",
            "scan_tier": "stage_only",
            "scan_reasons": ["active_position"],
            "winning_reason": "active_position",
        }
    )
    assert RoutingConflictCode.EFFECTIVE_TIER_TOO_LOW in {
        conflict.code for conflict in conflicts
    }

    unknowns = validate_scan_routing_row(
        {
            "exchange": "NSE",
            "symbol_id": "AAA",
            "scan_tier": "mystery",
            "scan_reasons": ["not_a_reason"],
        }
    )
    assert RoutingConflictCode.UNKNOWN_SCAN_TIER in {
        conflict.code for conflict in unknowns
    }


def test_invalid_scan_routing_is_not_attached_to_opportunity_bundle(
    stage_factory,
) -> None:
    bundle = stage_factory()
    bundles, rejections = _attach_routing(
        (),
        [
            {
                "exchange": "NSE",
                "symbol_id": "AAA",
                "scan_tier": "stage_only",
                "scan_reasons": "['active_position']",
                "active_position": "true",
            }
        ],
        NOW,
    )
    assert not bundles
    assert len(rejections) == 1
    assert "requires at least position_monitor" in rejections[0].reason
    assert bundle.effective_stage is WeinsteinStage.STAGE_2


def test_routed_investigator_excludes_invalid_deep_scan_rows(
    tmp_path, monkeypatch
) -> None:
    data_root = tmp_path / "runtime"
    data_root.mkdir()
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    routing_path = tmp_path / "deep_scan_universe.csv"
    pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "scan_tier": "stage_only",
                "scan_reasons": ["active_position"],
                "winning_reason": "active_position",
            }
        ]
    ).to_csv(routing_path, index=False)
    context = StageContext(
        tmp_path,
        tmp_path / "ohlcv.duckdb",
        "phase3c2-test",
        "2026-07-14",
        "investigator",
        1,
        artifacts={
            "scan_router": {
                "deep_scan_universe": StageArtifact.from_file(
                    "deep_scan_universe", routing_path, row_count=1
                )
            }
        },
    )
    result = InvestigatorService().run_routed_shadow(context)
    assert result.metadata["routed_shadow"] == "degraded"
    assert result.metadata["routed_symbols"] == 0
    assert result.metadata["routing_conflicts"] == 1


def test_provisional_early_entry_sector_policy_fails_closed(
    stage_factory, sector_factory
) -> None:
    from tests.domains.opportunities.orchestration.test_policies import _bundle

    stock = stage_factory(
        status=StageStatus.PROVISIONAL,
        provisional=WeinsteinStage.TRANSITION_1_TO_2,
        confidence=90,
    )
    trigger = BreakoutEvidence(True, False, 90, "A", "triggered")
    base = replace(
        _bundle(stage_factory, sector_factory, stock=stock), breakout_events=(trigger,)
    )

    from ai_trading_system.domains.opportunities.orchestration.contracts import SectorGateEvidence

    unknown = replace(base, sector_stage=None, sector_gate=SectorGateEvidence(
        taxonomy_cause="sector_locked_snapshot_missing",
    ))
    unknown_result = evaluate_transition(
        CandidateState.READY, unknown, config=OpportunityShadowConfig()
    )
    assert "sector_locked_snapshot_missing" in unknown_result.blockers
    monitoring_route = decide_scan_route(
        symbol_id="AAA",
        stage_discovery=True,
        stock_stage=WeinsteinStage.TRANSITION_1_TO_2,
        sector_stage=WeinsteinStage.UNKNOWN,
    )
    assert monitoring_route.effective_scan_tier is ScanTier.LIGHT_PATTERN
    assert not monitoring_route.validation_conflicts

    provisional_sector = replace(
        base,
        sector_stage=sector_factory(
            stage=stage_factory(
                status=StageStatus.PROVISIONAL, provisional=WeinsteinStage.STAGE_2
            )
        ),
        sector_gate=SectorGateEvidence(
            prior_locked_stage=WeinsteinStage.STAGE_1,
            current_provisional_stage=WeinsteinStage.TRANSITION_1_TO_2,
            taxonomy_cause="sector_not_stage_2",
            calibration_cohort="stage_1_improving_blocked_v1",
        ),
    )
    assert (
        "sector_not_stage_2"
        in evaluate_transition(
            CandidateState.READY, provisional_sector, config=OpportunityShadowConfig()
        ).blockers
    )

    locked_stage1_sector = replace(
        base,
        sector_stage=sector_factory(
            stage=stage_factory(
                status=StageStatus.LOCKED, locked=WeinsteinStage.STAGE_1
            )
        ),
        sector_gate=SectorGateEvidence(
            prior_locked_stage=WeinsteinStage.STAGE_1,
            taxonomy_cause="sector_not_stage_2",
        ),
    )
    assert (
        "sector_not_stage_2"
        in evaluate_transition(
            CandidateState.READY, locked_stage1_sector, config=OpportunityShadowConfig()
        ).blockers
    )

    locked_stage2_sector = replace(
        base,
        sector_stage=sector_factory(
            stage=stage_factory(
                status=StageStatus.LOCKED, locked=WeinsteinStage.STAGE_2
            )
        ),
        sector_gate=SectorGateEvidence(
            prior_locked_stage=WeinsteinStage.STAGE_2,
        ),
    )
    assert evaluate_transition(
        CandidateState.READY, locked_stage2_sector, config=OpportunityShadowConfig()
    ).allowed
