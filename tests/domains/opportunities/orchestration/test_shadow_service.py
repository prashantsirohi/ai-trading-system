from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

import duckdb
import pandas as pd

from ai_trading_system.domains.opportunities.contracts import (
    CandidateState,
    StageStatus,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.coverage import (
    build_sector_coverage,
    persist_stage_history,
)
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    BreakoutEvidence,
    OpportunitySourceBundle,
)
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    OpportunityRegistryMode,
    OpportunityShadowConfig,
)
from ai_trading_system.domains.opportunities.orchestration.service import (
    OpportunityArtifactSet,
    OpportunityShadowOrchestrator,
    _attach_sector_gate_evidence,
    _technical_evidence_cohorts,
)
from ai_trading_system.domains.opportunities.orchestration.transitions import (
    evaluate_transition,
)
from ai_trading_system.domains.opportunities.routing import StageCoverageConfig
from ai_trading_system.domains.opportunities.stage_governance import MembershipTrust
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.pipeline.registry import RegistryStore


NOW = datetime(2026, 7, 14, tzinfo=timezone.utc)


def _artifact(tmp_path, name, content):
    path = tmp_path / f"{name}.csv"
    path.write_text(content, encoding="utf-8")
    return StageArtifact.from_file(name, path, attempt_number=1)


def _artifacts(tmp_path):
    return OpportunityArtifactSet(
        ranked_signals=_artifact(
            tmp_path,
            "ranked_signals",
            "symbol_id,exchange,composite_score,sector_name\nABC,NSE,95,Capital Goods\n",
        ),
        investigator_scores=_artifact(
            tmp_path,
            "investigator_scores",
            "symbol_id,exchange,final_score,verdict,early_accumulation_score,pattern_score,extension_risk,failure_risk\nABC,NSE,90,HIGH_CONVICTION,85,90,low,low\n",
        ),
        breakout_scan=_artifact(
            tmp_path,
            "breakout_scan",
            "symbol_id,exchange,breakout_state,candidate_tier,breakout_score,qualified\nABC,NSE,QUALIFIED,A,90,true\n",
        ),
        pattern_scan=_artifact(
            tmp_path,
            "pattern_scan",
            "symbol_id,exchange,pattern_family,pattern_state,pattern_score,qualified\nABC,NSE,VCP,READY,90,true\n",
        ),
        stock_scan=_artifact(
            tmp_path,
            "stock_scan",
            "symbol_id,exchange,weekly_stage_label,weekly_stage_confidence,week_end_date\nABC,NSE,S1_TO_S2,0.80,2026-07-14\n",
        ),
        sector_dashboard=_artifact(
            tmp_path,
            "sector_dashboard",
            "Sector,sector_stage,sector_stage_confidence,sector_stage_status,week_end_date,created_at,RS_rank_pct,Quadrant\nCapital Goods,S2,0.85,locked,2026-07-10,2026-07-10T12:00:00+00:00,Improving,Leading\n",
        ),
    )


def _momentum_artifacts(tmp_path):
    return OpportunityArtifactSet(
        ranked_signals=_artifact(
            tmp_path,
            "momentum_ranked_signals",
            "symbol_id,exchange,composite_score,sector_name\n"
            "ABC,NSE,95,Capital Goods\n",
        )
    )


def _breakout_artifacts(tmp_path):
    return replace(
        _momentum_artifacts(tmp_path),
        breakout_scan=_artifact(
            tmp_path,
            "superseding_breakout_scan",
            "symbol_id,exchange,breakout_state,candidate_tier,breakout_score,qualified\n"
            "ABC,NSE,QUALIFIED,A,90,true\n",
        ),
    )


def test_shadow_service_writes_and_replay_is_idempotent(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = _artifacts(tmp_path)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    first = service.run(
        run_id="run-1",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    second = service.run(
        run_id="run-1",
        stage_attempt=2,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    assert first.summary["new_episodes_opened"] == 1
    assert first.summary["snapshots_created"] == 1
    admission = first.artifact_rows["candidate_admissions"][0]
    assert admission["primary_admission_reason"] == "qualified_breakout"
    assert admission["primary_setup_family"] == "breakout"
    assert "rank_threshold" in json.loads(admission["satisfied_admission_rules"])
    assert len(json.loads(admission["rule_evaluations"])) == 9
    episode = service.registry.list_open_episodes()[0]
    assert (
        episode.satisfied_admission_rules_json == admission["satisfied_admission_rules"]
    )
    assert episode.rule_evaluations_json == admission["rule_evaluations"]
    assert second.summary["registry_duplicates"] == 1
    assert first.summary["breakout_scan_receipt_status"] == "SUCCESS_ROWS"
    assert first.summary["pattern_scan_receipt_status"] == "SUCCESS_ROWS"
    assert first.summary["opportunity_integrity_status"] == "PASS"
    assert first.summary["opportunity_registry_freshness_status"] == "PASS"
    assert (
        len(first.artifact_rows["candidate_transitions"])
        == first.summary["transitions_created"]
    )
    assert all(
        row["status"] == "PASS"
        for row in first.artifact_rows["opportunity_integrity_receipt"]
    )
    first_freshness = first.artifact_rows["opportunity_registry_freshness"][0]
    assert first_freshness["current_run_session_transition_count"] == 1
    assert first_freshness["current_session_transition_count"] == 1
    assert len(service.registry.list_open_episodes()) == 1
    with registry._connect(read_only=True) as conn:  # noqa: SLF001
        assert (
            conn.execute(
                "SELECT count(*) FROM symbol_technical_evidence_observation"
            ).fetchone()[0]
            == 1
        )
        snapshot = conn.execute(
            """
            SELECT stage_label, stage_confidence, pattern_family, pattern_state,
                   candidate_tier, qualified_breakout,
                   sector_relative_strength_bucket,
                   investigator_attribution_mode, investigator_context_json
            FROM candidate_snapshot
            """
        ).fetchone()
    context = json.loads(snapshot[8])
    assert snapshot[:4] == ("TRANSITION_1_TO_2", 80.0, "VCP", "READY")
    assert snapshot[4:8] == ("A", True, "HIGH", "OBSERVED_AT_DECISION")
    assert context["context_as_of"] == "2026-07-14T00:00:00+00:00"
    assert context["pattern_events"][0]["family"] == "VCP"
    assert context["breakout_events"][0]["tier"] == "A"


def test_registry_freshness_is_session_scoped_across_distinct_runs(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    artifacts = _artifacts(tmp_path)
    service.run(
        run_id="same-session-1",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )

    result = service.run(
        run_id="same-session-2",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )

    freshness = result.artifact_rows["opportunity_registry_freshness"][0]
    assert freshness["freshness_status"] == "PASS"
    assert freshness["current_run_session_snapshot_count"] == 1
    assert freshness["current_session_snapshot_count"] == 2


def test_source_row_count_mismatch_degrades_integrity_receipt(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = _artifacts(tmp_path)
    artifacts = replace(
        artifacts,
        ranked_signals=replace(artifacts.ranked_signals, row_count=2),
    )

    result = service.run(
        run_id="row-count-mismatch",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=OpportunityRegistryMode.SHADOW,
        config=OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW),
    )

    ranked_receipt = next(
        row
        for row in result.artifact_rows["opportunity_source_reconciliation"]
        if row["artifact_type"] == "ranked_signals"
    )
    assert ranked_receipt["status"] == "FAIL"
    assert ranked_receipt["declared_row_count"] == 2
    assert ranked_receipt["rows_read"] == 1
    assert result.summary["opportunity_integrity_status"] == "FAIL"
    assert result.status == "degraded"


def test_dashboard_payload_reconciles_its_declared_ranked_count(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    dashboard_path = tmp_path / "dashboard_payload.json"
    dashboard_path.write_text(
        json.dumps({"summary": {"ranked_count": 2}}), encoding="utf-8"
    )
    artifacts = replace(
        _artifacts(tmp_path),
        market_context=StageArtifact.from_file(
            "dashboard_payload", dashboard_path, row_count=2, attempt_number=1
        ),
    )

    result = service.run(
        run_id="dashboard-row-count",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=OpportunityRegistryMode.SHADOW,
        config=OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW),
    )

    receipt = next(
        row
        for row in result.artifact_rows["opportunity_source_reconciliation"]
        if row["artifact_type"] == "dashboard_payload"
    )
    assert receipt["status"] == "PASS"
    assert receipt["declared_row_count"] == receipt["rows_read"] == 2


def test_investigator_row_without_rank_score_is_evidence_only_not_rejected(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = replace(
        _momentum_artifacts(tmp_path),
        investigator_scores=_artifact(
            tmp_path,
            "evidence_only_investigator",
            "symbol_id,exchange,final_score,trigger_reason,move_tag,close\n"
            "DEF,NSE,60,WEEKLY_GAINER,WEEKLY_MOMENTUM,100\n",
        ),
    )

    result = service.run(
        run_id="evidence-only-investigator",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=OpportunityRegistryMode.SHADOW,
        config=OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW),
    )

    assert result.summary["investigator_evidence_only_rows"] == 1
    assert result.summary["investigator_evidence_only_weekly_gainers"] == 1
    assert result.summary["investigator_rank_context_fallback_rows"] == 0
    assert not any(
        row["source_artifact"] == "evidence_only_investigator"
        for row in result.artifact_rows["adapter_rejections"]
    )
    label = next(
        row
        for row in result.artifact_rows["technical_evidence_labels"]
        if row["symbol_id"] == "DEF"
    )
    assert label["weekly_gainer_state"] == "MET"
    assert not any(
        episode.symbol_id == "DEF" for episode in service.registry.list_open_episodes()
    )


def test_fundamental_lane_does_not_block_later_technical_episode(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    fundamental = _artifact(
        tmp_path,
        "parallel_fundamental_thesis",
        "symbol_id,exchange,primary_thesis,secondary_theses_json,evaluations_json,evidence_json,classification_status,admission_eligible,source_data_hash,statement_basis,source_report_date,source_available_at,taxonomy_version,rule_version,admission_version\n"
        'ABC,NSE,HIGH_GROWTH_EMERGING,"[]","[]","{}",QUALIFIED,true,hash-1,consolidated,2026-03-31,2026-05-15,fundamental-discovery-taxonomy-v1,fundamental-thesis-rules-v1,fundamental-thesis-admission-v1\n',
    )
    fundamental_only = replace(
        _momentum_artifacts(tmp_path), fundamental_thesis_universe=fundamental
    )
    service.run(
        run_id="fundamental-only",
        stage_attempt=1,
        artifact_set=fundamental_only,
        as_of=NOW,
        mode=OpportunityRegistryMode.SHADOW,
        config=OpportunityShadowConfig(
            mode=OpportunityRegistryMode.SHADOW,
            rank_admission_percentile=101,
            rank_velocity_floor=-999,
        ),
    )
    assert {
        episode.setup_family for episode in service.registry.list_open_episodes()
    } == {"fundamental_thesis"}

    result = service.run(
        run_id="technical-after-fundamental",
        stage_attempt=1,
        artifact_set=_breakout_artifacts(tmp_path),
        as_of=NOW + timedelta(days=1),
        mode=OpportunityRegistryMode.SHADOW,
        config=OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW),
    )

    assert result.summary["registry_conflicts"] == 0
    assert {
        episode.setup_family for episode in service.registry.list_open_episodes()
    } == {
        "breakout",
        "fundamental_thesis",
    }


def test_shadow_service_emits_unified_convergence_view_and_readiness(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    fundamental = _artifact(
        tmp_path,
        "convergence_fundamental",
        "as_of,symbol_id,exchange,primary_thesis,classification_status,admission_eligible,source_data_hash\n"
        "2026-07-14,ABC,NSE,QUALITY_COMPOUNDER,QUALIFIED,true,fundamental-row-hash\n",
    )
    artifacts = replace(
        _artifacts(tmp_path),
        investigator_scores=_artifact(
            tmp_path,
            "convergence_investigator",
            "symbol_id,exchange,trade_date,final_score,verdict,trigger_reason,move_tag\n"
            "ABC,NSE,2026-07-14,72,HIGH_CONVICTION,WEEKLY_GAINER,WEEKLY_MOMENTUM\n",
        ),
        investigator_intake_receipt=_artifact(
            tmp_path,
            "convergence_investigator_receipt",
            "symbol_id,exchange,trade_date,tracked,selected_trigger_reason,decision_state,reason_codes\n"
            "ABC,NSE,2026-07-14,true,WEEKLY_GAINER,TRACKED,TRACKED_WEEKLY_GAINER\n",
        ),
        fundamental_thesis_universe=fundamental,
        pattern_lane_assessments=_artifact(
            tmp_path,
            "convergence_pattern_assessments",
            "exchange,symbol_id,session_date,pattern_evaluation_state,pattern_member,primary_pattern_family,primary_pattern_state,lane_freshness,evidence_hash,signal_count\n"
            "NSE,ABC,2026-07-14,KNOWN,true,vcp,confirmed,FRESH,pattern-row-hash,1\n",
        ),
    )

    result = service.run(
        run_id="convergence-run",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=OpportunityRegistryMode.SHADOW,
        config=OpportunityShadowConfig(
            mode=OpportunityRegistryMode.SHADOW,
            dry_run=True,
        ),
    )

    convergence = result.artifact_rows["opportunity_convergence_view"]
    assert len(convergence) == 1
    assert convergence[0]["convergence_cohort"] == "I_F_P"
    checks = [
        row
        for row in result.artifact_rows["investigator_readiness_inputs"]
        if row["category"] == "opportunity_convergence"
    ]
    assert len(checks) == 9
    assert all(row["status"] == "PASS" for row in checks)
    assert result.summary["opportunity_convergence_cohorts"] == {"I_F_P": 1}
    assert len(result.artifact_rows["opportunity_convergence_observations"]) == 1
    assert {
        row["anchor_type"]
        for row in result.artifact_rows["opportunity_convergence_anchors"]
    } == {"DISCOVERY_CLOSE"}
    assert len(result.artifact_rows["opportunity_convergence_horizons"]) == 4
    assert (
        len(result.artifact_rows["opportunity_convergence_performance_readiness"]) == 7
    )


def test_rejected_transition_write_never_emits_phantom_transition(
    tmp_path, monkeypatch
):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)

    def reject_bundle(_bundle):
        raise ValueError("forced registry rejection")

    monkeypatch.setattr(service.registry, "apply_orchestration_bundle", reject_bundle)
    result = service.run(
        run_id="rejected-transition",
        stage_attempt=1,
        artifact_set=_artifacts(tmp_path),
        as_of=NOW,
        mode=OpportunityRegistryMode.SHADOW,
        config=OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW),
    )

    assert result.summary["rejected_writes"] == 1
    assert result.summary["transitions_created"] == 0
    assert result.artifact_rows["candidate_transitions"] == ()
    transition_receipt = next(
        row
        for row in result.artifact_rows["opportunity_integrity_receipt"]
        if row["check_id"] == "TRANSITION_ARTIFACT_RECONCILIATION"
    )
    assert transition_receipt["status"] == "PASS"
    assert result.artifact_rows["registry_conflicts"][0]["reason_code"] == (
        "REGISTRY_WRITE_REJECTED"
    )


def test_registry_receipt_preserves_missing_market_sessions_without_backfill(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    market_db = tmp_path / "ohlcv.duckdb"
    with duckdb.connect(str(market_db)) as conn:
        conn.execute(
            "CREATE TABLE _catalog ("
            "exchange VARCHAR, symbol_id VARCHAR, timestamp TIMESTAMP, "
            "adjusted_close DOUBLE, open DOUBLE, high DOUBLE, low DOUBLE, "
            "close DOUBLE, is_benchmark BOOLEAN)"
        )
        conn.executemany(
            "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, FALSE)",
            [
                ("NSE", "ABC", "2026-07-14 15:30:00", 100.0, 99.0, 101.0, 98.0, 100.0),
                ("NSE", "ABC", "2026-07-15 15:30:00", 101.0, 100.0, 102.0, 99.0, 101.0),
                (
                    "NSE",
                    "ABC",
                    "2026-07-16 15:30:00",
                    102.0,
                    101.0,
                    103.0,
                    100.0,
                    102.0,
                ),
                (
                    "NSE",
                    "ABC",
                    "2026-07-17 15:30:00",
                    103.0,
                    102.0,
                    104.0,
                    101.0,
                    103.0,
                ),
            ],
        )
    service = OpportunityShadowOrchestrator(registry)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    service.run(
        run_id="session-14",
        stage_attempt=1,
        artifact_set=_artifacts(tmp_path),
        as_of=NOW,
        mode=config.mode,
        config=config,
        ohlcv_db_path=market_db,
    )

    result = service.run(
        run_id="session-17",
        stage_attempt=1,
        artifact_set=_artifacts(tmp_path),
        as_of=NOW + timedelta(days=3),
        mode=config.mode,
        config=config,
        ohlcv_db_path=market_db,
    )

    freshness = result.artifact_rows["opportunity_registry_freshness"][0]
    assert freshness["freshness_status"] == "PASS"
    assert freshness["continuity_status"] == "FAIL"
    assert json.loads(freshness["missing_sessions"]) == [
        "2026-07-15",
        "2026-07-16",
    ]
    assert result.status == "degraded"
    with registry._reader() as conn:  # noqa: SLF001
        sessions = conn.execute(
            "SELECT DISTINCT CAST(as_of AS DATE) FROM candidate_snapshot ORDER BY 1"
        ).fetchall()
    assert sessions == [
        (NOW.date(),),
        ((NOW + timedelta(days=3)).date(),),
    ]


def test_shadow_service_uses_investigator_sector_and_fractional_percentile(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = replace(
        _artifacts(tmp_path),
        ranked_signals=_artifact(
            tmp_path,
            "ranked_signals_without_sector",
            "symbol_id,exchange,composite_score\nABC,NSE,95\n",
        ),
        investigator_scores=_artifact(
            tmp_path,
            "investigator_scores_with_sector",
            "symbol_id,exchange,sector_name,final_score,verdict,trigger_reason,move_tag,RS_rank_pct_sector,pattern_evaluation_state,pattern_classification_state\n"
            "ABC,NSE,Pharma,90,HIGH_CONVICTION,WEEKLY_GAINER,WEEKLY_MOMENTUM,0.53,NONE,NONE\n",
        ),
        breakout_scan=None,
        pattern_scan=None,
        sector_dashboard=None,
    )

    service.run(
        run_id="investigator-sector-run",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=OpportunityRegistryMode.SHADOW,
        config=OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW),
    )

    with registry._connect(read_only=True) as conn:  # noqa: SLF001
        snapshot = conn.execute(
            """
            SELECT sector_relative_strength_bucket, investigator_context_json
            FROM candidate_snapshot
            """
        ).fetchone()
    assert snapshot[0] == "MID"
    context = json.loads(snapshot[1])
    assert context["evaluation_states"]["pattern_attempted"] == "NONE"
    assert context["evaluation_states"]["pattern"] == "NONE"


def test_shadow_service_distinguishes_successful_zero_row_scan(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = replace(
        _momentum_artifacts(tmp_path),
        breakout_scan=_artifact(
            tmp_path,
            "empty_breakout_scan",
            "symbol_id,exchange,breakout_state,qualified\n",
        ),
    )

    result = service.run(
        run_id="zero-breakout-run",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=OpportunityRegistryMode.SHADOW,
        config=OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW),
    )

    assert result.summary["breakout_rows_read"] == 0
    assert result.summary["breakout_scan_receipt_status"] == "SUCCESS_ZERO_ROWS"
    assert result.summary["pattern_scan_receipt_status"] == "MISSING"


def test_fundamental_episode_is_parallel_and_persists_observation(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    fundamental = _artifact(
        tmp_path,
        "fundamental_thesis_universe",
        "symbol_id,exchange,primary_thesis,secondary_theses_json,evaluations_json,evidence_json,classification_status,admission_eligible,source_data_hash,statement_basis,source_report_date,source_available_at,taxonomy_version,rule_version,admission_version\n"
        'ABC,NSE,HIGH_GROWTH_EMERGING,"[]","[]","{}",QUALIFIED,true,hash-1,consolidated,2026-03-31,2026-05-15,fundamental-discovery-taxonomy-v1,fundamental-thesis-rules-v1,fundamental-thesis-admission-v1\n',
    )
    artifacts = replace(
        _artifacts(tmp_path),
        investigator_scores=_artifact(
            tmp_path,
            "investigator_scores_with_technical_labels",
            "symbol_id,exchange,final_score,verdict,early_accumulation_score,pattern_score,extension_risk,failure_risk,trigger_reason,close,sma_20,high_52w\n"
            "ABC,NSE,90,HIGH_CONVICTION,85,90,low,low,WEEKLY_GAINER,99,95,100\n",
        ),
        fundamental_thesis_universe=fundamental,
    )
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    result = service.run(
        run_id="fundamental-run",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    families = {item.setup_family for item in service.registry.list_open_episodes()}
    assert {"investigator_primary", "fundamental_thesis"}.issubset(families)
    assert len(result.artifact_rows["candidate_fundamental_observations"]) == 1
    with registry._connect(read_only=True) as conn:  # noqa: SLF001
        assert (
            conn.execute(
                "SELECT count(*) FROM candidate_fundamental_observation"
            ).fetchone()[0]
            == 1
        )
        snapshots = conn.execute(
            """
            SELECT e.setup_family, s.investigator_price,
                   s.technical_evidence_observation_id
            FROM candidate_snapshot s
            JOIN candidate_episode e USING (candidate_id)
            ORDER BY e.setup_family
            """
        ).fetchall()
        technical = conn.execute(
            """
            SELECT weekly_gainer_state, near_52w_high_10_state,
                   above_sma20_state, entry_confirmed_state
            FROM symbol_technical_evidence_observation
            """
        ).fetchone()
    assert len({row[2] for row in snapshots}) == 1
    assert dict((row[0], row[1]) for row in snapshots)["fundamental_thesis"] is None
    assert technical == ("MET", "MET", "MET", "MET")
    label_row = result.artifact_rows["technical_evidence_labels"][0]
    assert label_row["evidence_lanes"] == "INVESTIGATOR|FUNDAMENTAL_THESIS"
    assert label_row["fundamental_thesis_state"] == "MET"
    with registry._writer() as conn:  # noqa: SLF001
        events = conn.execute(
            """
            SELECT pe.event_id, ep.setup_family
            FROM investigator_performance_event pe
            JOIN candidate_episode ep USING (candidate_id)
            WHERE pe.event_type = 'CANDIDATE_DISCOVERED'
            """
        ).fetchall()
        for event_id, family in events:
            conn.execute(
                """
                INSERT INTO investigator_performance_horizon (
                    event_id, horizon_sessions, next_open_entry_return_pct,
                    data_quality_status
                ) VALUES (?, ?, ?, ?)
                """,
                [
                    event_id,
                    20,
                    12.0 if family == "fundamental_thesis" else 8.0,
                    "MATURED",
                ],
            )
    cohorts = _technical_evidence_cohorts(registry)
    assert {(row["cohort_type"], row["sample_count"]) for row in cohorts} == {
        ("FUNDAMENTAL_AND_TECHNICAL", 1),
        ("TECHNICAL_ONLY", 1),
    }


def test_not_admitted_reconciliation_surfaces_rule_evaluations(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = _momentum_artifacts(tmp_path)
    config = OpportunityShadowConfig(
        mode=OpportunityRegistryMode.SHADOW,
        rank_admission_percentile=101,
        rank_velocity_floor=-999,
    )
    result = service.run(
        run_id="blocked-run",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    row = result.artifact_rows["candidate_reconciliation"][0]
    assert row["outcome"] == "not_admitted"
    evaluations = json.loads(row["rule_evaluations"])
    assert len(evaluations) == 9
    assert not any(item["passed"] for item in evaluations)


def test_dry_run_writes_no_registry_records(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW, dry_run=True)
    result = service.run(
        run_id="run-dry",
        stage_attempt=1,
        artifact_set=_artifacts(tmp_path),
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    assert result.summary["no_database_writes_performed"] is True
    assert service.registry.list_open_episodes() == ()


def test_momentum_breakout_supersession_is_atomic_and_replay_idempotent(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    service.run(
        run_id="momentum-run",
        stage_attempt=1,
        artifact_set=_momentum_artifacts(tmp_path),
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    momentum = service.registry.list_open_episodes()[0]
    breakout_at = NOW + timedelta(days=1)
    superseded = service.run(
        run_id="breakout-run",
        stage_attempt=1,
        artifact_set=_breakout_artifacts(tmp_path),
        as_of=breakout_at,
        mode=config.mode,
        config=config,
    )
    assert superseded.summary["registry_conflicts"] == 0
    assert superseded.summary["episodes_superseded"] == 1
    assert len(superseded.artifact_rows["candidate_supersessions"]) == 1
    episodes = service.registry.list_candidate_episodes(exchange="NSE", symbol_id="ABC")
    assert len(episodes) == 2
    assert episodes[0].closing_reason == "superseded_by_new_episode"
    assert episodes[1].setup_family == "breakout"
    assert (
        service.registry.list_episode_relations(momentum.candidate_id)[
            0
        ].successor_candidate_id
        == episodes[1].candidate_id
    )

    replay = service.run(
        run_id="breakout-run",
        stage_attempt=1,
        artifact_set=_breakout_artifacts(tmp_path),
        as_of=breakout_at,
        mode=config.mode,
        config=config,
    )
    assert replay.summary["registry_duplicates"] == 1
    assert len(service.registry.list_episode_relations(momentum.candidate_id)) == 1
    assert (
        len(service.registry.list_candidate_episodes(exchange="NSE", symbol_id="ABC"))
        == 2
    )


def test_dry_run_reports_supersession_without_mutation(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    live = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    service.run(
        run_id="momentum-run",
        stage_attempt=1,
        artifact_set=_momentum_artifacts(tmp_path),
        as_of=NOW,
        mode=live.mode,
        config=live,
    )
    momentum = service.registry.list_open_episodes()[0]
    dry = replace(live, dry_run=True)
    result = service.run(
        run_id="breakout-dry-run",
        stage_attempt=1,
        artifact_set=_breakout_artifacts(tmp_path),
        as_of=NOW + timedelta(days=1),
        mode=dry.mode,
        config=dry,
    )
    assert result.summary["episodes_superseded"] == 1
    assert len(result.artifact_rows["candidate_supersessions"]) == 1
    assert (
        service.registry.get_candidate_episode(
            momentum.candidate_id
        ).episode_status.value
        == "OPEN"
    )
    assert service.registry.list_episode_relations(momentum.candidate_id) == ()


def test_sector_gate_taxonomy_is_emitted_in_summary_and_update_artifact(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = _artifacts(tmp_path)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    service.run(
        run_id="gate-ready",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    blocked = service.run(
        run_id="gate-trigger",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    assert blocked.summary["sector_gate_taxonomy_counts"] == {
        "latest_only_untrusted_membership": 1
    }
    update = blocked.artifact_rows["candidate_updates"][0]
    assert update["sector_gate_taxonomy"] == "latest_only_untrusted_membership"
    assert "latest_only_untrusted_membership" in update["transition_blockers"]


def test_changed_artifact_hash_in_same_run_is_not_misclassified_as_exact_replay(
    tmp_path,
):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = _artifacts(tmp_path)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    service.run(
        run_id="run-change",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    rank_path = tmp_path / "ranked_signals.csv"
    rank_path.write_text(
        "symbol_id,exchange,composite_score,sector_name\nABC,NSE,96,Capital Goods\n",
        encoding="utf-8",
    )
    changed = replace(
        artifacts,
        ranked_signals=StageArtifact.from_file(
            "ranked_signals", rank_path, attempt_number=1
        ),
    )
    result = service.run(
        run_id="run-change",
        stage_attempt=2,
        artifact_set=changed,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    assert result.summary["snapshots_created"] == 1
    assert result.summary["registry_duplicates"] == 0


def test_bulk_gate_evidence_makes_prior_locked_stage2_trigger_reachable(
    tmp_path, stage_factory, sector_factory
):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    prior_stock = pd.DataFrame(
        [
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "sector_id": "capital-goods",
                "sector_name": "Capital Goods",
                "sector_membership_trust": MembershipTrust.POINT_IN_TIME_VERIFIED.value,
                "sector_membership_observation_id": "membership-1",
                "as_of": "2026-07-10",
                "source_week_start": "2026-07-06",
                "source_week_end": "2026-07-10",
                "stage_status": "locked",
                "effective_stage": WeinsteinStage.STAGE_2.value,
                "classifier_version": "weekly-stage-v1",
                "source_artifact_hash": "prior-stock",
                "price_vs_weekly_ma_30_pct": 2.0,
                "weekly_ma_30_slope": 0.2,
                "weekly_ma_30_slope_acceleration": 0.1,
                "weekly_rs_slope": 1.0,
            }
        ]
    )
    prior_sector = build_sector_coverage(
        prior_stock, config=StageCoverageConfig(minimum_sector_constituents=1)
    )
    persist_stage_history(
        registry,
        prior_stock,
        prior_sector,
        run_id="prior-week",
        attempt=1,
        recorded_at=datetime(2026, 7, 10, 18, tzinfo=timezone.utc),
    )
    from tests.domains.opportunities.orchestration.test_policies import _bundle

    stock = stage_factory(
        status=StageStatus.PROVISIONAL,
        provisional=WeinsteinStage.TRANSITION_1_TO_2,
        confidence=90,
    )
    current_sector = sector_factory(
        stage=stage_factory(
            status=StageStatus.PROVISIONAL,
            provisional=WeinsteinStage.TRANSITION_1_TO_2,
        )
    )
    bundle: OpportunitySourceBundle = replace(
        _bundle(stage_factory, sector_factory, stock=stock),
        sector_stage=current_sector,
        breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
    )
    attached = _attach_sector_gate_evidence(
        registry,
        (bundle,),
        raw_stock=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "sector_membership_trust": MembershipTrust.POINT_IN_TIME_VERIFIED.value,
            }
        ],
        raw_sector=[
            {
                "sector_id": "capital-goods",
                "sector_name": "Capital Goods",
                "effective_stage": WeinsteinStage.TRANSITION_1_TO_2.value,
                "stage_breadth_velocity": 0.2,
            }
        ],
        as_of=NOW,
    )[0]
    assert attached.sector_gate is not None
    assert attached.sector_gate.prior_locked_stage is WeinsteinStage.STAGE_2
    assert (
        attached.sector_gate.current_provisional_stage
        is WeinsteinStage.TRANSITION_1_TO_2
    )
    assert attached.sector_gate.taxonomy_cause is None
    assert evaluate_transition(CandidateState.READY, attached).allowed
