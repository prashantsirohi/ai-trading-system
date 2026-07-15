from __future__ import annotations

import shutil
import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.domains.opportunities.contracts import (
    ActionEligibility,
    CandidateAction,
    CandidateDecision,
    CandidateSnapshot,
    CandidateState,
    DecisionContextSnapshot,
    EvidenceSnapshot,
    EvidenceVerdict,
    FollowthroughStatus,
    OpportunitySnapshot,
    OutcomeAttribution,
    OutcomeAttributionRecord,
    ProgressStatus,
    RiskLevel,
    StageStatus,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.coverage import (
    build_sector_coverage,
    persist_stage_history,
    read_sector_stage_as_of,
    read_stock_stage_as_of,
)
from ai_trading_system.domains.opportunities.registry.models import (
    AttributionObservation,
    DecisionContextObservation,
    OpenEpisodeRequest,
    SnapshotObservation,
    SourceLineage,
    StageObservation,
    StageScope,
)
from ai_trading_system.domains.opportunities.registry.store import DuckDBOpportunityRegistryStore
from ai_trading_system.domains.opportunities.routing import StageCoverageConfig
from ai_trading_system.domains.opportunities.stage_governance import (
    CorrectionAuthority,
    CorrectionImpactLinkStatus,
    MembershipTrust,
    SectorMembershipRecord,
    StageGovernanceAction,
    StageGovernanceConflictError,
    StageGovernanceCycleError,
    append_stage_governance,
    annotate_legacy_stage_history,
    append_sector_memberships,
    read_sector_membership_as_of,
    resolve_historical_sector_mapping,
)
from ai_trading_system.interfaces.cli.annotate_phase3c1_governance import annotate_copied_store
from ai_trading_system.pipeline.registry import RegistryStore


T1 = datetime(2026, 7, 17, 18, tzinfo=timezone.utc)
T2 = T1 + timedelta(days=2)
T3 = T2 + timedelta(days=2)


def _membership(
    *,
    sector: str = "tech",
    trust: MembershipTrust = MembershipTrust.POINT_IN_TIME_VERIFIED,
    start: date = date(2026, 7, 1),
    end: date = date(2026, 7, 31),
    recorded_at: datetime = T1,
    supersedes: str | None = None,
) -> SectorMembershipRecord:
    return SectorMembershipRecord(
        exchange="NSE", symbol_id="ABC", sector_id=sector,
        sector_name=sector.title(), valid_from=start, valid_to=end,
        membership_trust=trust, source_type="exchange_reference",
        source_hash=f"membership-{sector}-{recorded_at.isoformat()}", recorded_at=recorded_at,
        run_id="membership-run", stage_attempt=1,
        supersedes_membership_observation_id=supersedes,
    )


def _stock_row(
    *,
    stage: str = WeinsteinStage.STAGE_1.value,
    source_hash: str = "stock-v1",
    membership_id: str = "membership-v1",
    status: str = "locked",
) -> dict[str, object]:
    return {
        "exchange": "NSE", "symbol_id": "ABC", "sector_id": "tech", "sector_name": "Tech",
        "sector_membership_trust": MembershipTrust.POINT_IN_TIME_VERIFIED.value,
        "sector_membership_observation_id": membership_id,
        "as_of": "2026-07-17", "source_week_start": "2026-07-13",
        "source_week_end": "2026-07-17", "stage_status": status,
        "effective_stage": stage, "classifier_version": "weekly-stage-v1",
        "source_artifact_hash": source_hash, "price_vs_weekly_ma_30_pct": 2.0,
        "weekly_ma_30_slope": 0.2, "weekly_ma_30_slope_acceleration": 0.1,
        "weekly_rs_slope": 1.0,
    }


def _stock_observation_id(row: dict[str, object]) -> str:
    return hashlib.sha256(
        f"stock|{row['exchange']}|{row['symbol_id']}|{row['source_week_end']}|"
        f"{row['stage_status']}|{row['classifier_version']}|{row['source_artifact_hash']}".encode()
    ).hexdigest()


def _insert_stock_history(conn, row: dict[str, object], *, run_id: str, recorded_at: datetime) -> str:
    observation_id = _stock_observation_id(row)
    conn.execute(
        """INSERT INTO weekly_stock_stage_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(observation_id) DO NOTHING""",
        [
            observation_id, row["exchange"], row["symbol_id"], row.get("sector_id"), row.get("sector_name"),
            row["as_of"], row["source_week_start"], row["source_week_end"], row["stage_status"],
            row["effective_stage"], row["classifier_version"], row["source_artifact_hash"],
            json.dumps(row, default=str, sort_keys=True), run_id, 1,
            recorded_at.astimezone(timezone.utc).replace(tzinfo=None),
        ],
    )
    return observation_id


def test_effective_membership_boundaries_corrections_and_overlap_rejection(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    original = _membership()
    assert append_sector_memberships(registry, [original]) == {"created": 1, "duplicates": 0}
    assert append_sector_memberships(registry, [original]) == {"created": 0, "duplicates": 1}
    with pytest.raises(ValueError, match="overlapping sector membership"):
        append_sector_memberships(registry, [_membership(sector="banks", recorded_at=T2)])

    corrected = _membership(
        sector="technology", recorded_at=T2,
        supersedes=original.membership_observation_id,
    )
    append_sector_memberships(registry, [corrected])
    before = read_sector_membership_as_of(
        registry, effective_at="2026-07-01", available_at=T1, exchange="NSE"
    )
    after = read_sector_membership_as_of(
        registry, effective_at="2026-07-31", available_at=T3, exchange="NSE"
    )
    outside = read_sector_membership_as_of(
        registry, effective_at="2026-08-01", available_at=T3, exchange="NSE"
    )
    assert before.iloc[0]["sector_id"] == "tech"
    assert after.iloc[0]["sector_id"] == "technology"
    assert outside.empty


def test_latest_only_membership_is_quarantined_from_authoritative_reads_and_sector_aggregation(
    tmp_path: Path,
) -> None:
    rows = [
        _stock_row(),
        {**_stock_row(source_hash="stock-v2"), "symbol_id": "XYZ",
         "sector_membership_trust": MembershipTrust.LATEST_ONLY_BACKFILL.value},
    ]
    sector = build_sector_coverage(
        pd.DataFrame(rows), config=StageCoverageConfig(minimum_sector_constituents=1)
    )
    assert sector.iloc[0]["eligible_constituents"] == 1
    assert sector.iloc[0]["membership_trust"] == MembershipTrust.POINT_IN_TIME_VERIFIED.value
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    latest_only = pd.DataFrame([{
        **_stock_row(),
        "sector_membership_trust": MembershipTrust.LATEST_ONLY_BACKFILL.value,
    }])
    persist_stage_history(
        registry, latest_only, pd.DataFrame(), run_id="latest-only", attempt=1, recorded_at=T1
    )
    canonical_stock = read_stock_stage_as_of(
        registry, as_of="2026-07-17", available_at=T2
    )
    assert canonical_stock.iloc[0]["symbol_id"] == "ABC"
    assert canonical_stock.iloc[0]["sector_membership_trust"] == MembershipTrust.LATEST_ONLY_BACKFILL.value


def test_historical_master_fallback_is_tagged_and_replay_safe(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    first = resolve_historical_sector_mapping(
        registry, {"ABC": ("tech", "Tech")}, exchange="NSE",
        effective_at="2026-07-10", available_at=T1,
        run_id="backfill", stage_attempt=1,
    )
    second = resolve_historical_sector_mapping(
        registry, {"ABC": ("tech", "Tech")}, exchange="NSE",
        effective_at="2026-07-10", available_at=T2,
        run_id="backfill-replay", stage_attempt=1,
    )
    assert first["ABC"][2] == MembershipTrust.LATEST_ONLY_BACKFILL.value
    assert second == first
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute("SELECT COUNT(*) FROM sector_membership_history").fetchone()[0] == 1


def test_locked_correction_chain_and_late_availability_are_canonical(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    first = pd.DataFrame([_stock_row()])
    second = pd.DataFrame([_stock_row(stage=WeinsteinStage.STAGE_2.value, source_hash="stock-v2")])
    third = pd.DataFrame([_stock_row(stage=WeinsteinStage.STAGE_3.value, source_hash="stock-v3")])
    persist_stage_history(registry, first, pd.DataFrame(), run_id="run-1", attempt=1, recorded_at=T1)
    persist_stage_history(registry, second, pd.DataFrame(), run_id="run-2", attempt=1, recorded_at=T2)
    persist_stage_history(registry, third, pd.DataFrame(), run_id="run-3", attempt=1, recorded_at=T3)

    at_t1 = read_stock_stage_as_of(
        registry, as_of="2026-07-17", available_at=T1 + timedelta(seconds=1)
    )
    at_t2 = read_stock_stage_as_of(
        registry, as_of="2026-07-17", available_at=T2 + timedelta(seconds=1)
    )
    at_t3 = read_stock_stage_as_of(
        registry, as_of="2026-07-17", available_at=T3 + timedelta(seconds=1)
    )
    assert at_t1.iloc[0]["effective_stage"] == WeinsteinStage.STAGE_1.value
    assert at_t2.iloc[0]["effective_stage"] == WeinsteinStage.STAGE_2.value
    assert at_t3.iloc[0]["effective_stage"] == WeinsteinStage.STAGE_3.value
    with registry._reader() as conn:  # noqa: SLF001
        chain = conn.execute(
            """SELECT governance_action, COUNT(*) FROM stage_observation_governance
               WHERE observation_scope = 'STOCK' GROUP BY governance_action"""
        ).fetchall()
    assert dict(chain) == {"ORIGINAL": 1, "CORRECTION": 2}


def test_competing_terminal_corrections_use_authority_not_insertion_order(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    original = _stock_row(stage=WeinsteinStage.STAGE_1.value)
    data_repair = _stock_row(stage=WeinsteinStage.STAGE_2.value, source_hash="stock-repair")
    reviewed = _stock_row(stage=WeinsteinStage.STAGE_3.value, source_hash="stock-reviewed")
    persist_stage_history(registry, pd.DataFrame([original]), pd.DataFrame(), run_id="run-original", attempt=1, recorded_at=T1)
    with registry._writer() as conn:  # noqa: SLF001
        original_id = _stock_observation_id(original)
        reviewed_id = _insert_stock_history(conn, reviewed, run_id="run-reviewed", recorded_at=T2)
        append_stage_governance(
            conn, scope="STOCK", observation_id=reviewed_id,
            action=StageGovernanceAction.CORRECTION,
            supersedes_observation_id=original_id,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED,
            recorded_at=T2, run_id="run-reviewed", stage_attempt=1,
            correction_reason="operator reviewed correction",
            correction_authority=CorrectionAuthority.REVIEWED_OPERATOR_CORRECTION,
            authority_reference="operator-ticket-1", authority_recorded_at=T2,
        )
        repair_id = _insert_stock_history(conn, data_repair, run_id="run-repair", recorded_at=T3)
        append_stage_governance(
            conn, scope="STOCK", observation_id=repair_id,
            action=StageGovernanceAction.CORRECTION,
            supersedes_observation_id=original_id,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED,
            recorded_at=T3, run_id="run-repair", stage_attempt=1,
            correction_reason="repair replay",
            correction_authority=CorrectionAuthority.DATA_REPAIR_PIPELINE,
            authority_reference="repair-run-1", authority_recorded_at=T3,
        )
    resolved = read_stock_stage_as_of(registry, as_of="2026-07-17", available_at=T3 + timedelta(seconds=1))
    assert resolved.iloc[0]["effective_stage"] == WeinsteinStage.STAGE_3.value


def test_correction_with_earlier_payload_as_of_still_supersedes_original(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    original = _stock_row(stage=WeinsteinStage.STAGE_1.value)
    corrected = {
        **_stock_row(stage=WeinsteinStage.STAGE_2.value, source_hash="stock-corrected"),
        "as_of": "2026-07-16",
    }
    with registry._writer() as conn:  # noqa: SLF001
        original_id = _insert_stock_history(conn, original, run_id="run-original", recorded_at=T1)
        corrected_id = _insert_stock_history(conn, corrected, run_id="run-corrected", recorded_at=T2)
        append_stage_governance(
            conn, scope="STOCK", observation_id=original_id,
            action=StageGovernanceAction.ORIGINAL,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED,
            recorded_at=T1, run_id="run-original", stage_attempt=1,
        )
        append_stage_governance(
            conn, scope="STOCK", observation_id=corrected_id,
            action=StageGovernanceAction.CORRECTION,
            supersedes_observation_id=original_id,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED,
            recorded_at=T2, run_id="run-corrected", stage_attempt=1,
            correction_reason="corrected effective timestamp",
            correction_authority=CorrectionAuthority.DATA_REPAIR_PIPELINE,
        )

    resolved = read_stock_stage_as_of(
        registry, as_of="2026-07-17", available_at=T2 + timedelta(seconds=1)
    )
    assert resolved.iloc[0]["effective_stage"] == WeinsteinStage.STAGE_2.value


def test_multiple_governance_events_use_highest_authority_for_observation(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    original = _stock_row(stage=WeinsteinStage.STAGE_1.value)
    reviewed = _stock_row(stage=WeinsteinStage.STAGE_2.value, source_hash="stock-reviewed")
    repair = _stock_row(stage=WeinsteinStage.STAGE_3.value, source_hash="stock-repair")
    with registry._writer() as conn:  # noqa: SLF001
        original_id = _insert_stock_history(conn, original, run_id="run-original", recorded_at=T1)
        reviewed_id = _insert_stock_history(conn, reviewed, run_id="run-reviewed", recorded_at=T2)
        repair_id = _insert_stock_history(conn, repair, run_id="run-repair", recorded_at=T3)
        append_stage_governance(
            conn, scope="STOCK", observation_id=original_id,
            action=StageGovernanceAction.ORIGINAL,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED,
            recorded_at=T1, run_id="run-original", stage_attempt=1,
        )
        append_stage_governance(
            conn, scope="STOCK", observation_id=reviewed_id,
            action=StageGovernanceAction.CORRECTION,
            supersedes_observation_id=original_id,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED,
            recorded_at=T2, run_id="run-reviewed", stage_attempt=1,
            correction_authority=CorrectionAuthority.REVIEWED_OPERATOR_CORRECTION,
        )
        append_stage_governance(
            conn, scope="STOCK", observation_id=reviewed_id,
            action=StageGovernanceAction.CORRECTION,
            supersedes_observation_id=original_id,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED,
            recorded_at=T2 + timedelta(seconds=1), run_id="run-migration", stage_attempt=1,
            correction_authority=CorrectionAuthority.CLASSIFIER_VERSION_MIGRATION,
        )
        append_stage_governance(
            conn, scope="STOCK", observation_id=repair_id,
            action=StageGovernanceAction.CORRECTION,
            supersedes_observation_id=original_id,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED,
            recorded_at=T3, run_id="run-repair", stage_attempt=1,
            correction_authority=CorrectionAuthority.DATA_REPAIR_PIPELINE,
        )

    resolved = read_stock_stage_as_of(
        registry, as_of="2026-07-17", available_at=T3 + timedelta(seconds=1)
    )
    assert resolved.iloc[0]["effective_stage"] == WeinsteinStage.STAGE_2.value


def test_equal_authority_competing_terminals_raise_conflict(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    original = _stock_row(stage=WeinsteinStage.STAGE_1.value)
    left = _stock_row(stage=WeinsteinStage.STAGE_2.value, source_hash="stock-left")
    right = _stock_row(stage=WeinsteinStage.STAGE_3.value, source_hash="stock-right")
    persist_stage_history(registry, pd.DataFrame([original]), pd.DataFrame(), run_id="run-original", attempt=1, recorded_at=T1)
    with registry._writer() as conn:  # noqa: SLF001
        original_id = _stock_observation_id(original)
        for row, run_id, when in ((left, "run-left", T2), (right, "run-right", T3)):
            observation_id = _insert_stock_history(conn, row, run_id=run_id, recorded_at=when)
            append_stage_governance(
                conn, scope="STOCK", observation_id=observation_id,
                action=StageGovernanceAction.CORRECTION,
                supersedes_observation_id=original_id,
                membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED,
                recorded_at=when, run_id=run_id, stage_attempt=1,
                correction_reason="competing repair",
                correction_authority=CorrectionAuthority.DATA_REPAIR_PIPELINE,
                authority_reference=run_id, authority_recorded_at=when,
            )
    with pytest.raises(StageGovernanceConflictError, match="no unique authority winner") as exc:
        read_stock_stage_as_of(registry, as_of="2026-07-17", available_at=T3 + timedelta(seconds=1))
    assert set(exc.value.conflict.terminal_observation_ids) == {
        _stock_observation_id(left), _stock_observation_id(right)
    }


def test_supersession_cycles_are_rejected_and_malformed_cycles_conflict(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    a = _stock_row(stage=WeinsteinStage.STAGE_1.value, source_hash="stock-a")
    b = _stock_row(stage=WeinsteinStage.STAGE_2.value, source_hash="stock-b")
    c = _stock_row(stage=WeinsteinStage.STAGE_3.value, source_hash="stock-c")
    with registry._writer() as conn:  # noqa: SLF001
        a_id = _insert_stock_history(conn, a, run_id="run-a", recorded_at=T1)
        b_id = _insert_stock_history(conn, b, run_id="run-b", recorded_at=T2)
        c_id = _insert_stock_history(conn, c, run_id="run-c", recorded_at=T3)
        append_stage_governance(
            conn, scope="STOCK", observation_id=a_id, action=StageGovernanceAction.ORIGINAL,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED, recorded_at=T1,
            run_id="run-a", stage_attempt=1,
            correction_authority=CorrectionAuthority.ORIGINAL_OBSERVATION,
        )
        with pytest.raises(StageGovernanceCycleError, match="cannot supersede itself"):
            append_stage_governance(
                conn, scope="STOCK", observation_id=a_id, action=StageGovernanceAction.CORRECTION,
                supersedes_observation_id=a_id,
                membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED, recorded_at=T2,
                run_id="run-self", stage_attempt=1,
                correction_authority=CorrectionAuthority.DATA_REPAIR_PIPELINE,
            )
        before = conn.execute("SELECT COUNT(*) FROM stage_observation_governance").fetchone()[0]
        append_stage_governance(
            conn, scope="STOCK", observation_id=b_id, action=StageGovernanceAction.CORRECTION,
            supersedes_observation_id=a_id,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED, recorded_at=T2,
            run_id="run-b", stage_attempt=1,
            correction_authority=CorrectionAuthority.DATA_REPAIR_PIPELINE,
        )
        append_stage_governance(
            conn, scope="STOCK", observation_id=c_id, action=StageGovernanceAction.CORRECTION,
            supersedes_observation_id=b_id,
            membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED, recorded_at=T3,
            run_id="run-c", stage_attempt=1,
            correction_authority=CorrectionAuthority.DATA_REPAIR_PIPELINE,
        )
        with pytest.raises(StageGovernanceCycleError, match="create a cycle"):
            append_stage_governance(
                conn, scope="STOCK", observation_id=a_id, action=StageGovernanceAction.CORRECTION,
                supersedes_observation_id=c_id,
                membership_trust=MembershipTrust.POINT_IN_TIME_VERIFIED, recorded_at=T3,
                run_id="run-cycle", stage_attempt=1,
                correction_authority=CorrectionAuthority.DATA_REPAIR_PIPELINE,
            )
        after = conn.execute("SELECT COUNT(*) FROM stage_observation_governance").fetchone()[0]
        assert after == before + 2
        conn.execute(
            """INSERT INTO stage_observation_governance (
                   governance_event_id, observation_scope, observation_id, governance_action,
                   supersedes_observation_id, membership_trust, authoritative, correction_reason,
                   correction_authority, policy_version, recorded_at, run_id, stage_attempt,
                   event_hash, authority_reference, authority_recorded_at, governance_policy_version
               ) VALUES (?, 'STOCK', ?, 'CORRECTION', ?, 'POINT_IN_TIME_VERIFIED', TRUE,
                         'malformed import', 'data_repair_pipeline', 'stage-governance-v1',
                         ?, 'bad-import', 1, ?, 'bad-import', ?, 'stage-governance-authority-v1')""",
            ["bad-cycle", a_id, c_id, T3.replace(tzinfo=None), "bad-cycle", T3.replace(tzinfo=None)],
        )
    with pytest.raises(StageGovernanceConflictError, match="cycle detected"):
        read_stock_stage_as_of(registry, as_of="2026-07-17", available_at=T3 + timedelta(seconds=1))


def test_membership_change_recalculates_sector_and_records_dependencies(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    stock_v1 = pd.DataFrame([_stock_row(membership_id="membership-v1")])
    stock_v2 = pd.DataFrame([_stock_row(source_hash="stock-v2", membership_id="membership-v2")])
    sector_v1 = build_sector_coverage(stock_v1, config=StageCoverageConfig(minimum_sector_constituents=1))
    sector_v2 = build_sector_coverage(stock_v2, config=StageCoverageConfig(minimum_sector_constituents=1))
    assert sector_v1.iloc[0]["source_artifact_hash"] != sector_v2.iloc[0]["source_artifact_hash"]
    persist_stage_history(registry, stock_v1, sector_v1, run_id="run-1", attempt=1, recorded_at=T1)
    persist_stage_history(registry, stock_v2, sector_v2, run_id="run-2", attempt=1, recorded_at=T2)
    resolved = read_sector_stage_as_of(
        registry, as_of="2026-07-17", available_at=T3
    )
    assert resolved.iloc[0]["constituent_membership_observation_ids"] == "membership-v2"
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute(
            "SELECT COUNT(*) FROM stage_observation_dependency WHERE dependency_type = 'SECTOR_MEMBERSHIP'"
        ).fetchone()[0] == 2


def test_correction_flags_candidate_snapshot_decision_and_attribution(
    tmp_path: Path,
    stage_factory,
    sector_factory,
) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    opportunity_store = DuckDBOpportunityRegistryStore(registry)
    lineage = SourceLineage("candidate-run", "opportunities", 1, "shadow", "/tmp/shadow.csv", "candidate-hash")
    episode = opportunity_store.open_episode(OpenEpisodeRequest(
        symbol_id="ABC", exchange="NSE", setup_family="base_building",
        admission_identity="candidate-run:ABC", episode_started_at=T1,
        episode_type="analytical_shadow", opening_reason="test", lineage=lineage,
        contract_version="opportunity-contract-v1",
    ))
    stock_stage = replace(
        stage_factory(), source_week_start=date(2026, 7, 13),
        source_week_end=date(2026, 7, 17), stage_as_of=T1, stage_locked_at=T1,
    )
    stock_result = opportunity_store.append_stage_observation(StageObservation(
        episode.candidate_id, episode.setup_id, StageScope.STOCK, episode.symbol_id,
        episode.symbol_id, stock_stage, T1, lineage,
    ))
    opportunity = OpportunitySnapshot(
        85, 2, 99, -3, ProgressStatus.IMPROVING,
        {"relative_strength": 90}, "rank-v1", T1,
    )
    evidence = EvidenceSnapshot(
        88, EvidenceVerdict.HIGH_CONVICTION, 80, 85, 90, 82, 76, 85, 80,
        RiskLevel.LOW, RiskLevel.LOW, ("volume expansion",), (), (),
        "investigator-v1", T1,
    )
    candidate_snapshot = CandidateSnapshot(
        candidate_id=episode.candidate_id, setup_id=episode.setup_id,
        symbol_id=episode.symbol_id, exchange=episode.exchange, as_of=T1,
        opportunity=opportunity, evidence=evidence, lifecycle_state=CandidateState.DISCOVERED,
        followthrough_status=FollowthroughStatus.NOT_APPLICABLE, stock_stage=stock_stage,
        sector_stage=sector_factory(stage=stock_stage), market_regime="bull", sector_regime="leading",
        days_in_state=1, days_without_progress=0, active_position=False,
        latest_action=CandidateAction.WATCH, eligibility=ActionEligibility.NOT_APPLICABLE,
    )
    opportunity_store.append_snapshot(SnapshotObservation(
        candidate_snapshot, T1, lineage,
        stock_stage_observation_id=stock_result.record_id,
    ))
    decision = CandidateDecision(
        episode.candidate_id, episode.setup_id, CandidateAction.WATCH,
        ActionEligibility.NOT_APPLICABLE, 80, 0, ("monitor",), (), (),
        "wait", "action-v1", T1,
    )
    context = DecisionContextSnapshot(
        decision_stage=WeinsteinStage.STAGE_2, decision_stage_status=StageStatus.LOCKED,
        decision_stage_as_of=T1, decision_locked_stage=WeinsteinStage.STAGE_2,
        decision_provisional_stage=WeinsteinStage.UNKNOWN, decision_stage_confidence=80,
        decision_sector_stage=WeinsteinStage.STAGE_2,
        decision_sector_stage_status=StageStatus.LOCKED, decision_sector_stage_confidence=80,
        opportunity_score=85, evidence_score=88, lifecycle_state=CandidateState.DISCOVERED,
        followthrough_status=FollowthroughStatus.NOT_APPLICABLE, market_regime="bull",
        sector_regime="leading", rank_model_version="rank-v1",
        evidence_model_version="investigator-v1", stage_classifier_version="weekly-stage-v1",
        action_policy_version="action-v1", execution_policy_version="execution-v1",
        portfolio_context_summary={"blocked": False},
    )
    opportunity_store.append_decision_context(DecisionContextObservation(decision, context, lineage))
    attribution = OutcomeAttributionRecord(
        episode.candidate_id, episode.setup_id,
        OutcomeAttribution.VALID_SIGNAL_NORMAL_FAILURE, None, 70,
        "attribution-v1", ("structure observed",), None, T2,
    )
    opportunity_store.append_attribution(AttributionObservation(attribution, lineage))

    first = pd.DataFrame([_stock_row(stage=WeinsteinStage.STAGE_2.value)])
    corrected = pd.DataFrame([_stock_row(stage=WeinsteinStage.STAGE_3.value, source_hash="stock-v2")])
    persist_stage_history(registry, first, pd.DataFrame(), run_id="run-1", attempt=1, recorded_at=T1)
    persist_stage_history(registry, corrected, pd.DataFrame(), run_id="run-2", attempt=1, recorded_at=T3)
    with registry._reader() as conn:  # noqa: SLF001
        types = {row[0] for row in conn.execute(
            "SELECT affected_record_type FROM stage_correction_impact"
        ).fetchall()}
        statuses = conn.execute(
            """SELECT impact_status, authoritative_calibration_eligible, review_required, COUNT(*)
               FROM stage_correction_impact
               GROUP BY 1, 2, 3"""
        ).fetchall()
    assert types == {
        "candidate_episode", "candidate_snapshot", "candidate_decision_context",
        "candidate_outcome_attribution",
    }
    assert statuses == [(CorrectionImpactLinkStatus.LINKED.value, True, False, 4)]


def test_legacy_correction_impact_no_match_is_quarantined_and_exact_matches_are_all_linked(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    opportunity_store = DuckDBOpportunityRegistryStore(registry)
    lineage = SourceLineage("candidate-run", "opportunities", 1, "shadow", "/tmp/shadow.csv", "candidate-hash")
    opportunity_store.open_episode(OpenEpisodeRequest(
        symbol_id="ABC", exchange="NSE", setup_family="base_building",
        admission_identity="candidate-run:ABC", episode_started_at=T1,
        episode_type="analytical_shadow", opening_reason="test", lineage=lineage,
        contract_version="opportunity-contract-v1",
    ))
    first = pd.DataFrame([_stock_row(stage=WeinsteinStage.STAGE_1.value)])
    corrected = pd.DataFrame([_stock_row(stage=WeinsteinStage.STAGE_2.value, source_hash="stock-v2")])
    persist_stage_history(registry, first, pd.DataFrame(), run_id="run-1", attempt=1, recorded_at=T1)
    persist_stage_history(registry, corrected, pd.DataFrame(), run_id="run-2", attempt=1, recorded_at=T2)
    with registry._reader() as conn:  # noqa: SLF001
        no_match = conn.execute(
            """SELECT affected_record_type, impact_status, match_count,
                      authoritative_calibration_eligible, review_required
               FROM stage_correction_impact
               WHERE impact_status = ?
               ORDER BY affected_record_type""",
            [CorrectionImpactLinkStatus.UNRESOLVED_LEGACY_NO_MATCH.value],
        ).fetchall()
    assert no_match == [
        ("candidate_decision_context", CorrectionImpactLinkStatus.UNRESOLVED_LEGACY_NO_MATCH.value, 0, False, True),
        ("candidate_outcome_attribution", CorrectionImpactLinkStatus.UNRESOLVED_LEGACY_NO_MATCH.value, 0, False, True),
        ("candidate_snapshot", CorrectionImpactLinkStatus.UNRESOLVED_LEGACY_NO_MATCH.value, 0, False, True),
    ]

    registry2 = RegistryStore(tmp_path, db_path=tmp_path / "ambiguous.duckdb")
    opportunity_store2 = DuckDBOpportunityRegistryStore(registry2)
    episode2 = opportunity_store2.open_episode(OpenEpisodeRequest(
        symbol_id="ABC", exchange="NSE", setup_family="base_building",
        admission_identity="candidate-run:ABC:ambiguous", episode_started_at=T1,
        episode_type="analytical_shadow", opening_reason="test", lineage=lineage,
        contract_version="opportunity-contract-v1",
    ))
    with registry2._writer() as conn:  # noqa: SLF001
        for index in range(2):
            conn.execute(
                """INSERT INTO candidate_snapshot (
                       snapshot_id, candidate_id, setup_id, as_of, observed_at, run_id,
                       stage_name, stage_attempt, source_artifact_type, source_artifact_path,
                       source_artifact_hash, lifecycle_state, followthrough_status,
                       opportunity_score, rank_position, rank_percentile, rank_velocity,
                       evidence_score, evidence_verdict, days_in_state, days_without_progress,
                       progress_status, active_position, latest_action, eligibility,
                       stock_stage_observation_id, sector_stage_observation_id,
                       contract_version, serialization_version, snapshot_json,
                       semantic_payload_hash, idempotency_key
                   ) VALUES (?, ?, ?, ?, ?, 'candidate-run', 'opportunities', 1,
                             'shadow', '/tmp/shadow.csv', ?, 'discovered',
                             'not_applicable', 80, 1, 99, 0, 80, 'high_conviction',
                             1, 0, 'stable', FALSE, 'watch', 'not_applicable',
                             NULL, NULL, 'opportunity-contract-v1', 'snapshot-v1',
                             '{}', ?, ?)""",
                [
                    f"snapshot-{index}", episode2.candidate_id, episode2.setup_id,
                    T1.replace(tzinfo=None), T1.replace(tzinfo=None),
                    f"hash-{index}", f"semantic-{index}", f"idempotency-{index}",
                ],
            )
    persist_stage_history(registry2, first, pd.DataFrame(), run_id="run-1", attempt=1, recorded_at=T1)
    persist_stage_history(registry2, corrected, pd.DataFrame(), run_id="run-2", attempt=1, recorded_at=T2)
    with registry2._reader() as conn:  # noqa: SLF001
        linked = conn.execute(
            """SELECT impact_status, match_count, authoritative_calibration_eligible,
                      review_required, affected_record_id, match_evidence
               FROM stage_correction_impact
               WHERE affected_record_type = 'candidate_snapshot'
               ORDER BY affected_record_id"""
        ).fetchall()
    assert [row[:5] for row in linked] == [
        (CorrectionImpactLinkStatus.LINKED.value, 2, True, False, "snapshot-0"),
        (CorrectionImpactLinkStatus.LINKED.value, 2, True, False, "snapshot-1"),
    ]
    assert all("snapshot-0" in row[5] and "snapshot-1" in row[5] for row in linked)


def test_copied_store_legacy_annotation_is_idempotent_and_payload_immutable(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    registry = RegistryStore(tmp_path, db_path=source)
    stock = pd.DataFrame([_stock_row()])
    persist_stage_history(registry, stock, pd.DataFrame(), run_id="legacy", attempt=1, recorded_at=T1)
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute("DELETE FROM stage_observation_governance")
        before = conn.execute(
            "SELECT observation_id, observation_json, source_artifact_hash FROM weekly_stock_stage_history"
        ).fetchone()
    copied = tmp_path / "copied-control-plane.duckdb"
    shutil.copy2(source, copied)

    preview = annotate_copied_store(
        copied, apply=False, confirmed_copied_store=False, run_id="phase3c1-test"
    )
    assert preview["annotations"]["total"] == 1
    applied = annotate_copied_store(
        copied, apply=True, confirmed_copied_store=True, run_id="phase3c1-test"
    )
    assert applied["applied"]["total"] == 1
    copied_registry = RegistryStore(tmp_path, db_path=copied)
    assert annotate_legacy_stage_history(
        copied_registry, run_id="phase3c1-test", recorded_at=T3, apply=True
    )["total"] == 0
    with copied_registry._reader() as conn:  # noqa: SLF001
        after = conn.execute(
            "SELECT observation_id, observation_json, source_artifact_hash FROM weekly_stock_stage_history"
        ).fetchone()
    assert after == before
