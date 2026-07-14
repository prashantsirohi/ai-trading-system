from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from ai_trading_system.domains.opportunities.contracts import (
    ActionEligibility,
    CandidateAction,
    CandidateSnapshot,
    CandidateState,
    EvidenceSnapshot,
    EvidenceVerdict,
    FollowthroughStatus,
    OpportunitySnapshot,
    ProgressStatus,
    RiskLevel,
)
from ai_trading_system.domains.opportunities.registry.models import (
    OpenEpisodeRequest,
    SnapshotObservation,
    SourceLineage,
)
from ai_trading_system.domains.opportunities.registry.store import DuckDBOpportunityRegistryStore
from ai_trading_system.pipeline.registry import RegistryStore


NOW = datetime(2026, 7, 14, 10, tzinfo=timezone.utc)


@pytest.fixture
def lineage() -> SourceLineage:
    return SourceLineage("run-1", "rank", 1, "ranked_signals", "/archive/rank.csv", "hash-1")


@pytest.fixture
def opportunity_store(tmp_path) -> DuckDBOpportunityRegistryStore:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    return DuckDBOpportunityRegistryStore(registry)


@pytest.fixture
def episode_request(lineage) -> OpenEpisodeRequest:
    return OpenEpisodeRequest(
        symbol_id="abc", exchange="nse", setup_family="early accumulation",
        admission_identity="run-1:ABC", episode_started_at=NOW, episode_type="SETUP",
        opening_reason="rank admission", lineage=lineage, contract_version="opportunity-contract-v1",
    )


def opportunity(at: datetime = NOW, score: float = 85) -> OpportunitySnapshot:
    return OpportunitySnapshot(score, 2, 99, -3, ProgressStatus.IMPROVING,
                               {"relative_strength": 90}, "rank-v1", at)


def evidence(at: datetime = NOW, score: float = 88) -> EvidenceSnapshot:
    return EvidenceSnapshot(score, EvidenceVerdict.HIGH_CONVICTION, 80, 85, 90, 82, 76, 85, 80,
                            RiskLevel.LOW, RiskLevel.LOW, ("volume expansion",), (), (), "investigator-v1", at)


@pytest.fixture
def snapshot_builder(stage_factory, sector_factory, lineage):
    def build(episode, *, at: datetime = NOW, lifecycle=CandidateState.DISCOVERED,
              opportunity_score: float = 85, evidence_score: float = 88):
        stock = replace(stage_factory(), stage_as_of=at, stage_locked_at=at)
        sector = replace(sector_factory(), stage_snapshot=stock)
        snapshot = CandidateSnapshot(
            candidate_id=episode.candidate_id, setup_id=episode.setup_id, symbol_id=episode.symbol_id,
            exchange=episode.exchange, as_of=at, opportunity=opportunity(at, opportunity_score),
            evidence=evidence(at, evidence_score), lifecycle_state=lifecycle,
            followthrough_status=FollowthroughStatus.NOT_APPLICABLE, stock_stage=stock,
            sector_stage=sector, market_regime="bull", sector_regime="leading", days_in_state=1,
            days_without_progress=0, active_position=False, latest_action=CandidateAction.WATCH,
            eligibility=ActionEligibility.NOT_APPLICABLE,
        )
        return SnapshotObservation(snapshot, at, lineage)
    return build
