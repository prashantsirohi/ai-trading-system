"""Use-case facade for the persistent opportunity registry."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

from .models import (
    AttributionObservation,
    CandidateTimeline,
    DecisionContextObservation,
    EpisodeStatus,
    EvidenceObservation,
    OpenEpisodeRequest,
    OpportunityObservation,
    ProgressObservation,
    OrchestrationBundle,
    SnapshotObservation,
    SourceLineage,
    StageObservation,
    TransitionObservation,
)
from .store import DuckDBOpportunityRegistryStore


class OpportunityRegistryService:
    """Business-safe registry API; it does not infer admissions or transitions."""

    def __init__(self, store: DuckDBOpportunityRegistryStore):
        self.store = store

    def open_candidate_episode(self, request: OpenEpisodeRequest):
        return self.store.open_episode(request)

    def open_candidate_episode_with_snapshot(self, request: OpenEpisodeRequest, observation: SnapshotObservation):
        return self.store.open_episode_with_initial_snapshot(request, observation)

    def find_open_candidate_episode(self, **identity):
        return self.store.find_open_episode(**identity)

    def append_candidate_snapshot(self, observation: SnapshotObservation):
        return self.store.append_snapshot(observation)

    def append_opportunity_observation(self, observation: OpportunityObservation):
        return self.store.append_opportunity_observation(observation)

    def append_evidence_observation(self, observation: EvidenceObservation):
        return self.store.append_evidence_observation(observation)

    def append_stage_observation(self, observation: StageObservation):
        return self.store.append_stage_observation(observation)

    def append_progress_observation(self, observation: ProgressObservation):
        return self.store.append_progress(observation)

    def record_transition(self, observation: TransitionObservation):
        return self.store.append_transition(observation)

    def record_decision_context(self, observation: DecisionContextObservation):
        return self.store.append_decision_context(observation)

    def record_outcome_attribution(self, observation: AttributionObservation):
        return self.store.append_attribution(observation)

    def close_candidate_episode(
        self, candidate_id: str, *, status: EpisodeStatus, closed_at: datetime,
        closing_reason: str, lineage: SourceLineage,
    ):
        return self.store.close_episode(candidate_id, status=status, closed_at=closed_at,
                                        closing_reason=closing_reason, lineage=lineage)

    def get_candidate_current_state(self, candidate_id: str):
        return self.store.current_state(candidate_id)

    def get_candidate_episode(self, candidate_id: str):
        return self.store.get_episode(candidate_id)

    def get_candidate_state_as_of(self, candidate_id: str, as_of: datetime):
        return self.store.state_as_of(candidate_id, as_of)

    def get_candidate_timeline(self, candidate_id: str) -> CandidateTimeline:
        return self.store.timeline(candidate_id)

    def list_open_candidates(self):
        return self.store.list_open_candidates()

    def list_open_episodes(self):
        return self.store.list_open_episodes()

    def observation_hashes_for_run(self, run_id: str):
        return self.store.observation_hashes_for_run(run_id)

    def query_current_states(self, **filters):
        return self.store.query_current_states(**filters)

    def append_snapshots_batch(self, observations: Iterable[SnapshotObservation]):
        return self.store.append_snapshots_batch(observations)

    def append_stage_observations_batch(self, observations: Iterable[StageObservation]):
        return self.store.append_stage_observations_batch(observations)

    def append_evidence_observations_batch(self, observations: Iterable[EvidenceObservation]):
        return self.store.append_evidence_observations_batch(observations)

    def append_opportunity_observations_batch(self, observations: Iterable[OpportunityObservation]):
        return self.store.append_opportunity_observations_batch(observations)

    def append_progress_observations_batch(self, observations: Iterable[ProgressObservation]):
        return self.store.append_progress_observations_batch(observations)

    def apply_orchestration_bundle(self, bundle: OrchestrationBundle):
        return self.store.append_orchestration_bundle(bundle)

    def append_snapshot_bundle(
        self, *, snapshot: SnapshotObservation, stock_stage: StageObservation, sector_stage: StageObservation
    ):
        return self.store.append_snapshot_bundle(
            snapshot=snapshot, stock_stage=stock_stage, sector_stage=sector_stage
        )

    def append_transition_with_snapshot(
        self, *, snapshot: SnapshotObservation, transition: TransitionObservation
    ):
        return self.store.append_transition_with_snapshot(snapshot=snapshot, transition=transition)
