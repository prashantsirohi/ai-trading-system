"""Shadow-mode coordinator from registered artifacts to the opportunity registry."""

from __future__ import annotations

import csv
import hashlib
import json
import time
from dataclasses import replace
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from ai_trading_system.domains.opportunities.adapters import (
    adapt_breakout_rows,
    adapt_investigator_rows,
    adapt_lifecycle_rows,
    adapt_pattern_rows,
    adapt_ranking_rows,
    adapt_sector_stage_rows,
    adapt_stock_stage_rows,
)
from ai_trading_system.domains.opportunities.contracts import (
    CandidateState,
    FollowthroughStatus,
    OPPORTUNITY_CONTRACT_VERSION,
    ProgressSnapshot,
    ProgressStatus,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.coverage import (
    read_locked_sector_stage_prior_completed_week,
)
from ai_trading_system.domains.opportunities.routing import (
    parse_scan_reasons,
    validate_scan_routing_row,
)
from ai_trading_system.domains.opportunities.position_monitoring import (
    PositionEpisodeCompatibility,
    PositionRecoveryMode,
    evaluate_position_episode_compatibility,
    make_position_cycle_id,
    make_recovery_proposal_id,
    recovery_payload_hash,
)
from ai_trading_system.domains.opportunities.registry import (
    DuckDBOpportunityRegistryStore,
    EpisodeClosure,
    EpisodeStatus,
    EvidenceObservation,
    OpenEpisodeRequest,
    OpportunityObservation,
    OrchestrationBundle,
    OpportunityRegistryConflictError,
    OpportunityRegistryService,
    ProgressObservation,
    SnapshotObservation,
    SourceLineage,
    StageObservation,
    StageScope,
    TransitionObservation,
    make_candidate_id,
    make_setup_id,
)
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.pipeline.registry import RegistryStore

from .admission import evaluate_admission
from .assembler import assemble_candidate_snapshot
from .contracts import (
    AdapterWarning,
    OpportunityRegistryMode,
    OpportunityShadowConfig,
    OpportunityShadowRunResult,
    OpportunitySourceBundle,
    RejectedSourceRow,
    SECTOR_GATE_RULES,
    SectorGateEvidence,
    SetupMatchOutcome,
    SourceDescriptor,
)
from .matching import match_open_episode
from .retention import evaluate_retention
from .transitions import evaluate_transition


class OpportunityShadowSourceError(RuntimeError):
    """Required source artifacts are unavailable for an enabled shadow run."""


@dataclass(frozen=True, slots=True)
class OpportunityArtifactSet:
    ranked_signals: StageArtifact
    investigator_scores: StageArtifact | None = None
    breakout_scan: StageArtifact | None = None
    pattern_scan: StageArtifact | None = None
    stock_scan: StageArtifact | None = None
    sector_dashboard: StageArtifact | None = None
    lifecycle_state: StageArtifact | None = None
    scan_routing: StageArtifact | None = None


class OpportunityShadowOrchestrator:
    """Coordinates Phase 3 without changing any upstream or execution artifact."""

    def __init__(self, registry: RegistryStore):
        self.registry_store = DuckDBOpportunityRegistryStore(registry)
        self.registry = OpportunityRegistryService(self.registry_store)

    def run(
        self,
        *,
        run_id: str,
        stage_attempt: int,
        artifact_set: OpportunityArtifactSet,
        as_of: datetime,
        mode: OpportunityRegistryMode,
        config: OpportunityShadowConfig,
        ohlcv_db_path: Path | None = None,
        policy_snapshot_id: str | None = None,
    ) -> OpportunityShadowRunResult:
        if mode is OpportunityRegistryMode.OFF:
            return OpportunityShadowRunResult(
                "skipped", config.dry_run, {"mode": "off"}, {}
            )
        started = time.perf_counter()
        open_states = self.registry.list_open_candidates()
        open_episodes = self.registry.list_open_episodes()
        run_observation_hashes = self.registry.observation_hashes_for_run(run_id)
        prior_positions = {
            (state.exchange, state.symbol_id): state.latest_rank_position
            for state in open_states
            if state.latest_rank_position is not None
        }
        artifacts = artifact_set
        raw_rank = _read_csv(artifacts.ranked_signals)
        if not raw_rank:
            raise OpportunityShadowSourceError(
                "ranked_signals is required and contains no usable rows"
            )
        raw_investigator = _read_csv(artifacts.investigator_scores)
        raw_breakout = _read_csv(artifacts.breakout_scan)
        raw_pattern = _read_csv(artifacts.pattern_scan)
        raw_stock = _read_csv(artifacts.stock_scan)
        raw_sector = _read_csv(artifacts.sector_dashboard)
        raw_lifecycle = _read_csv(artifacts.lifecycle_state)
        raw_routing = _read_csv(artifacts.scan_routing)
        if raw_stock and ohlcv_db_path is not None:
            raw_stock = _enrich_stock_stage(raw_stock, ohlcv_db_path, as_of)

        descriptors = {
            "rank": _descriptor(
                artifacts.ranked_signals,
                "rank",
                "ranked_signals",
                run_id,
                stage_attempt,
            ),
            "investigator": _descriptor_optional(
                artifacts.investigator_scores,
                "investigator",
                "investigator_scores",
                run_id,
                stage_attempt,
            ),
            "breakout": _descriptor_optional(
                artifacts.breakout_scan, "rank", "breakout_scan", run_id, stage_attempt
            ),
            "pattern": _descriptor_optional(
                artifacts.pattern_scan, "rank", "pattern_scan", run_id, stage_attempt
            ),
            "stock": _descriptor_optional(
                artifacts.stock_scan, "rank", "stock_scan", run_id, stage_attempt
            ),
            "sector": _descriptor_optional(
                artifacts.sector_dashboard,
                "rank",
                "sector_dashboard",
                run_id,
                stage_attempt,
            ),
            "lifecycle": _descriptor_optional(
                artifacts.lifecycle_state,
                "investigator",
                "stage1_current_state",
                run_id,
                stage_attempt,
            ),
        }
        adapter_started = time.perf_counter()
        rank_result = adapt_ranking_rows(
            raw_rank,
            source=descriptors["rank"],
            as_of=as_of,
            prior_rank_positions=prior_positions,
        )
        evidence_result = (
            adapt_investigator_rows(
                raw_investigator, source=descriptors["investigator"], as_of=as_of
            )
            if descriptors["investigator"]
            else None
        )
        breakout_result = (
            adapt_breakout_rows(
                raw_breakout, source=descriptors["breakout"], as_of=as_of
            )
            if descriptors["breakout"]
            else None
        )
        pattern_result = (
            adapt_pattern_rows(raw_pattern, source=descriptors["pattern"], as_of=as_of)
            if descriptors["pattern"]
            else None
        )
        stock_result = (
            adapt_stock_stage_rows(raw_stock, source=descriptors["stock"], as_of=as_of)
            if descriptors["stock"]
            else None
        )
        sector_result = (
            adapt_sector_stage_rows(
                raw_sector, source=descriptors["sector"], as_of=as_of
            )
            if descriptors["sector"]
            else None
        )
        lifecycle_result = (
            adapt_lifecycle_rows(
                raw_lifecycle, source=descriptors["lifecycle"], as_of=as_of
            )
            if descriptors["lifecycle"]
            else None
        )
        results = tuple(
            item
            for item in (
                rank_result,
                evidence_result,
                breakout_result,
                pattern_result,
                stock_result,
                sector_result,
                lifecycle_result,
            )
            if item is not None
        )
        bundles, routing_rejections = _attach_routing(
            _reconcile(results, raw_rank, raw_stock, as_of), raw_routing, as_of
        )
        bundles = _attach_sector_gate_evidence(
            self.registry_store.registry,
            bundles,
            raw_stock=raw_stock,
            raw_sector=raw_sector,
            as_of=as_of,
        )
        adapter_seconds = time.perf_counter() - adapter_started

        rows: dict[str, list[dict[str, Any]]] = {
            name: []
            for name in (
                "candidate_admissions",
                "candidate_updates",
                "candidate_transitions",
                "candidate_closures",
                "candidate_reconciliation",
                "adapter_warnings",
                "adapter_rejections",
                "registry_conflicts",
                "current_candidate_state",
                "position_episode_compatibility",
                "position_recovery_proposals",
                "position_recovery_actions",
                "position_monitor_reconciliation",
            )
        }
        for result in results:
            rows["adapter_warnings"].extend(asdict(item) for item in result.warnings)
            rows["adapter_rejections"].extend(
                asdict(item) for item in result.rejected_rows
            )
        rows["adapter_rejections"].extend(asdict(item) for item in routing_rejections)
        if not raw_investigator:
            rows["adapter_warnings"].append(
                asdict(
                    AdapterWarning(
                        "investigator_scores",
                        "*",
                        "missing_investigator",
                        "Investigator output unavailable; evidence is not synthesized",
                    )
                )
            )

        state_by_id = {state.candidate_id: state for state in open_states}
        persistence_started = time.perf_counter()
        counters = _initial_counts(
            raw_rank,
            raw_investigator,
            raw_breakout,
            raw_pattern,
            raw_stock,
            raw_sector,
            raw_lifecycle,
            bundles,
        )
        sector_gate_taxonomy_counts: dict[str, int] = {}
        for bundle in bundles:
            if bundle.active_position:
                counters["active_positions_total"] += 1
                counters["active_positions_with_position_monitor"] += int(
                    bundle.scan_tier == "position_monitor" and bool(bundle.routing_decision_id)
                )
                counters["active_positions_with_complete_market_data"] += int(
                    bundle.market_data_complete
                )
                counters["active_positions_with_complete_evidence"] += int(
                    bool(
                        bundle.market_data_complete
                        and bundle.evidence
                        and not bundle.evidence.missing_evidence
                    )
                )
            matching_for_symbol = [
                episode
                for episode in open_episodes
                if episode.exchange == bundle.exchange
                and episode.symbol_id == bundle.symbol_id
            ]
            admission = evaluate_admission(bundle, config, policy_snapshot_id)
            recovery = False
            episode = None
            compatibility = None
            if bundle.active_position:
                cycle_id = bundle.position_cycle_id or make_position_cycle_id(
                    exchange=bundle.exchange,
                    symbol_id=bundle.symbol_id,
                    position_opened_at=bundle.position_cycle_opened_at or bundle.as_of,
                )
                all_symbol_episodes = self.registry.list_candidate_episodes(
                    exchange=bundle.exchange, symbol_id=bundle.symbol_id
                )
                compatibility = evaluate_position_episode_compatibility(
                    position_cycle_id=cycle_id,
                    position_opened_at=_aware_datetime(bundle.position_cycle_opened_at, as_of),
                    episodes=all_symbol_episodes,
                    current_states=open_states,
                )
                rows["position_episode_compatibility"].append({
                    "position_cycle_id": cycle_id,
                    "exchange": bundle.exchange,
                    "symbol_id": bundle.symbol_id,
                    "compatibility_status": compatibility.status.value,
                    "candidate_id": compatibility.candidate_id,
                    "open_episode_ids": list(compatibility.open_episode_ids),
                    "compatibility_reasons": list(compatibility.reasons),
                    "policy_version": config.position_episode_compatibility_policy_version,
                })
                if compatibility.status is PositionEpisodeCompatibility.COMPATIBLE:
                    episode = next(
                        item for item in all_symbol_episodes
                        if item.candidate_id == compatibility.candidate_id
                    )
                    match_outcome = SetupMatchOutcome.EXACT
                    counters["compatible_episode_attachments"] += 1
                    if bundle.market_data_complete and bundle.routing_decision_id:
                        counters["active_positions_fully_monitored"] += 1
                else:
                    if compatibility.status is PositionEpisodeCompatibility.AMBIGUOUS_MULTIPLE_EPISODES:
                        counters["ambiguous_episode_conflicts"] += 1
                    elif compatibility.status not in {
                        PositionEpisodeCompatibility.NO_OPEN_EPISODE,
                        PositionEpisodeCompatibility.CLOSED_EPISODE,
                    }:
                        counters["incompatible_episode_conflicts"] += 1
                    proposal = _recovery_proposal(
                        bundle=bundle,
                        cycle_id=cycle_id,
                        compatibility=compatibility,
                        config=config,
                        run_id=run_id,
                    )
                    rows["position_recovery_proposals"].append(proposal)
                    rows["position_monitor_reconciliation"].append({
                        "position_cycle_id": cycle_id,
                        "symbol_id": bundle.symbol_id,
                        "exchange": bundle.exchange,
                        "outcome": "POSITION_RECOVERY_REQUIRED",
                        "compatibility_status": compatibility.status.value,
                        "recovery_proposal_id": proposal["recovery_proposal_id"],
                    })
                    counters["recovery_proposals"] += 1
                    if not config.dry_run:
                        _persist_recovery_proposal(self.registry_store.registry, proposal)
                    recovery = _recovery_allowed(config)
                    if not recovery:
                        _conflict(
                            rows,
                            bundle,
                            "position episode compatibility failed; report-only recovery proposal created",
                        )
                        counters["registry_conflicts"] += 1
                        continue
                    bundle = _recovery_bundle(bundle)
                    match_outcome = SetupMatchOutcome.NEW_EPISODE
            elif matching_for_symbol and not admission.admitted:
                counters["not_admitted"] += 1
                rows["candidate_reconciliation"].append(
                    _reconciliation_row(
                        bundle,
                        "not_admitted",
                        "same-symbol open episode was not attached without setup-family admission",
                    )
                )
                continue
            elif recovery:
                match_outcome = SetupMatchOutcome.NEW_EPISODE
            elif admission.admitted and admission.setup_family:
                match = match_open_episode(
                    exchange=bundle.exchange,
                    symbol_id=bundle.symbol_id,
                    setup_family=admission.setup_family,
                    as_of=as_of,
                    episodes=open_episodes,
                    current_states=open_states,
                    progression_max_days=config.setup_progression_max_days,
                )
                match_outcome = match.outcome
                episode = next(
                    (
                        item
                        for item in open_episodes
                        if item.candidate_id == match.candidate_id
                    ),
                    None,
                )
                if match.outcome is SetupMatchOutcome.CONFLICT:
                    _conflict(rows, bundle, "; ".join(match.warnings))
                    counters["registry_conflicts"] += 1
                    continue
            else:
                counters["not_admitted"] += 1
                rows["candidate_reconciliation"].append(
                    _reconciliation_row(
                        bundle, "not_admitted", "; ".join(admission.blockers)
                    )
                )
                continue

            lineage = _combined_lineage(bundle, run_id, stage_attempt, policy_snapshot_id)
            episode_request = None
            if episode is None:
                if recovery:
                    setup_family = "position_state_recovery"
                    opening_reason = "position_state_recovery"
                    cycle_id = bundle.position_cycle_id or make_position_cycle_id(
                        exchange=bundle.exchange,
                        symbol_id=bundle.symbol_id,
                        position_opened_at=bundle.position_cycle_opened_at or bundle.as_of,
                    )
                    admission_identity = f"{cycle_id}|{config.position_recovery_policy_version}"
                    episode_started_at = _aware_datetime(
                        bundle.position_cycle_opened_at, as_of
                    )
                else:
                    assert (
                        admission.admission_identity
                        and admission.setup_family
                        and admission.reason
                    )
                    setup_family = admission.setup_family.value
                    opening_reason = admission.reason.value
                    admission_identity = admission.admission_identity
                    episode_started_at = as_of
                request = OpenEpisodeRequest(
                    symbol_id=bundle.symbol_id,
                    exchange=bundle.exchange,
                    setup_family=setup_family,
                    admission_identity=admission_identity,
                    episode_started_at=episode_started_at,
                    episode_type=(
                        "position_state_recovery" if recovery else "analytical_shadow"
                    ),
                    opening_reason=opening_reason,
                    lineage=lineage,
                    contract_version=OPPORTUNITY_CONTRACT_VERSION,
                )
                setup_id = make_setup_id(
                    exchange=bundle.exchange,
                    symbol_id=bundle.symbol_id,
                    setup_family=setup_family,
                    admission_identity=admission_identity,
                    episode_started_at=episode_started_at,
                )
                candidate_id = make_candidate_id(setup_id)
                prior_episode = self.registry.get_candidate_episode(candidate_id)
                if (
                    prior_episode is not None
                    and prior_episode.episode_status is not EpisodeStatus.OPEN
                ):
                    counters["registry_duplicates"] += 1
                    rows["candidate_reconciliation"].append(
                        _reconciliation_row(
                            bundle, "closed_admission_replay", candidate_id
                        )
                    )
                    continue
                episode_request = request
                episode = _dry_episode(request, candidate_id, setup_id)
                counters["new_episodes_opened"] += 1
                rows["candidate_admissions"].append(
                    {
                        "candidate_id": candidate_id,
                        "setup_id": setup_id,
                        "exchange": bundle.exchange,
                        "symbol_id": bundle.symbol_id,
                        "reason": opening_reason,
                        "setup_family": setup_family,
                        "rule_version": admission.rule_version,
                    }
                )
            else:
                counters["existing_episodes_matched"] += 1

            current = state_by_id.get(episode.candidate_id)
            if (
                current is not None
                and current.last_observed_run_id == run_id
                and lineage.source_artifact_hash
                in run_observation_hashes.get(episode.candidate_id, ())
            ):
                counters["registry_duplicates"] += 1
                rows["candidate_reconciliation"].append(
                    _reconciliation_row(
                        bundle, "exact_run_replay", episode.candidate_id
                    )
                )
                continue
            previous_state = (
                CandidateState(current.current_lifecycle_state)
                if current and current.current_lifecycle_state
                else (
                    CandidateState.INVESTIGATING if recovery else CandidateState.DISCOVERED
                )
            )
            progress = _progress_from_current(bundle, current)
            days_without_progress = (
                0
                if progress.status is ProgressStatus.IMPROVING
                else int(current.days_without_progress or 0) + 1 if current else 0
            )
            days_in_state = (
                max((as_of - current.last_transition_at).days, 0)
                if current and current.last_transition_at
                else int(current.days_in_state or 0) if current else 0
            )
            active_position = bundle.active_position
            transition = evaluate_transition(
                previous_state,
                bundle,
                progress_status=progress.status,
                active_position=active_position,
                config=config,
            )
            if (
                _uses_sector_gate(bundle)
                and bundle.sector_gate
                and bundle.sector_gate.taxonomy_cause
                and bundle.sector_gate.taxonomy_cause in transition.blockers
            ):
                cause = bundle.sector_gate.taxonomy_cause
                sector_gate_taxonomy_counts[cause] = (
                    sector_gate_taxonomy_counts.get(cause, 0) + 1
                )
            lifecycle_state = (
                transition.proposed_state if transition.allowed else previous_state
            )
            snapshot = assemble_candidate_snapshot(
                candidate_id=episode.candidate_id,
                setup_id=episode.setup_id,
                bundle=bundle,
                lifecycle_state=lifecycle_state,
                days_in_state=0 if transition.allowed else days_in_state,
                days_without_progress=days_without_progress,
                active_position=active_position,
            )
            try:
                if transition.allowed:
                    rows["candidate_transitions"].append(
                        {
                            "candidate_id": episode.candidate_id,
                            "from_state": previous_state.value,
                            "to_state": lifecycle_state.value,
                            "reason": transition.transition_reason.value,
                            "rule_version": transition.rule_version,
                            **_sector_gate_artifact_fields(bundle.sector_gate),
                        }
                    )
                retention = evaluate_retention(
                    state=lifecycle_state,
                    days_in_state=0 if transition.allowed else days_in_state,
                    days_without_progress=days_without_progress,
                    progress_status=progress.status,
                    stock_stage=(
                        bundle.stock_stage.effective_stage
                        if bundle.stock_stage
                        else WeinsteinStage.UNKNOWN
                    ),
                    followthrough_status=bundle.followthrough_status,
                    active_position=active_position,
                    config=config,
                )
                closure = None
                if retention.close_episode:
                    close_status = (
                        EpisodeStatus.ARCHIVED
                        if retention.archive
                        else EpisodeStatus.CLOSED
                    )
                    closure = EpisodeClosure(
                        close_status,
                        as_of,
                        retention.reason.value if retention.reason else "policy_close",
                        lineage,
                    )
                    counters["episodes_closed"] += 1
                    counters["episodes_archived"] += int(retention.archive)
                    rows["candidate_closures"].append(
                        {
                            "candidate_id": episode.candidate_id,
                            "reason": (
                                retention.reason.value
                                if retention.reason
                                else "policy_close"
                            ),
                            "archived": retention.archive,
                        }
                    )
                else:
                    counters["episodes_retained"] += 1
                if not config.dry_run:
                    write_result = self.registry.apply_orchestration_bundle(
                        _write_bundle(
                            episode_request=episode_request,
                            candidate_id=episode.candidate_id,
                            setup_id=episode.setup_id,
                            bundle=bundle,
                            progress=progress,
                            days_without_progress=days_without_progress,
                            snapshot=snapshot,
                            transition=(transition if transition.allowed else None),
                            previous_state=previous_state,
                            lineage=lineage,
                            closure=closure,
                        )
                    )
                    _count_append_results(counters, write_result.append_results)
                rows["candidate_updates"].append(
                    {
                        "candidate_id": episode.candidate_id,
                        "symbol_id": bundle.symbol_id,
                        "lifecycle_state": lifecycle_state.value,
                        "progress_status": progress.status.value,
                        "snapshot_complete": snapshot is not None,
                        "evidence_complete": bool(
                            bundle.evidence
                            and not bundle.evidence.missing_evidence
                            and bundle.market_data_complete
                        ),
                        "positive_action_suppressed": bool(
                            bundle.active_position
                            and (
                                not bundle.market_data_complete
                                or bundle.evidence is None
                                or bool(bundle.evidence.missing_evidence)
                            )
                        ),
                        "suppression_reasons": (
                            list(bundle.missing_data_fields)
                            + (["investigator_evidence_incomplete"] if bundle.evidence is None or bundle.evidence.missing_evidence else [])
                        ),
                        "transition_blockers": list(transition.blockers),
                        **_sector_gate_artifact_fields(bundle.sector_gate),
                    }
                )
                if recovery:
                    proposal_id = make_recovery_proposal_id(
                        position_cycle_id=cycle_id,
                        symbol_id=bundle.symbol_id,
                        exchange=bundle.exchange,
                        recovery_mode=config.position_recovery_mode,
                        policy_version=config.position_recovery_policy_version,
                    )
                    action = {
                        "recovery_action_id": f"action-{proposal_id}",
                        "recovery_proposal_id": proposal_id,
                        "position_cycle_id": cycle_id,
                        "candidate_id": episode.candidate_id,
                        "recovery_mode": config.position_recovery_mode.value,
                        "reviewed_by": config.position_recovery_reviewed_by,
                        "reviewed_at": config.position_recovery_reviewed_at,
                        "review_notes": config.position_recovery_review_notes,
                        "pre_entry_history_available": False,
                        "recovered_from_position_state": True,
                        "created_run_id": run_id,
                    }
                    rows["position_recovery_actions"].append(action)
                    counters[
                        "reviewed_recoveries"
                        if config.position_recovery_mode is PositionRecoveryMode.REVIEWED
                        else "automatic_recoveries"
                    ] += 1
                    if bundle.market_data_complete and bundle.routing_decision_id:
                        counters["active_positions_fully_monitored"] += 1
                    if not config.dry_run:
                        _persist_recovery_action(self.registry_store.registry, action)
                rows["candidate_reconciliation"].append(
                    _reconciliation_row(
                        bundle, match_outcome.value, episode.candidate_id
                    )
                )
            except OpportunityRegistryConflictError as exc:
                counters["registry_conflicts"] += 1
                _conflict(rows, bundle, str(exc), exc)
            except ValueError as exc:
                counters["rejected_writes"] += 1
                _conflict(rows, bundle, f"rejected write: {exc}")

        if not config.dry_run:
            for state in self.registry.query_current_states():
                rows["current_candidate_state"].append(asdict(state))
        persistence_seconds = time.perf_counter() - persistence_started
        counters.update(
            {
                "adapter_warnings": len(rows["adapter_warnings"]),
                "rejected_rows": len(rows["adapter_rejections"]),
                "dry_run": config.dry_run,
                "no_database_writes_performed": config.dry_run,
                "adapter_seconds": round(adapter_seconds, 6),
                "persistence_seconds": round(persistence_seconds, 6),
                "total_seconds": round(time.perf_counter() - started, 6),
                "mode": mode.value,
                "status": (
                    "degraded"
                    if rows["registry_conflicts"] or rows["adapter_rejections"]
                    else "completed"
                ),
                "unmatched_sector_mappings": sum(
                    item.sector_stage is None for item in bundles
                ),
                "missing_critical_sources": 0,
                "sector_gate_taxonomy_counts": dict(
                    sorted(sector_gate_taxonomy_counts.items())
                ),
                "sector_gate_calibration_cohort_counts": _stage_distribution(
                    item.sector_gate.calibration_cohort
                    for item in bundles
                    if _uses_sector_gate(item)
                    and item.sector_gate is not None
                    and item.sector_gate.calibration_cohort is not None
                ),
                "active_positions_missing_coverage": (
                    counters["active_positions_total"]
                    - counters["active_positions_fully_monitored"]
                ),
                "state_distribution": {
                    state.value: sum(
                        row.get("lifecycle_state") == state.value
                        for row in rows["candidate_updates"]
                    )
                    for state in CandidateState
                },
                "stock_stage_distribution": _stage_distribution(
                    item.stock_stage.effective_stage.value
                    for item in bundles
                    if item.stock_stage is not None
                ),
                "sector_stage_distribution": _stage_distribution(
                    item.sector_stage.stage_snapshot.effective_stage.value
                    for item in bundles
                    if item.sector_stage is not None
                ),
                "stage_status_distribution": _stage_distribution(
                    snapshot.stage_status.value
                    for item in bundles
                    for snapshot in (
                        *((item.stock_stage,) if item.stock_stage is not None else ()),
                        *(
                            (item.sector_stage.stage_snapshot,)
                            if item.sector_stage is not None
                            else ()
                        ),
                    )
                ),
            }
        )
        return OpportunityShadowRunResult(
            counters["status"],
            config.dry_run,
            counters,
            {key: tuple(value) for key, value in rows.items()},
        )


def _write_bundle(
    *,
    episode_request: OpenEpisodeRequest | None,
    candidate_id: str,
    setup_id: str,
    bundle: OpportunitySourceBundle,
    progress: ProgressSnapshot,
    days_without_progress: int,
    snapshot: Any,
    transition: Any,
    previous_state: CandidateState,
    lineage: SourceLineage,
    closure: EpisodeClosure | None,
) -> OrchestrationBundle:
    stages: list[StageObservation] = []
    if bundle.stock_stage:
        stages.append(
            StageObservation(
                candidate_id,
                setup_id,
                StageScope.STOCK,
                bundle.symbol_id,
                bundle.symbol_id,
                bundle.stock_stage,
                bundle.as_of,
                lineage,
            )
        )
    if bundle.sector_stage:
        stages.append(
            StageObservation(
                candidate_id,
                setup_id,
                StageScope.SECTOR,
                bundle.sector_stage.sector_id,
                bundle.sector_stage.sector_name,
                bundle.sector_stage,
                bundle.as_of,
                lineage,
            )
        )
    snapshot_observation = (
        SnapshotObservation(snapshot, bundle.as_of, lineage)
        if snapshot is not None
        else None
    )
    transition_observation = None
    if transition is not None and snapshot is not None:
        transition_observation = TransitionObservation(
            candidate_id,
            setup_id,
            previous_state,
            transition.proposed_state,
            transition.transition_reason.value,
            bundle.as_of,
            "pending",
            transition.rule_version,
            transition.metadata,
            lineage,
        )
    return OrchestrationBundle(
        candidate_id=candidate_id,
        episode_request=episode_request,
        opportunity=(
            OpportunityObservation(
                candidate_id,
                setup_id,
                bundle.as_of,
                bundle.as_of,
                bundle.opportunity,
                lineage,
            )
            if bundle.opportunity
            else None
        ),
        evidence=(
            EvidenceObservation(
                candidate_id,
                setup_id,
                bundle.as_of,
                bundle.as_of,
                "investigator",
                "investigator",
                "final_score",
                bundle.evidence,
                {"followthrough_status": bundle.followthrough_status.value},
                lineage,
            )
            if bundle.evidence
            else None
        ),
        stages=tuple(stages),
        progress=ProgressObservation(
            candidate_id,
            setup_id,
            bundle.as_of,
            progress,
            days_without_progress,
            "opportunity-progress-v1",
            {},
            lineage,
        ),
        snapshot=snapshot_observation,
        transition=transition_observation,
        closure=closure,
    )


def _count_append_results(counters: dict[str, Any], results: Iterable[Any]) -> None:
    prefixes = {
        "snapshot_": "snapshots_created",
        "transition_": "transitions_created",
        "opportunity_": "opportunity_observations_created",
        "evidence_": "evidence_observations_created",
        "stage_stock_": "stock_stage_observations_created",
        "stage_sector_": "sector_stage_observations_created",
        "progress_": "progress_observations_created",
    }
    for result in results:
        if result.duplicate:
            counters["registry_duplicates"] += 1
            if result.record_id.startswith("snapshot_"):
                counters["duplicate_snapshots"] += 1
        for prefix, counter in prefixes.items():
            if result.created and result.record_id.startswith(prefix):
                counters[counter] += 1
                break


def _stage_distribution(values: Iterable[str]) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        result[value] = result.get(value, 0) + 1
    return dict(sorted(result.items()))


def _progress_from_current(
    bundle: OpportunitySourceBundle, current: Any
) -> ProgressSnapshot:
    if current is None:
        return bundle.progress_hint or ProgressSnapshot(
            ProgressStatus.UNKNOWN, bundle.as_of, notes=("no prior registry state",)
        )
    rank_signal = (
        _direction(
            bundle.opportunity.rank_position,
            current.latest_rank_position,
            lower_is_better=True,
        )
        if bundle.opportunity and current.latest_rank_position is not None
        else None
    )
    evidence_signal = (
        _direction(bundle.evidence.evidence_score, current.latest_evidence_score)
        if bundle.evidence and current.latest_evidence_score is not None
        else None
    )
    hard = bool(
        bundle.stock_stage
        and bundle.stock_stage.effective_stage
        in {
            WeinsteinStage.TRANSITION_2_TO_3,
            WeinsteinStage.STAGE_3,
            WeinsteinStage.TRANSITION_3_TO_4,
            WeinsteinStage.STAGE_4,
        }
    )
    comparable = [item for item in (rank_signal, evidence_signal) if item is not None]
    if hard or sum(item is False for item in comparable) >= 2:
        status = ProgressStatus.DETERIORATING
    elif sum(item is True for item in comparable) >= 2:
        status = ProgressStatus.IMPROVING
    elif comparable:
        status = ProgressStatus.STABLE
    else:
        status = (
            bundle.progress_hint.status
            if bundle.progress_hint
            else ProgressStatus.UNKNOWN
        )
    return ProgressSnapshot(
        status,
        bundle.as_of,
        rank_velocity_improved=rank_signal,
        evidence_score_improved=evidence_signal,
        notes=(("hard structural deterioration",) if hard else ()),
    )


def _direction(
    current: float, prior: float, *, lower_is_better: bool = False
) -> bool | None:
    if current == prior:
        return None
    return current < prior if lower_is_better else current > prior


def _normalize_sector_id(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_")


def _coerce_stage(value: Any) -> WeinsteinStage:
    try:
        return WeinsteinStage(str(value or WeinsteinStage.UNKNOWN.value).lower())
    except ValueError:
        return WeinsteinStage.UNKNOWN


def _coerce_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _attach_sector_gate_evidence(
    registry: RegistryStore,
    bundles: tuple[OpportunitySourceBundle, ...],
    *,
    raw_stock: list[dict[str, Any]],
    raw_sector: list[dict[str, Any]],
    as_of: datetime,
) -> tuple[OpportunitySourceBundle, ...]:
    """Attach governed prior-week evidence without using current-week stage to gate."""
    stock_rows = {
        (
            str(row.get("exchange") or "NSE").upper(),
            str(row.get("symbol_id") or row.get("symbol") or "").upper(),
        ): row
        for row in raw_stock
    }
    sector_rows: dict[str, dict[str, Any]] = {}
    for row in raw_sector:
        for value in (row.get("sector_id"), row.get("sector_name"), row.get("sector")):
            normalized = _normalize_sector_id(value)
            if normalized:
                sector_rows[normalized] = row
    sector_ids = sorted(
        {
            _normalize_sector_id(
                bundle.sector_stage.sector_id
                if bundle.sector_stage is not None
                else bundle.sector_name
            )
            for bundle in bundles
            if str(bundle.sector_name or "").strip().lower()
            not in {"", "unknown", "nan", "none", "<na>"}
        }
    )
    prior_records = (
        read_locked_sector_stage_prior_completed_week(
            registry,
            as_of=as_of.isoformat(),
            sector_ids=sector_ids,
            available_at=as_of,
        ).to_dict(orient="records")
        if sector_ids
        else []
    )
    prior_by_sector = {
        _normalize_sector_id(row.get("sector_id")): row for row in prior_records
    }
    attached: list[OpportunitySourceBundle] = []
    for bundle in bundles:
        mapped = str(bundle.sector_name or "").strip().lower() not in {
            "", "unknown", "nan", "none", "<na>"
        }
        sector_id = _normalize_sector_id(
            bundle.sector_stage.sector_id
            if bundle.sector_stage is not None
            else bundle.sector_name
        )
        stock_row = stock_rows.get((bundle.exchange, bundle.symbol_id), {})
        membership_trust = str(
            stock_row.get("sector_membership_trust") or "UNKNOWN"
        ).upper()
        current_row = sector_rows.get(sector_id) or sector_rows.get(
            _normalize_sector_id(bundle.sector_name)
        )
        prior = prior_by_sector.get(sector_id) or prior_by_sector.get(
            _normalize_sector_id(bundle.sector_name)
        )
        prior_stage = _coerce_stage(
            (prior or {}).get("locked_stage") or (prior or {}).get("effective_stage")
        )
        current_stage = (
            bundle.sector_stage.stage_snapshot.provisional_stage
            if bundle.sector_stage is not None
            else WeinsteinStage.UNKNOWN
        )
        velocity_value = (current_row or {}).get("stage_breadth_velocity")
        try:
            velocity = float(velocity_value) if velocity_value not in (None, "") else None
        except (TypeError, ValueError):
            velocity = None
        coverage_unknown = (
            _coerce_stage((current_row or {}).get("effective_stage"))
            is WeinsteinStage.UNKNOWN
            if current_row is not None
            else False
        ) or (
            prior is not None
            and prior_stage is WeinsteinStage.UNKNOWN
        )
        coverage_status = "insufficient" if coverage_unknown else "sufficient"
        taxonomy: str | None
        if not mapped:
            taxonomy = "missing_sector_mapping"
        elif membership_trust not in SECTOR_GATE_RULES["trusted_membership_states"]:
            taxonomy = "latest_only_untrusted_membership"
        elif coverage_status == "insufficient":
            taxonomy = "insufficient_constituent_coverage"
        elif prior is None:
            taxonomy = "sector_locked_snapshot_missing"
        elif prior_stage.value not in SECTOR_GATE_RULES["passing_prior_locked_stages"]:
            taxonomy = "sector_not_stage_2"
        else:
            taxonomy = None
        improving = (
            current_stage.value
            in SECTOR_GATE_RULES["calibration_current_provisional_stages"]
            or (
                velocity is not None
                and velocity
                > SECTOR_GATE_RULES["calibration_improving_velocity_floor_exclusive"]
            )
        )
        cohort = (
            "stage_1_improving_blocked_v1"
            if taxonomy == "sector_not_stage_2"
            and prior_stage.value == SECTOR_GATE_RULES["calibration_prior_locked_stage"]
            and improving
            else None
        )
        attached.append(
            replace(
                bundle,
                sector_gate=SectorGateEvidence(
                    prior_locked_stage=prior_stage,
                    prior_locked_week_end=_coerce_date(
                        (prior or {}).get("source_week_end")
                    ),
                    prior_locked_confidence=(
                        float((prior or {})["stage_confidence_score"])
                        if (prior or {}).get("stage_confidence_score") not in (None, "")
                        else None
                    ),
                    current_provisional_stage=current_stage,
                    current_stage_velocity=velocity,
                    membership_trust=membership_trust,
                    coverage_status=coverage_status,
                    taxonomy_cause=taxonomy,
                    calibration_cohort=cohort,
                ),
            )
        )
    return tuple(attached)


def _uses_sector_gate(bundle: OpportunitySourceBundle) -> bool:
    return bool(
        bundle.stock_stage
        and bundle.stock_stage.stage_status.value == "provisional"
        and bundle.stock_stage.provisional_stage is WeinsteinStage.TRANSITION_1_TO_2
    )


def _sector_gate_artifact_fields(
    evidence: SectorGateEvidence | None,
) -> dict[str, Any]:
    return {
        "sector_locked_stage_prior_completed_week": (
            evidence.prior_locked_stage.value if evidence else None
        ),
        "sector_locked_week_end_prior_completed_week": (
            evidence.prior_locked_week_end.isoformat()
            if evidence and evidence.prior_locked_week_end
            else None
        ),
        "sector_locked_confidence_prior_completed_week": (
            evidence.prior_locked_confidence if evidence else None
        ),
        "sector_provisional_stage_current_week": (
            evidence.current_provisional_stage.value if evidence else None
        ),
        "sector_stage_velocity_current_week": (
            evidence.current_stage_velocity if evidence else None
        ),
        "sector_membership_trust": evidence.membership_trust if evidence else None,
        "sector_coverage_status": evidence.coverage_status if evidence else None,
        "sector_gate_taxonomy": evidence.taxonomy_cause if evidence else None,
        "sector_gate_cohort": evidence.calibration_cohort if evidence else None,
    }


def _reconcile(
    results: Iterable[Any],
    raw_rank: list[dict[str, Any]],
    raw_stock: list[dict[str, Any]],
    as_of: datetime,
) -> tuple[OpportunitySourceBundle, ...]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    sector_by_key = {
        (
            str(row.get("exchange") or "NSE").upper(),
            str(row.get("symbol_id") or row.get("symbol") or "").upper(),
        ): str(row.get("sector_name") or row.get("sector") or "unknown")
        for row in raw_rank
    }
    for row in raw_stock:
        sector_name = str(row.get("sector_name") or row.get("sector") or "").strip()
        if sector_name.lower() in {"", "nan", "none", "<na>"}:
            continue
        key = (
            str(row.get("exchange") or "NSE").upper(),
            str(row.get("symbol_id") or row.get("symbol") or "").upper(),
        )
        sector_by_key[key] = sector_name
    sector_records: dict[str, Any] = {}
    for result in results:
        for record in result.records:
            value = record.value
            if hasattr(value, "sector_name") and hasattr(value, "stage_snapshot"):
                sector_records[value.sector_name.strip().lower()] = value
                continue
            key = (record.exchange, record.symbol_id)
            item = by_key.setdefault(
                key, {"sources": [], "rows": [], "breakouts": [], "patterns": []}
            )
            item["sources"].append(record.source)
            item["rows"].append(record.row_identity)
            name = value.__class__.__name__
            if name == "OpportunitySnapshot":
                item["opportunity"] = value
            elif name == "EvidenceSnapshot":
                item["evidence"] = value
            elif name == "StageSnapshot":
                item["stock_stage"] = value
            elif name == "BreakoutEvidence":
                item["breakouts"].append(value)
            elif name == "PatternEvidence":
                item["patterns"].append(value)
            elif name == "LifecycleEvidence":
                item["lifecycle_hint"] = value.lifecycle_state
                item["followthrough"] = value.followthrough_status
                item["progress_hint"] = value.progress
    bundles: list[OpportunitySourceBundle] = []
    for key in sorted(by_key):
        item = by_key[key]
        sector_name = sector_by_key.get(key, "unknown")
        sector = sector_records.get(sector_name.lower())
        sources = {source.artifact_hash: source for source in item["sources"]}
        bundles.append(
            OpportunitySourceBundle(
                symbol_id=key[1],
                exchange=key[0],
                as_of=as_of,
                opportunity=item.get("opportunity"),
                evidence=item.get("evidence"),
                stock_stage=item.get("stock_stage"),
                sector_stage=sector,
                lifecycle_hint=item.get("lifecycle_hint"),
                followthrough_status=item.get(
                    "followthrough", FollowthroughStatus.UNKNOWN
                ),
                progress_hint=item.get("progress_hint"),
                breakout_events=tuple(item["breakouts"]),
                pattern_events=tuple(item["patterns"]),
                source_lineage=tuple(sources[key] for key in sorted(sources)),
                source_row_identities=tuple(sorted(item["rows"])),
                sector_name=sector_name,
            )
        )
    return tuple(bundles)


def _attach_routing(
    bundles: tuple[OpportunitySourceBundle, ...],
    rows: list[dict[str, Any]],
    as_of: datetime,
) -> tuple[tuple[OpportunitySourceBundle, ...], tuple[RejectedSourceRow, ...]]:
    routing = {
        (
            str(row.get("exchange") or "NSE").upper(),
            str(row.get("symbol_id") or "").upper(),
        ): row
        for row in rows
        if str(row.get("symbol_id") or "").strip()
    }
    by_key = {(bundle.exchange, bundle.symbol_id): bundle for bundle in bundles}
    rejections: list[RejectedSourceRow] = []
    for key, row in routing.items():
        conflicts = validate_scan_routing_row(row)
        if conflicts:
            rejections.append(
                RejectedSourceRow(
                    "scan_routing",
                    f"{key[0]}:{key[1]}",
                    "; ".join(conflict.message for conflict in conflicts),
                    tuple(conflict.field for conflict in conflicts if conflict.field),
                )
            )
            continue
        reasons = parse_scan_reasons(
            row.get("all_selection_reasons") or row.get("scan_reasons") or ()
        )
        existing = by_key.get(
            key, OpportunitySourceBundle(symbol_id=key[1], exchange=key[0], as_of=as_of)
        )
        by_key[key] = replace(
            existing,
            scan_tier=str(
                row.get("effective_scan_tier") or row.get("scan_tier") or "stage_only"
            ),
            scan_reasons=tuple(str(item) for item in reasons),
            active_position=str(row.get("active_position") or "").lower()
            in {"true", "1"},
            recently_exited=str(row.get("recently_exited") or "").lower()
            in {"true", "1"},
            position_cycle_opened_at=str(row.get("position_cycle_opened_at") or "")
            or None,
            position_cycle_id=str(row.get("position_cycle_id") or "") or None,
            routing_decision_id=str(row.get("routing_decision_id") or "") or None,
            market_data_complete=str(row.get("market_data_complete") or "").lower()
            in {"true", "1"},
            missing_data_fields=tuple(
                str(item) for item in _list_value(row.get("missing_data_fields"))
            ),
        )
    return tuple(by_key[key] for key in sorted(by_key)), tuple(rejections)


def _recovery_bundle(bundle: OpportunitySourceBundle) -> OpportunitySourceBundle:
    return replace(
        bundle,
        lifecycle_hint=CandidateState.INVESTIGATING,
    )


def _recovery_allowed(config: OpportunityShadowConfig) -> bool:
    if config.position_recovery_mode is PositionRecoveryMode.AUTOMATIC:
        return bool(config.recover_position_only_episodes)
    if config.position_recovery_mode is PositionRecoveryMode.REVIEWED:
        return bool(
            config.position_recovery_reviewed_by
            and config.position_recovery_reviewed_at
            and config.position_recovery_review_notes
        )
    return False


def _recovery_proposal(
    *, bundle: OpportunitySourceBundle, cycle_id: str, compatibility: Any,
    config: OpportunityShadowConfig, run_id: str,
) -> dict[str, Any]:
    proposal_id = make_recovery_proposal_id(
        position_cycle_id=cycle_id,
        symbol_id=bundle.symbol_id,
        exchange=bundle.exchange,
        recovery_mode=config.position_recovery_mode,
        policy_version=config.position_recovery_policy_version,
    )
    payload = {
        "recovery_proposal_id": proposal_id,
        "position_cycle_id": cycle_id,
        "symbol_id": bundle.symbol_id,
        "exchange": bundle.exchange,
        "position_opened_at": bundle.position_cycle_opened_at,
        "compatibility_status": compatibility.status.value,
        "open_episode_ids": list(compatibility.open_episode_ids),
        "conflict_reasons": list(compatibility.reasons),
        "proposed_setup_family": "position_state_recovery",
        "proposed_initial_candidate_state": CandidateState.INVESTIGATING.value,
        "pre_entry_history_available": False,
        "missing_history_fields": [
            "discovery_timestamp", "historical_rank", "historical_opportunity_score",
            "historical_investigator_score", "trigger_transition",
            "followthrough_status", "stage_at_entry",
        ],
        "recovery_mode": config.position_recovery_mode.value,
        "proposal_status": "PROPOSED",
        "policy_version": config.position_recovery_policy_version,
        "source_lineage": [asdict(source) for source in bundle.source_lineage],
        "created_run_id": run_id,
    }
    payload["payload_hash"] = recovery_payload_hash(payload)
    return payload


def _persist_recovery_proposal(registry: RegistryStore, proposal: dict[str, Any]) -> None:
    with registry._writer() as conn:  # noqa: SLF001
        existing = conn.execute(
            "SELECT payload_hash FROM position_recovery_proposal WHERE recovery_proposal_id = ?",
            [proposal["recovery_proposal_id"]],
        ).fetchone()
        if existing and existing[0] != proposal["payload_hash"]:
            raise OpportunityRegistryConflictError(
                record_type="position_recovery_proposal",
                candidate_id=proposal["position_cycle_id"],
                idempotency_key=proposal["recovery_proposal_id"],
                existing_payload_hash=existing[0],
                incoming_payload_hash=proposal["payload_hash"],
            )
        conn.execute(
            """INSERT INTO position_recovery_proposal
               (recovery_proposal_id, position_cycle_id, symbol_id, exchange,
                recovery_mode, proposal_status, compatibility_status, payload_hash,
                payload_json, created_run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(recovery_proposal_id) DO NOTHING""",
            [
                proposal["recovery_proposal_id"], proposal["position_cycle_id"],
                proposal["symbol_id"], proposal["exchange"], proposal["recovery_mode"],
                proposal["proposal_status"], proposal["compatibility_status"],
                proposal["payload_hash"], json.dumps(proposal, sort_keys=True, default=str),
                proposal["created_run_id"],
            ],
        )


def _persist_recovery_action(registry: RegistryStore, action: dict[str, Any]) -> None:
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute(
            """INSERT INTO position_recovery_action
               (recovery_action_id, recovery_proposal_id, position_cycle_id, candidate_id,
                recovery_mode, reviewed_by, reviewed_at, review_notes, payload_json, created_run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(recovery_action_id) DO NOTHING""",
            [
                action["recovery_action_id"], action["recovery_proposal_id"],
                action["position_cycle_id"], action["candidate_id"], action["recovery_mode"],
                action["reviewed_by"], action["reviewed_at"], action["review_notes"],
                json.dumps(action, sort_keys=True, default=str), action["created_run_id"],
            ],
        )


def _aware_datetime(value: str | None, fallback: datetime) -> datetime:
    if not value:
        return fallback
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return (
            parsed
            if parsed.tzinfo is not None
            else parsed.replace(tzinfo=fallback.tzinfo)
        )
    except ValueError:
        return fallback


def _read_csv(artifact: StageArtifact | None) -> list[dict[str, Any]]:
    if artifact is None:
        return []
    path = Path(artifact.uri)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _descriptor(
    artifact: StageArtifact, stage: str, artifact_type: str, run_id: str, attempt: int
) -> SourceDescriptor:
    path = Path(artifact.uri)
    digest = artifact.content_hash or hashlib.sha256(path.read_bytes()).hexdigest()
    return SourceDescriptor(
        stage,
        artifact_type,
        str(path),
        digest,
        run_id,
        artifact.attempt_number or attempt,
        artifact.row_count or 0,
    )


def _descriptor_optional(
    artifact: StageArtifact | None,
    stage: str,
    artifact_type: str,
    run_id: str,
    attempt: int,
) -> SourceDescriptor | None:
    return (
        _descriptor(artifact, stage, artifact_type, run_id, attempt)
        if artifact
        else None
    )


def _combined_lineage(
    bundle: OpportunitySourceBundle, run_id: str, attempt: int,
    policy_snapshot_id: str | None = None,
) -> SourceLineage:
    hashes = sorted(source.artifact_hash for source in bundle.source_lineage)
    digest = hashlib.sha256("|".join(hashes).encode()).hexdigest()
    paths = sorted(source.artifact_path for source in bundle.source_lineage)
    source_attempt = max(
        (source.stage_attempt for source in bundle.source_lineage), default=attempt
    )
    return SourceLineage(
        run_id,
        "opportunities",
        source_attempt,
        "reconciled_bundle",
        "|".join(paths) or "reconciled:unknown",
        digest or hashlib.sha256(b"unknown").hexdigest(),
        policy_snapshot_id=policy_snapshot_id,
    )


def _enrich_stock_stage(
    rows: list[dict[str, Any]], db_path: Path, as_of: datetime
) -> list[dict[str, Any]]:
    try:
        from ai_trading_system.domains.ranking.stage_store import read_latest_snapshot

        symbols = [
            str(row.get("symbol_id") or row.get("symbol") or "").upper() for row in rows
        ]
        latest = read_latest_snapshot(
            db_path, symbols=symbols, asof=as_of.date().isoformat()
        )
        stored = {
            str(row["symbol"]).upper(): row.to_dict() for _, row in latest.iterrows()
        }
        return [
            {
                **row,
                **stored.get(
                    str(row.get("symbol_id") or row.get("symbol") or "").upper(), {}
                ),
            }
            for row in rows
        ]
    except Exception:
        return rows


def _initial_counts(*args: Any) -> dict[str, Any]:
    rank, investigator, breakout, pattern, stock, sector, lifecycle, bundles = args
    return {
        "rank_rows_read": len(rank),
        "investigator_rows_read": len(investigator),
        "breakout_rows_read": len(breakout),
        "pattern_rows_read": len(pattern),
        "stock_stage_rows_read": len(stock),
        "sector_stage_rows_read": len(sector),
        "lifecycle_rows_read": len(lifecycle),
        "unique_symbols_seen": len(bundles),
        "source_bundles_assembled": len(bundles),
        "new_episodes_opened": 0,
        "existing_episodes_matched": 0,
        "snapshots_created": 0,
        "duplicate_snapshots": 0,
        "transitions_created": 0,
        "opportunity_observations_created": 0,
        "evidence_observations_created": 0,
        "stock_stage_observations_created": 0,
        "sector_stage_observations_created": 0,
        "progress_observations_created": 0,
        "episodes_retained": 0,
        "episodes_closed": 0,
        "episodes_archived": 0,
        "registry_duplicates": 0,
        "registry_conflicts": 0,
        "rejected_writes": 0,
        "not_admitted": 0,
        "compatible_episode_attachments": 0,
        "incompatible_episode_conflicts": 0,
        "ambiguous_episode_conflicts": 0,
        "recovery_proposals": 0,
        "reviewed_recoveries": 0,
        "automatic_recoveries": 0,
        "recovery_conflicts": 0,
        "active_positions_total": 0,
        "active_positions_with_position_monitor": 0,
        "active_positions_with_complete_market_data": 0,
        "active_positions_with_complete_evidence": 0,
        "active_positions_fully_monitored": 0,
    }


def _list_value(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple)):
        return list(value)
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text.replace("'", '"'))
        return parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        return [item for item in text.split("|") if item]


def _reconciliation_row(
    bundle: OpportunitySourceBundle, outcome: str, detail: str
) -> dict[str, Any]:
    return {
        "exchange": bundle.exchange,
        "symbol_id": bundle.symbol_id,
        "outcome": outcome,
        "detail": detail,
        "as_of": bundle.as_of.isoformat(),
        "scan_tier": bundle.scan_tier,
        "scan_reasons": "|".join(bundle.scan_reasons),
        "position_selected": bundle.active_position,
        "recent_exit_selected": bundle.recently_exited,
        "rank_selected": "rank_selected" in bundle.scan_reasons,
        "stage_selected": any(
            reason.startswith("stage_") for reason in bundle.scan_reasons
        ),
        "followthrough_selected": any(
            reason in {"triggered_candidate", "pending_followthrough"}
            for reason in bundle.scan_reasons
        ),
    }


def _conflict(
    rows: dict[str, list[dict[str, Any]]],
    bundle: OpportunitySourceBundle,
    message: str,
    exc: OpportunityRegistryConflictError | None = None,
) -> None:
    rows["registry_conflicts"].append(
        {
            "exchange": bundle.exchange,
            "symbol_id": bundle.symbol_id,
            "message": message,
            "record_type": exc.record_type if exc else "reconciliation",
            "idempotency_key": exc.idempotency_key if exc else "",
            "existing_payload_hash": exc.existing_payload_hash if exc else "",
            "incoming_payload_hash": exc.incoming_payload_hash if exc else "",
        }
    )


def _dry_episode(request: OpenEpisodeRequest, candidate_id: str, setup_id: str):
    from ai_trading_system.domains.opportunities.registry.models import (
        CandidateEpisodeRecord,
        REGISTRY_SCHEMA_VERSION,
    )

    return CandidateEpisodeRecord(
        candidate_id,
        setup_id,
        request.symbol_id,
        request.exchange,
        0,
        request.episode_type,
        request.setup_family,
        request.admission_identity,
        request.episode_started_at,
        None,
        EpisodeStatus.OPEN,
        request.opening_reason,
        None,
        request.lineage.run_id,
        request.lineage.stage_name,
        request.lineage.source_artifact_hash,
        None,
        None,
        request.contract_version,
        REGISTRY_SCHEMA_VERSION,
        request.episode_started_at,
        request.episode_started_at,
    )
