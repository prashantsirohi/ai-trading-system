"""Shadow-mode coordinator from registered artifacts to the opportunity registry."""

from __future__ import annotations

import csv
import hashlib
import json
import time
from collections import defaultdict
from dataclasses import replace
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import duckdb
from ai_trading_system.domains.fundamentals.contracts import (
    FUNDAMENTAL_DISCOVERY_TAXONOMY_VERSION,
    FUNDAMENTAL_THESIS_ADMISSION_VERSION,
    FUNDAMENTAL_THESIS_RULE_VERSION,
    FundamentalThesisEvaluation,
    FundamentalThesisFamily,
    FundamentalThesisSnapshot,
)

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
from ai_trading_system.domains.opportunities.performance_evaluation import (
    mature_performance_events,
)
from ai_trading_system.domains.opportunities.registry import (
    DuckDBOpportunityRegistryStore,
    EpisodeClosure,
    EpisodeSupersession,
    EpisodeStatus,
    EvidenceObservation,
    OpenEpisodeRequest,
    OpportunityObservation,
    OrchestrationBundle,
    OpportunityRegistryConflictError,
    OpportunityRegistryService,
    PerformanceEventObservation,
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

from .admission import (
    evaluate_admission,
    rule_evaluations_json,
    satisfied_rules_json,
)
from .assembler import assemble_candidate_snapshot
from .contracts import (
    AdmissionReason,
    AdapterWarning,
    ClosureReason,
    EpisodeRelationType,
    INVESTIGATOR_ATTRIBUTION_POLICY_VERSION,
    OpportunityRegistryMode,
    OpportunityShadowConfig,
    OpportunityShadowRunResult,
    OpportunitySourceBundle,
    RejectedSourceRow,
    SECTOR_GATE_RULES,
    SETUP_FAMILY_RULE_VERSION,
    SectorGateEvidence,
    SetupMatchOutcome,
    SourceDescriptor,
)
from .matching import match_open_episode
from .retention import advance_session_counters, evaluate_retention
from .transitions import evaluate_transition


class OpportunityShadowSourceError(RuntimeError):
    """Required source artifacts are unavailable for an enabled shadow run."""


@dataclass(frozen=True, slots=True)
class OpportunityArtifactSet:
    ranked_signals: StageArtifact
    investigator_scores: StageArtifact | None = None
    routed_investigator_scores: StageArtifact | None = None
    breakout_scan: StageArtifact | None = None
    pattern_scan: StageArtifact | None = None
    supplemental_pattern_scan: StageArtifact | None = None
    stock_scan: StageArtifact | None = None
    sector_dashboard: StageArtifact | None = None
    supplemental_sector_dashboard: StageArtifact | None = None
    lifecycle_state: StageArtifact | None = None
    scan_routing: StageArtifact | None = None
    market_context: StageArtifact | None = None
    fundamental_thesis_universe: StageArtifact | None = None


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
        observed_session_date: date | None = None,
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
        raw_routed_investigator = _read_csv(artifacts.routed_investigator_scores)
        raw_breakout = _read_csv(artifacts.breakout_scan)
        raw_pattern = _read_csv(artifacts.pattern_scan)
        raw_supplemental_pattern = _read_csv(artifacts.supplemental_pattern_scan)
        raw_stock = _read_csv(artifacts.stock_scan)
        raw_sector = _read_csv(artifacts.sector_dashboard)
        raw_supplemental_sector = _read_csv(
            artifacts.supplemental_sector_dashboard
        )
        raw_lifecycle = _read_csv(artifacts.lifecycle_state)
        raw_routing = _read_csv(artifacts.scan_routing)
        raw_market_context = _read_json(artifacts.market_context)
        raw_fundamental = _read_csv(artifacts.fundamental_thesis_universe)
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
            "supplemental_pattern": _descriptor_optional(
                artifacts.supplemental_pattern_scan,
                "investigator",
                "routed_pattern_scan",
                run_id,
                stage_attempt,
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
            "supplemental_sector": _descriptor_optional(
                artifacts.supplemental_sector_dashboard,
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
        rank_keys = {
            (
                str(row.get("exchange") or "NSE").upper(),
                str(row.get("symbol_id") or row.get("symbol") or "").upper(),
            )
            for row in raw_rank
        }
        investigator_rank_fallback_rows = [
            row
            for row in raw_investigator
            if (
                str(row.get("exchange") or "NSE").upper(),
                str(row.get("symbol_id") or row.get("symbol") or "").upper(),
            )
            not in rank_keys
        ]
        investigator_rank_result = (
            adapt_ranking_rows(
                investigator_rank_fallback_rows,
                source=descriptors["investigator"],
                as_of=as_of,
                prior_rank_positions=prior_positions,
            )
            if descriptors["investigator"] and investigator_rank_fallback_rows
            else None
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
        supplemental_pattern_result = (
            adapt_pattern_rows(
                raw_supplemental_pattern,
                source=descriptors["supplemental_pattern"],
                as_of=as_of,
            )
            if descriptors["supplemental_pattern"]
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
        supplemental_sector_result = (
            adapt_sector_stage_rows(
                raw_supplemental_sector,
                source=descriptors["supplemental_sector"],
                as_of=as_of,
            )
            if descriptors["supplemental_sector"]
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
                investigator_rank_result,
                evidence_result,
                breakout_result,
                pattern_result,
                supplemental_pattern_result,
                stock_result,
                sector_result,
                supplemental_sector_result,
                lifecycle_result,
            )
            if item is not None
        )
        bundles, routing_rejections = _attach_routing(
            _reconcile(results, raw_rank, raw_stock, as_of),
            raw_routing,
            as_of,
            descriptor=_descriptor_optional(
                artifacts.scan_routing,
                "scan_router",
                "scan_routing",
                run_id,
                stage_attempt,
            ),
        )
        bundles = _attach_market_context(
            bundles,
            raw_market_context,
            descriptor=_descriptor_optional(
                artifacts.market_context,
                "rank",
                "dashboard_payload",
                run_id,
                stage_attempt,
            ),
        )
        bundles = _attach_fundamental_thesis_bundles(
            bundles,
            raw_fundamental,
            as_of=as_of,
            descriptor=_descriptor_optional(
                artifacts.fundamental_thesis_universe,
                "fundamental_discovery",
                "fundamental_thesis_universe",
                run_id,
                stage_attempt,
            ),
        )
        bundles = _attach_sector_gate_evidence(
            self.registry_store.registry,
            bundles,
            raw_stock=raw_stock,
            raw_sector=raw_sector,
            as_of=as_of,
        )
        observed_session = observed_session_date or _resolve_observed_session(
            ohlcv_db_path,
            cutoff=as_of.date(),
            exchanges={bundle.exchange for bundle in bundles},
        )
        bundles = _attach_session_prices(
            bundles, ohlcv_db_path=ohlcv_db_path, session_date=observed_session
        )
        adapter_seconds = time.perf_counter() - adapter_started

        rows: dict[str, list[dict[str, Any]]] = {
            name: []
            for name in (
                "candidate_admissions",
                "candidate_updates",
                "candidate_transitions",
                "candidate_closures",
                "candidate_supersessions",
                "candidate_reconciliation",
                "adapter_warnings",
                "adapter_rejections",
                "registry_conflicts",
                "current_candidate_state",
                "position_episode_compatibility",
                "position_recovery_proposals",
                "position_recovery_actions",
                "position_monitor_reconciliation",
                "investigator_performance_events",
                "investigator_performance_horizons",
                "investigator_discovery_scorecard",
                "investigator_entry_scorecard",
                "investigator_executable_scorecard",
                "investigator_transition_matrix",
                "investigator_evaluation_transitions",
                "investigator_attribution_coverage",
                "investigator_coverage_receipt",
                "investigator_readiness_inputs",
                "investigator_missing_data_reasons",
                "investigator_symbol_sensitivity",
                "investigator_primary_cohorts",
                "investigator_diagnostic_cohorts",
                "investigator_research_cohorts",
                "investigator_calendar_windows",
                "investigator_primary_sampling",
                "investigator_source_fidelity",
                "candidate_fundamental_observations",
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
        authoritative_context = {
            (record.exchange, record.symbol_id): record.value.investigator_context
            for record in (evidence_result.records if evidence_result else ())
            if record.value.investigator_context is not None
        }
        captured_context: dict[tuple[str, str], Any] = {}
        for bundle in bundles:
            if bundle.active_position:
                counters["active_positions_total"] += 1
                counters["active_positions_with_position_monitor"] += int(
                    bundle.scan_tier == "position_monitor"
                    and bool(bundle.routing_decision_id)
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
            admission_evaluations_json = rule_evaluations_json(admission)
            admission_satisfied_json = satisfied_rules_json(admission)
            recovery = False
            episode = None
            predecessor_episode = None
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
                    position_opened_at=_aware_datetime(
                        bundle.position_cycle_opened_at, as_of
                    ),
                    episodes=all_symbol_episodes,
                    current_states=open_states,
                )
                rows["position_episode_compatibility"].append(
                    {
                        "position_cycle_id": cycle_id,
                        "exchange": bundle.exchange,
                        "symbol_id": bundle.symbol_id,
                        "compatibility_status": compatibility.status.value,
                        "candidate_id": compatibility.candidate_id,
                        "open_episode_ids": list(compatibility.open_episode_ids),
                        "compatibility_reasons": list(compatibility.reasons),
                        "policy_version": config.position_episode_compatibility_policy_version,
                    }
                )
                if compatibility.status is PositionEpisodeCompatibility.COMPATIBLE:
                    episode = next(
                        item
                        for item in all_symbol_episodes
                        if item.candidate_id == compatibility.candidate_id
                    )
                    match_outcome = SetupMatchOutcome.EXACT
                    counters["compatible_episode_attachments"] += 1
                    if bundle.market_data_complete and bundle.routing_decision_id:
                        counters["active_positions_fully_monitored"] += 1
                else:
                    if (
                        compatibility.status
                        is PositionEpisodeCompatibility.AMBIGUOUS_MULTIPLE_EPISODES
                    ):
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
                    if not config.dry_run:
                        _persist_recovery_proposal(
                            self.registry_store.registry, proposal
                        )
                    rows["position_recovery_proposals"].append(proposal)
                    rows["position_monitor_reconciliation"].append(
                        {
                            "position_cycle_id": cycle_id,
                            "symbol_id": bundle.symbol_id,
                            "exchange": bundle.exchange,
                            "outcome": "POSITION_RECOVERY_REQUIRED",
                            "compatibility_status": compatibility.status.value,
                            "recovery_proposal_id": proposal["recovery_proposal_id"],
                        }
                    )
                    counters["recovery_proposals"] += 1
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
                    {
                        **_reconciliation_row(
                            bundle,
                            "not_admitted",
                            "same-symbol open episode was not attached without setup-family admission",
                        ),
                        "rule_evaluations": admission_evaluations_json,
                    }
                )
                continue
            elif recovery:
                match_outcome = SetupMatchOutcome.NEW_EPISODE
            elif (
                admission.admitted
                and admission.reason in {
                    AdmissionReason.INVESTIGATOR_PRIMARY_ONSET,
                    AdmissionReason.FUNDAMENTAL_THESIS,
                }
            ):
                exact_family = (
                    "fundamental_thesis"
                    if admission.reason is AdmissionReason.FUNDAMENTAL_THESIS
                    else "investigator_primary"
                )
                exact_primary = [
                    item
                    for item in matching_for_symbol
                    if item.setup_family == exact_family
                ]
                if len(exact_primary) > 1:
                    _conflict(
                        rows,
                        bundle,
                        f"multiple open {exact_family} episodes",
                    )
                    counters["registry_conflicts"] += 1
                    continue
                episode = exact_primary[0] if exact_primary else None
                match_outcome = (
                    SetupMatchOutcome.EXACT
                    if episode is not None
                    else SetupMatchOutcome.NEW_EPISODE
                )
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
                if match.outcome is SetupMatchOutcome.SUPERSEDES:
                    predecessor_episode = episode
                    episode = None
                if match.outcome is SetupMatchOutcome.CONFLICT:
                    _conflict(rows, bundle, "; ".join(match.warnings))
                    counters["registry_conflicts"] += 1
                    continue
            else:
                counters["not_admitted"] += 1
                rows["candidate_reconciliation"].append(
                    {
                        **_reconciliation_row(
                            bundle, "not_admitted", "; ".join(admission.blockers)
                        ),
                        "rule_evaluations": admission_evaluations_json,
                    }
                )
                continue

            lineage = _combined_lineage(
                bundle, run_id, stage_attempt, policy_snapshot_id
            )
            episode_request = None
            if episode is None:
                if recovery:
                    setup_family = "position_state_recovery"
                    opening_reason = "position_state_recovery"
                    cycle_id = bundle.position_cycle_id or make_position_cycle_id(
                        exchange=bundle.exchange,
                        symbol_id=bundle.symbol_id,
                        position_opened_at=bundle.position_cycle_opened_at
                        or bundle.as_of,
                    )
                    admission_identity = (
                        f"{cycle_id}|{config.position_recovery_policy_version}"
                    )
                    episode_started_at = _aware_datetime(
                        bundle.position_cycle_opened_at, as_of
                    )
                    persisted_satisfied_json = None
                    persisted_evaluations_json = None
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
                    persisted_satisfied_json = admission_satisfied_json
                    persisted_evaluations_json = admission_evaluations_json
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
                    satisfied_admission_rules_json=persisted_satisfied_json,
                    rule_evaluations_json=persisted_evaluations_json,
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
                if predecessor_episode is not None:
                    supersession = EpisodeSupersession(
                        predecessor_candidate_id=predecessor_episode.candidate_id,
                        relation_type=(
                            EpisodeRelationType.MOMENTUM_SUPERSEDED_BY_BREAKOUT.value
                        ),
                        related_at=as_of,
                        closing_reason=ClosureReason.SUPERSEDED_BY_NEW_EPISODE.value,
                        rule_version=SETUP_FAMILY_RULE_VERSION,
                        lineage=lineage,
                        contract_version=OPPORTUNITY_CONTRACT_VERSION,
                    )
                    counters["episodes_superseded"] += 1
                    rows["candidate_supersessions"].append(
                        {
                            "predecessor_candidate_id": predecessor_episode.candidate_id,
                            "successor_candidate_id": candidate_id,
                            "exchange": bundle.exchange,
                            "symbol_id": bundle.symbol_id,
                            "relation_type": supersession.relation_type,
                            "rule_version": supersession.rule_version,
                        }
                    )
                else:
                    supersession = None
                rows["candidate_admissions"].append(
                    {
                        "candidate_id": candidate_id,
                        "setup_id": setup_id,
                        "exchange": bundle.exchange,
                        "symbol_id": bundle.symbol_id,
                        "reason": opening_reason,
                        "setup_family": setup_family,
                        "primary_admission_reason": opening_reason,
                        "primary_setup_family": setup_family,
                        "satisfied_admission_rules": persisted_satisfied_json,
                        "rule_evaluations": persisted_evaluations_json,
                        "rule_version": admission.rule_version,
                    }
                )
            else:
                counters["existing_episodes_matched"] += 1
                supersession = None

            current = state_by_id.get(episode.candidate_id)
            if (
                current is not None
                and current.last_observed_run_id == run_id
                and lineage.source_artifact_hash
                in run_observation_hashes.get(episode.candidate_id, ())
            ):
                if bundle.investigator_context is not None:
                    captured_context[(bundle.exchange, bundle.symbol_id)] = (
                        bundle.investigator_context
                    )
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
                    CandidateState.INVESTIGATING
                    if recovery
                    else CandidateState.DISCOVERED
                )
            )
            progress = _progress_from_current(bundle, current)
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
            if (
                bundle.followthrough_status is FollowthroughStatus.CONFIRMED
                and lifecycle_state is not CandidateState.CONFIRMED
            ):
                rows["adapter_warnings"].append(
                    asdict(
                        AdapterWarning(
                            "lifecycle_state",
                            f"{bundle.exchange}:{bundle.symbol_id}",
                            "confirmed_followthrough_without_confirmed_transition",
                            "confirmed follow-through was not treated as an entry event because the canonical lifecycle did not transition to CONFIRMED",
                        )
                    )
                )
            counter_state = advance_session_counters(
                previous_counted_session=(
                    current.last_retention_counted_session if current else None
                ),
                previous_sessions_in_state=int(current.days_in_state or 0)
                if current
                else 0,
                previous_sessions_without_progress=(
                    int(current.days_without_progress or 0) if current else 0
                ),
                previous_last_progress_at=current.last_progress_at if current else None,
                observed_session=observed_session,
                observed_at=as_of,
                progress_improving=progress.status is ProgressStatus.IMPROVING,
                transition_occurred=transition.allowed,
                legacy_last_snapshot_session=(
                    current.last_snapshot_at.date()
                    if current and current.last_snapshot_at
                    else None
                ),
            )
            days_in_state = counter_state.sessions_in_state
            days_without_progress = counter_state.sessions_without_progress
            snapshot = assemble_candidate_snapshot(
                candidate_id=episode.candidate_id,
                setup_id=episode.setup_id,
                bundle=bundle,
                lifecycle_state=lifecycle_state,
                days_in_state=days_in_state,
                days_without_progress=days_without_progress,
                active_position=active_position,
            )
            if snapshot is not None:
                captured_context[(bundle.exchange, bundle.symbol_id)] = (
                    snapshot.investigator_context
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
                    days_in_state=days_in_state,
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
                            last_progress_at=counter_state.last_progress_at,
                            last_retention_counted_session=counter_state.counted_session,
                            snapshot=snapshot,
                            transition=(transition if transition.allowed else None),
                            previous_state=previous_state,
                            lineage=lineage,
                            closure=closure,
                            supersession=supersession,
                        )
                    )
                    _count_append_results(counters, write_result.append_results)
                    if bundle.fundamental_thesis is not None:
                        observation = _persist_fundamental_observation(
                            self.registry_store.registry,
                            candidate_id=episode.candidate_id,
                            setup_id=episode.setup_id,
                            bundle=bundle,
                            run_id=run_id,
                            policy_snapshot_id=policy_snapshot_id,
                        )
                        rows["candidate_fundamental_observations"].append(observation)
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
                            + (
                                ["investigator_evidence_incomplete"]
                                if bundle.evidence is None
                                or bundle.evidence.missing_evidence
                                else []
                            )
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
                        if config.position_recovery_mode
                        is PositionRecoveryMode.REVIEWED
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

        if not config.dry_run and ohlcv_db_path is not None:
            performance_outputs = mature_performance_events(
                self.registry_store.registry,
                ohlcv_db_path=ohlcv_db_path,
            )
            for name, output_rows in performance_outputs.items():
                rows[name].extend(output_rows)
        sampling_rows, fidelity_rows = _primary_sampling_evidence(
            authoritative_context=authoritative_context,
            captured_context=captured_context,
            raw_routed_investigator=raw_routed_investigator,
            policy_snapshot_id=policy_snapshot_id,
        )
        rows["investigator_primary_sampling"].extend(sampling_rows)
        rows["investigator_source_fidelity"].extend(fidelity_rows)
        rows["investigator_readiness_inputs"].extend(
            _runtime_readiness_inputs(sampling_rows, fidelity_rows)
        )
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
                "discovery_event_count": sum(
                    row.get("event_type") == "CANDIDATE_DISCOVERED"
                    for row in rows["investigator_performance_events"]
                ),
                "entry_confirmed_event_count": sum(
                    row.get("event_type") == "ENTRY_CONFIRMED"
                    for row in rows["investigator_performance_events"]
                ),
                "transition_evidence_sufficient": bool(
                    rows["investigator_transition_matrix"]
                ),
                "primary_qualifying_observations": sampling_rows[0]["denominator"],
                "primary_observations_captured": sampling_rows[0]["numerator"],
                "investigator_source_fidelity_pct": fidelity_rows[0][
                    "fidelity_pct"
                ],
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
    last_progress_at: datetime | None,
    last_retention_counted_session: date,
    snapshot: Any,
    transition: Any,
    previous_state: CandidateState,
    lineage: SourceLineage,
    closure: EpisodeClosure | None,
    supersession: EpisodeSupersession | None,
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
        SnapshotObservation(
            snapshot,
            bundle.as_of,
            lineage,
            last_progress_at=last_progress_at,
            last_retention_counted_session=last_retention_counted_session,
        )
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
    performance_events: list[PerformanceEventObservation] = []
    if snapshot is not None:
        context = snapshot.investigator_context
        if context.context_as_of is not None and context.context_as_of > bundle.as_of:
            raise ValueError(
                "investigator context cannot be later than the decision timestamp"
            )
        event_dq = (
            "PENDING" if bundle.market_close is not None else "INSUFFICIENT_PRICE_DATA"
        )
        event_reason = (
            None
            if bundle.market_close is not None
            else "decision_session_close_missing"
        )
        common = {
            "candidate_id": candidate_id,
            "setup_id": setup_id,
            "symbol_id": bundle.symbol_id,
            "exchange": bundle.exchange,
            "sector_name": bundle.sector_name,
            "event_at": bundle.as_of,
            "session_date": last_retention_counted_session,
            "anchor_price": bundle.market_close,
            "anchor_price_basis": "DECISION_SESSION_CLOSE",
            "investigator_context": context,
            "lineage": lineage,
            "data_quality_status": event_dq,
            "data_quality_reason": event_reason,
        }
        if (
            episode_request is not None
            and episode_request.episode_type != "position_state_recovery"
        ):
            performance_events.append(
                PerformanceEventObservation(
                    event_type="CANDIDATE_DISCOVERED",
                    lifecycle_evaluable=(
                        snapshot.lifecycle_state is CandidateState.PENDING_FOLLOWTHROUGH
                    ),
                    **common,
                )
            )
        if (
            transition is not None
            and transition.allowed
            and transition.proposed_state is CandidateState.CONFIRMED
        ):
            performance_events.append(
                PerformanceEventObservation(
                    event_type="ENTRY_CONFIRMED",
                    lifecycle_evaluable=True,
                    **common,
                )
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
        supersession=supersession,
        performance_events=tuple(performance_events),
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
        "performance_": "performance_events_created",
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
            "",
            "unknown",
            "nan",
            "none",
            "<na>",
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
            velocity = (
                float(velocity_value) if velocity_value not in (None, "") else None
            )
        except (TypeError, ValueError):
            velocity = None
        coverage_unknown = (
            _coerce_stage((current_row or {}).get("effective_stage"))
            is WeinsteinStage.UNKNOWN
            if current_row is not None
            else False
        ) or (prior is not None and prior_stage is WeinsteinStage.UNKNOWN)
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
        improving = current_stage.value in SECTOR_GATE_RULES[
            "calibration_current_provisional_stages"
        ] or (
            velocity is not None
            and velocity
            > SECTOR_GATE_RULES["calibration_improving_velocity_floor_exclusive"]
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
    sector_records: dict[str, list[tuple[Any, SourceDescriptor]]] = defaultdict(list)
    for result in results:
        for record in result.records:
            value = record.value
            if hasattr(value, "sector_name") and hasattr(value, "stage_snapshot"):
                sector_records[value.sector_name.strip().lower()].append(
                    (value, record.source)
                )
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
                item["investigator_context"] = value.investigator_context
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
        sector_evidence = sector_records.get(sector_name.lower(), [])
        sector = _merge_sector_snapshots(sector_evidence)
        sources = {
            source.artifact_hash: source
            for source in (
                *item["sources"],
                *(source for _, source in sector_evidence),
            )
        }
        bundles.append(
            OpportunitySourceBundle(
                symbol_id=key[1],
                exchange=key[0],
                as_of=as_of,
                opportunity=item.get("opportunity"),
                evidence=item.get("evidence"),
                investigator_context=item.get("investigator_context"),
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
    *,
    descriptor: SourceDescriptor | None = None,
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
        sources = existing.source_lineage
        if descriptor is not None and all(
            item.artifact_hash != descriptor.artifact_hash for item in sources
        ):
            sources = (*sources, descriptor)
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
            source_lineage=sources,
        )
    return tuple(by_key[key] for key in sorted(by_key)), tuple(rejections)


def _merge_sector_snapshots(
    evidence: list[tuple[Any, SourceDescriptor]],
) -> Any | None:
    """Combine structural stage and rank RS without allowing either to erase the other."""
    if not evidence:
        return None
    structural = next(
        (
            value
            for value, _ in evidence
            if value.stage_snapshot.effective_stage is not WeinsteinStage.UNKNOWN
        ),
        evidence[0][0],
    )
    relative_strength = next(
        (
            value.sector_relative_strength_state
            for value, _ in evidence
            if str(value.sector_relative_strength_state or "").strip().lower()
            not in {"", "unknown", "none", "nan", "<na>"}
        ),
        structural.sector_relative_strength_state,
    )
    rotation = next(
        (
            value.sector_rotation_state
            for value, _ in evidence
            if str(value.sector_rotation_state or "").strip().lower()
            not in {"", "unknown", "none", "nan", "<na>"}
        ),
        structural.sector_rotation_state,
    )
    return replace(
        structural,
        sector_relative_strength_state=relative_strength,
        sector_rotation_state=rotation,
    )


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
    *,
    bundle: OpportunitySourceBundle,
    cycle_id: str,
    compatibility: Any,
    config: OpportunityShadowConfig,
    run_id: str,
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
            "discovery_timestamp",
            "historical_rank",
            "historical_opportunity_score",
            "historical_investigator_score",
            "trigger_transition",
            "followthrough_status",
            "stage_at_entry",
        ],
        "recovery_mode": config.position_recovery_mode.value,
        "proposal_status": "PROPOSED",
        "policy_version": config.position_recovery_policy_version,
        "source_lineage": [asdict(source) for source in bundle.source_lineage],
        "created_run_id": run_id,
    }
    payload["payload_hash"] = recovery_payload_hash(payload)
    return payload


def _persist_recovery_proposal(
    registry: RegistryStore, proposal: dict[str, Any]
) -> None:
    with registry._writer() as conn:  # noqa: SLF001
        existing = conn.execute(
            """SELECT payload_hash, payload_json
               FROM position_recovery_proposal
               WHERE recovery_proposal_id = ?""",
            [proposal["recovery_proposal_id"]],
        ).fetchone()
        existing_semantic_hash = None
        existing_payload = None
        if existing:
            try:
                existing_payload = json.loads(existing[1])
                existing_semantic_hash = recovery_payload_hash(existing_payload)
            except (TypeError, ValueError):
                existing_semantic_hash = None
        incoming_semantic_hash = recovery_payload_hash(proposal)
        if existing and (
            existing[0] == proposal["payload_hash"]
            or existing_semantic_hash == incoming_semantic_hash
        ):
            return
        if existing_payload is not None and _recovery_proposal_stable_hash(
            existing_payload
        ) == _recovery_proposal_stable_hash(proposal):
            proposal["recovery_proposal_id"] = _recovery_proposal_revision_id(
                base_proposal_id=proposal["recovery_proposal_id"],
                assessment_hash=incoming_semantic_hash,
            )
            proposal["payload_hash"] = recovery_payload_hash(proposal)
            revised = conn.execute(
                """SELECT payload_hash, payload_json
                   FROM position_recovery_proposal
                   WHERE recovery_proposal_id = ?""",
                [proposal["recovery_proposal_id"]],
            ).fetchone()
            if revised:
                revised_payload = json.loads(revised[1])
                if recovery_payload_hash(revised_payload) == recovery_payload_hash(
                    proposal
                ):
                    return
                raise OpportunityRegistryConflictError(
                    record_type="position_recovery_proposal",
                    candidate_id=proposal["position_cycle_id"],
                    idempotency_key=proposal["recovery_proposal_id"],
                    existing_payload_hash=revised[0],
                    incoming_payload_hash=proposal["payload_hash"],
                )
            existing = None
        if existing:
            raise OpportunityRegistryConflictError(
                record_type="position_recovery_proposal",
                candidate_id=proposal["position_cycle_id"],
                idempotency_key=proposal["recovery_proposal_id"],
                existing_payload_hash=existing[0],
                incoming_payload_hash=incoming_semantic_hash,
            )
        conn.execute(
            """INSERT INTO position_recovery_proposal
               (recovery_proposal_id, position_cycle_id, symbol_id, exchange,
                recovery_mode, proposal_status, compatibility_status, payload_hash,
                payload_json, created_run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(recovery_proposal_id) DO NOTHING""",
            [
                proposal["recovery_proposal_id"],
                proposal["position_cycle_id"],
                proposal["symbol_id"],
                proposal["exchange"],
                proposal["recovery_mode"],
                proposal["proposal_status"],
                proposal["compatibility_status"],
                proposal["payload_hash"],
                json.dumps(proposal, sort_keys=True, default=str),
                proposal["created_run_id"],
            ],
        )


def _recovery_proposal_stable_hash(payload: dict[str, Any]) -> str:
    evolving_compatibility_fields = {
        "compatibility_status",
        "open_episode_ids",
        "conflict_reasons",
    }
    stable_payload = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "payload_hash",
            "created_run_id",
            "source_lineage",
            *evolving_compatibility_fields,
        }
    }
    return hashlib.sha256(
        json.dumps(
            stable_payload, sort_keys=True, default=str, separators=(",", ":")
        ).encode()
    ).hexdigest()


def _recovery_proposal_revision_id(
    *, base_proposal_id: str, assessment_hash: str
) -> str:
    digest = hashlib.sha256(
        f"{base_proposal_id}|{assessment_hash}".encode()
    ).hexdigest()
    return f"position-recovery-{digest[:24]}"


def _persist_recovery_action(registry: RegistryStore, action: dict[str, Any]) -> None:
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute(
            """INSERT INTO position_recovery_action
               (recovery_action_id, recovery_proposal_id, position_cycle_id, candidate_id,
                recovery_mode, reviewed_by, reviewed_at, review_notes, payload_json, created_run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(recovery_action_id) DO NOTHING""",
            [
                action["recovery_action_id"],
                action["recovery_proposal_id"],
                action["position_cycle_id"],
                action["candidate_id"],
                action["recovery_mode"],
                action["reviewed_by"],
                action["reviewed_at"],
                action["review_notes"],
                json.dumps(action, sort_keys=True, default=str),
                action["created_run_id"],
            ],
        )


def _persist_fundamental_observation(
    registry: RegistryStore,
    *,
    candidate_id: str,
    setup_id: str,
    bundle: OpportunitySourceBundle,
    run_id: str,
    policy_snapshot_id: str | None,
) -> dict[str, Any]:
    thesis = bundle.fundamental_thesis
    assert thesis is not None and thesis.primary_thesis is not None
    evaluations = [
        {
            "family": item.family.value,
            "passed": item.passed,
            "observed": dict(item.observed),
            "required": dict(item.required),
            "blockers": list(item.blockers),
            "warnings": list(item.warnings),
            "rule_version": item.rule_version,
        }
        for item in thesis.evaluations
    ]
    idempotency_key = hashlib.sha256(
        (
            f"{candidate_id}|{thesis.as_of}|{thesis.source_data_hash}|"
            f"{thesis.taxonomy_version}|{thesis.rule_version}|{thesis.admission_version}"
        ).encode()
    ).hexdigest()
    observation_id = f"fundamental-observation-{idempotency_key[:24]}"
    row = {
        "observation_id": observation_id,
        "candidate_id": candidate_id,
        "setup_id": setup_id,
        "symbol_id": thesis.symbol_id,
        "exchange": thesis.exchange,
        "observed_at": bundle.as_of,
        "primary_thesis": thesis.primary_thesis.value,
        "secondary_theses": [item.value for item in thesis.secondary_theses],
        "evaluations": evaluations,
        "evidence": dict(thesis.evidence),
        "blockers": list(thesis.admission_blockers),
        "source_data_hash": thesis.source_data_hash,
        "statement_basis": thesis.statement_basis,
        "source_report_date": thesis.source_report_date,
        "source_available_at": thesis.source_available_at,
        "taxonomy_version": thesis.taxonomy_version,
        "rule_version": thesis.rule_version,
        "admission_version": thesis.admission_version,
        "policy_snapshot_id": policy_snapshot_id,
        "source_run_id": run_id,
        "idempotency_key": idempotency_key,
    }
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute(
            """
            INSERT INTO candidate_fundamental_observation VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                current_timestamp AT TIME ZONE 'UTC'
            ) ON CONFLICT(idempotency_key) DO NOTHING
            """,
            [
                observation_id, candidate_id, setup_id, thesis.symbol_id, thesis.exchange,
                bundle.as_of, thesis.primary_thesis.value,
                json.dumps(row["secondary_theses"], sort_keys=True),
                json.dumps(evaluations, sort_keys=True, default=str),
                json.dumps(row["evidence"], sort_keys=True, default=str),
                json.dumps(row["blockers"], sort_keys=True), thesis.source_data_hash,
                thesis.statement_basis, thesis.source_report_date, thesis.source_available_at,
                thesis.taxonomy_version, thesis.rule_version, thesis.admission_version,
                policy_snapshot_id, run_id, idempotency_key,
            ],
        )
    return row


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


def _read_json(artifact: StageArtifact | None) -> dict[str, Any]:
    if artifact is None:
        return {}
    path = Path(artifact.uri)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_csv(artifact: StageArtifact | None) -> list[dict[str, Any]]:
    if artifact is None:
        return []
    path = Path(artifact.uri)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _attach_market_context(
    bundles: tuple[OpportunitySourceBundle, ...],
    payload: dict[str, Any],
    *,
    descriptor: SourceDescriptor | None,
) -> tuple[OpportunitySourceBundle, ...]:
    market = payload.get("market_regime") if isinstance(payload, dict) else {}
    market = market if isinstance(market, dict) else {}
    confirmed = str(market.get("regime") or market.get("confirmed_regime") or "unknown")
    raw = str(market.get("raw_regime") or confirmed or "unknown")
    confidence = _optional_float(
        market.get("regime_confidence_capped", market.get("regime_confidence"))
    )
    velocity_bucket = str(market.get("breadth_velocity_bucket") or "unknown")
    velocity_quantile = str(market.get("breadth_velocity_quantile") or "unknown")
    score_change = _optional_float(market.get("regime_score_chg_5d"))
    attached: list[OpportunitySourceBundle] = []
    for bundle in bundles:
        sources = bundle.source_lineage
        if descriptor is not None and all(
            item.artifact_hash != descriptor.artifact_hash for item in sources
        ):
            sources = (*sources, descriptor)
        attached.append(
            replace(
                bundle,
                market_regime=confirmed,
                raw_market_regime=raw,
                regime_confidence=confidence,
                breadth_velocity_bucket=velocity_bucket,
                breadth_velocity_quantile=velocity_quantile,
                regime_score_chg_5d=score_change,
                source_lineage=sources,
            )
        )
    return tuple(attached)


def _attach_fundamental_thesis_bundles(
    bundles: tuple[OpportunitySourceBundle, ...],
    rows: list[dict[str, Any]],
    *,
    as_of: datetime,
    descriptor: SourceDescriptor | None,
) -> tuple[OpportunitySourceBundle, ...]:
    """Append a separate fundamental-family bundle, preserving technical episodes."""

    if descriptor is None or not rows:
        return bundles
    by_key = {(item.exchange, item.symbol_id): item for item in bundles}
    fundamental_bundles: list[OpportunitySourceBundle] = []
    for row in rows:
        if str(row.get("admission_eligible") or "").strip().lower() not in {"true", "1"}:
            continue
        symbol = str(row.get("symbol_id") or "").upper().strip()
        exchange = str(row.get("exchange") or "NSE").upper().strip()
        primary_text = str(row.get("primary_thesis") or "").strip()
        if not symbol or not primary_text:
            continue
        try:
            primary = FundamentalThesisFamily(primary_text)
            secondary = tuple(
                FundamentalThesisFamily(value)
                for value in json.loads(row.get("secondary_theses_json") or "[]")
            )
            evidence = json.loads(row.get("evidence_json") or "{}")
            evaluations = tuple(
                FundamentalThesisEvaluation(
                    family=FundamentalThesisFamily(item["family"]),
                    passed=bool(item["passed"]),
                    observed=item.get("observed") or {},
                    required=item.get("required") or {},
                    blockers=tuple(item.get("blockers") or ()),
                    warnings=tuple(item.get("warnings") or ()),
                    rule_version=str(
                        item.get("rule_version")
                        or row.get("rule_version")
                        or FUNDAMENTAL_THESIS_RULE_VERSION
                    ),
                )
                for item in json.loads(row.get("evaluations_json") or "[]")
            )
        except (ValueError, TypeError, json.JSONDecodeError):
            continue
        thesis = FundamentalThesisSnapshot(
            symbol_id=symbol,
            exchange=exchange,
            as_of=as_of.date(),
            primary_thesis=primary,
            secondary_theses=secondary,
            evaluations=evaluations,
            source_data_hash=str(row.get("source_data_hash") or ""),
            statement_basis=str(row.get("statement_basis") or "unknown"),
            source_report_date=_optional_date(row.get("source_report_date")),
            source_available_at=_optional_date(row.get("source_available_at")),
            classification_status=str(row.get("classification_status") or "QUALIFIED"),
            admission_eligible=True,
            admission_blockers=(),
            evidence=evidence,
            taxonomy_version=str(
                row.get("taxonomy_version") or FUNDAMENTAL_DISCOVERY_TAXONOMY_VERSION
            ),
            rule_version=str(row.get("rule_version") or FUNDAMENTAL_THESIS_RULE_VERSION),
            admission_version=str(
                row.get("admission_version") or FUNDAMENTAL_THESIS_ADMISSION_VERSION
            ),
        )
        base = by_key.get(
            (exchange, symbol),
            OpportunitySourceBundle(symbol_id=symbol, exchange=exchange, as_of=as_of),
        )
        sources = tuple({item.artifact_hash: item for item in (*base.source_lineage, descriptor)}.values())
        fundamental_bundles.append(
            replace(
                base,
                investigator_context=None,
                fundamental_thesis=thesis,
                source_lineage=sources,
                source_row_identities=(*base.source_row_identities, f"fundamental:{exchange}:{symbol}:{thesis.source_data_hash}"),
            )
        )
    return (*bundles, *fundamental_bundles)


def _optional_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if parsed != parsed else parsed


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
    bundle: OpportunitySourceBundle,
    run_id: str,
    attempt: int,
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


def _resolve_observed_session(
    db_path: Path | None, *, cutoff: date, exchanges: set[str]
) -> date:
    """Resolve the latest actual OHLCV session, never a weekend rerun date."""
    if db_path is None:
        return cutoff
    normalized = sorted({item.upper() for item in exchanges if item})
    if not normalized:
        raise OpportunityShadowSourceError(
            "cannot resolve observed trading session without an exchange"
        )
    placeholders = ", ".join("?" for _ in normalized)
    try:
        with duckdb.connect(str(db_path), read_only=True) as conn:
            row = conn.execute(
                f"SELECT MAX(CAST(timestamp AS DATE)) FROM _catalog "  # noqa: S608
                f"WHERE UPPER(exchange) IN ({placeholders}) "
                "AND CAST(timestamp AS DATE) <= ?",
                [*normalized, cutoff],
            ).fetchone()
    except (duckdb.Error, OSError) as exc:
        raise OpportunityShadowSourceError(
            f"cannot resolve observed trading session from OHLCV store: {exc}"
        ) from exc
    session = row[0] if row else None
    if session is None:
        raise OpportunityShadowSourceError(
            "OHLCV store has no observed trading session at or before the run date"
        )
    return session


def _attach_session_prices(
    bundles: tuple[OpportunitySourceBundle, ...],
    *,
    ohlcv_db_path: Path | None,
    session_date: date,
) -> tuple[OpportunitySourceBundle, ...]:
    if ohlcv_db_path is None or not bundles:
        return bundles
    keys = sorted({(item.exchange, item.symbol_id) for item in bundles})
    symbols = sorted({symbol for _, symbol in keys})
    exchanges = sorted({exchange for exchange, _ in keys})
    symbol_placeholders = ", ".join("?" for _ in symbols)
    exchange_placeholders = ", ".join("?" for _ in exchanges)
    try:
        with duckdb.connect(str(ohlcv_db_path), read_only=True) as conn:
            rows = conn.execute(
                f"""
                SELECT UPPER(exchange), UPPER(symbol_id), open, close
                FROM _catalog
                WHERE UPPER(symbol_id) IN ({symbol_placeholders})
                  AND UPPER(exchange) IN ({exchange_placeholders})
                  AND CAST(timestamp AS DATE) = ?
                  AND COALESCE(is_benchmark, FALSE) = FALSE
                """,
                [*symbols, *exchanges, session_date],
            ).fetchall()
    except (duckdb.Error, OSError):
        return bundles
    prices = {
        (str(exchange), str(symbol)): (
            _optional_float(open_price),
            _optional_float(close_price),
        )
        for exchange, symbol, open_price, close_price in rows
    }
    return tuple(
        replace(
            bundle,
            market_open=prices.get((bundle.exchange, bundle.symbol_id), (None, None))[
                0
            ],
            market_close=prices.get((bundle.exchange, bundle.symbol_id), (None, None))[
                1
            ],
        )
        for bundle in bundles
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
        "episodes_superseded": 0,
        "snapshots_created": 0,
        "duplicate_snapshots": 0,
        "transitions_created": 0,
        "opportunity_observations_created": 0,
        "evidence_observations_created": 0,
        "stock_stage_observations_created": 0,
        "sector_stage_observations_created": 0,
        "progress_observations_created": 0,
        "performance_events_created": 0,
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


def _primary_sampling_evidence(
    *,
    authoritative_context: dict[tuple[str, str], Any],
    captured_context: dict[tuple[str, str], Any],
    raw_routed_investigator: list[dict[str, Any]],
    policy_snapshot_id: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    primary = {
        key: context
        for key, context in authoritative_context.items()
        if bool(context.review_eligible)
    }
    captured = {
        key: captured_context[key]
        for key in primary
        if key in captured_context and captured_context[key].review_eligible
    }
    denominator = len(primary)
    numerator = len(captured)
    sampling_pct = round(100.0 * numerator / denominator, 6) if denominator else 100.0
    sampling = [
        {
            "metric_name": "primary_observation_capture",
            "numerator": numerator,
            "denominator": denominator,
            "capture_pct": sampling_pct,
            "target_pct": 100.0,
            "status": "PASS" if sampling_pct >= 100.0 else "FAIL",
            "missing_symbols": sorted(
                f"{exchange}:{symbol}" for exchange, symbol in set(primary) - set(captured)
            ),
            "policy_version": INVESTIGATOR_ATTRIBUTION_POLICY_VERSION,
            "policy_snapshot_id": policy_snapshot_id,
        }
    ]
    fields = (
        "move_tag",
        "trigger_reason",
        "final_score",
        "breakout_type",
    )
    mismatches: list[str] = []
    for key, expected in primary.items():
        observed = captured.get(key)
        if observed is None:
            mismatches.append(f"{key[0]}:{key[1]}:snapshot_missing")
            continue
        for field in fields:
            if _fidelity_value(getattr(expected, field, None)) != _fidelity_value(
                getattr(observed, field, None)
            ):
                mismatches.append(f"{key[0]}:{key[1]}:{field}")
    routed_by_key = {
        (
            str(row.get("exchange") or "NSE").upper(),
            str(row.get("symbol_id") or row.get("symbol") or "").upper(),
        ): row
        for row in raw_routed_investigator
        if str(row.get("symbol_id") or row.get("symbol") or "").strip()
    }
    routed_divergence = 0
    for key, expected in authoritative_context.items():
        routed = routed_by_key.get(key)
        if routed is None:
            continue
        if any(
            _fidelity_value(getattr(expected, field, None))
            != _fidelity_value(routed.get(field))
            for field in fields
        ):
            routed_divergence += 1
    fidelity_numerator = max(denominator - len({item.rsplit(":", 1)[0] for item in mismatches}), 0)
    fidelity_pct = (
        round(100.0 * fidelity_numerator / denominator, 6)
        if denominator
        else 100.0
    )
    fidelity = [
        {
            "metric_name": "authoritative_investigator_source_fidelity",
            "numerator": fidelity_numerator,
            "denominator": denominator,
            "fidelity_pct": fidelity_pct,
            "target_pct": 100.0,
            "status": "PASS" if fidelity_pct >= 100.0 else "FAIL",
            "mismatch_reasons": sorted(mismatches),
            "routed_sidecar_divergence_count": routed_divergence,
            "authoritative_artifact": "investigator_scores",
            "policy_version": INVESTIGATOR_ATTRIBUTION_POLICY_VERSION,
            "policy_snapshot_id": policy_snapshot_id,
        }
    ]
    return sampling, fidelity


def _runtime_readiness_inputs(
    sampling_rows: list[dict[str, Any]],
    fidelity_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    sampling = sampling_rows[0]
    fidelity = fidelity_rows[0]
    return [
        {
            "check_id": "INVESTIGATOR_PRIMARY_OBSERVATION_CAPTURE",
            "category": "investigator_sampling",
            "status": sampling["status"],
            "observed": sampling["capture_pct"],
            "expected": ">=100.0",
            "production_blocking": True,
            "policy_version": sampling["policy_version"],
        },
        {
            "check_id": "INVESTIGATOR_SOURCE_FIDELITY",
            "category": "investigator_sampling",
            "status": fidelity["status"],
            "observed": fidelity["fidelity_pct"],
            "expected": ">=100.0",
            "production_blocking": True,
            "policy_version": fidelity["policy_version"],
        },
    ]


def _fidelity_value(value: Any) -> Any:
    parsed = _optional_float(value)
    if parsed is not None:
        return round(parsed, 8)
    return str(value or "UNKNOWN").strip().upper().replace(" ", "_").replace("-", "_")


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
