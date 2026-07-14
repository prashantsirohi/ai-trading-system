"""Shadow-mode coordinator from registered artifacts to the opportunity registry."""

from __future__ import annotations

import csv
import hashlib
import time
from dataclasses import asdict, dataclass
from datetime import datetime
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


class OpportunityShadowOrchestrator:
    """Coordinates Phase 3 without changing any upstream or execution artifact."""

    def __init__(self, registry: RegistryStore):
        self.registry_store = DuckDBOpportunityRegistryStore(registry)
        self.registry = OpportunityRegistryService(self.registry_store)

    def run(
        self, *, run_id: str, stage_attempt: int, artifact_set: OpportunityArtifactSet,
        as_of: datetime, mode: OpportunityRegistryMode, config: OpportunityShadowConfig,
        ohlcv_db_path: Path | None = None,
    ) -> OpportunityShadowRunResult:
        if mode is OpportunityRegistryMode.OFF:
            return OpportunityShadowRunResult("skipped", config.dry_run, {"mode": "off"}, {})
        started = time.perf_counter()
        open_states = self.registry.list_open_candidates()
        open_episodes = self.registry.list_open_episodes()
        run_observation_hashes = self.registry.observation_hashes_for_run(run_id)
        prior_positions = {
            (state.exchange, state.symbol_id): state.latest_rank_position
            for state in open_states if state.latest_rank_position is not None
        }
        artifacts = artifact_set
        raw_rank = _read_csv(artifacts.ranked_signals)
        if not raw_rank:
            raise OpportunityShadowSourceError("ranked_signals is required and contains no usable rows")
        raw_investigator = _read_csv(artifacts.investigator_scores)
        raw_breakout = _read_csv(artifacts.breakout_scan)
        raw_pattern = _read_csv(artifacts.pattern_scan)
        raw_stock = _read_csv(artifacts.stock_scan)
        raw_sector = _read_csv(artifacts.sector_dashboard)
        raw_lifecycle = _read_csv(artifacts.lifecycle_state)
        if raw_stock and ohlcv_db_path is not None:
            raw_stock = _enrich_stock_stage(raw_stock, ohlcv_db_path, as_of)

        descriptors = {
            "rank": _descriptor(artifacts.ranked_signals, "rank", "ranked_signals", run_id, stage_attempt),
            "investigator": _descriptor_optional(artifacts.investigator_scores, "investigator", "investigator_scores", run_id, stage_attempt),
            "breakout": _descriptor_optional(artifacts.breakout_scan, "rank", "breakout_scan", run_id, stage_attempt),
            "pattern": _descriptor_optional(artifacts.pattern_scan, "rank", "pattern_scan", run_id, stage_attempt),
            "stock": _descriptor_optional(artifacts.stock_scan, "rank", "stock_scan", run_id, stage_attempt),
            "sector": _descriptor_optional(artifacts.sector_dashboard, "rank", "sector_dashboard", run_id, stage_attempt),
            "lifecycle": _descriptor_optional(artifacts.lifecycle_state, "investigator", "stage1_current_state", run_id, stage_attempt),
        }
        adapter_started = time.perf_counter()
        rank_result = adapt_ranking_rows(raw_rank, source=descriptors["rank"], as_of=as_of, prior_rank_positions=prior_positions)
        evidence_result = adapt_investigator_rows(raw_investigator, source=descriptors["investigator"], as_of=as_of) if descriptors["investigator"] else None
        breakout_result = adapt_breakout_rows(raw_breakout, source=descriptors["breakout"], as_of=as_of) if descriptors["breakout"] else None
        pattern_result = adapt_pattern_rows(raw_pattern, source=descriptors["pattern"], as_of=as_of) if descriptors["pattern"] else None
        stock_result = adapt_stock_stage_rows(raw_stock, source=descriptors["stock"], as_of=as_of) if descriptors["stock"] else None
        sector_result = adapt_sector_stage_rows(raw_sector, source=descriptors["sector"], as_of=as_of) if descriptors["sector"] else None
        lifecycle_result = adapt_lifecycle_rows(raw_lifecycle, source=descriptors["lifecycle"], as_of=as_of) if descriptors["lifecycle"] else None
        results = tuple(item for item in (rank_result, evidence_result, breakout_result, pattern_result, stock_result, sector_result, lifecycle_result) if item is not None)
        bundles = _reconcile(results, raw_rank, as_of)
        adapter_seconds = time.perf_counter() - adapter_started

        rows: dict[str, list[dict[str, Any]]] = {name: [] for name in (
            "candidate_admissions", "candidate_updates", "candidate_transitions", "candidate_closures",
            "candidate_reconciliation", "adapter_warnings", "adapter_rejections", "registry_conflicts",
            "current_candidate_state",
        )}
        for result in results:
            rows["adapter_warnings"].extend(asdict(item) for item in result.warnings)
            rows["adapter_rejections"].extend(asdict(item) for item in result.rejected_rows)
        if not raw_investigator:
            rows["adapter_warnings"].append(asdict(AdapterWarning("investigator_scores", "*", "missing_investigator", "Investigator output unavailable; evidence is not synthesized")))

        state_by_id = {state.candidate_id: state for state in open_states}
        persistence_started = time.perf_counter()
        counters = _initial_counts(raw_rank, raw_investigator, raw_breakout, raw_pattern, raw_stock, raw_sector, raw_lifecycle, bundles)
        for bundle in bundles:
            matching_for_symbol = [episode for episode in open_episodes if episode.exchange == bundle.exchange and episode.symbol_id == bundle.symbol_id]
            admission = evaluate_admission(bundle, config)
            if matching_for_symbol and not admission.admitted:
                if len(matching_for_symbol) == 1:
                    episode = matching_for_symbol[0]
                    match_outcome = SetupMatchOutcome.EXACT
                else:
                    _conflict(rows, bundle, "multiple open episodes require an explicit setup-family match")
                    counters["registry_conflicts"] += 1
                    continue
            elif admission.admitted and admission.setup_family:
                match = match_open_episode(
                    exchange=bundle.exchange, symbol_id=bundle.symbol_id, setup_family=admission.setup_family,
                    as_of=as_of, episodes=open_episodes, current_states=open_states,
                    progression_max_days=config.setup_progression_max_days,
                )
                match_outcome = match.outcome
                episode = next((item for item in open_episodes if item.candidate_id == match.candidate_id), None)
                if match.outcome is SetupMatchOutcome.CONFLICT:
                    _conflict(rows, bundle, "; ".join(match.warnings))
                    counters["registry_conflicts"] += 1
                    continue
            else:
                counters["not_admitted"] += 1
                rows["candidate_reconciliation"].append(_reconciliation_row(bundle, "not_admitted", "; ".join(admission.blockers)))
                continue

            lineage = _combined_lineage(bundle, run_id, stage_attempt)
            episode_request = None
            if episode is None:
                assert admission.admission_identity and admission.setup_family and admission.reason
                request = OpenEpisodeRequest(
                    symbol_id=bundle.symbol_id, exchange=bundle.exchange,
                    setup_family=admission.setup_family.value, admission_identity=admission.admission_identity,
                    episode_started_at=as_of, episode_type="analytical_shadow",
                    opening_reason=admission.reason.value, lineage=lineage,
                    contract_version=OPPORTUNITY_CONTRACT_VERSION,
                )
                setup_id = make_setup_id(exchange=bundle.exchange, symbol_id=bundle.symbol_id, setup_family=admission.setup_family.value, admission_identity=admission.admission_identity, episode_started_at=as_of)
                candidate_id = make_candidate_id(setup_id)
                prior_episode = self.registry.get_candidate_episode(candidate_id)
                if prior_episode is not None and prior_episode.episode_status is not EpisodeStatus.OPEN:
                    counters["registry_duplicates"] += 1
                    rows["candidate_reconciliation"].append(_reconciliation_row(bundle, "closed_admission_replay", candidate_id))
                    continue
                episode_request = request
                episode = _dry_episode(request, candidate_id, setup_id)
                counters["new_episodes_opened"] += 1
                rows["candidate_admissions"].append({
                    "candidate_id": candidate_id, "setup_id": setup_id, "exchange": bundle.exchange,
                    "symbol_id": bundle.symbol_id, "reason": admission.reason.value,
                    "setup_family": admission.setup_family.value, "rule_version": admission.rule_version,
                })
            else:
                counters["existing_episodes_matched"] += 1

            current = state_by_id.get(episode.candidate_id)
            if (
                current is not None
                and current.last_observed_run_id == run_id
                and lineage.source_artifact_hash in run_observation_hashes.get(episode.candidate_id, ())
            ):
                counters["registry_duplicates"] += 1
                rows["candidate_reconciliation"].append(_reconciliation_row(bundle, "exact_run_replay", episode.candidate_id))
                continue
            previous_state = CandidateState(current.current_lifecycle_state) if current and current.current_lifecycle_state else CandidateState.DISCOVERED
            progress = _progress_from_current(bundle, current)
            days_without_progress = 0 if progress.status is ProgressStatus.IMPROVING else int(current.days_without_progress or 0) + 1 if current else 0
            days_in_state = max((as_of - current.last_transition_at).days, 0) if current and current.last_transition_at else int(current.days_in_state or 0) if current else 0
            active_position = False
            transition = evaluate_transition(previous_state, bundle, progress_status=progress.status, active_position=active_position, config=config)
            lifecycle_state = transition.proposed_state if transition.allowed else previous_state
            snapshot = assemble_candidate_snapshot(
                candidate_id=episode.candidate_id, setup_id=episode.setup_id, bundle=bundle,
                lifecycle_state=lifecycle_state, days_in_state=0 if transition.allowed else days_in_state,
                days_without_progress=days_without_progress, active_position=active_position,
            )
            try:
                if transition.allowed:
                    rows["candidate_transitions"].append({
                        "candidate_id": episode.candidate_id, "from_state": previous_state.value,
                        "to_state": lifecycle_state.value, "reason": transition.transition_reason.value,
                        "rule_version": transition.rule_version,
                    })
                retention = evaluate_retention(
                    state=lifecycle_state, days_in_state=0 if transition.allowed else days_in_state,
                    days_without_progress=days_without_progress, progress_status=progress.status,
                    stock_stage=(bundle.stock_stage.effective_stage if bundle.stock_stage else WeinsteinStage.UNKNOWN),
                    followthrough_status=bundle.followthrough_status, active_position=active_position, config=config,
                )
                closure = None
                if retention.close_episode:
                    close_status = EpisodeStatus.ARCHIVED if retention.archive else EpisodeStatus.CLOSED
                    closure = EpisodeClosure(
                        close_status, as_of,
                        retention.reason.value if retention.reason else "policy_close", lineage,
                    )
                    counters["episodes_closed"] += 1
                    counters["episodes_archived"] += int(retention.archive)
                    rows["candidate_closures"].append({"candidate_id": episode.candidate_id, "reason": retention.reason.value if retention.reason else "policy_close", "archived": retention.archive})
                else:
                    counters["episodes_retained"] += 1
                if not config.dry_run:
                    write_result = self.registry.apply_orchestration_bundle(_write_bundle(
                        episode_request=episode_request, candidate_id=episode.candidate_id,
                        setup_id=episode.setup_id, bundle=bundle, progress=progress,
                        days_without_progress=days_without_progress, snapshot=snapshot,
                        transition=(transition if transition.allowed else None),
                        previous_state=previous_state, lineage=lineage, closure=closure,
                    ))
                    _count_append_results(counters, write_result.append_results)
                rows["candidate_updates"].append({
                    "candidate_id": episode.candidate_id, "symbol_id": bundle.symbol_id,
                    "lifecycle_state": lifecycle_state.value, "progress_status": progress.status.value,
                    "snapshot_complete": snapshot is not None,
                })
                rows["candidate_reconciliation"].append(_reconciliation_row(bundle, match_outcome.value, episode.candidate_id))
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
        counters.update({
            "adapter_warnings": len(rows["adapter_warnings"]),
            "rejected_rows": len(rows["adapter_rejections"]),
            "dry_run": config.dry_run,
            "no_database_writes_performed": config.dry_run,
            "adapter_seconds": round(adapter_seconds, 6),
            "persistence_seconds": round(persistence_seconds, 6),
            "total_seconds": round(time.perf_counter() - started, 6),
            "mode": mode.value,
            "status": "degraded" if rows["registry_conflicts"] or rows["adapter_rejections"] else "completed",
            "unmatched_sector_mappings": sum(item.sector_stage is None for item in bundles),
            "missing_critical_sources": 0,
            "state_distribution": {
                state.value: sum(row.get("lifecycle_state") == state.value for row in rows["candidate_updates"])
                for state in CandidateState
            },
            "stock_stage_distribution": _stage_distribution(
                item.stock_stage.effective_stage.value for item in bundles if item.stock_stage is not None
            ),
            "sector_stage_distribution": _stage_distribution(
                item.sector_stage.stage_snapshot.effective_stage.value
                for item in bundles if item.sector_stage is not None
            ),
            "stage_status_distribution": _stage_distribution(
                snapshot.stage_status.value
                for item in bundles
                for snapshot in (
                    *((item.stock_stage,) if item.stock_stage is not None else ()),
                    *((item.sector_stage.stage_snapshot,) if item.sector_stage is not None else ()),
                )
            ),
        })
        return OpportunityShadowRunResult(counters["status"], config.dry_run, counters, {key: tuple(value) for key, value in rows.items()})


def _write_bundle(
    *, episode_request: OpenEpisodeRequest | None, candidate_id: str, setup_id: str,
    bundle: OpportunitySourceBundle, progress: ProgressSnapshot, days_without_progress: int,
    snapshot: Any, transition: Any, previous_state: CandidateState,
    lineage: SourceLineage, closure: EpisodeClosure | None,
) -> OrchestrationBundle:
    stages: list[StageObservation] = []
    if bundle.stock_stage:
        stages.append(StageObservation(candidate_id, setup_id, StageScope.STOCK, bundle.symbol_id, bundle.symbol_id, bundle.stock_stage, bundle.as_of, lineage))
    if bundle.sector_stage:
        stages.append(StageObservation(candidate_id, setup_id, StageScope.SECTOR, bundle.sector_stage.sector_id, bundle.sector_stage.sector_name, bundle.sector_stage, bundle.as_of, lineage))
    snapshot_observation = SnapshotObservation(snapshot, bundle.as_of, lineage) if snapshot is not None else None
    transition_observation = None
    if transition is not None and snapshot is not None:
        transition_observation = TransitionObservation(
            candidate_id, setup_id, previous_state, transition.proposed_state,
            transition.transition_reason.value, bundle.as_of, "pending", transition.rule_version,
            transition.metadata, lineage,
        )
    return OrchestrationBundle(
        candidate_id=candidate_id,
        episode_request=episode_request,
        opportunity=(OpportunityObservation(candidate_id, setup_id, bundle.as_of, bundle.as_of, bundle.opportunity, lineage) if bundle.opportunity else None),
        evidence=(EvidenceObservation(candidate_id, setup_id, bundle.as_of, bundle.as_of, "investigator", "investigator", "final_score", bundle.evidence, {"followthrough_status": bundle.followthrough_status.value}, lineage) if bundle.evidence else None),
        stages=tuple(stages),
        progress=ProgressObservation(candidate_id, setup_id, bundle.as_of, progress, days_without_progress, "opportunity-progress-v1", {}, lineage),
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


def _progress_from_current(bundle: OpportunitySourceBundle, current: Any) -> ProgressSnapshot:
    if current is None:
        return bundle.progress_hint or ProgressSnapshot(ProgressStatus.UNKNOWN, bundle.as_of, notes=("no prior registry state",))
    rank_signal = _direction(bundle.opportunity.rank_position, current.latest_rank_position, lower_is_better=True) if bundle.opportunity and current.latest_rank_position is not None else None
    evidence_signal = _direction(bundle.evidence.evidence_score, current.latest_evidence_score) if bundle.evidence and current.latest_evidence_score is not None else None
    hard = bool(bundle.stock_stage and bundle.stock_stage.effective_stage in {WeinsteinStage.TRANSITION_2_TO_3, WeinsteinStage.STAGE_3, WeinsteinStage.TRANSITION_3_TO_4, WeinsteinStage.STAGE_4})
    comparable = [item for item in (rank_signal, evidence_signal) if item is not None]
    if hard or sum(item is False for item in comparable) >= 2:
        status = ProgressStatus.DETERIORATING
    elif sum(item is True for item in comparable) >= 2:
        status = ProgressStatus.IMPROVING
    elif comparable:
        status = ProgressStatus.STABLE
    else:
        status = bundle.progress_hint.status if bundle.progress_hint else ProgressStatus.UNKNOWN
    return ProgressSnapshot(status, bundle.as_of, rank_velocity_improved=rank_signal, evidence_score_improved=evidence_signal, notes=(("hard structural deterioration",) if hard else ()))


def _direction(current: float, prior: float, *, lower_is_better: bool = False) -> bool | None:
    if current == prior:
        return None
    return current < prior if lower_is_better else current > prior


def _reconcile(results: Iterable[Any], raw_rank: list[dict[str, Any]], as_of: datetime) -> tuple[OpportunitySourceBundle, ...]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    sector_by_key = {
        (str(row.get("exchange") or "NSE").upper(), str(row.get("symbol_id") or row.get("symbol") or "").upper()): str(row.get("sector_name") or row.get("sector") or "unknown")
        for row in raw_rank
    }
    sector_records: dict[str, Any] = {}
    for result in results:
        for record in result.records:
            value = record.value
            if hasattr(value, "sector_name") and hasattr(value, "stage_snapshot"):
                sector_records[value.sector_name.strip().lower()] = value
                continue
            key = (record.exchange, record.symbol_id)
            item = by_key.setdefault(key, {"sources": [], "rows": [], "breakouts": [], "patterns": []})
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
        bundles.append(OpportunitySourceBundle(
            symbol_id=key[1], exchange=key[0], as_of=as_of,
            opportunity=item.get("opportunity"), evidence=item.get("evidence"),
            stock_stage=item.get("stock_stage"), sector_stage=sector,
            lifecycle_hint=item.get("lifecycle_hint"),
            followthrough_status=item.get("followthrough", FollowthroughStatus.UNKNOWN),
            progress_hint=item.get("progress_hint"), breakout_events=tuple(item["breakouts"]),
            pattern_events=tuple(item["patterns"]), source_lineage=tuple(sources[key] for key in sorted(sources)),
            source_row_identities=tuple(sorted(item["rows"])), sector_name=sector_name,
        ))
    return tuple(bundles)


def _read_csv(artifact: StageArtifact | None) -> list[dict[str, Any]]:
    if artifact is None:
        return []
    path = Path(artifact.uri)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _descriptor(artifact: StageArtifact, stage: str, artifact_type: str, run_id: str, attempt: int) -> SourceDescriptor:
    path = Path(artifact.uri)
    digest = artifact.content_hash or hashlib.sha256(path.read_bytes()).hexdigest()
    return SourceDescriptor(stage, artifact_type, str(path), digest, run_id, artifact.attempt_number or attempt, artifact.row_count or 0)


def _descriptor_optional(artifact: StageArtifact | None, stage: str, artifact_type: str, run_id: str, attempt: int) -> SourceDescriptor | None:
    return _descriptor(artifact, stage, artifact_type, run_id, attempt) if artifact else None


def _combined_lineage(bundle: OpportunitySourceBundle, run_id: str, attempt: int) -> SourceLineage:
    hashes = sorted(source.artifact_hash for source in bundle.source_lineage)
    digest = hashlib.sha256("|".join(hashes).encode()).hexdigest()
    paths = sorted(source.artifact_path for source in bundle.source_lineage)
    source_attempt = max((source.stage_attempt for source in bundle.source_lineage), default=attempt)
    return SourceLineage(run_id, "opportunities", source_attempt, "reconciled_bundle", "|".join(paths) or "reconciled:unknown", digest or hashlib.sha256(b"unknown").hexdigest())


def _enrich_stock_stage(rows: list[dict[str, Any]], db_path: Path, as_of: datetime) -> list[dict[str, Any]]:
    try:
        from ai_trading_system.domains.ranking.stage_store import read_latest_snapshot
        symbols = [str(row.get("symbol_id") or row.get("symbol") or "").upper() for row in rows]
        latest = read_latest_snapshot(db_path, symbols=symbols, asof=as_of.date().isoformat())
        stored = {str(row["symbol"]).upper(): row.to_dict() for _, row in latest.iterrows()}
        return [{**row, **stored.get(str(row.get("symbol_id") or row.get("symbol") or "").upper(), {})} for row in rows]
    except Exception:
        return rows


def _initial_counts(*args: Any) -> dict[str, Any]:
    rank, investigator, breakout, pattern, stock, sector, lifecycle, bundles = args
    return {
        "rank_rows_read": len(rank), "investigator_rows_read": len(investigator),
        "breakout_rows_read": len(breakout), "pattern_rows_read": len(pattern),
        "stock_stage_rows_read": len(stock), "sector_stage_rows_read": len(sector),
        "lifecycle_rows_read": len(lifecycle), "unique_symbols_seen": len(bundles),
        "source_bundles_assembled": len(bundles), "new_episodes_opened": 0,
        "existing_episodes_matched": 0, "snapshots_created": 0, "duplicate_snapshots": 0,
        "transitions_created": 0, "opportunity_observations_created": 0,
        "evidence_observations_created": 0, "stock_stage_observations_created": 0,
        "sector_stage_observations_created": 0,
        "progress_observations_created": 0, "episodes_retained": 0, "episodes_closed": 0,
        "episodes_archived": 0, "registry_duplicates": 0, "registry_conflicts": 0,
        "rejected_writes": 0, "not_admitted": 0,
    }


def _reconciliation_row(bundle: OpportunitySourceBundle, outcome: str, detail: str) -> dict[str, Any]:
    return {"exchange": bundle.exchange, "symbol_id": bundle.symbol_id, "outcome": outcome, "detail": detail, "as_of": bundle.as_of.isoformat()}


def _conflict(rows: dict[str, list[dict[str, Any]]], bundle: OpportunitySourceBundle, message: str, exc: OpportunityRegistryConflictError | None = None) -> None:
    rows["registry_conflicts"].append({
        "exchange": bundle.exchange, "symbol_id": bundle.symbol_id, "message": message,
        "record_type": exc.record_type if exc else "reconciliation",
        "idempotency_key": exc.idempotency_key if exc else "",
        "existing_payload_hash": exc.existing_payload_hash if exc else "",
        "incoming_payload_hash": exc.incoming_payload_hash if exc else "",
    })


def _dry_episode(request: OpenEpisodeRequest, candidate_id: str, setup_id: str):
    from ai_trading_system.domains.opportunities.registry.models import CandidateEpisodeRecord, REGISTRY_SCHEMA_VERSION
    return CandidateEpisodeRecord(
        candidate_id, setup_id, request.symbol_id, request.exchange, 0, request.episode_type,
        request.setup_family, request.admission_identity, request.episode_started_at, None,
        EpisodeStatus.OPEN, request.opening_reason, None, request.lineage.run_id,
        request.lineage.stage_name, request.lineage.source_artifact_hash, None, None,
        request.contract_version, REGISTRY_SCHEMA_VERSION, request.episode_started_at, request.episode_started_at,
    )
