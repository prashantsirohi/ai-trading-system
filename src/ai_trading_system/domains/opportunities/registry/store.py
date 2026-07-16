"""Transactional DuckDB persistence for canonical opportunity history."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Protocol, TypeVar

import duckdb

from ai_trading_system.domains.opportunities.contracts import (
    OPPORTUNITY_CONTRACT_VERSION,
    TransitionReason,
)
from ai_trading_system.domains.opportunities.serialization import to_dict
from ai_trading_system.pipeline.registry import RegistryStore

from .identity import (
    canonical_json,
    make_candidate_id,
    make_record_identity,
    make_setup_id,
    normalize_exchange,
    normalize_setup_family,
    normalize_symbol,
    require_aware,
    stable_digest,
)
from .models import (
    AppendResult,
    AppendStatus,
    AttributionObservation,
    BatchAppendResult,
    CandidateCurrentState,
    CandidateEpisodeRecord,
    CandidateTimeline,
    DecisionContextObservation,
    EpisodeStatus,
    EvidenceObservation,
    OpenEpisodeRequest,
    OrchestrationBundle,
    OrchestrationBundleResult,
    OpportunityObservation,
    OpportunityRegistryConflictError,
    ProgressObservation,
    REGISTRY_SCHEMA_VERSION,
    REGISTRY_SERIALIZATION_VERSION,
    SnapshotObservation,
    SourceLineage,
    StageObservation,
    StageScope,
    TimelineEntry,
    TransitionObservation,
)
from .schema import verify_schema


T = TypeVar("T")


class OpportunityRegistryStore(Protocol):
    def initialize_schema(self) -> None: ...
    def open_episode(self, request: OpenEpisodeRequest) -> CandidateEpisodeRecord: ...
    def get_episode(self, candidate_id: str) -> CandidateEpisodeRecord | None: ...
    def append_snapshot(self, observation: SnapshotObservation) -> AppendResult: ...
    def append_stage_observation(self, observation: StageObservation) -> AppendResult: ...
    def append_evidence_observation(self, observation: EvidenceObservation) -> AppendResult: ...
    def append_opportunity_observation(self, observation: OpportunityObservation) -> AppendResult: ...
    def append_progress(self, observation: ProgressObservation) -> AppendResult: ...
    def append_transition(self, observation: TransitionObservation) -> AppendResult: ...
    def append_decision_context(self, observation: DecisionContextObservation) -> AppendResult: ...
    def append_attribution(self, observation: AttributionObservation) -> AppendResult: ...
    def close_episode(
        self, candidate_id: str, *, status: EpisodeStatus, closed_at: datetime,
        closing_reason: str, lineage: SourceLineage,
    ) -> CandidateEpisodeRecord: ...
    def current_state(self, candidate_id: str) -> CandidateCurrentState | None: ...
    def state_as_of(self, candidate_id: str, as_of: datetime) -> CandidateCurrentState | None: ...
    def timeline(self, candidate_id: str) -> CandidateTimeline: ...
    def list_open_candidates(self) -> tuple[CandidateCurrentState, ...]: ...
    def list_open_episodes(self) -> tuple[CandidateEpisodeRecord, ...]: ...
    def append_orchestration_bundle(self, bundle: OrchestrationBundle) -> OrchestrationBundleResult: ...
    def observation_hashes_for_run(self, run_id: str) -> dict[str, tuple[str, ...]]: ...


def _db_time(value: datetime) -> datetime:
    require_aware(value, "datetime")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _enum(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _row_dict(cursor: duckdb.DuckDBPyConnection, row: tuple[Any, ...] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {item[0]: value for item, value in zip(cursor.description, row, strict=True)}


def _require_text(value: str, field_name: str) -> None:
    if not str(value or "").strip():
        raise ValueError(f"{field_name} must be non-empty")


class DuckDBOpportunityRegistryStore:
    """Opportunity repository sharing the control-plane store's transactions."""

    def __init__(self, registry: RegistryStore):
        self.registry = registry
        self.initialize_schema()

    def initialize_schema(self) -> None:
        self.registry._ensure_initialized()  # noqa: SLF001
        with self.registry._reader() as conn:  # noqa: SLF001
            verify_schema(conn)

    @contextmanager
    def _transaction(self):
        """Open an explicit transaction inside the shared writer context."""
        with self.registry._writer() as conn:  # noqa: SLF001
            conn.execute("BEGIN TRANSACTION")
            yield conn

    @staticmethod
    def _episode_from_row(row: dict[str, Any]) -> CandidateEpisodeRecord:
        return CandidateEpisodeRecord(
            candidate_id=row["candidate_id"], setup_id=row["setup_id"], symbol_id=row["symbol_id"],
            exchange=row["exchange"], episode_number=row["episode_number"], episode_type=row["episode_type"],
            setup_family=row["setup_family"], admission_identity=row["admission_identity"],
            episode_started_at=_aware(row["episode_started_at"]), episode_closed_at=_aware(row["episode_closed_at"]),
            episode_status=EpisodeStatus(row["episode_status"]), opening_reason=row["opening_reason"],
            closing_reason=row["closing_reason"], created_run_id=row["created_run_id"],
            created_stage=row["created_stage"], created_artifact_hash=row["created_artifact_hash"],
            closed_run_id=row["closed_run_id"], closed_stage=row["closed_stage"],
            contract_version=row["contract_version"], schema_version=row["schema_version"],
            created_at=_aware(row["created_at"]), updated_at=_aware(row["updated_at"]),
            policy_snapshot_id=row.get("policy_snapshot_id"),
            closed_policy_snapshot_id=row.get("closed_policy_snapshot_id"),
        )

    def _get_episode(self, conn: duckdb.DuckDBPyConnection, candidate_id: str) -> CandidateEpisodeRecord | None:
        cursor = conn.execute("SELECT * FROM candidate_episode WHERE candidate_id = ?", [candidate_id])
        row = _row_dict(cursor, cursor.fetchone())
        return self._episode_from_row(row) if row else None

    def get_episode(self, candidate_id: str) -> CandidateEpisodeRecord | None:
        with self.registry._reader() as conn:  # noqa: SLF001
            return self._get_episode(conn, candidate_id)

    def get_episode_by_setup(self, setup_id: str) -> CandidateEpisodeRecord | None:
        with self.registry._reader() as conn:  # noqa: SLF001
            cursor = conn.execute("SELECT * FROM candidate_episode WHERE setup_id = ?", [setup_id])
            row = _row_dict(cursor, cursor.fetchone())
            return self._episode_from_row(row) if row else None

    def find_open_episode(
        self, *, exchange: str, symbol_id: str, setup_family: str | None = None
    ) -> CandidateEpisodeRecord | None:
        conditions = ["exchange = ?", "symbol_id = ?", "episode_status = 'OPEN'"]
        params: list[Any] = [normalize_exchange(exchange), normalize_symbol(symbol_id)]
        if setup_family is not None:
            conditions.append("setup_family = ?")
            params.append(normalize_setup_family(setup_family))
        with self.registry._reader() as conn:  # noqa: SLF001
            cursor = conn.execute(
                f"SELECT * FROM candidate_episode WHERE {' AND '.join(conditions)} "  # noqa: S608
                "ORDER BY episode_started_at DESC, episode_number DESC LIMIT 1",
                params,
            )
            row = _row_dict(cursor, cursor.fetchone())
            return self._episode_from_row(row) if row else None

    def list_episodes(self, *, exchange: str, symbol_id: str) -> tuple[CandidateEpisodeRecord, ...]:
        with self.registry._reader() as conn:  # noqa: SLF001
            cursor = conn.execute(
                "SELECT * FROM candidate_episode WHERE exchange = ? AND symbol_id = ? "
                "ORDER BY episode_number, candidate_id",
                [normalize_exchange(exchange), normalize_symbol(symbol_id)],
            )
            names = [item[0] for item in cursor.description]
            return tuple(self._episode_from_row(dict(zip(names, row, strict=True))) for row in cursor.fetchall())

    def latest_episode(self, *, exchange: str, symbol_id: str) -> CandidateEpisodeRecord | None:
        episodes = self.list_episodes(exchange=exchange, symbol_id=symbol_id)
        return episodes[-1] if episodes else None

    def list_open_candidates(self) -> tuple[CandidateCurrentState, ...]:
        with self.registry._reader() as conn:  # noqa: SLF001
            cursor = conn.execute(
                "SELECT * FROM candidate_current_state WHERE episode_status = 'OPEN' "
                "ORDER BY exchange, symbol_id, episode_started_at, candidate_id"
            )
            names = [item[0] for item in cursor.description]
            return tuple(self._current_from_row(dict(zip(names, row, strict=True))) for row in cursor.fetchall())

    def list_open_episodes(self) -> tuple[CandidateEpisodeRecord, ...]:
        with self.registry._reader() as conn:  # noqa: SLF001
            cursor = conn.execute(
                "SELECT * FROM candidate_episode WHERE episode_status = 'OPEN' "
                "ORDER BY exchange, symbol_id, episode_started_at, candidate_id"
            )
            names = [item[0] for item in cursor.description]
            return tuple(self._episode_from_row(dict(zip(names, row, strict=True))) for row in cursor.fetchall())

    def observation_hashes_for_run(self, run_id: str) -> dict[str, tuple[str, ...]]:
        """Return persisted semantic source hashes for exact-run replay checks."""
        _require_text(run_id, "run_id")
        tables = (
            "candidate_snapshot", "candidate_stage_observation", "candidate_evidence_observation",
            "candidate_opportunity_observation", "candidate_transition", "candidate_progress_observation",
            "candidate_decision_context", "candidate_outcome_attribution",
        )
        query = " UNION ALL ".join(
            f"SELECT candidate_id, source_artifact_hash FROM {table} WHERE run_id = ?"  # noqa: S608
            for table in tables
        )
        with self.registry._reader() as conn:  # noqa: SLF001
            rows = conn.execute(query, [run_id] * len(tables)).fetchall()
        grouped: dict[str, set[str]] = {}
        for candidate_id, source_hash in rows:
            grouped.setdefault(str(candidate_id), set()).add(str(source_hash))
        return {candidate_id: tuple(sorted(values)) for candidate_id, values in grouped.items()}

    def query_current_states(
        self,
        *,
        episode_status: EpisodeStatus | None = None,
        lifecycle_state: str | None = None,
        stock_stage: str | None = None,
        sector_stage: str | None = None,
        progress_status: str | None = None,
        followthrough_status: str | None = None,
        minimum_opportunity_score: float | None = None,
        minimum_evidence_score: float | None = None,
        as_of: datetime | None = None,
    ) -> tuple[CandidateCurrentState, ...]:
        if as_of is not None:
            require_aware(as_of, "as_of")
            with self.registry._reader() as conn:  # noqa: SLF001
                ids = [row[0] for row in conn.execute(
                    "SELECT candidate_id FROM candidate_episode WHERE episode_started_at <= ? "
                    "ORDER BY exchange, symbol_id, episode_number",
                    [_db_time(as_of)],
                ).fetchall()]
            states = tuple(state for candidate_id in ids if (state := self.state_as_of(candidate_id, as_of)))
        else:
            with self.registry._reader() as conn:  # noqa: SLF001
                cursor = conn.execute("SELECT * FROM candidate_current_state ORDER BY exchange, symbol_id, candidate_id")
                names = [item[0] for item in cursor.description]
                states = tuple(self._current_from_row(dict(zip(names, row, strict=True))) for row in cursor.fetchall())
        return tuple(
            state for state in states
            if (episode_status is None or state.episode_status is episode_status)
            and (lifecycle_state is None or state.current_lifecycle_state == _enum(lifecycle_state))
            and (stock_stage is None or state.current_stock_stage == _enum(stock_stage))
            and (sector_stage is None or state.current_sector_stage == _enum(sector_stage))
            and (progress_status is None or state.current_progress_status == _enum(progress_status))
            and (followthrough_status is None or state.current_followthrough_status == _enum(followthrough_status))
            and (minimum_opportunity_score is None or (
                state.latest_opportunity_score is not None and state.latest_opportunity_score >= minimum_opportunity_score
            ))
            and (minimum_evidence_score is None or (
                state.latest_evidence_score is not None and state.latest_evidence_score >= minimum_evidence_score
            ))
        )

    def open_episode(self, request: OpenEpisodeRequest) -> CandidateEpisodeRecord:
        with self._transaction() as conn:
            return self._open_episode(conn, request)

    def _open_episode(
        self, conn: duckdb.DuckDBPyConnection, request: OpenEpisodeRequest
    ) -> CandidateEpisodeRecord:
        require_aware(request.episode_started_at, "episode_started_at")
        exchange = normalize_exchange(request.exchange)
        symbol = normalize_symbol(request.symbol_id)
        family = normalize_setup_family(request.setup_family)
        for name in ("admission_identity", "episode_type", "opening_reason", "contract_version"):
            _require_text(getattr(request, name), name)
        setup_id = make_setup_id(
            exchange=exchange, symbol_id=symbol, setup_family=family,
            admission_identity=request.admission_identity, episode_started_at=request.episode_started_at,
        )
        candidate_id = make_candidate_id(setup_id)
        existing = self._get_episode(conn, candidate_id)
        if existing is not None:
            return existing
        episode_number = int(
            conn.execute(
                "SELECT COALESCE(MAX(episode_number), 0) + 1 FROM candidate_episode "
                "WHERE exchange = ? AND symbol_id = ?",
                [exchange, symbol],
            ).fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO candidate_episode (
                candidate_id, setup_id, symbol_id, exchange, episode_number, episode_type,
                setup_family, admission_identity, episode_started_at, episode_status,
                opening_reason, created_run_id, created_stage, created_artifact_hash,
                contract_version, schema_version, policy_snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?)
            """,
            [candidate_id, setup_id, symbol, exchange, episode_number, request.episode_type,
             family, request.admission_identity.strip(), _db_time(request.episode_started_at),
             request.opening_reason, request.lineage.run_id, request.lineage.stage_name,
             request.lineage.source_artifact_hash, request.contract_version, REGISTRY_SCHEMA_VERSION,
             request.lineage.policy_snapshot_id],
        )
        created = self._get_episode(conn, candidate_id)
        assert created is not None
        return created

    def open_episode_with_initial_snapshot(
        self, request: OpenEpisodeRequest, observation: SnapshotObservation
    ) -> tuple[CandidateEpisodeRecord, AppendResult]:
        """Atomically open an episode and persist its caller-built first snapshot."""
        with self._transaction() as conn:
            episode = self._open_episode(conn, request)
            self._validate_episode_identity(
                episode, observation.snapshot.candidate_id, observation.snapshot.setup_id
            )
            return episode, self._append_snapshot(conn, observation)

    def close_episode(
        self,
        candidate_id: str,
        *,
        status: EpisodeStatus,
        closed_at: datetime,
        closing_reason: str,
        lineage: SourceLineage,
    ) -> CandidateEpisodeRecord:
        require_aware(closed_at, "closed_at")
        if status is EpisodeStatus.OPEN:
            raise ValueError("close status must be terminal")
        _require_text(closing_reason, "closing_reason")
        with self._transaction() as conn:
            return self._close_episode(conn, candidate_id, status=status, closed_at=closed_at,
                                       closing_reason=closing_reason, lineage=lineage)

    def _close_episode(
        self, conn: duckdb.DuckDBPyConnection, candidate_id: str, *, status: EpisodeStatus,
        closed_at: datetime, closing_reason: str, lineage: SourceLineage,
    ) -> CandidateEpisodeRecord:
        require_aware(closed_at, "closed_at")
        if status is EpisodeStatus.OPEN:
            raise ValueError("close status must be terminal")
        _require_text(closing_reason, "closing_reason")
        episode = self._require_open_or_closed(conn, candidate_id, allow_closed=True)
        requested = (status, _db_time(closed_at), closing_reason, lineage.run_id, lineage.stage_name)
        if episode.episode_status is not EpisodeStatus.OPEN:
            existing = (
                episode.episode_status, _db_time(episode.episode_closed_at), episode.closing_reason,
                episode.closed_run_id, episode.closed_stage,
            )
            if existing == requested:
                return episode
            raise OpportunityRegistryConflictError(
                record_type="candidate_episode_close", candidate_id=candidate_id,
                idempotency_key=stable_digest(requested), existing_payload_hash=stable_digest(existing),
                incoming_payload_hash=stable_digest(requested),
            )
        if closed_at < episode.episode_started_at:
            raise ValueError("episode close cannot precede episode start")
        conn.execute(
            """
            UPDATE candidate_episode SET episode_closed_at = ?, episode_status = ?, closing_reason = ?,
                closed_run_id = ?, closed_stage = ?, closed_policy_snapshot_id = ?,
                updated_at = (current_timestamp AT TIME ZONE 'UTC')
            WHERE candidate_id = ?
            """,
            [_db_time(closed_at), status.value, closing_reason, lineage.run_id, lineage.stage_name,
             lineage.policy_snapshot_id, candidate_id],
        )
        result = self._get_episode(conn, candidate_id)
        assert result is not None
        return result

    def _require_open_or_closed(
        self, conn: duckdb.DuckDBPyConnection, candidate_id: str, *, allow_closed: bool = False
    ) -> CandidateEpisodeRecord:
        episode = self._get_episode(conn, candidate_id)
        if episode is None:
            raise KeyError(f"unknown candidate_id: {candidate_id}")
        if not allow_closed and episode.episode_status is not EpisodeStatus.OPEN:
            raise ValueError(f"candidate episode is closed: {candidate_id}")
        return episode

    @staticmethod
    def _validate_episode_identity(episode: CandidateEpisodeRecord, candidate_id: str, setup_id: str) -> None:
        if episode.candidate_id != candidate_id or episode.setup_id != setup_id:
            raise ValueError("record identity does not match candidate episode")

    def _insert_append(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        table: str,
        id_column: str,
        record_id: str,
        candidate_id: str,
        idempotency_key: str,
        semantic_hash: str,
        columns: tuple[str, ...],
        values: list[Any],
    ) -> AppendResult:
        existing = conn.execute(
            f"SELECT {id_column}, semantic_payload_hash FROM {table} WHERE idempotency_key = ?",  # noqa: S608
            [idempotency_key],
        ).fetchone()
        if existing is not None:
            if existing[1] == semantic_hash:
                return AppendResult(existing[0], AppendStatus.DUPLICATE, idempotency_key, True, False)
            raise OpportunityRegistryConflictError(
                record_type=table, candidate_id=candidate_id, idempotency_key=idempotency_key,
                existing_payload_hash=existing[1], incoming_payload_hash=semantic_hash,
            )
        placeholders = ", ".join("?" for _ in columns)
        conn.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",  # noqa: S608
            values,
        )
        return AppendResult(record_id, AppendStatus.CREATED, idempotency_key, False, True)

    def _identity(
        self, *, candidate_id: str, record_type: str, as_of: datetime, lineage: SourceLineage,
        contract_version: str, payload: Any
    ) -> tuple[str, str, str]:
        return make_record_identity(
            candidate_id=candidate_id, record_type=record_type, as_of=as_of, run_id=lineage.run_id,
            stage_attempt=lineage.stage_attempt, source_artifact_hash=lineage.source_artifact_hash,
            contract_version=contract_version, semantic_payload=payload,
        )

    def append_snapshot(self, observation: SnapshotObservation) -> AppendResult:
        with self._transaction() as conn:
            return self._append_snapshot(conn, observation)

    def _append_snapshot(self, conn: duckdb.DuckDBPyConnection, observation: SnapshotObservation) -> AppendResult:
        snapshot = observation.snapshot
        episode = self._require_open_or_closed(conn, snapshot.candidate_id)
        self._validate_episode_identity(episode, snapshot.candidate_id, snapshot.setup_id)
        require_aware(observation.observed_at, "observed_at")
        if snapshot.as_of < episode.episode_started_at:
            raise ValueError("snapshot cannot precede episode start")
        for observation_id, scope in (
            (observation.stock_stage_observation_id, "STOCK"),
            (observation.sector_stage_observation_id, "SECTOR"),
        ):
            if observation_id is not None:
                row = conn.execute(
                    "SELECT candidate_id, scope FROM candidate_stage_observation WHERE stage_observation_id = ?",
                    [observation_id],
                ).fetchone()
                if row is None or row != (snapshot.candidate_id, scope):
                    raise ValueError(f"linked {scope.lower()} stage observation does not belong to candidate")
        payload = to_dict(snapshot)
        record_id, key, semantic_hash = self._identity(
            candidate_id=snapshot.candidate_id, record_type="snapshot", as_of=snapshot.as_of,
            lineage=observation.lineage, contract_version=snapshot.contract_version, payload=payload,
        )
        columns = (
            "snapshot_id", "candidate_id", "setup_id", "as_of", "observed_at", "run_id", "stage_name",
            "stage_attempt", "source_artifact_type", "source_artifact_path", "source_artifact_hash",
            "lifecycle_state", "followthrough_status", "opportunity_score", "rank_position", "rank_percentile",
            "rank_velocity", "evidence_score", "evidence_verdict", "days_in_state", "days_without_progress",
            "progress_status", "active_position", "latest_action", "eligibility", "stock_stage_observation_id",
            "sector_stage_observation_id", "contract_version", "serialization_version", "snapshot_json",
            "semantic_payload_hash", "idempotency_key",
        )
        values = [
            record_id, snapshot.candidate_id, snapshot.setup_id, _db_time(snapshot.as_of),
            _db_time(observation.observed_at), observation.lineage.run_id, observation.lineage.stage_name,
            observation.lineage.stage_attempt, observation.lineage.source_artifact_type,
            observation.lineage.source_artifact_path, observation.lineage.source_artifact_hash,
            snapshot.lifecycle_state.value, snapshot.followthrough_status.value, snapshot.opportunity.opportunity_score,
            snapshot.opportunity.rank_position, snapshot.opportunity.rank_percentile, snapshot.opportunity.rank_velocity,
            snapshot.evidence.evidence_score, snapshot.evidence.investigator_verdict.value, snapshot.days_in_state,
            snapshot.days_without_progress, None, snapshot.active_position, snapshot.latest_action.value,
            snapshot.eligibility.value, observation.stock_stage_observation_id, observation.sector_stage_observation_id,
            snapshot.contract_version, REGISTRY_SERIALIZATION_VERSION, canonical_json(snapshot), semantic_hash, key,
        ]
        return self._insert_append(conn, table="candidate_snapshot", id_column="snapshot_id", record_id=record_id,
                                   candidate_id=snapshot.candidate_id, idempotency_key=key,
                                   semantic_hash=semantic_hash, columns=columns, values=values)

    def append_stage_observation(self, observation: StageObservation) -> AppendResult:
        with self._transaction() as conn:
            return self._append_stage(conn, observation)

    def _append_stage(self, conn: duckdb.DuckDBPyConnection, observation: StageObservation) -> AppendResult:
        episode = self._require_open_or_closed(conn, observation.candidate_id)
        self._validate_episode_identity(episode, observation.candidate_id, observation.setup_id)
        require_aware(observation.observed_at, "observed_at")
        if observation.scope is StageScope.SECTOR:
            if not hasattr(observation.snapshot, "stage_snapshot"):
                raise ValueError("sector scope requires SectorStageSnapshot")
            stage = observation.snapshot.stage_snapshot
        else:
            if hasattr(observation.snapshot, "stage_snapshot"):
                raise ValueError("stock scope requires StageSnapshot")
            stage = observation.snapshot
        if stage.stage_as_of < episode.episode_started_at:
            raise ValueError("stage observation cannot precede episode start")
        payload = to_dict(observation.snapshot)
        record_id, key, semantic_hash = self._identity(
            candidate_id=observation.candidate_id,
            record_type=f"stage_{observation.scope.value.lower()}", as_of=stage.stage_as_of,
            lineage=observation.lineage, contract_version=stage.contract_version, payload={"scope": observation.scope.value, "snapshot": payload},
        )
        columns = (
            "stage_observation_id", "candidate_id", "setup_id", "scope", "entity_id", "entity_name", "as_of",
            "observed_at", "provisional_stage", "locked_stage", "effective_stage", "stage_status", "confidence_score",
            "confidence_band", "confidence_components_json", "stage_locked_at", "source_week_start", "source_week_end",
            "previous_locked_stage", "weeks_in_locked_stage", "provisional_persistence_days", "transition_reason",
            "classifier_version", "confidence_formula_version", "contract_version", "run_id", "stage_name",
            "stage_attempt", "source_artifact_type", "source_artifact_path", "source_artifact_hash", "observation_json",
            "semantic_payload_hash", "idempotency_key",
        )
        values = [
            record_id, observation.candidate_id, observation.setup_id, observation.scope.value, observation.entity_id,
            observation.entity_name, _db_time(stage.stage_as_of), _db_time(observation.observed_at),
            stage.provisional_stage.value, stage.locked_stage.value, stage.effective_stage.value, stage.stage_status.value,
            stage.confidence_score, stage.confidence_band.value, canonical_json(stage.confidence_components),
            _db_time(stage.stage_locked_at) if stage.stage_locked_at else None, stage.source_week_start, stage.source_week_end,
            stage.previous_locked_stage.value if stage.previous_locked_stage else None, stage.weeks_in_locked_stage,
            stage.provisional_persistence_days, stage.transition_reason.value, stage.classifier_version,
            stage.confidence_formula_version, stage.contract_version, observation.lineage.run_id,
            observation.lineage.stage_name, observation.lineage.stage_attempt, observation.lineage.source_artifact_type,
            observation.lineage.source_artifact_path, observation.lineage.source_artifact_hash,
            canonical_json(observation.snapshot), semantic_hash, key,
        ]
        return self._insert_append(conn, table="candidate_stage_observation", id_column="stage_observation_id",
                                   record_id=record_id, candidate_id=observation.candidate_id, idempotency_key=key,
                                   semantic_hash=semantic_hash, columns=columns, values=values)

    def append_evidence_observation(self, observation: EvidenceObservation) -> AppendResult:
        with self._transaction() as conn:
            return self._append_evidence(conn, observation)

    def _append_evidence(self, conn: duckdb.DuckDBPyConnection, observation: EvidenceObservation) -> AppendResult:
        episode = self._require_open_or_closed(conn, observation.candidate_id)
        self._validate_episode_identity(episode, observation.candidate_id, observation.setup_id)
        require_aware(observation.as_of, "as_of")
        require_aware(observation.observed_at, "observed_at")
        for name in ("evidence_type", "source_module", "source_component"):
            _require_text(getattr(observation, name), name)
        if observation.as_of < episode.episode_started_at:
            raise ValueError("evidence observation cannot precede episode start")
        payload = {"snapshot": to_dict(observation.snapshot), "details": observation.details,
                   "evidence_type": observation.evidence_type, "source_module": observation.source_module,
                   "source_component": observation.source_component}
        record_id, key, semantic_hash = self._identity(
            candidate_id=observation.candidate_id, record_type="evidence", as_of=observation.as_of,
            lineage=observation.lineage, contract_version=observation.snapshot.contract_version, payload=payload,
        )
        s = observation.snapshot
        columns = (
            "evidence_observation_id", "candidate_id", "setup_id", "as_of", "observed_at", "evidence_type",
            "source_module", "source_component", "score", "verdict", "positive_evidence_json", "negative_evidence_json",
            "missing_evidence_json", "details_json", "evidence_model_version", "contract_version", "run_id", "stage_name",
            "stage_attempt", "source_artifact_type", "source_artifact_path", "source_artifact_hash", "observation_json",
            "semantic_payload_hash", "idempotency_key",
        )
        values = [record_id, observation.candidate_id, observation.setup_id, _db_time(observation.as_of),
                  _db_time(observation.observed_at), observation.evidence_type, observation.source_module,
                  observation.source_component, s.evidence_score, s.investigator_verdict.value,
                  canonical_json(s.positive_evidence), canonical_json(s.negative_evidence), canonical_json(s.missing_evidence),
                  canonical_json(observation.details), s.evidence_model_version, s.contract_version, observation.lineage.run_id,
                  observation.lineage.stage_name, observation.lineage.stage_attempt, observation.lineage.source_artifact_type,
                  observation.lineage.source_artifact_path, observation.lineage.source_artifact_hash,
                  canonical_json(s), semantic_hash, key]
        return self._insert_append(conn, table="candidate_evidence_observation", id_column="evidence_observation_id",
                                   record_id=record_id, candidate_id=observation.candidate_id, idempotency_key=key,
                                   semantic_hash=semantic_hash, columns=columns, values=values)

    def append_opportunity_observation(self, observation: OpportunityObservation) -> AppendResult:
        with self._transaction() as conn:
            return self._append_opportunity(conn, observation)

    def _append_opportunity(self, conn: duckdb.DuckDBPyConnection, observation: OpportunityObservation) -> AppendResult:
        episode = self._require_open_or_closed(conn, observation.candidate_id)
        self._validate_episode_identity(episode, observation.candidate_id, observation.setup_id)
        require_aware(observation.as_of, "as_of")
        require_aware(observation.observed_at, "observed_at")
        if observation.as_of < episode.episode_started_at:
            raise ValueError("opportunity observation cannot precede episode start")
        s = observation.snapshot
        payload = to_dict(s)
        record_id, key, semantic_hash = self._identity(
            candidate_id=observation.candidate_id, record_type="opportunity", as_of=observation.as_of,
            lineage=observation.lineage, contract_version=s.contract_version, payload=payload,
        )
        columns = ("opportunity_observation_id", "candidate_id", "setup_id", "as_of", "observed_at",
                   "opportunity_score", "rank_position", "rank_percentile", "rank_velocity", "rank_velocity_state",
                   "factor_scores_json", "rank_model_version", "contract_version", "run_id", "stage_name", "stage_attempt",
                   "source_artifact_type", "source_artifact_path", "source_artifact_hash", "observation_json",
                   "semantic_payload_hash", "idempotency_key")
        values = [record_id, observation.candidate_id, observation.setup_id, _db_time(observation.as_of),
                  _db_time(observation.observed_at), s.opportunity_score, s.rank_position, s.rank_percentile,
                  s.rank_velocity, s.rank_velocity_state.value, canonical_json(s.factor_scores), s.rank_model_version,
                  s.contract_version, observation.lineage.run_id, observation.lineage.stage_name,
                  observation.lineage.stage_attempt, observation.lineage.source_artifact_type,
                  observation.lineage.source_artifact_path, observation.lineage.source_artifact_hash,
                  canonical_json(s), semantic_hash, key]
        return self._insert_append(conn, table="candidate_opportunity_observation", id_column="opportunity_observation_id",
                                   record_id=record_id, candidate_id=observation.candidate_id, idempotency_key=key,
                                   semantic_hash=semantic_hash, columns=columns, values=values)

    def append_progress(self, observation: ProgressObservation) -> AppendResult:
        with self._transaction() as conn:
            return self._append_progress(conn, observation)

    def _append_progress(self, conn: duckdb.DuckDBPyConnection, observation: ProgressObservation) -> AppendResult:
        episode = self._require_open_or_closed(conn, observation.candidate_id)
        self._validate_episode_identity(episode, observation.candidate_id, observation.setup_id)
        require_aware(observation.as_of, "as_of")
        if observation.days_without_progress < 0:
            raise ValueError("days_without_progress must be non-negative")
        if observation.as_of < episode.episode_started_at:
            raise ValueError("progress observation cannot precede episode start")
        s = observation.snapshot
        payload = {"snapshot": to_dict(s), "days_without_progress": observation.days_without_progress,
                   "rule_version": observation.rule_version, "details": observation.details}
        record_id, key, semantic_hash = self._identity(
            candidate_id=observation.candidate_id, record_type="progress", as_of=observation.as_of,
            lineage=observation.lineage, contract_version=OPPORTUNITY_CONTRACT_VERSION, payload=payload,
        )
        columns = ("progress_observation_id", "candidate_id", "setup_id", "as_of", "observed_at", "progress_status",
                   "rank_velocity_improved", "evidence_score_improved", "base_contraction_improved", "volume_dry_up_improved",
                   "weekly_ma_slope_improved", "distance_to_pivot_narrowed", "relative_strength_improved",
                   "sector_alignment_improved", "days_without_progress", "details_json", "rule_version", "run_id",
                   "stage_name", "stage_attempt", "source_artifact_hash", "observation_json", "semantic_payload_hash",
                   "idempotency_key")
        values = [record_id, observation.candidate_id, observation.setup_id, _db_time(observation.as_of),
                  _db_time(s.observed_at), s.status.value, s.rank_velocity_improved, s.evidence_score_improved,
                  s.base_contraction_improved, s.volume_dry_up_improved, s.weekly_ma_slope_improved,
                  s.distance_to_pivot_narrowed, s.relative_strength_improved, s.sector_alignment_improved,
                  observation.days_without_progress, canonical_json(observation.details), observation.rule_version,
                  observation.lineage.run_id, observation.lineage.stage_name, observation.lineage.stage_attempt,
                  observation.lineage.source_artifact_hash, canonical_json(s), semantic_hash, key]
        return self._insert_append(conn, table="candidate_progress_observation", id_column="progress_observation_id",
                                   record_id=record_id, candidate_id=observation.candidate_id, idempotency_key=key,
                                   semantic_hash=semantic_hash, columns=columns, values=values)

    def append_transition(self, observation: TransitionObservation) -> AppendResult:
        with self._transaction() as conn:
            return self._append_transition(conn, observation)

    def _append_transition(self, conn: duckdb.DuckDBPyConnection, observation: TransitionObservation) -> AppendResult:
        episode = self._require_open_or_closed(conn, observation.candidate_id)
        self._validate_episode_identity(episode, observation.candidate_id, observation.setup_id)
        require_aware(observation.transitioned_at, "transitioned_at")
        if observation.from_state is observation.to_state:
            raise ValueError("transition from_state and to_state must differ")
        reason = TransitionReason(_enum(observation.transition_reason)).value
        if observation.transitioned_at < episode.episode_started_at:
            raise ValueError("transition cannot precede episode start")
        snapshot = conn.execute(
            "SELECT candidate_id, as_of FROM candidate_snapshot WHERE snapshot_id = ?",
            [observation.triggering_snapshot_id],
        ).fetchone()
        if snapshot is None or snapshot[0] != observation.candidate_id:
            raise ValueError("triggering snapshot must belong to the candidate")
        latest = conn.execute(
            "SELECT to_state, transitioned_at FROM candidate_transition WHERE candidate_id = ? "
            "ORDER BY transitioned_at DESC, created_at DESC, transition_id DESC LIMIT 1",
            [observation.candidate_id],
        ).fetchone()
        if latest is not None:
            if _db_time(observation.transitioned_at) < latest[1]:
                raise ValueError("transition chronology must be non-decreasing")
            if latest[0] != observation.from_state.value:
                raise ValueError("transition from_state does not match latest persisted state")
        payload = {"from_state": observation.from_state.value, "to_state": observation.to_state.value,
                   "transition_reason": reason, "transitioned_at": observation.transitioned_at,
                   "triggering_snapshot_id": observation.triggering_snapshot_id,
                   "rule_version": observation.rule_version, "metadata": observation.metadata}
        record_id, key, semantic_hash = self._identity(
            candidate_id=observation.candidate_id, record_type="transition", as_of=observation.transitioned_at,
            lineage=observation.lineage, contract_version=OPPORTUNITY_CONTRACT_VERSION, payload=payload,
        )
        columns = ("transition_id", "candidate_id", "setup_id", "from_state", "to_state", "transition_reason",
                   "transitioned_at", "triggering_snapshot_id", "rule_version", "metadata_json", "run_id", "stage_name",
                   "stage_attempt", "source_artifact_hash", "policy_snapshot_id", "semantic_payload_hash",
                   "idempotency_key")
        values = [record_id, observation.candidate_id, observation.setup_id, observation.from_state.value,
                  observation.to_state.value, reason, _db_time(observation.transitioned_at),
                  observation.triggering_snapshot_id, observation.rule_version, canonical_json(observation.metadata),
                  observation.lineage.run_id, observation.lineage.stage_name, observation.lineage.stage_attempt,
                  observation.lineage.source_artifact_hash, observation.lineage.policy_snapshot_id,
                  semantic_hash, key]
        return self._insert_append(conn, table="candidate_transition", id_column="transition_id", record_id=record_id,
                                   candidate_id=observation.candidate_id, idempotency_key=key,
                                   semantic_hash=semantic_hash, columns=columns, values=values)

    def append_decision_context(self, observation: DecisionContextObservation) -> AppendResult:
        with self._transaction() as conn:
            return self._append_decision(conn, observation)

    def _append_decision(self, conn: duckdb.DuckDBPyConnection, observation: DecisionContextObservation) -> AppendResult:
        d, c = observation.decision, observation.context
        episode = self._require_open_or_closed(conn, d.candidate_id)
        self._validate_episode_identity(episode, d.candidate_id, d.setup_id)
        if d.decided_at < episode.episode_started_at:
            raise ValueError("decision cannot precede episode start")
        payload = {"decision": to_dict(d), "context": to_dict(c)}
        record_id, key, semantic_hash = self._identity(
            candidate_id=d.candidate_id, record_type="decision", as_of=d.decided_at, lineage=observation.lineage,
            contract_version=c.contract_version, payload=payload,
        )
        columns = ("decision_context_id", "candidate_id", "setup_id", "decided_at", "action", "eligibility",
                   "decision_confidence", "size_multiplier", "decision_stage", "decision_stage_status",
                   "decision_stage_as_of", "decision_locked_stage", "decision_provisional_stage",
                   "decision_stage_confidence", "decision_sector_stage", "decision_sector_stage_status",
                   "decision_sector_stage_confidence", "opportunity_score", "evidence_score", "lifecycle_state",
                   "followthrough_status", "market_regime", "sector_regime", "rank_model_version", "evidence_model_version",
                   "stage_classifier_version", "action_policy_version", "execution_policy_version", "portfolio_context_json",
                   "reasons_json", "blockers_json", "warnings_json", "next_required_event", "contract_version",
                   "decision_json", "run_id", "stage_name", "stage_attempt", "source_artifact_hash",
                   "policy_snapshot_id", "semantic_payload_hash", "idempotency_key")
        values = [record_id, d.candidate_id, d.setup_id, _db_time(d.decided_at), d.action.value, d.eligibility.value,
                  d.confidence, d.size_multiplier, c.decision_stage.value, c.decision_stage_status.value,
                  _db_time(c.decision_stage_as_of), c.decision_locked_stage.value, c.decision_provisional_stage.value,
                  c.decision_stage_confidence, c.decision_sector_stage.value, c.decision_sector_stage_status.value,
                  c.decision_sector_stage_confidence, c.opportunity_score, c.evidence_score, c.lifecycle_state.value,
                  c.followthrough_status.value, c.market_regime, c.sector_regime, c.rank_model_version,
                  c.evidence_model_version, c.stage_classifier_version, c.action_policy_version,
                  c.execution_policy_version, canonical_json(c.portfolio_context_summary), canonical_json(d.reasons),
                  canonical_json(d.blockers), canonical_json(d.warnings), d.next_required_event, c.contract_version,
                  canonical_json(payload), observation.lineage.run_id, observation.lineage.stage_name,
                  observation.lineage.stage_attempt, observation.lineage.source_artifact_hash,
                  observation.lineage.policy_snapshot_id, semantic_hash, key]
        return self._insert_append(conn, table="candidate_decision_context", id_column="decision_context_id",
                                   record_id=record_id, candidate_id=d.candidate_id, idempotency_key=key,
                                   semantic_hash=semantic_hash, columns=columns, values=values)

    def append_attribution(self, observation: AttributionObservation) -> AppendResult:
        with self._transaction() as conn:
            return self._append_attribution(conn, observation)

    def _append_attribution(self, conn: duckdb.DuckDBPyConnection, observation: AttributionObservation) -> AppendResult:
        a = observation.attribution
        episode = self._require_open_or_closed(conn, a.candidate_id, allow_closed=True)
        self._validate_episode_identity(episode, a.candidate_id, a.setup_id)
        if a.resolved_at < episode.episode_started_at:
            raise ValueError("attribution cannot precede episode start")
        payload = to_dict(a)
        record_id, key, semantic_hash = self._identity(
            candidate_id=a.candidate_id, record_type="attribution", as_of=a.resolved_at,
            lineage=observation.lineage, contract_version=OPPORTUNITY_CONTRACT_VERSION, payload=payload,
        )
        columns = ("attribution_id", "candidate_id", "setup_id", "attribution_category", "attribution_subcategory",
                   "attribution_confidence", "attribution_rule_version", "supporting_evidence_json",
                   "counterfactual_notes", "resolved_at", "contract_version", "attribution_json", "run_id", "stage_name",
                   "stage_attempt", "source_artifact_hash", "semantic_payload_hash", "idempotency_key")
        values = [record_id, a.candidate_id, a.setup_id, a.attribution_category.value, a.attribution_subcategory,
                  a.attribution_confidence, a.attribution_rule_version, canonical_json(a.supporting_evidence),
                  a.counterfactual_notes, _db_time(a.resolved_at), OPPORTUNITY_CONTRACT_VERSION, canonical_json(a),
                  observation.lineage.run_id, observation.lineage.stage_name, observation.lineage.stage_attempt,
                  observation.lineage.source_artifact_hash, semantic_hash, key]
        return self._insert_append(conn, table="candidate_outcome_attribution", id_column="attribution_id",
                                   record_id=record_id, candidate_id=a.candidate_id, idempotency_key=key,
                                   semantic_hash=semantic_hash, columns=columns, values=values)

    def _batch(self, observations: Iterable[T], appender: Any) -> BatchAppendResult:
        with self._transaction() as conn:
            results = tuple(appender(conn, item) for item in observations)
        return BatchAppendResult(results, sum(r.created for r in results), sum(r.duplicate for r in results))

    def append_snapshots_batch(self, observations: Iterable[SnapshotObservation]) -> BatchAppendResult:
        return self._batch(observations, self._append_snapshot)

    def append_stage_observations_batch(self, observations: Iterable[StageObservation]) -> BatchAppendResult:
        return self._batch(observations, self._append_stage)

    def append_evidence_observations_batch(self, observations: Iterable[EvidenceObservation]) -> BatchAppendResult:
        return self._batch(observations, self._append_evidence)

    def append_opportunity_observations_batch(self, observations: Iterable[OpportunityObservation]) -> BatchAppendResult:
        return self._batch(observations, self._append_opportunity)

    def append_progress_observations_batch(self, observations: Iterable[ProgressObservation]) -> BatchAppendResult:
        return self._batch(observations, self._append_progress)

    def append_orchestration_bundle(self, bundle: OrchestrationBundle) -> OrchestrationBundleResult:
        """Atomically apply one Phase 3 candidate write intent."""
        from dataclasses import replace

        with self._transaction() as conn:
            episode = (
                self._open_episode(conn, bundle.episode_request)
                if bundle.episode_request is not None
                else self._require_open_or_closed(conn, bundle.candidate_id)
            )
            if episode.candidate_id != bundle.candidate_id:
                raise ValueError("orchestration bundle candidate_id does not match episode identity")
            results: list[AppendResult] = []
            if bundle.opportunity is not None:
                results.append(self._append_opportunity(conn, bundle.opportunity))
            if bundle.evidence is not None:
                results.append(self._append_evidence(conn, bundle.evidence))
            results.extend(self._append_stage(conn, item) for item in bundle.stages)
            if bundle.progress is not None:
                results.append(self._append_progress(conn, bundle.progress))
            if bundle.snapshot is not None:
                snapshot_result = self._append_snapshot(conn, bundle.snapshot)
                results.append(snapshot_result)
                if bundle.transition is not None:
                    results.append(self._append_transition(
                        conn, replace(bundle.transition, triggering_snapshot_id=snapshot_result.record_id)
                    ))
            elif bundle.transition is not None:
                raise ValueError("orchestration transition requires a triggering snapshot")
            closed = False
            if bundle.closure is not None:
                self._close_episode(
                    conn, bundle.candidate_id, status=bundle.closure.status,
                    closed_at=bundle.closure.closed_at, closing_reason=bundle.closure.closing_reason,
                    lineage=bundle.closure.lineage,
                )
                closed = True
            result_episode = self._get_episode(conn, bundle.candidate_id)
            assert result_episode is not None
            return OrchestrationBundleResult(result_episode, tuple(results), closed)

    def append_snapshot_bundle(
        self, *, snapshot: SnapshotObservation, stock_stage: StageObservation, sector_stage: StageObservation
    ) -> tuple[AppendResult, AppendResult, AppendResult]:
        with self._transaction() as conn:
            stock_result = self._append_stage(conn, stock_stage)
            sector_result = self._append_stage(conn, sector_stage)
            linked = SnapshotObservation(snapshot.snapshot, snapshot.observed_at, snapshot.lineage,
                                         stock_result.record_id, sector_result.record_id)
            snapshot_result = self._append_snapshot(conn, linked)
            return snapshot_result, stock_result, sector_result

    def append_transition_with_snapshot(
        self, *, snapshot: SnapshotObservation, transition: TransitionObservation
    ) -> tuple[AppendResult, AppendResult]:
        """Atomically persist a triggering snapshot and its explicit transition."""
        from dataclasses import replace

        with self._transaction() as conn:
            snapshot_result = self._append_snapshot(conn, snapshot)
            linked = replace(transition, triggering_snapshot_id=snapshot_result.record_id)
            return snapshot_result, self._append_transition(conn, linked)

    @staticmethod
    def _current_from_row(row: dict[str, Any]) -> CandidateCurrentState:
        allowed = CandidateCurrentState.__dataclass_fields__
        values = {key: value for key, value in row.items() if key in allowed}
        values["episode_status"] = EpisodeStatus(values["episode_status"])
        for key in ("episode_started_at", "episode_closed_at", "last_snapshot_at", "last_transition_at"):
            values[key] = _aware(values.get(key))
        return CandidateCurrentState(**values)

    def current_state(self, candidate_id: str) -> CandidateCurrentState | None:
        with self.registry._reader() as conn:  # noqa: SLF001
            cursor = conn.execute("SELECT * FROM candidate_current_state WHERE candidate_id = ?", [candidate_id])
            row = _row_dict(cursor, cursor.fetchone())
            return self._current_from_row(row) if row else None

    @staticmethod
    def _latest_before(
        conn: duckdb.DuckDBPyConnection, table: str, candidate_id: str, cutoff: datetime,
        time_column: str, id_column: str, *, extra: str = "", extra_params: list[Any] | None = None,
    ) -> dict[str, Any] | None:
        params: list[Any] = [candidate_id, _db_time(cutoff), *(extra_params or [])]
        cursor = conn.execute(
            f"SELECT * FROM {table} WHERE candidate_id = ? AND {time_column} <= ? {extra} "  # noqa: S608
            f"ORDER BY {time_column} DESC, " + ("observed_at DESC, " if time_column == "as_of" else "") +
            f"created_at DESC, {id_column} DESC LIMIT 1",
            params,
        )
        return _row_dict(cursor, cursor.fetchone())

    def state_as_of(self, candidate_id: str, as_of: datetime) -> CandidateCurrentState | None:
        require_aware(as_of, "as_of")
        with self.registry._reader() as conn:  # noqa: SLF001
            episode = self._get_episode(conn, candidate_id)
            if episode is None or episode.episode_started_at > as_of:
                return None
            snapshot = self._latest_before(conn, "candidate_snapshot", candidate_id, as_of, "as_of", "snapshot_id")
            opportunity = self._latest_before(conn, "candidate_opportunity_observation", candidate_id, as_of, "as_of", "opportunity_observation_id")
            evidence = self._latest_before(conn, "candidate_evidence_observation", candidate_id, as_of, "as_of", "evidence_observation_id")
            stock = self._latest_before(conn, "candidate_stage_observation", candidate_id, as_of, "as_of", "stage_observation_id", extra="AND scope = ?", extra_params=["STOCK"])
            sector = self._latest_before(conn, "candidate_stage_observation", candidate_id, as_of, "as_of", "stage_observation_id", extra="AND scope = ?", extra_params=["SECTOR"])
            progress = self._latest_before(conn, "candidate_progress_observation", candidate_id, as_of, "as_of", "progress_observation_id")
            decision = self._latest_before(conn, "candidate_decision_context", candidate_id, as_of, "decided_at", "decision_context_id")
            transition = self._latest_before(conn, "candidate_transition", candidate_id, as_of, "transitioned_at", "transition_id")
            status = episode.episode_status
            closed_at = episode.episode_closed_at
            if closed_at is None or closed_at > as_of:
                status = EpisodeStatus.OPEN
                closed_at = None
            def pick(primary: dict[str, Any] | None, pkey: str, fallback: dict[str, Any] | None, fkey: str) -> Any:
                return (primary or {}).get(pkey) if (primary or {}).get(pkey) is not None else (fallback or {}).get(fkey)
            last_sources = [item for item in (decision, progress, sector, stock, evidence, opportunity, snapshot) if item]
            last_sources.sort(
                key=lambda item: item.get("decided_at") or item.get("as_of") or datetime.min,
                reverse=True,
            )
            lifecycle = (snapshot or {}).get("lifecycle_state")
            if transition and (snapshot is None or transition["transitioned_at"] > snapshot["as_of"]):
                lifecycle = transition["to_state"]
            return CandidateCurrentState(
                candidate_id=episode.candidate_id, setup_id=episode.setup_id, symbol_id=episode.symbol_id,
                exchange=episode.exchange, episode_status=status, episode_started_at=episode.episode_started_at,
                episode_closed_at=closed_at, current_lifecycle_state=lifecycle,
                current_followthrough_status=(snapshot or {}).get("followthrough_status"),
                latest_opportunity_score=pick(opportunity, "opportunity_score", snapshot, "opportunity_score"),
                latest_rank_position=pick(opportunity, "rank_position", snapshot, "rank_position"),
                latest_rank_percentile=pick(opportunity, "rank_percentile", snapshot, "rank_percentile"),
                latest_rank_velocity=pick(opportunity, "rank_velocity", snapshot, "rank_velocity"),
                latest_evidence_score=pick(evidence, "score", snapshot, "evidence_score"),
                latest_evidence_verdict=pick(evidence, "verdict", snapshot, "evidence_verdict"),
                current_stock_stage=(stock or {}).get("effective_stage"),
                current_stock_stage_status=(stock or {}).get("stage_status"),
                current_stock_stage_confidence=(stock or {}).get("confidence_score"),
                current_sector_stage=(sector or {}).get("effective_stage"),
                current_sector_stage_status=(sector or {}).get("stage_status"),
                current_sector_stage_confidence=(sector or {}).get("confidence_score"),
                current_progress_status=(progress or {}).get("progress_status"),
                days_in_state=(snapshot or {}).get("days_in_state"),
                days_without_progress=pick(progress, "days_without_progress", snapshot, "days_without_progress"),
                latest_action=pick(decision, "action", snapshot, "latest_action"),
                current_eligibility=pick(decision, "eligibility", snapshot, "eligibility"),
                last_snapshot_at=_aware((snapshot or {}).get("as_of")),
                last_transition_at=_aware((transition or {}).get("transitioned_at")),
                last_observed_run_id=(last_sources[0].get("run_id") if last_sources else episode.created_run_id),
            )

    def timeline(self, candidate_id: str) -> CandidateTimeline:
        with self.registry._reader() as conn:  # noqa: SLF001
            episode = self._get_episode(conn, candidate_id)
            if episode is None:
                raise KeyError(f"unknown candidate_id: {candidate_id}")
            entries = [TimelineEntry("episode_open", episode.candidate_id, episode.episode_started_at,
                                     episode.created_at, {"status": "OPEN", "opening_reason": episode.opening_reason})]
            specs = (
                ("snapshot", "candidate_snapshot", "snapshot_id", "as_of", "snapshot_json"),
                ("opportunity", "candidate_opportunity_observation", "opportunity_observation_id", "as_of", "observation_json"),
                ("evidence", "candidate_evidence_observation", "evidence_observation_id", "as_of", "observation_json"),
                ("stage", "candidate_stage_observation", "stage_observation_id", "as_of", "observation_json"),
                ("progress", "candidate_progress_observation", "progress_observation_id", "as_of", "observation_json"),
                ("transition", "candidate_transition", "transition_id", "transitioned_at", "metadata_json"),
                ("decision", "candidate_decision_context", "decision_context_id", "decided_at", "decision_json"),
                ("attribution", "candidate_outcome_attribution", "attribution_id", "resolved_at", "attribution_json"),
            )
            for kind, table, id_col, time_col, payload_col in specs:
                rows = conn.execute(
                    f"SELECT {id_col}, {time_col}, created_at, {payload_col} FROM {table} WHERE candidate_id = ?",  # noqa: S608
                    [candidate_id],
                ).fetchall()
                entries.extend(TimelineEntry(kind, row[0], _aware(row[1]), _aware(row[2]), json.loads(row[3])) for row in rows)
            if episode.episode_closed_at is not None:
                entries.append(TimelineEntry("episode_close", episode.candidate_id, episode.episode_closed_at,
                                             episode.updated_at, {"status": episode.episode_status.value,
                                                                  "closing_reason": episode.closing_reason}))
            entries.sort(key=lambda item: (item.event_at, item.created_at, item.record_type, item.record_id))
            return CandidateTimeline(episode, tuple(entries))
