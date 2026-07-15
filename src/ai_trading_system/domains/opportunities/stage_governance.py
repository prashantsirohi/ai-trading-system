"""Append-only Phase 3C-1 sector membership and stage correction governance."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Iterable

import duckdb
import pandas as pd


STAGE_GOVERNANCE_POLICY_VERSION = "stage-governance-v1"
SECTOR_MEMBERSHIP_POLICY_VERSION = "sector-membership-v1"
CORRECTION_IMPACT_POLICY_VERSION = "stage-correction-impact-v1"


class MembershipTrust(str, Enum):
    POINT_IN_TIME_VERIFIED = "POINT_IN_TIME_VERIFIED"
    OBSERVED_AT_RUN = "OBSERVED_AT_RUN"
    LATEST_ONLY_BACKFILL = "LATEST_ONLY_BACKFILL"


class StageGovernanceAction(str, Enum):
    ORIGINAL = "ORIGINAL"
    CORRECTION = "CORRECTION"
    WITHDRAWAL = "WITHDRAWAL"
    LEGACY_ANNOTATION = "LEGACY_ANNOTATION"


class CorrectionImpactStatus(str, Enum):
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    UNRESOLVED_LEGACY = "UNRESOLVED_LEGACY"


@dataclass(frozen=True, slots=True)
class SectorMembershipRecord:
    exchange: str
    symbol_id: str
    sector_id: str
    sector_name: str
    valid_from: date
    valid_to: date
    membership_trust: MembershipTrust
    source_type: str
    source_hash: str
    recorded_at: datetime
    run_id: str
    stage_attempt: int
    industry_name: str | None = None
    supersedes_membership_observation_id: str | None = None
    policy_version: str = SECTOR_MEMBERSHIP_POLICY_VERSION

    def __post_init__(self) -> None:
        if self.valid_to < self.valid_from:
            raise ValueError("sector membership valid_to cannot precede valid_from")
        if self.recorded_at.tzinfo is None:
            raise ValueError("sector membership recorded_at must be timezone-aware")
        for name in ("exchange", "symbol_id", "sector_id", "sector_name", "source_type", "source_hash", "run_id"):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"{name} must be non-empty")
        if self.stage_attempt < 1:
            raise ValueError("stage_attempt must be at least 1")

    @property
    def point_in_time_valid(self) -> bool:
        return self.membership_trust is MembershipTrust.POINT_IN_TIME_VERIFIED

    @property
    def membership_observation_id(self) -> str:
        return _digest({
            "exchange": self.exchange.upper(), "symbol_id": self.symbol_id.upper(),
            "sector_id": self.sector_id, "sector_name": self.sector_name,
            "industry_name": self.industry_name, "valid_from": self.valid_from,
            "valid_to": self.valid_to, "membership_trust": self.membership_trust.value,
            "source_type": self.source_type, "source_hash": self.source_hash,
            "supersedes": self.supersedes_membership_observation_id,
            "policy_version": self.policy_version,
        })


@dataclass(frozen=True, slots=True)
class StageGovernanceRecord:
    governance_event_id: str
    observation_scope: str
    observation_id: str
    governance_action: StageGovernanceAction
    supersedes_observation_id: str | None
    membership_trust: MembershipTrust
    authoritative: bool
    correction_reason: str | None
    correction_authority: str
    policy_version: str
    recorded_at: datetime
    run_id: str
    stage_attempt: int


def append_sector_memberships(registry: Any, records: Iterable[SectorMembershipRecord]) -> dict[str, int]:
    """Append effective-dated memberships, rejecting undeclared conflicting overlaps."""
    created = duplicates = 0
    with registry._writer() as conn:  # noqa: SLF001
        for record in records:
            observation_id = record.membership_observation_id
            if conn.execute(
                "SELECT 1 FROM sector_membership_history WHERE membership_observation_id = ?",
                [observation_id],
            ).fetchone():
                duplicates += 1
                continue
            overlaps = conn.execute(
                """SELECT membership_observation_id, sector_id, sector_name, source_hash
                   FROM sector_membership_history
                   WHERE exchange = ? AND symbol_id = ?
                     AND valid_from <= ? AND valid_to >= ?
                     AND NOT EXISTS (
                         SELECT 1 FROM sector_membership_history correction
                         WHERE correction.supersedes_membership_observation_id = sector_membership_history.membership_observation_id
                           AND correction.recorded_at <= ?
                     )""",
                [record.exchange.upper(), record.symbol_id.upper(), record.valid_to, record.valid_from, _db_time(record.recorded_at)],
            ).fetchall()
            conflicting = [row for row in overlaps if row[0] != record.supersedes_membership_observation_id]
            if conflicting:
                raise ValueError(
                    f"overlapping sector membership for {record.exchange.upper()}:{record.symbol_id.upper()} "
                    "requires an explicit supersedes_membership_observation_id"
                )
            if record.supersedes_membership_observation_id and not any(
                row[0] == record.supersedes_membership_observation_id for row in overlaps
            ):
                raise ValueError("superseded sector membership must be the active overlapping observation")
            conn.execute(
                """INSERT INTO sector_membership_history (
                       membership_observation_id, exchange, symbol_id, sector_id, sector_name, industry_name,
                       valid_from, valid_to, membership_trust, point_in_time_valid, source_type, source_hash,
                       supersedes_membership_observation_id, policy_version, recorded_at, run_id, stage_attempt
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [observation_id, record.exchange.upper(), record.symbol_id.upper(), record.sector_id,
                 record.sector_name, record.industry_name, record.valid_from, record.valid_to,
                 record.membership_trust.value, record.point_in_time_valid, record.source_type,
                 record.source_hash, record.supersedes_membership_observation_id, record.policy_version,
                 _db_time(record.recorded_at), record.run_id, record.stage_attempt],
            )
            created += 1
    return {"created": created, "duplicates": duplicates}


def read_sector_membership_as_of(
    registry: Any,
    *,
    effective_at: date | str,
    available_at: datetime | str,
    exchange: str = "NSE",
    include_latest_only: bool = False,
) -> pd.DataFrame:
    """Resolve memberships known by available_at and valid at effective_at."""
    effective = date.fromisoformat(str(effective_at)[:10])
    available = _coerce_datetime(available_at)
    trust_clause = "" if include_latest_only else "AND membership_trust <> 'LATEST_ONLY_BACKFILL'"
    with registry._reader() as conn:  # noqa: SLF001
        rows = conn.execute(
            f"""SELECT membership_observation_id, exchange, symbol_id, sector_id, sector_name,
                       industry_name, valid_from, valid_to, membership_trust, point_in_time_valid,
                       source_type, source_hash, policy_version, recorded_at
                FROM sector_membership_history membership
                WHERE exchange = ? AND valid_from <= ? AND valid_to >= ? AND recorded_at <= ?
                  {trust_clause}
                  AND NOT EXISTS (
                      SELECT 1 FROM sector_membership_history correction
                      WHERE correction.supersedes_membership_observation_id = membership.membership_observation_id
                        AND correction.recorded_at <= ?
                  )
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY exchange, symbol_id
                    ORDER BY CASE membership_trust
                        WHEN 'POINT_IN_TIME_VERIFIED' THEN 3
                        WHEN 'OBSERVED_AT_RUN' THEN 2 ELSE 1 END DESC,
                        recorded_at DESC, membership_observation_id DESC
                ) = 1""",
            [exchange.upper(), effective, effective, _db_time(available), _db_time(available)],
        ).fetchdf()
    return rows


def observe_sector_mapping(
    registry: Any,
    mapping: dict[str, tuple[str, str]],
    *,
    exchange: str,
    as_of: date | str,
    run_id: str,
    stage_attempt: int,
    recorded_at: datetime | None = None,
) -> dict[str, tuple[str, str, str, str]]:
    """Persist one-day latest-master observations and prefer verified memberships."""
    effective = date.fromisoformat(str(as_of)[:10])
    observed_at = recorded_at or datetime.now(timezone.utc)
    existing = read_sector_membership_as_of(
        registry, effective_at=effective, available_at=observed_at,
        exchange=exchange, include_latest_only=False,
    )
    existing_by_symbol = {str(row.symbol_id).upper(): row for row in existing.itertuples(index=False)}
    verified = {
        symbol: row for symbol, row in existing_by_symbol.items()
        if row.membership_trust == MembershipTrust.POINT_IN_TIME_VERIFIED.value
    }
    records = []
    for symbol, (sector_id, sector_name) in sorted(mapping.items()):
        if symbol.upper() in verified:
            continue
        source_hash = _digest({"symbol_id": symbol.upper(), "sector_id": sector_id, "sector_name": sector_name, "observed_on": effective})
        prior = existing_by_symbol.get(symbol.upper())
        if prior is not None and str(prior.source_hash) == source_hash:
            continue
        supersedes = (
            str(prior.membership_observation_id)
            if prior is not None and str(prior.source_hash) != source_hash else None
        )
        records.append(SectorMembershipRecord(
            exchange=exchange, symbol_id=symbol, sector_id=sector_id, sector_name=sector_name,
            valid_from=effective, valid_to=effective, membership_trust=MembershipTrust.OBSERVED_AT_RUN,
            source_type="masterdata_latest_snapshot", source_hash=source_hash, recorded_at=observed_at,
            run_id=run_id, stage_attempt=stage_attempt,
            supersedes_membership_observation_id=supersedes,
        ))
    append_sector_memberships(registry, records)
    resolved = read_sector_membership_as_of(
        registry, effective_at=effective, available_at=observed_at,
        exchange=exchange, include_latest_only=False,
    )
    return {
        str(row.symbol_id).upper(): (
            str(row.sector_id), str(row.sector_name), str(row.membership_trust),
            str(row.membership_observation_id),
        )
        for row in resolved.itertuples(index=False)
    }


def resolve_historical_sector_mapping(
    registry: Any,
    latest_mapping: dict[str, tuple[str, str]],
    *,
    exchange: str,
    effective_at: date | str,
    available_at: datetime,
    run_id: str,
    stage_attempt: int,
) -> dict[str, tuple[str, str, str, str]]:
    """Resolve prior-date membership and explicitly tag latest-only fallbacks."""
    effective = date.fromisoformat(str(effective_at)[:10])
    trusted = read_sector_membership_as_of(
        registry, effective_at=effective, available_at=available_at,
        exchange=exchange, include_latest_only=False,
    )
    trusted_symbols = {str(row.symbol_id).upper() for row in trusted.itertuples(index=False)}
    fallback_records = []
    for symbol, (sector_id, sector_name) in sorted(latest_mapping.items()):
        if symbol.upper() in trusted_symbols:
            continue
        fallback_records.append(SectorMembershipRecord(
            exchange=exchange, symbol_id=symbol, sector_id=sector_id, sector_name=sector_name,
            valid_from=effective, valid_to=effective,
            membership_trust=MembershipTrust.LATEST_ONLY_BACKFILL,
            source_type="masterdata_latest_historical_backfill",
            source_hash=_digest({
                "symbol_id": symbol.upper(), "sector_id": sector_id,
                "sector_name": sector_name, "effective_at": effective,
                "trust": MembershipTrust.LATEST_ONLY_BACKFILL.value,
            }),
            recorded_at=available_at, run_id=run_id, stage_attempt=stage_attempt,
        ))
    append_sector_memberships(registry, fallback_records)
    resolved = read_sector_membership_as_of(
        registry, effective_at=effective, available_at=available_at,
        exchange=exchange, include_latest_only=True,
    )
    return {
        str(row.symbol_id).upper(): (
            str(row.sector_id), str(row.sector_name), str(row.membership_trust),
            str(row.membership_observation_id),
        )
        for row in resolved.itertuples(index=False)
    }


def append_stage_governance(
    conn: Any,
    *,
    scope: str,
    observation_id: str,
    action: StageGovernanceAction,
    membership_trust: MembershipTrust,
    recorded_at: datetime,
    run_id: str,
    stage_attempt: int,
    supersedes_observation_id: str | None = None,
    correction_reason: str | None = None,
    correction_authority: str = "pipeline",
    policy_version: str = STAGE_GOVERNANCE_POLICY_VERSION,
) -> StageGovernanceRecord:
    event_payload = {
        "scope": scope.upper(), "observation_id": observation_id, "action": action.value,
        "supersedes": supersedes_observation_id, "membership_trust": membership_trust.value,
        "correction_reason": correction_reason, "correction_authority": correction_authority,
        "policy_version": policy_version,
    }
    event_hash = _digest(event_payload)
    governance_event_id = event_hash
    authoritative = (
        action is not StageGovernanceAction.WITHDRAWAL
        and (scope.upper() == "STOCK" or membership_trust is not MembershipTrust.LATEST_ONLY_BACKFILL)
    )
    conn.execute(
        """INSERT INTO stage_observation_governance (
               governance_event_id, observation_scope, observation_id, governance_action,
               supersedes_observation_id, membership_trust, authoritative, correction_reason,
               correction_authority, policy_version, recorded_at, run_id, stage_attempt, event_hash
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(governance_event_id) DO NOTHING""",
        [governance_event_id, scope.upper(), observation_id, action.value, supersedes_observation_id,
         membership_trust.value, authoritative, correction_reason, correction_authority,
         policy_version, _db_time(recorded_at), run_id, stage_attempt, event_hash],
    )
    return StageGovernanceRecord(
        governance_event_id, scope.upper(), observation_id, action, supersedes_observation_id,
        membership_trust, authoritative, correction_reason, correction_authority,
        policy_version, recorded_at, run_id, stage_attempt,
    )


def append_sector_dependencies(
    conn: Any,
    *,
    sector_observation_id: str,
    stock_observations: Iterable[tuple[str, str, str | None]],
) -> None:
    for stock_observation_id, source_hash, membership_observation_id in stock_observations:
        _append_dependency(conn, sector_observation_id, "STOCK_STAGE", stock_observation_id, source_hash)
        if membership_observation_id:
            _append_dependency(conn, sector_observation_id, "SECTOR_MEMBERSHIP", membership_observation_id, source_hash)


def record_correction_impacts(
    conn: Any,
    *,
    governance: StageGovernanceRecord,
    entity_id: str,
    source_week_end: date | str,
) -> int:
    """Append conservative potential-impact links without changing candidate history."""
    if governance.governance_action is not StageGovernanceAction.CORRECTION:
        return 0
    scope = governance.observation_scope.upper()
    candidate_ids: set[str] = set()
    if scope == "STOCK":
        candidate_ids.update(row[0] for row in conn.execute(
            "SELECT candidate_id FROM candidate_episode WHERE symbol_id = ? AND episode_status = 'OPEN'",
            [entity_id.upper()],
        ).fetchall())
        candidate_ids.update(row[0] for row in conn.execute(
            """SELECT DISTINCT candidate_id FROM candidate_stage_observation
               WHERE scope = 'STOCK' AND entity_id = ? AND source_week_end = ?""",
            [entity_id.upper(), date.fromisoformat(str(source_week_end)[:10])],
        ).fetchall())
    else:
        candidate_ids.update(row[0] for row in conn.execute(
            """SELECT DISTINCT candidate_id FROM candidate_stage_observation
               WHERE scope = 'SECTOR' AND entity_id = ? AND source_week_end = ?""",
            [entity_id, date.fromisoformat(str(source_week_end)[:10])],
        ).fetchall())
    affected: set[tuple[str, str, str | None]] = set()
    for candidate_id in candidate_ids:
        affected.add(("candidate_episode", candidate_id, candidate_id))
        for table, id_column in (
            ("candidate_snapshot", "snapshot_id"),
            ("candidate_decision_context", "decision_context_id"),
            ("candidate_outcome_attribution", "attribution_id"),
        ):
            affected.update(
                (table, str(row[0]), candidate_id)
                for row in conn.execute(
                    f"SELECT {id_column} FROM {table} WHERE candidate_id = ?",  # noqa: S608
                    [candidate_id],
                ).fetchall()
            )
    for record_type, record_id, candidate_id in sorted(affected):
        impact_id = _digest({
            "governance_event_id": governance.governance_event_id,
            "affected_record_type": record_type, "affected_record_id": record_id,
            "policy_version": CORRECTION_IMPACT_POLICY_VERSION,
        })
        conn.execute(
            """INSERT INTO stage_correction_impact (
                   impact_id, correction_governance_event_id, corrected_observation_scope,
                   corrected_observation_id, affected_record_type, affected_record_id,
                   candidate_id, impact_status, impact_reason, policy_version, detected_at, run_id
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(impact_id) DO NOTHING""",
            [impact_id, governance.governance_event_id, scope, governance.observation_id,
             record_type, record_id, candidate_id, CorrectionImpactStatus.REVIEW_REQUIRED.value,
             "stage observation was corrected; append-only candidate history requires review",
             CORRECTION_IMPACT_POLICY_VERSION, _db_time(governance.recorded_at), governance.run_id],
        )
    return len(affected)


def annotate_legacy_stage_history(
    registry: Any,
    *,
    run_id: str,
    recorded_at: datetime | None = None,
    apply: bool = False,
) -> dict[str, int]:
    """Preview or append governance overlays for unannotated Phase 3B observations."""
    observed_at = recorded_at or datetime.now(timezone.utc)
    counts: dict[str, int] = {}
    context = registry._writer() if apply else registry._reader()  # noqa: SLF001
    with context as conn:
        for scope, table in (("STOCK", "weekly_stock_stage_history"), ("SECTOR", "weekly_sector_stage_history")):
            rows = conn.execute(
                f"""SELECT observation_id FROM {table} history
                    WHERE NOT EXISTS (
                        SELECT 1 FROM stage_observation_governance governance
                        WHERE governance.observation_scope = ?
                          AND governance.observation_id = history.observation_id
                    ) ORDER BY observation_id""",  # noqa: S608
                [scope],
            ).fetchall()
            counts[scope.lower()] = len(rows)
            if apply:
                for (observation_id,) in rows:
                    append_stage_governance(
                        conn, scope=scope, observation_id=observation_id,
                        action=StageGovernanceAction.LEGACY_ANNOTATION,
                        membership_trust=MembershipTrust.OBSERVED_AT_RUN,
                        recorded_at=observed_at, run_id=run_id, stage_attempt=1,
                        correction_reason="Phase 3B observation annotated without payload mutation",
                        correction_authority="phase3c1_legacy_annotation",
                    )
    counts["total"] = counts.get("stock", 0) + counts.get("sector", 0)
    return counts


def preview_legacy_stage_history(db_path: Any) -> dict[str, int]:
    """Count Phase 3B observations lacking overlays without migrating the store."""
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        governed = "stage_observation_governance" in tables
        counts: dict[str, int] = {}
        for scope, table in (("STOCK", "weekly_stock_stage_history"), ("SECTOR", "weekly_sector_stage_history")):
            if table not in tables:
                counts[scope.lower()] = 0
            elif governed:
                counts[scope.lower()] = int(conn.execute(
                    f"""SELECT COUNT(*) FROM {table} history WHERE NOT EXISTS (
                        SELECT 1 FROM stage_observation_governance governance
                        WHERE governance.observation_scope = ?
                          AND governance.observation_id = history.observation_id
                    )""",  # noqa: S608
                    [scope],
                ).fetchone()[0])
            else:
                counts[scope.lower()] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])  # noqa: S608
        counts["total"] = counts["stock"] + counts["sector"]
        return counts
    finally:
        conn.close()


def _append_dependency(conn: Any, sector_id: str, kind: str, dependency_id: str, source_hash: str) -> None:
    identity = _digest({"sector": sector_id, "kind": kind, "dependency": dependency_id})
    conn.execute(
        """INSERT INTO stage_observation_dependency VALUES (?, ?, ?, ?, ?, current_timestamp)
           ON CONFLICT(dependency_id) DO NOTHING""",
        [identity, sector_id, kind, dependency_id, source_hash],
    )


def _digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(value, default=str, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _db_time(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _coerce_datetime(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
