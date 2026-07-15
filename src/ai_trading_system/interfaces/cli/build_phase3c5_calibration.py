"""Safe Phase 3C-5 calibration dataset and readiness artifact builder."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.domains.opportunities.calibration import (
    CalibrationConfig,
    build_calibration_dataset,
    write_calibration_artifacts,
)
from ai_trading_system.interfaces.cli.benchmark_phase3c4 import validate_benchmark_paths


PROFILES = ("small_fixture", "winner_only", "critical_leakage", "copied_realistic")

_MIGRATION_034_036_TABLES = frozenset({
    "sector_membership_history",
    "stage_observation_governance",
    "stage_observation_dependency",
    "stage_correction_impact",
    "pipeline_alert_incident",
    "position_recovery_proposal",
    "position_recovery_action",
})
_MIGRATION_035_COLUMNS = {
    "stage_observation_governance": frozenset({
        "authority_reference", "authority_recorded_at", "governance_policy_version",
    }),
    "stage_correction_impact": frozenset({
        "match_count", "match_rule_version", "match_evidence",
        "authoritative_calibration_eligible", "review_required",
    }),
}


def fixture_rows(profile: str = "small_fixture", *, count: int = 72) -> list[dict[str, Any]]:
    base = datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc)
    regimes = ("bull", "neutral", "risk_off")
    stages = ("stage_1", "transition_1_to_2", "stage_2", "stage_3")
    tiers = ("light_pattern", "full_investigator", "position_monitor")
    families = ("early_accumulation", "base_building", "breakout")
    labels = ("positive", "negative", "neutral")
    rows: list[dict[str, Any]] = []
    for index in range(count):
        decision = base + timedelta(days=index)
        label = "positive" if profile == "winner_only" else labels[index % len(labels)]
        rows.append({
            "entity_type": "candidate_decision", "entity_id": f"candidate-{index:04d}",
            "candidate_id": f"candidate-{index:04d}", "decision_context_id": f"decision-{index:04d}",
            "symbol_id": f"CAL{index:04d}", "exchange": "NSE",
            "decision_at": decision.isoformat(), "input_available_at": decision.isoformat(),
            "outcome_window_end": (decision + timedelta(days=28)).isoformat(),
            "outcome_available_at": (decision + timedelta(days=28, minutes=1)).isoformat(),
            "outcome_horizon": 20, "outcome_status": "COMPLETE", "outcome_label": label,
            "outcome_return": float((index % 9) - 4),
            "stage_status": "LOCKED", "stage_observation_verified": True,
            "stock_stage": stages[index % len(stages)], "sector_stage": "stage_2",
            "membership_trust": "POINT_IN_TIME_VERIFIED",
            "membership_recorded_at": decision.isoformat(),
            "correction_impact_status": "linked", "correction_review_required": False,
            "authoritative_calibration_eligible": True,
            "recovered_from_position_state": False, "pre_entry_history_available": True,
            "was_in_universe_as_of_decision": True,
            "universe_source": "historical_point_in_time",
            "listing_status_as_of_decision": "listed",
            "delisting_status": "delisted_later" if index % 19 == 0 else "not_delisted",
            "symbol_identity_valid": True, "market_data_complete": True,
            "market_regime": regimes[index % len(regimes)],
            "breadth_velocity_bucket": ("accelerating", "stable", "decelerating")[index % 3],
            "scan_tier": tiers[index % len(tiers)], "setup_family": families[index % len(families)],
            "candidate_state": ("ready", "triggered", "confirmed")[index % 3],
            "lookback_sessions": 252, "required_lookback_sessions": 200,
        })
    if profile == "critical_leakage" and rows:
        rows[0]["input_available_at"] = (base + timedelta(days=1)).isoformat()
        rows[0]["lookahead_input"] = True
    return rows


def load_copied_rows(path: Path, *, as_of: str) -> list[dict[str, Any]]:
    """Read candidate decisions with copied-store governance evidence.

    Missing tables or evidence remain fail-closed.  In particular, the adapter
    never treats the mere presence of a decision as proof of point-in-time
    membership, governed stage history, or a resolved correction impact.
    """
    rows: list[dict[str, Any]] = []
    with duckdb.connect(str(path), read_only=True) as conn:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        if not {"candidate_decision_context", "candidate_episode"}.issubset(tables):
            return rows
        decisions = conn.execute(
            """SELECT d.decision_context_id, d.candidate_id, e.symbol_id, e.exchange,
                      d.decided_at, d.decision_stage_status, d.decision_stage,
                      d.decision_sector_stage, d.market_regime, e.setup_family
               FROM candidate_decision_context d
               JOIN candidate_episode e ON e.candidate_id = d.candidate_id
               WHERE d.decided_at <= CAST(? AS TIMESTAMP)
               ORDER BY d.decided_at, d.decision_context_id""",
            [as_of],
        ).fetchall()
        for item in decisions:
            decided = _utc(item[4])
            decision_id, candidate_id = str(item[0]), str(item[1])
            symbol_id, exchange = str(item[2]), str(item[3])
            membership = _membership_evidence(
                conn, tables=tables, exchange=exchange, symbol_id=symbol_id, decision_at=decided,
            )
            correction = _correction_evidence(
                conn, tables=tables, decision_context_id=decision_id, decision_at=decided,
            )
            lookback_sessions = _stage_lookback_sessions(
                conn, tables=tables, exchange=exchange, symbol_id=symbol_id, decision_at=decided,
            )
            governed = {
                "stage_observation_governance", "stage_correction_impact",
            }.issubset(tables)
            stage_status = str(item[5]).upper()
            point_in_time_membership = (
                membership["membership_trust"] == "POINT_IN_TIME_VERIFIED"
                and membership["point_in_time_valid"]
                and not membership["membership_overlap"]
            )
            rows.append({
                "entity_type": "candidate_decision", "entity_id": decision_id,
                "decision_context_id": decision_id, "candidate_id": candidate_id,
                "symbol_id": symbol_id, "exchange": exchange,
                "decision_at": decided.isoformat(), "input_available_at": decided.isoformat(),
                "outcome_horizon": 20, "outcome_status": "PENDING", "outcome_label": None,
                "stage_status": stage_status,
                "stage_observation_verified": governed and stage_status == "LOCKED"
                and correction["stage_governance_verified"],
                "stage_governance_conflict": correction["stage_governance_conflict"],
                "stage_governance_cycle": correction["stage_governance_cycle"],
                "stock_stage": str(item[6]), "sector_stage": str(item[7]),
                "market_regime": str(item[8]), "setup_family": str(item[9]),
                "membership_trust": membership["membership_trust"],
                "membership_recorded_at": membership["membership_recorded_at"],
                "membership_overlap": membership["membership_overlap"],
                "correction_impact_status": correction["impact_status"],
                "correction_review_required": correction["review_required"],
                "authoritative_calibration_eligible": correction["calibration_eligible"],
                "correction_recorded_at": correction["correction_recorded_at"],
                "correction_used": correction["correction_used"],
                "was_in_universe_as_of_decision": point_in_time_membership,
                "universe_source": "historical_point_in_time" if point_in_time_membership else "unknown",
                "listing_status_as_of_decision": "listed" if point_in_time_membership else "unknown",
                "delisting_status": "not_delisted" if point_in_time_membership else "unknown",
                "symbol_identity_valid": point_in_time_membership,
                "market_data_complete": lookback_sessions > 0,
                "lookback_sessions": lookback_sessions, "required_lookback_sessions": 200,
            })
    return rows


def copied_store_readiness_evidence(path: Path) -> dict[str, Any]:
    """Derive migration and real-history readiness from an immutable copy."""
    with duckdb.connect(str(path), read_only=True) as conn:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        missing_tables = sorted(_MIGRATION_034_036_TABLES - tables)
        missing_columns = sorted(
            f"{table}.{column}"
            for table, required in _MIGRATION_035_COLUMNS.items()
            if table in tables
            for column in required - {
                str(row[0]) for row in conn.execute(f"DESCRIBE {table}").fetchall()  # noqa: S608
            }
        )
        real_history_rows = 0
        if {
            "weekly_stock_stage_history", "pipeline_run", "pipeline_stage_run",
        }.issubset(tables) and {
            "run_id", "stage_attempt", "source_artifact_hash",
        }.issubset(_table_columns(conn, "weekly_stock_stage_history")) and {
            "run_id", "status",
        }.issubset(_table_columns(conn, "pipeline_run")) and {
            "run_id", "stage_name", "attempt_number", "status",
        }.issubset(_table_columns(conn, "pipeline_stage_run")):
            real_history_rows = int(conn.execute(
                """SELECT COUNT(*)
                   FROM weekly_stock_stage_history history
                   JOIN pipeline_run run ON run.run_id = history.run_id
                   JOIN pipeline_stage_run stage_run
                     ON stage_run.run_id = history.run_id
                    AND stage_run.stage_name = 'weekly_stage'
                    AND stage_run.attempt_number = history.stage_attempt
                   WHERE lower(run.status) = 'completed'
                     AND lower(stage_run.status) = 'completed'
                     AND length(trim(history.source_artifact_hash)) > 0"""
            ).fetchone()[0])
        return {
            "operator_migrations_applied": not missing_tables and not missing_columns,
            "missing_migration_tables": missing_tables,
            "missing_migration_columns": missing_columns,
            "real_phase3b_history_present": real_history_rows > 0,
            "real_phase3b_history_rows": real_history_rows,
        }


def _membership_evidence(
    conn: duckdb.DuckDBPyConnection, *, tables: set[str], exchange: str,
    symbol_id: str, decision_at: datetime,
) -> dict[str, Any]:
    empty = {
        "membership_trust": "", "membership_recorded_at": None,
        "point_in_time_valid": False, "membership_overlap": False,
    }
    required_columns = {
        "membership_observation_id", "exchange", "symbol_id", "valid_from", "valid_to",
        "membership_trust", "point_in_time_valid", "recorded_at",
        "supersedes_membership_observation_id",
    }
    if (
        "sector_membership_history" not in tables
        or not required_columns.issubset(_table_columns(conn, "sector_membership_history"))
    ):
        return empty
    matches = conn.execute(
        """SELECT membership_trust, recorded_at, point_in_time_valid
           FROM sector_membership_history membership
           WHERE exchange = ? AND symbol_id = ?
             AND valid_from <= CAST(? AS DATE) AND valid_to >= CAST(? AS DATE)
             AND recorded_at <= CAST(? AS TIMESTAMP)
             AND NOT EXISTS (
                 SELECT 1 FROM sector_membership_history correction
                 WHERE correction.supersedes_membership_observation_id = membership.membership_observation_id
                   AND correction.recorded_at <= CAST(? AS TIMESTAMP)
             )
           ORDER BY recorded_at DESC, membership_observation_id""",
        [exchange, symbol_id, decision_at, decision_at, decision_at, decision_at],
    ).fetchall()
    if not matches:
        return empty
    return {
        "membership_trust": str(matches[0][0]).upper(),
        "membership_recorded_at": _utc(matches[0][1]).isoformat(),
        "point_in_time_valid": bool(matches[0][2]),
        "membership_overlap": len(matches) > 1,
    }


def _correction_evidence(
    conn: duckdb.DuckDBPyConnection, *, tables: set[str],
    decision_context_id: str, decision_at: datetime,
) -> dict[str, Any]:
    missing = {
        "impact_status": "", "review_required": True, "calibration_eligible": False,
        "correction_recorded_at": None, "correction_used": False,
        "stage_governance_verified": False, "stage_governance_conflict": False,
        "stage_governance_cycle": False,
    }
    impact_columns = {
        "impact_id", "correction_governance_event_id", "affected_record_type",
        "affected_record_id", "impact_status", "review_required",
        "authoritative_calibration_eligible",
    }
    governance_columns = {
        "governance_event_id", "recorded_at", "authoritative", "governance_action",
        "observation_id", "supersedes_observation_id",
    }
    if (
        not {"stage_correction_impact", "stage_observation_governance"}.issubset(tables)
        or not impact_columns.issubset(_table_columns(conn, "stage_correction_impact"))
        or not governance_columns.issubset(_table_columns(conn, "stage_observation_governance"))
    ):
        return missing
    impacts = conn.execute(
        """SELECT impact.impact_status, impact.review_required,
                  impact.authoritative_calibration_eligible, governance.recorded_at,
                  governance.authoritative, governance.governance_action,
                  governance.observation_id, governance.supersedes_observation_id
           FROM stage_correction_impact impact
           JOIN stage_observation_governance governance
             ON governance.governance_event_id = impact.correction_governance_event_id
           WHERE impact.affected_record_type = 'candidate_decision_context'
             AND impact.affected_record_id = ?
           ORDER BY governance.recorded_at, impact.impact_id""",
        [decision_context_id],
    ).fetchall()
    if not impacts:
        return {
            **missing, "impact_status": "resolved", "review_required": False,
            "calibration_eligible": True, "stage_governance_verified": True,
        }
    statuses = [str(row[0]).lower() for row in impacts]
    review_required = any(bool(row[1]) for row in impacts)
    calibration_eligible = all(bool(row[2]) for row in impacts) and not review_required
    latest_recorded = max(_utc(row[3]) for row in impacts)
    graph = {str(row[6]): str(row[7]) for row in impacts if row[7]}
    cycle = any(_has_cycle(graph, observation_id) for observation_id in graph)
    conflict = len({str(row[6]) for row in impacts if bool(row[4])}) > 1
    impact_status = "linked" if all(status in {"linked", "resolved"} for status in statuses) else statuses[-1]
    return {
        "impact_status": impact_status, "review_required": review_required,
        "calibration_eligible": calibration_eligible,
        "correction_recorded_at": latest_recorded.isoformat(),
        "correction_used": latest_recorded <= decision_at,
        "stage_governance_verified": calibration_eligible and not conflict and not cycle,
        "stage_governance_conflict": conflict, "stage_governance_cycle": cycle,
    }


def _has_cycle(graph: dict[str, str], start: str) -> bool:
    seen: set[str] = set()
    current: str | None = start
    while current is not None and current in graph:
        if current in seen:
            return True
        seen.add(current)
        current = graph.get(current)
    return False


def _stage_lookback_sessions(
    conn: duckdb.DuckDBPyConnection, *, tables: set[str], exchange: str,
    symbol_id: str, decision_at: datetime,
) -> int:
    if (
        "weekly_stock_stage_history" not in tables
        or not {"exchange", "symbol_id", "source_week_end", "as_of"}.issubset(
            _table_columns(conn, "weekly_stock_stage_history")
        )
    ):
        return 0
    weeks = int(conn.execute(
        """SELECT COUNT(DISTINCT source_week_end)
           FROM weekly_stock_stage_history
           WHERE exchange = ? AND symbol_id = ? AND as_of <= CAST(? AS TIMESTAMP)""",
        [exchange, symbol_id, decision_at],
    ).fetchone()[0])
    return weeks * 5


def _table_columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    """Inspect a trusted internal table name without mutating the copied store."""
    return {str(row[0]) for row in conn.execute(f"DESCRIBE {table}").fetchall()}  # noqa: S608


def run_build(
    *, profile: str, output_root: Path, as_of: str,
    copied_control_plane: Path | None = None,
    fail_on_not_ready: bool = False,
) -> dict[str, Any]:
    if profile not in PROFILES:
        raise ValueError(f"unsupported profile: {profile}")
    output, copied = validate_benchmark_paths(output_root, copied_control_plane=copied_control_plane)
    if profile == "copied_realistic":
        if copied is None:
            raise ValueError("copied_realistic requires --copied-control-plane")
        rows = load_copied_rows(copied, as_of=as_of)
        readiness_evidence = copied_store_readiness_evidence(copied)
        source_hashes = {"control_plane.duckdb": _file_hash(copied)}
    else:
        rows = fixture_rows(profile)
        readiness_evidence = {
            "operator_migrations_applied": False,
            "real_phase3b_history_present": False,
        }
        source_hashes = {"fixture": _hash_json(rows)}
    result = build_calibration_dataset(
        rows, dataset_name=f"phase3c5_{profile}", dataset_purpose="entry",
        as_of=as_of, config=CalibrationConfig(),
        source_database_hashes=source_hashes,
        source_schema_versions={"opportunity_registry": "opportunity-registry-schema-v1"},
        operator_migrations_applied=bool(readiness_evidence["operator_migrations_applied"]),
        real_phase3b_history_present=bool(readiness_evidence["real_phase3b_history_present"]),
        readiness_evidence=readiness_evidence,
    )
    paths = write_calibration_artifacts(result, output)
    exit_code = int(fail_on_not_ready and result.verdict.value == "NOT_READY")
    return {
        "status": "completed", "exit_code": exit_code,
        "output_root": str(output), "profile": profile,
        "manifest_id": result.manifest["manifest_id"],
        "eligible_dataset_hash": result.manifest["eligible_dataset_hash"],
        "verdict": result.verdict.value,
        "phase4_development_ready": result.phase4_development_ready,
        "phase4_production_ready": result.phase4_production_ready,
        "artifacts": [str(path) for path in paths],
        "quality_summary": result.quality_summary,
        "limitations": [item.limitation_id for item in result.limitations],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build immutable Phase 3C-5 calibration/readiness evidence.")
    parser.add_argument("--profile", choices=PROFILES, default="small_fixture")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--as-of", default="2026-07-15T23:59:59+00:00")
    parser.add_argument("--copied-control-plane", type=Path)
    parser.add_argument("--data-root", type=Path, help="Reserved copied-data lineage root; never used as an output target.")
    parser.add_argument("--fail-on-not-ready", action="store_true")
    args = parser.parse_args(argv)
    result = run_build(
        profile=args.profile, output_root=args.output_root, as_of=args.as_of,
        copied_control_plane=args.copied_control_plane,
        fail_on_not_ready=args.fail_on_not_ready,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return int(result["exit_code"])


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _file_hash(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_json(value: Any) -> str:
    import hashlib

    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
