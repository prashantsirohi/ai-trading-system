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
    """Read candidate decisions conservatively; missing outcomes remain pending."""
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
        rows.append({
            "entity_type": "candidate_decision", "entity_id": str(item[0]),
            "decision_context_id": str(item[0]), "candidate_id": str(item[1]),
            "symbol_id": str(item[2]), "exchange": str(item[3]),
            "decision_at": decided.isoformat(), "input_available_at": decided.isoformat(),
            "outcome_horizon": 20, "outcome_status": "PENDING", "outcome_label": None,
            "stage_status": str(item[5]).upper(), "stage_observation_verified": True,
            "stock_stage": str(item[6]), "sector_stage": str(item[7]),
            "market_regime": str(item[8]), "setup_family": str(item[9]),
            "membership_trust": "", "correction_impact_status": "",
            "authoritative_calibration_eligible": False,
            "was_in_universe_as_of_decision": False, "universe_source": "unknown",
            "listing_status_as_of_decision": "unknown", "delisting_status": "unknown",
            "symbol_identity_valid": False, "market_data_complete": True,
            "lookback_sessions": 0, "required_lookback_sessions": 200,
        })
    return rows


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
        source_hashes = {"control_plane.duckdb": _file_hash(copied)}
    else:
        rows = fixture_rows(profile)
        source_hashes = {"fixture": _hash_json(rows)}
    result = build_calibration_dataset(
        rows, dataset_name=f"phase3c5_{profile}", dataset_purpose="entry",
        as_of=as_of, config=CalibrationConfig(),
        source_database_hashes=source_hashes,
        source_schema_versions={"opportunity_registry": "opportunity-registry-schema-v1"},
        operator_migrations_applied=False, real_phase3b_history_present=False,
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
