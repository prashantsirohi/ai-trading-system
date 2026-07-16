from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pytest

from ai_trading_system.domains.opportunities.registry import (
    OpportunityRegistryConflictError,
)
from ai_trading_system.domains.opportunities.position_monitoring import (
    PositionRecoveryMode,
    make_position_cycle_id,
    make_recovery_proposal_id,
    recovery_payload_hash,
)
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    OpportunityShadowConfig,
)
from ai_trading_system.domains.opportunities.orchestration.service import (
    _persist_recovery_proposal,
    _recovery_allowed,
)
from ai_trading_system.pipeline.alerts import AlertManager
from ai_trading_system.pipeline.registry import RegistryStore


NOW = datetime(2026, 7, 15, tzinfo=timezone.utc)


def test_position_and_recovery_identity_are_deterministic_per_cycle() -> None:
    first = make_position_cycle_id(
        exchange="NSE", symbol_id="ABC", position_opened_at=NOW
    )
    replay = make_position_cycle_id(
        exchange="nse", symbol_id="abc", position_opened_at=NOW
    )
    second = make_position_cycle_id(
        exchange="NSE", symbol_id="ABC", position_opened_at="2026-07-16T00:00:00+00:00"
    )
    assert first == replay
    assert first != second
    proposal = make_recovery_proposal_id(
        position_cycle_id=first,
        symbol_id="ABC",
        exchange="NSE",
        recovery_mode=PositionRecoveryMode.REPORT_ONLY,
    )
    assert proposal == make_recovery_proposal_id(
        position_cycle_id=first,
        symbol_id="ABC",
        exchange="NSE",
        recovery_mode=PositionRecoveryMode.REPORT_ONLY,
    )


def _proposal(*, run_id: str, source_uri: str, compatibility_status: str = "no_open_episode") -> dict:
    payload = {
        "recovery_proposal_id": "position-recovery-1",
        "position_cycle_id": "position-cycle-1",
        "symbol_id": "PHOENIXLTD",
        "exchange": "NSE",
        "recovery_mode": "report_only",
        "proposal_status": "PROPOSED",
        "compatibility_status": compatibility_status,
        "created_run_id": run_id,
        "source_lineage": [{"source_uri": source_uri, "source_run_id": run_id}],
    }
    payload["payload_hash"] = recovery_payload_hash(payload)
    return payload


def test_recovery_payload_hash_ignores_run_provenance_but_not_decision_state() -> None:
    first = _proposal(run_id="run-1", source_uri="/runs/1/positions.csv")
    replay = _proposal(run_id="run-2", source_uri="/runs/2/positions.csv")
    changed = _proposal(
        run_id="run-2",
        source_uri="/runs/2/positions.csv",
        compatibility_status="ambiguous_multiple_episodes",
    )

    assert first["payload_hash"] == replay["payload_hash"]
    assert first["payload_hash"] != changed["payload_hash"]


def test_recovery_proposal_accepts_legacy_replay_and_revises_changed_compatibility(tmp_path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    original = _proposal(run_id="run-1", source_uri="/runs/1/positions.csv")
    legacy_payload = {key: value for key, value in original.items() if key != "payload_hash"}
    legacy_hash = hashlib.sha256(
        json.dumps(legacy_payload, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()
    stored_payload = {**original, "payload_hash": legacy_hash}
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute(
            """INSERT INTO position_recovery_proposal
               (recovery_proposal_id, position_cycle_id, symbol_id, exchange,
                recovery_mode, proposal_status, compatibility_status, payload_hash,
                payload_json, created_run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                original["recovery_proposal_id"], original["position_cycle_id"],
                original["symbol_id"], original["exchange"], original["recovery_mode"],
                original["proposal_status"], original["compatibility_status"], legacy_hash,
                json.dumps(stored_payload, sort_keys=True), original["created_run_id"],
            ],
        )

    _persist_recovery_proposal(
        registry,
        _proposal(run_id="run-2", source_uri="/runs/2/positions.csv"),
    )

    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute(
            "SELECT COUNT(*) FROM position_recovery_proposal"
        ).fetchone()[0] == 1

    changed = _proposal(
        run_id="run-3",
        source_uri="/runs/3/positions.csv",
        compatibility_status="ambiguous_multiple_episodes",
    )
    _persist_recovery_proposal(registry, changed)
    with registry._reader() as conn:  # noqa: SLF001
        rows = conn.execute(
            """SELECT recovery_proposal_id, compatibility_status, payload_hash
               FROM position_recovery_proposal ORDER BY compatibility_status"""
        ).fetchall()
    assert len(rows) == 2
    assert changed["recovery_proposal_id"] != original["recovery_proposal_id"]
    assert (
        changed["recovery_proposal_id"],
        "ambiguous_multiple_episodes",
        changed["payload_hash"],
    ) in rows

    replay = _proposal(
        run_id="run-4",
        source_uri="/runs/4/positions.csv",
        compatibility_status="ambiguous_multiple_episodes",
    )
    _persist_recovery_proposal(registry, replay)
    assert replay["recovery_proposal_id"] == changed["recovery_proposal_id"]
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute(
            "SELECT COUNT(*) FROM position_recovery_proposal"
        ).fetchone()[0] == 2

    changed_policy = {**changed, "policy_version": "unexpected-policy"}
    changed_policy["payload_hash"] = recovery_payload_hash(changed_policy)
    with pytest.raises(OpportunityRegistryConflictError):
        _persist_recovery_proposal(registry, changed_policy)



def test_missing_data_incident_dedupes_resolves_and_recurs(tmp_path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    manager = AlertManager(registry)
    payload = {"position_cycle_id": "cycle-1", "missing_data_fields": ["current_close"]}
    first = manager.emit_incident(
        run_id="run-1", alert_type="active_position_missing_market_data",
        severity="critical", message="missing", stage_name="scan_router",
        dedupe_key="missing|cycle-1|close|2026-07-15", payload=payload,
    )
    replay = manager.emit_incident(
        run_id="run-2", alert_type="active_position_missing_market_data",
        severity="critical", message="missing", stage_name="scan_router",
        dedupe_key="missing|cycle-1|close|2026-07-15", payload=payload,
    )
    resolved = manager.resolve_incidents(
        run_id="run-3", alert_type="active_position_missing_market_data",
        position_cycle_id="cycle-1", resolution={"restored_market_session": "2026-07-15"},
    )
    recurrence = manager.emit_incident(
        run_id="run-4", alert_type="active_position_missing_market_data",
        severity="critical", message="missing again", stage_name="scan_router",
        dedupe_key="missing|cycle-1|close|2026-07-15", payload=payload,
    )
    assert (first, replay, resolved, recurrence) == ("EMITTED", "DEDUPLICATED", 1, "RECURRED")
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute("SELECT COUNT(*) FROM pipeline_alert").fetchone()[0] == 2
        assert conn.execute(
            "SELECT status, occurrence_count FROM pipeline_alert_incident"
        ).fetchone() == ("RECURRED", 3)


def test_reviewed_recovery_requires_complete_review_metadata() -> None:
    missing = OpportunityShadowConfig.from_mapping(
        {"opportunity_registry_mode": "shadow", "position_recovery_mode": "reviewed"}
    )
    complete = OpportunityShadowConfig.from_mapping({
        "opportunity_registry_mode": "shadow",
        "position_recovery_mode": "reviewed",
        "position_recovery_reviewed_by": "operator",
        "position_recovery_reviewed_at": "2026-07-15T10:00:00+00:00",
        "position_recovery_review_notes": "fill ledger reconciled",
    })
    assert _recovery_allowed(missing) is False
    assert _recovery_allowed(complete) is True
