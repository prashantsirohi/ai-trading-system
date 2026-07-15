from __future__ import annotations

from datetime import datetime, timezone

from ai_trading_system.domains.opportunities.position_monitoring import (
    PositionRecoveryMode,
    make_position_cycle_id,
    make_recovery_proposal_id,
)
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    OpportunityShadowConfig,
)
from ai_trading_system.domains.opportunities.orchestration.service import (
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
