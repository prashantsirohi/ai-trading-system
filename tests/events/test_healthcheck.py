"""Tests for ai-trading-healthcheck market-intel CLI."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from ai_trading_system.integrations import market_intel_client
from ai_trading_system.interfaces.cli import healthcheck


@pytest.fixture(autouse=True)
def _reset_cache():
    market_intel_client.reset_cache()
    yield
    market_intel_client.reset_cache()


def _seed_scheduler_state(db_path: str, *, age_minutes: float, error_count: int = 0) -> None:
    """Initialize a market_intel DB and write one heartbeat row.

    DuckDB's TIMESTAMP column doesn't carry tz info — values are stored as
    naive UTC. To match what the production collector writes, we strip
    tzinfo before INSERT and let _ensure_aware tag it UTC on read.
    """
    from market_intel.storage.db import Database

    Database(db_path=db_path)  # creates schema
    conn = duckdb.connect(db_path)
    try:
        hb = (datetime.now(timezone.utc) - timedelta(minutes=age_minutes)).replace(tzinfo=None)
        conn.execute(
            """
            INSERT INTO scheduler_state (last_heartbeat, last_cycle_at, cycle_stats_json, error_count)
            VALUES (?, ?, ?, ?)
            """,
            [hb, hb, "{}", error_count],
        )
    finally:
        conn.close()


def test_db_missing_returns_down(tmp_path):
    verdict = healthcheck.check_market_intel(
        db_path=str(tmp_path / "does_not_exist.duckdb"),
    )
    assert verdict["status"] == "down"
    assert verdict["exit_code"] == 2
    assert "not found" in verdict["reason"].lower()


def test_no_heartbeat_row_returns_down(tmp_path):
    db_path = str(tmp_path / "mi.duckdb")
    # Init schema but write no scheduler_state row
    from market_intel.storage.db import Database
    Database(db_path=db_path)

    verdict = healthcheck.check_market_intel(db_path=db_path)
    assert verdict["status"] == "down"
    assert verdict["exit_code"] == 2
    assert "never run" in verdict["reason"].lower()


def test_fresh_heartbeat_returns_ok(tmp_path):
    db_path = str(tmp_path / "mi.duckdb")
    _seed_scheduler_state(db_path, age_minutes=2.0)

    verdict = healthcheck.check_market_intel(db_path=db_path)
    assert verdict["status"] == "ok"
    assert verdict["exit_code"] == 0
    assert verdict["age_minutes"] == pytest.approx(2.0, abs=0.5)
    assert verdict["error_count"] == 0


def test_stale_heartbeat_returns_degraded(tmp_path):
    db_path = str(tmp_path / "mi.duckdb")
    _seed_scheduler_state(db_path, age_minutes=30.0)

    verdict = healthcheck.check_market_intel(
        db_path=db_path, max_stale_min=15,
    )
    assert verdict["status"] == "degraded"
    assert verdict["exit_code"] == 1
    assert "old" in verdict["reason"].lower()


def test_error_count_returns_degraded(tmp_path):
    db_path = str(tmp_path / "mi.duckdb")
    _seed_scheduler_state(db_path, age_minutes=2.0, error_count=5)

    verdict = healthcheck.check_market_intel(db_path=db_path)
    assert verdict["status"] == "degraded"
    assert verdict["exit_code"] == 1
    assert "error" in verdict["reason"].lower()


def test_cli_main_returns_proper_exit_code(tmp_path, capsys):
    db_path = str(tmp_path / "mi.duckdb")
    _seed_scheduler_state(db_path, age_minutes=2.0)

    rc = healthcheck.main([
        "market-intel", "--db", db_path, "--max-stale-min", "15",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "OK" in out


def test_cli_json_output(tmp_path, capsys):
    db_path = str(tmp_path / "mi.duckdb")
    _seed_scheduler_state(db_path, age_minutes=2.0)

    rc = healthcheck.main([
        "market-intel", "--db", db_path, "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["error_count"] == 0


def test_cli_returns_3_when_no_subcommand(capsys):
    rc = healthcheck.main([])
    assert rc == 3
