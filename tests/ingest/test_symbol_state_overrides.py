"""Tests for the symbol-state override registry and stale-quarantine sweeper."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from ai_trading_system.domains.ingest.trust import (
    SYMBOL_STATE_BLOCKING,
    clear_symbol_state,
    ensure_data_trust_schema,
    load_blocking_symbol_overrides,
    load_critical_symbol_universe,
    mark_symbol_state,
    sweep_stale_quarantine,
)


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(p))
    # Minimal _catalog schema mirroring the production columns the helpers touch.
    conn.execute("""
        CREATE TABLE _catalog (
            symbol_id VARCHAR NOT NULL,
            security_id VARCHAR,
            exchange VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume BIGINT,
            PRIMARY KEY (symbol_id, exchange, timestamp)
        )
    """)
    conn.close()
    ensure_data_trust_schema(str(p))
    return str(p)


def _seed_quarantine(db: str, symbol: str, trade_date: str, status: str = "active") -> None:
    conn = duckdb.connect(db)
    conn.execute(
        """
        INSERT INTO _catalog_quarantine
            (symbol_id, security_id, exchange, trade_date, reason, status, created_at)
        VALUES (?, ?, 'NSE', CAST(? AS DATE), ?, ?, CURRENT_TIMESTAMP)
        """,
        [symbol, symbol, trade_date, "test", status],
    )
    conn.close()


def _seed_catalog_row(db: str, symbol: str, trade_date: str) -> None:
    conn = duckdb.connect(db)
    conn.execute(
        """
        INSERT INTO _catalog (symbol_id, security_id, exchange, timestamp,
                              open, high, low, close, volume)
        VALUES (?, ?, 'NSE', CAST(? AS TIMESTAMP), 100, 110, 95, 105, 1000)
        """,
        [symbol, symbol, trade_date],
    )
    conn.close()


def test_mark_and_clear_symbol_state(db_path: str):
    mark_symbol_state(db_path, symbol_id="AEPL", exchange="NSE", state="suspended", reason="ASM stage 4")
    blocked = load_blocking_symbol_overrides(db_path)
    assert "AEPL" in blocked

    deleted = clear_symbol_state(db_path, symbol_id="AEPL", exchange="NSE")
    assert deleted == 1
    assert "AEPL" not in load_blocking_symbol_overrides(db_path)


def test_blocking_states_only_returned_for_blocking_symbols(db_path: str):
    mark_symbol_state(db_path, symbol_id="X", exchange="NSE", state="delisted")
    mark_symbol_state(db_path, symbol_id="Y", exchange="NSE", state="suspended")
    mark_symbol_state(db_path, symbol_id="Z", exchange="NSE", state="permanently_unavailable")
    mark_symbol_state(db_path, symbol_id="W", exchange="NSE", state="corporate_action")

    blocked = load_blocking_symbol_overrides(db_path)
    assert blocked == {"X", "Y", "Z"}
    assert "W" not in blocked


def test_load_critical_symbol_universe_excludes_blocked_overrides(db_path: str):
    # Seed two liquid symbols
    for d in range(1, 11):
        _seed_catalog_row(db_path, "GOOD", f"2026-04-{d:02d}")
        _seed_catalog_row(db_path, "BAD", f"2026-04-{d:02d}")

    # Without overrides, both qualify
    universe = load_critical_symbol_universe(db_path, run_date="2026-04-15", min_recent_days=1)
    assert "GOOD" in universe
    assert "BAD" in universe

    # Add a blocking override for BAD
    mark_symbol_state(db_path, symbol_id="BAD", exchange="NSE", state="suspended",
                      effective_from="2026-04-01")

    universe2 = load_critical_symbol_universe(db_path, run_date="2026-04-15", min_recent_days=1)
    assert "GOOD" in universe2
    assert "BAD" not in universe2


def test_sweep_stale_quarantine_flips_long_stuck_rows(db_path: str):
    # OLD: quarantined 30 days ago, no recent catalog rows → should flip
    _seed_quarantine(db_path, "OLD", "2026-04-01")
    # FRESH: quarantined 3 days ago → should NOT flip
    _seed_quarantine(db_path, "FRESH", "2026-05-01")
    # RECENT_DATA: quarantined 30 days ago BUT has recent catalog → should NOT flip
    _seed_quarantine(db_path, "RECENT_DATA", "2026-04-01")
    _seed_catalog_row(db_path, "RECENT_DATA", "2026-04-15")

    counts = sweep_stale_quarantine(db_path, run_date="2026-05-04", stale_days=14)
    assert counts["marked"] == 1

    # OLD got promoted to permanently_unavailable in overrides
    blocked = load_blocking_symbol_overrides(db_path, run_date="2026-05-04")
    assert "OLD" in blocked
    assert "FRESH" not in blocked
    assert "RECENT_DATA" not in blocked


def test_sweep_stale_quarantine_idempotent(db_path: str):
    _seed_quarantine(db_path, "OLD", "2026-04-01")
    counts1 = sweep_stale_quarantine(db_path, run_date="2026-05-04", stale_days=14)
    counts2 = sweep_stale_quarantine(db_path, run_date="2026-05-04", stale_days=14)
    assert counts1["marked"] == 1
    assert counts2["marked"] == 0


def test_blocking_states_constant_locked():
    # Locks contract — change requires deliberate review.
    assert SYMBOL_STATE_BLOCKING == frozenset({"delisted", "suspended", "permanently_unavailable"})
