from __future__ import annotations

from datetime import datetime, timezone

import duckdb

from ai_trading_system.domains.execution.models import FillRecord
from ai_trading_system.domains.execution.store import ExecutionStore


def _fill(fill_id: str, side: str, quantity: int, when: str) -> FillRecord:
    return FillRecord(
        fill_id=fill_id,
        order_id=f"order-{fill_id}",
        broker="paper",
        symbol_id="AAA",
        quantity=quantity,
        price=100.0,
        filled_at=datetime.fromisoformat(when).replace(tzinfo=timezone.utc),
        side=side,
        exchange="NSE",
    )


def test_position_cycles_and_recent_exit_use_fill_ledger_only(tmp_path) -> None:
    store = ExecutionStore(tmp_path, db_path=tmp_path / "execution.duckdb")
    store.append_fills([
        _fill("1", "BUY", 10, "2026-07-01T10:00:00"),
        _fill("2", "SELL", 10, "2026-07-08T10:00:00"),
    ])
    cycle = store.list_position_cycles()[0]
    assert cycle.active is False
    assert cycle.last_exited_at.startswith("2026-07-08")

    ohlcv = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(ohlcv))
    conn.execute("CREATE TABLE _catalog(timestamp TIMESTAMP)")
    conn.executemany("INSERT INTO _catalog VALUES (?)", [(f"2026-07-{day:02d}",) for day in range(1, 15)])
    conn.close()
    recent = store.list_recently_exited_positions(ohlcv_db_path=ohlcv, as_of="2026-07-14", cooling_sessions=10)
    assert [item.symbol_id for item in recent] == ["AAA"]


def test_recent_exit_boundary_expires_by_trading_session_and_new_cycle_wins(tmp_path) -> None:
    store = ExecutionStore(tmp_path, db_path=tmp_path / "execution.duckdb")
    store.append_fills([
        _fill("1", "BUY", 10, "2026-07-01T10:00:00"),
        _fill("2", "SELL", 10, "2026-07-02T10:00:00"),
    ])
    ohlcv = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(ohlcv))
    conn.execute("CREATE TABLE _catalog(timestamp TIMESTAMP)")
    sessions = [f"2026-07-{day:02d}" for day in range(2, 14)]
    conn.executemany("INSERT INTO _catalog VALUES (?)", [(session,) for session in sessions])
    conn.close()
    inside = store.list_recently_exited_positions(
        ohlcv_db_path=ohlcv, as_of="2026-07-11", cooling_sessions=10
    )
    expired = store.list_recently_exited_positions(
        ohlcv_db_path=ohlcv, as_of="2026-07-12", cooling_sessions=10
    )
    assert [item.symbol_id for item in inside] == ["AAA"]
    assert expired == []

    store.append_fills([_fill("3", "BUY", 5, "2026-07-13T10:00:00")])
    cycle = store.list_position_cycles()[0]
    assert cycle.active is True
    assert cycle.position_cycle_id
    assert store.list_recently_exited_positions(
        ohlcv_db_path=ohlcv, as_of="2026-07-13", cooling_sessions=10
    ) == []
