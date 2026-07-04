from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.investigator.exit_monitor import attach_exit_monitoring


def _seed_prices(path: Path, closes: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                close DOUBLE,
                is_benchmark BOOLEAN
            )
            """
        )
        rows = [
            ("AAA", "NSE", f"2026-05-{day:02d}", close, False)
            for day, close in enumerate(closes, start=1)
        ]
        conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


def test_exit_monitor_flags_invalidation_breach(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _seed_prices(db_path, [100, 99, 98, 94])
    gate = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-01",
                "final_score": 65,
                "invalidation_level": "95",
            }
        ]
    )

    monitored = attach_exit_monitoring(gate, ohlcv_db_path=db_path, as_of="2026-05-04")

    assert bool(monitored.iloc[0]["invalidation_breached"]) is True
    assert bool(monitored.iloc[0]["exit_triggered"]) is True
    assert monitored.iloc[0]["exit_reason"] == "INVALIDATION_BREACH"


def test_exit_monitor_flags_failed_3d_followthrough(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _seed_prices(db_path, [100, 99, 98, 97])
    gate = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-01",
                "final_score": 65,
                "invalidation_level": "90",
            }
        ]
    )

    monitored = attach_exit_monitoring(gate, ohlcv_db_path=db_path, as_of="2026-05-04")

    assert monitored.iloc[0]["followthrough_status"] == "FAILED_3D"
    assert bool(monitored.iloc[0]["exit_triggered"]) is True
    assert monitored.iloc[0]["exit_reason"] == "FAILED_3D_FOLLOWTHROUGH"


def test_exit_monitor_flags_score_below_55(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _seed_prices(db_path, [100, 101])
    gate = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-01",
                "final_score": 54,
                "invalidation_level": "90",
            }
        ]
    )

    monitored = attach_exit_monitoring(gate, ohlcv_db_path=db_path, as_of="2026-05-02")

    assert bool(monitored.iloc[0]["exit_triggered"]) is True
    assert monitored.iloc[0]["exit_reason"] == "SCORE_BELOW_55"


def test_exit_monitor_missing_price_data_is_unknown(tmp_path: Path) -> None:
    db_path = tmp_path / "missing.duckdb"
    gate = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-01",
                "final_score": 65,
                "invalidation_level": "90",
            }
        ]
    )

    monitored = attach_exit_monitoring(gate, ohlcv_db_path=db_path, as_of="2026-05-02")

    assert monitored.iloc[0]["followthrough_status"] == "UNKNOWN"
    assert bool(monitored.iloc[0]["exit_triggered"]) is False
    assert monitored.iloc[0]["exit_reason"] == "UNKNOWN_DATA"
