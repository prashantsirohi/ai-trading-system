from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.domains.investigator.intake import load_investigator_intake


def _create_catalog(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                is_benchmark BOOLEAN,
                instrument_type VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE _delivery (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                delivery_pct DOUBLE
            )
            """
        )


def _insert_symbol(path: Path, symbol: str, closes: list[float], latest_volume: float = 3000.0) -> str:
    start = date(2026, 5, 1)
    rows = []
    for offset, close in enumerate(closes):
        trade_date = start + timedelta(days=offset)
        volume = latest_volume if offset == len(closes) - 1 else 1000.0
        rows.append((symbol, "NSE", trade_date.isoformat(), close, close, close, close, volume, False, "equity"))
    with duckdb.connect(str(path)) as conn:
        conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.execute("INSERT INTO _delivery VALUES (?, 'NSE', ?, 55)", [symbol, (start + timedelta(days=len(closes) - 1)).isoformat()])
    return (start + timedelta(days=len(closes) - 1)).isoformat()


def _ranked(symbol: str = "AAA") -> pd.DataFrame:
    return pd.DataFrame([{"symbol_id": symbol, "market_cap_cr": 1000, "composite_score": 70, "rank": 10}])


def test_intake_preserves_ranked_sector_name(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _create_catalog(db_path)
    as_of = _insert_symbol(db_path, "AAA", [100, 100, 100, 100, 100, 106])
    ranked = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "market_cap_cr": 1000,
                "composite_score": 70,
                "rank": 10,
                "sector_name": "Pharma",
            }
        ]
    )

    intake = load_investigator_intake(ohlcv_db_path=db_path, ranked_signals=ranked, as_of=as_of)

    assert intake.iloc[0]["sector_name"] == "Pharma"
    assert intake.iloc[0]["sector"] == "Pharma"


def test_daily_gainer_remains_daily_gainer(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _create_catalog(db_path)
    as_of = _insert_symbol(db_path, "AAA", [100, 100, 100, 100, 100, 106])

    intake = load_investigator_intake(ohlcv_db_path=db_path, ranked_signals=_ranked(), as_of=as_of)

    assert intake.iloc[0]["symbol_id"] == "AAA"
    assert intake.iloc[0]["trigger_reason"] == "DAILY_GAINER"


def test_exact_daily_threshold_qualifies_as_daily_gainer(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _create_catalog(db_path)
    as_of = _insert_symbol(db_path, "AAA", [100, 100, 100, 100, 100, 105])

    intake = load_investigator_intake(ohlcv_db_path=db_path, ranked_signals=_ranked(), as_of=as_of)

    assert intake.iloc[0]["daily_return_pct"] == pytest.approx(5.0)
    assert intake.iloc[0]["trigger_reason"] == "DAILY_GAINER"


def test_weekly_gainer_without_daily_spike_qualifies(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _create_catalog(db_path)
    as_of = _insert_symbol(db_path, "WWW", [100, 101.5, 103, 104.5, 106, 108.5], latest_volume=1000)

    intake = load_investigator_intake(ohlcv_db_path=db_path, ranked_signals=_ranked("WWW"), as_of=as_of)

    assert intake.iloc[0]["trigger_reason"] == "WEEKLY_GAINER"
    assert intake.iloc[0]["return_5d"] >= 8.0
    assert intake.iloc[0]["max_daily_gain_5d"] < 5.0


def test_stealth_accumulation_qualifies(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _create_catalog(db_path)
    closes = [
        100.0,
        100.2,
        100.4,
        100.6,
        100.8,
        101.0,
        101.2,
        101.4,
        101.6,
        101.8,
        102.0,
        102.2,
        102.4,
        102.6,
        102.8,
        105.0,
        105.7,
        106.4,
        107.1,
        107.8,
        108.5,
    ]
    as_of = _insert_symbol(db_path, "STEALTH", closes, latest_volume=1000)

    intake = load_investigator_intake(ohlcv_db_path=db_path, ranked_signals=_ranked("STEALTH"), as_of=as_of)

    assert intake.iloc[0]["trigger_reason"] == "STEALTH_ACCUMULATION"
    assert intake.iloc[0]["return_5d"] >= 3.0
    assert intake.iloc[0]["return_20d"] >= 8.0
    assert intake.iloc[0]["green_days_5d"] >= 3


def test_missing_rank_artifacts_do_not_crash_intake(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _create_catalog(db_path)
    as_of = _insert_symbol(db_path, "NORANK", [100, 100, 100, 100, 100, 106])

    intake = load_investigator_intake(ohlcv_db_path=db_path, ranked_signals=pd.DataFrame(), as_of=as_of)

    assert intake.iloc[0]["symbol_id"] == "NORANK"
    assert intake.iloc[0]["trigger_reason"] == "DAILY_GAINER"
