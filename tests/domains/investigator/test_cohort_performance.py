from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.domains.investigator.cohort_performance import (
    build_performance_summary,
    build_threshold_recommendations,
    mature_investigator_cohorts,
)


def _create_cohort_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE investigator_cohort_performance (
            trade_date DATE,
            symbol_id VARCHAR,
            exchange VARCHAR,
            trigger_reason VARCHAR,
            verdict VARCHAR,
            final_score DOUBLE,
            hard_trap_flag BOOLEAN,
            credible_trigger BOOLEAN,
            move_tag VARCHAR,
            sector VARCHAR,
            close DOUBLE,
            fwd_3d_return DOUBLE,
            fwd_5d_return DOUBLE,
            fwd_10d_return DOUBLE,
            fwd_20d_return DOUBLE,
            fwd_3d_matured_at DATE,
            fwd_5d_matured_at DATE,
            fwd_10d_matured_at DATE,
            fwd_20d_matured_at DATE,
            data_quality_status VARCHAR,
            inserted_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """
    )


def _seed_prices(path: Path, symbol: str = "AAA", closes: list[float] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    closes = closes or [100 + i for i in range(25)]
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
            (symbol, "NSE", f"2026-05-{day:02d}", close, False)
            for day, close in enumerate(closes, start=1)
        ]
        conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


def test_mature_investigator_cohorts_calculates_forward_returns(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    _seed_prices(ohlcv)
    conn = duckdb.connect(":memory:")
    _create_cohort_table(conn)
    conn.execute(
        """
        INSERT INTO investigator_cohort_performance
        (trade_date, symbol_id, exchange, trigger_reason, verdict, final_score, hard_trap_flag, credible_trigger,
         move_tag, sector, close, data_quality_status, inserted_at, updated_at)
        VALUES ('2026-05-01', 'AAA', 'NSE', 'DAILY_GAINER', 'HIGH_CONVICTION', 85, false, true,
                'SECTOR_ROTATION', 'Finance', 100, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
    )

    assert mature_investigator_cohorts(conn, ohlcv_db_path=ohlcv) == 1
    row = conn.execute(
        """
        SELECT fwd_3d_return, fwd_5d_return, fwd_10d_return, fwd_20d_return,
               fwd_3d_matured_at, data_quality_status
        FROM investigator_cohort_performance
        """
    ).fetchone()

    assert row[0] == pytest.approx(3.0)
    assert row[1] == pytest.approx(5.0)
    assert row[2] == pytest.approx(10.0)
    assert row[3] == pytest.approx(20.0)
    assert str(row[4]) == "2026-05-04"
    assert row[5] == "MATURED"

    assert mature_investigator_cohorts(conn, ohlcv_db_path=ohlcv) == 0
    assert conn.execute("SELECT COUNT(*) FROM investigator_cohort_performance").fetchone()[0] == 1


def test_mature_investigator_cohorts_handles_partial_and_missing_prices(tmp_path: Path) -> None:
    partial_ohlcv = tmp_path / "partial.duckdb"
    _seed_prices(partial_ohlcv, closes=[100, 101, 102, 103, 104, 105])
    conn = duckdb.connect(":memory:")
    _create_cohort_table(conn)
    conn.execute(
        """
        INSERT INTO investigator_cohort_performance
        (trade_date, symbol_id, exchange, close, data_quality_status, inserted_at, updated_at)
        VALUES
        ('2026-05-01', 'AAA', 'NSE', 100, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('2026-05-01', 'MISSING', 'NSE', 100, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
    )

    mature_investigator_cohorts(conn, ohlcv_db_path=partial_ohlcv)
    rows = conn.execute(
        "SELECT symbol_id, fwd_3d_return, fwd_5d_return, fwd_10d_return, data_quality_status FROM investigator_cohort_performance ORDER BY symbol_id"
    ).fetchall()

    assert rows[0][0] == "AAA"
    assert rows[0][1] == pytest.approx(3.0)
    assert rows[0][2] == pytest.approx(5.0)
    assert rows[0][3] is None
    assert rows[0][4] == "PARTIAL_MATURED"
    assert rows[1][0] == "MISSING"
    assert rows[1][4] == "INSUFFICIENT_PRICE_DATA"


def test_performance_summary_groups_metrics_and_empty_table() -> None:
    conn = duckdb.connect(":memory:")
    _create_cohort_table(conn)

    empty_frame, empty_summary = build_performance_summary(conn)
    assert empty_frame.empty
    assert empty_summary["total_cohorts"] == 0

    rows = pd.DataFrame(
        [
            {
                "trade_date": "2026-05-01",
                "symbol_id": "A",
                "exchange": "NSE",
                "trigger_reason": "DAILY_GAINER",
                "verdict": "HIGH_CONVICTION",
                "final_score": 86,
                "hard_trap_flag": False,
                "credible_trigger": True,
                "move_tag": "SECTOR_ROTATION",
                "sector": "Finance",
                "close": 100,
                "fwd_5d_return": 6.0,
                "data_quality_status": "PARTIAL_MATURED",
            },
            {
                "trade_date": "2026-05-01",
                "symbol_id": "B",
                "exchange": "NSE",
                "trigger_reason": "DAILY_GAINER",
                "verdict": "MEDIUM_CONVICTION",
                "final_score": 66,
                "hard_trap_flag": False,
                "credible_trigger": True,
                "move_tag": "SECTOR_ROTATION",
                "sector": "Finance",
                "close": 100,
                "fwd_5d_return": -2.0,
                "data_quality_status": "PARTIAL_MATURED",
            },
        ]
    )
    conn.register("rows", rows)
    conn.execute("INSERT INTO investigator_cohort_performance BY NAME SELECT * FROM rows")
    conn.unregister("rows")

    frame, summary = build_performance_summary(conn)
    trigger_5d = frame.loc[
        frame["group_type"].eq("trigger_reason")
        & frame["group_value"].eq("DAILY_GAINER")
        & frame["horizon"].eq("5d")
    ].iloc[0]

    assert summary["total_cohorts"] == 2
    assert summary["matured_by_horizon"]["5d"] == 2
    assert trigger_5d["sample_count"] == 2
    assert trigger_5d["win_rate"] == 50.0
    assert trigger_5d["avg_return"] == pytest.approx(2.0)
    assert trigger_5d["hit_rate_above_5pct"] == 50.0


def test_threshold_recommendations_are_diagnostic_only() -> None:
    insufficient = build_threshold_recommendations(pd.DataFrame(), {"matured_by_horizon": {"5d": 99}})
    assert insufficient["insufficient_sample"] is True
    assert insufficient["recommendation"] == "Do not tune thresholds yet."

    frame = pd.DataFrame(
        [
            {
                "group_type": "trigger_reason",
                "group_value": "DAILY_GAINER",
                "horizon": "5d",
                "sample_count": 30,
                "win_rate": 60.0,
                "avg_return": 4.0,
            },
            {
                "group_type": "move_tag",
                "group_value": "WEAK_MOVE",
                "horizon": "5d",
                "sample_count": 30,
                "win_rate": 35.0,
                "avg_return": -1.0,
            },
        ]
    )
    sufficient = build_threshold_recommendations(frame, {"matured_by_horizon": {"5d": 100}})
    assert sufficient["insufficient_sample"] is False
    assert sufficient["recommendations"]
