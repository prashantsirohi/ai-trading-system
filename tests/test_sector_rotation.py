from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sqlite3

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from ai_trading_system.analytics.sector_rotation.compute import compute_sector_rotation
from ai_trading_system.analytics.sector_rotation.contracts import (
    bucket_outperformance,
    classify_quadrant,
)
from ai_trading_system.domains.ranking.sector_rotation import run_sector_rotation
from ai_trading_system.ui.execution_api.app import create_app
from tests.smoke.test_execution_api_smoke import API_HEADERS, _seed_execution_project


def test_quadrant_classification_all_states() -> None:
    assert classify_quadrant(101, 101) == "Leading"
    assert classify_quadrant(101, 99) == "Weakening"
    assert classify_quadrant(99, 99) == "Lagging"
    assert classify_quadrant(99, 101) == "Improving"


@pytest.mark.parametrize(
    ("alpha", "bucket"),
    [
        (0.10, "Major Outperformance"),
        (0.05, "Significant Outperformance"),
        (0.02, "Minor Outperformance"),
        (0.0, "Same as Benchmark"),
        (-0.02, "Minor Underperformance"),
        (-0.05, "Significant Underperformance"),
        (-0.10, "Major Underperformance"),
    ],
)
def test_outperformance_bucket_boundaries(alpha: float, bucket: str) -> None:
    assert bucket_outperformance(alpha) == bucket


def test_equal_weight_fallback_when_market_cap_missing(tmp_path: Path) -> None:
    ohlcv_db, master_db = _seed_rotation_datastores(tmp_path, include_market_cap=False, include_delivery=True)

    result = compute_sector_rotation(
        ohlcv_db_path=ohlcv_db,
        master_db_path=master_db,
        run_date="2026-04-30",
        ranked_df=_ranked_fixture(),
    )

    assert not result.sector_custom_indices.empty
    assert set(result.sector_custom_indices["weighting_method"]) == {"equal_weight"}


def test_no_crash_when_delivery_missing(tmp_path: Path) -> None:
    ohlcv_db, master_db = _seed_rotation_datastores(tmp_path, include_market_cap=True, include_delivery=False)

    result = compute_sector_rotation(
        ohlcv_db_path=ohlcv_db,
        master_db_path=master_db,
        run_date="2026-04-30",
        ranked_df=_ranked_fixture(),
    )

    assert not result.accumulation_distribution.empty
    assert set(result.accumulation_distribution["delivery_signal"]) == {"Neutral"}


def test_benchmark_fallback_from_nifty500_to_univ_top1000(tmp_path: Path) -> None:
    ohlcv_db, master_db = _seed_rotation_datastores(tmp_path, include_market_cap=True, include_delivery=True)

    result = compute_sector_rotation(
        ohlcv_db_path=ohlcv_db,
        master_db_path=master_db,
        run_date="2026-04-30",
        ranked_df=_ranked_fixture(),
    )

    assert result.metadata["benchmark_name"] == "UNIV_TOP1000"
    assert not result.sector_rotation.empty


def test_sector_rotation_artifact_creation(tmp_path: Path) -> None:
    ohlcv_db, master_db = _seed_rotation_datastores(tmp_path, include_market_cap=True, include_delivery=True)
    output_dir = tmp_path / "attempt_1"

    frames = run_sector_rotation(
        ohlcv_db_path=ohlcv_db,
        master_db_path=master_db,
        run_date="2026-04-30",
        output_dir=output_dir,
        ranked_df=_ranked_fixture(),
    )
    for artifact_type in (
        "sector_rotation",
        "stock_rotation",
        "accumulation_distribution",
        "sector_custom_indices",
    ):
        frames[artifact_type].to_csv(output_dir / f"{artifact_type}.csv", index=False)

    assert (output_dir / "sector_rotation.csv").exists()
    assert (output_dir / "stock_rotation.csv").exists()
    assert (output_dir / "accumulation_distribution.csv").exists()
    assert (output_dir / "sector_custom_indices.csv").exists()
    assert (output_dir / "sector_rotation_payload.json").exists()


def test_sector_rotation_workspace_endpoint(monkeypatch, tmp_path: Path) -> None:
    run_id = _seed_execution_project(tmp_path)
    rank_dir = tmp_path / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    pd.DataFrame(
        [
            {
                "date": "2026-04-10",
                "industry": "Banks",
                "sector_index": 110.0,
                "benchmark_index": 100.0,
                "rs_ratio": 101.0,
                "rs_momentum": 102.0,
                "quadrant": "Leading",
                "alpha_20d": 0.05,
            }
        ]
    ).to_csv(rank_dir / "sector_rotation.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "industry": "Banks",
                "quadrant": "Leading",
                "sector_quadrant": "Leading",
                "rotation_adjusted_score": 82.0,
                "delivery_signal": "Accumulation",
                "watchlist_candidate": True,
            }
        ]
    ).to_csv(rank_dir / "stock_rotation.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "delivery_signal": "Accumulation",
                "accumulation_score": 78.0,
            }
        ]
    ).to_csv(rank_dir / "accumulation_distribution.csv", index=False)
    pd.DataFrame(
        [
            {
                "date": "2026-04-10",
                "industry": "Banks",
                "sector_index": 110.0,
                "weighting_method": "equal_weight",
                "constituent_count": 1,
            }
        ]
    ).to_csv(rank_dir / "sector_custom_indices.csv", index=False)

    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    client = TestClient(create_app())

    response = client.get("/api/execution/workspace/sector-rotation", headers=API_HEADERS)

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == run_id
    assert payload["sectors"][0]["industry"] == "Banks"
    assert payload["stocks"][0]["symbol"] == "AAA"
    assert payload["accumulation"][0]["symbol"] == "AAA"
    assert payload["distribution"] == []


def _seed_rotation_datastores(
    root: Path,
    *,
    include_market_cap: bool,
    include_delivery: bool,
) -> tuple[Path, Path]:
    ohlcv_db = root / "ohlcv.duckdb"
    conn = duckdb.connect(str(ohlcv_db))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp DATE,
                close DOUBLE,
                volume DOUBLE
            )
            """
        )
        if include_delivery:
            conn.execute(
                """
                CREATE TABLE _delivery (
                    symbol_id VARCHAR,
                    exchange VARCHAR,
                    timestamp DATE,
                    delivery_pct DOUBLE
                )
                """
            )
        start = date(2026, 1, 1)
        rows = []
        delivery_rows = []
        symbols = ["AAA", "BBB", "CCC", "DDD"]
        for day in range(120):
            current = start + timedelta(days=day)
            for idx, symbol in enumerate(symbols):
                close = 100 + day * (1.0 + idx * 0.15) + idx
                rows.append((symbol, "NSE", current.isoformat(), close, 1000 + day * 10 + idx))
                if include_delivery:
                    delivery_rows.append((symbol, "NSE", current.isoformat(), 40 + (day % 25) + idx))
        conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?)", rows)
        if include_delivery:
            conn.executemany("INSERT INTO _delivery VALUES (?, ?, ?, ?)", delivery_rows)
    finally:
        conn.close()

    master_db = root / "masterdata.db"
    sqlite_conn = sqlite3.connect(master_db)
    try:
        market_cap_column = ", market_cap_cr REAL" if include_market_cap else ""
        sqlite_conn.execute(
            f'CREATE TABLE stock_details (Symbol TEXT, exchange TEXT, Sector TEXT, "Company Name" TEXT{market_cap_column})'
        )
        values = [
            ("AAA", "NSE", "Banks", "AAA Bank", 1000.0),
            ("BBB", "NSE", "Banks", "BBB Bank", 900.0),
            ("CCC", "NSE", "IT", "CCC Tech", 800.0),
            ("DDD", "NSE", "IT", "DDD Tech", 700.0),
        ]
        if include_market_cap:
            sqlite_conn.executemany("INSERT INTO stock_details VALUES (?, ?, ?, ?, ?)", values)
        else:
            sqlite_conn.executemany(
                "INSERT INTO stock_details VALUES (?, ?, ?, ?)",
                [row[:4] for row in values],
            )
        sqlite_conn.commit()
    finally:
        sqlite_conn.close()
    return ohlcv_db, master_db


def _ranked_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 82.0, "prox_high": 5.0},
            {"symbol_id": "BBB", "composite_score": 74.0, "prox_high": 8.0},
            {"symbol_id": "CCC", "composite_score": 68.0, "prox_high": 20.0},
            {"symbol_id": "DDD", "composite_score": 72.0, "prox_high": 10.0},
        ]
    )
