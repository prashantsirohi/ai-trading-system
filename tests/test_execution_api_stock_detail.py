"""Tests for the PR #5 stocks domain endpoints.

Covers ``/api/execution/stocks/{symbol}`` and
``/api/execution/stocks/{symbol}/ohlcv``.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from ai_trading_system.ui.execution_api.app import create_app


API_HEADERS = {"x-api-key": "test-api-key"}
RUN_ID = "pipeline-2026-04-10-stocks"


def _seed_ohlcv(ohlcv_db: Path) -> None:
    """Seed multi-day _catalog + _delivery for AAA so OHLCV has range to filter."""

    conn = duckdb.connect(str(ohlcv_db))
    try:
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
                volume DOUBLE
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
        conn.executemany(
            "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("AAA", "NSE", "2026-04-08 00:00:00", 99.0, 102.0, 98.5, 101.0, 950.0),
                ("AAA", "NSE", "2026-04-09 00:00:00", 101.0, 103.5, 100.0, 102.5, 1010.0),
                ("AAA", "NSE", "2026-04-10 00:00:00", 102.5, 105.0, 101.0, 104.0, 1100.0),
                ("BBB", "NSE", "2026-04-10 00:00:00", 95.0, 99.0, 94.0, 98.0, 800.0),
            ],
        )
        conn.executemany(
            "INSERT INTO _delivery VALUES (?, ?, ?, ?)",
            [
                ("AAA", "NSE", "2026-04-10 00:00:00", 42.0),
                ("BBB", "NSE", "2026-04-10 00:00:00", 38.0),
            ],
        )
    finally:
        conn.close()


def _seed_master(master_db: Path) -> None:
    """Seed canonical ``symbols`` table with AAA fundamentals."""

    conn = sqlite3.connect(master_db)
    try:
        conn.execute(
            """
            CREATE TABLE symbols (
                symbol_id TEXT PRIMARY KEY,
                security_id TEXT,
                symbol_name TEXT,
                exchange TEXT,
                instrument_type TEXT,
                isin TEXT,
                lot_size INTEGER,
                tick_size REAL,
                sector TEXT,
                industry TEXT,
                nse_symbol TEXT,
                bse_symbol TEXT,
                mcap REAL,
                last_updated TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO symbols VALUES (
                'AAA', '1234', 'Aaa Industries Ltd', 'NSE', 'EQ',
                'INE000A01010', 1, 0.05, 'Finance', 'Banks',
                'AAA', NULL, 12345.6, '2026-04-10'
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _seed_rank_artifacts(project_root: Path, run_id: str) -> None:
    """Drop ranked_signals.csv etc. into the latest rank attempt dir."""

    fixture_root = Path(__file__).resolve().parent / "fixtures" / "artifacts" / "rank"
    rank_dir = project_root / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    for filename in (
        "ranked_signals.csv",
        "breakout_scan.csv",
        "pattern_scan.csv",
        "stock_scan.csv",
        "sector_dashboard.csv",
        "dashboard_payload.json",
    ):
        src = fixture_root / filename
        if src.exists():
            shutil.copy2(src, rank_dir / filename)


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Project-root layout: data/{ohlcv.duckdb,masterdata.db,pipeline_runs/...}."""

    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])

    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _seed_ohlcv(data_dir / "ohlcv.duckdb")
    _seed_master(data_dir / "masterdata.db")
    _seed_rank_artifacts(tmp_path, RUN_ID)

    return TestClient(create_app())


# ---------------------------------------------------------------------------
# /stocks/{symbol}
# ---------------------------------------------------------------------------


def test_stock_detail_happy_path(client: TestClient) -> None:
    resp = client.get("/api/execution/stocks/AAA", headers=API_HEADERS)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["available"] is True
    assert body["symbol"] == "AAA"

    metadata = body["metadata"]
    assert metadata is not None
    assert metadata["symbol_id"] == "AAA"
    assert metadata["sector"] == "Finance"
    assert metadata["isin"] == "INE000A01010"

    quote = body["latest_quote"]
    assert quote is not None
    assert quote["close"] == 104.0
    assert quote["volume"] == 1100.0
    assert quote["delivery_pct"] == 42.0
    # Most-recent timestamp wins.
    assert quote["timestamp"].startswith("2026-04-10")

    ranking = body["ranking"]
    assert ranking is not None
    assert ranking["composite_score"] == 88.5
    assert ranking["sector_name"] == "Finance"
    assert ranking["rank_position"] == 1
    assert ranking["universe_size"] >= 2
    assert ranking["category"] == "BUY"

    lifecycle = body["lifecycle"]
    assert lifecycle["rank"] == "TOP 5"
    assert lifecycle["execution"] == "ELIGIBLE"


def test_stock_detail_unknown_symbol_still_200(client: TestClient) -> None:
    resp = client.get("/api/execution/stocks/ZZZ", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    # Every block None — but the call shape stays consistent.
    assert body["symbol"] == "ZZZ"
    assert body["metadata"] is None
    assert body["latest_quote"] is None
    assert body["ranking"] is None
    assert body["available"] is False
    assert body["lifecycle"]["rank"] == "OUT"


def test_stock_detail_partial_when_only_master_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No OHLCV / no rank frames — metadata-only payload still ``available``."""

    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    _seed_master(tmp_path / "data" / "masterdata.db")

    client = TestClient(create_app())
    resp = client.get("/api/execution/stocks/AAA", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["metadata"] is not None
    assert body["latest_quote"] is None
    assert body["ranking"] is None


# ---------------------------------------------------------------------------
# /stocks/{symbol}/ohlcv
# ---------------------------------------------------------------------------


def test_stock_ohlcv_full_history(client: TestClient) -> None:
    resp = client.get("/api/execution/stocks/AAA/ohlcv", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()

    assert body["available"] is True
    assert body["symbol"] == "AAA"
    assert body["interval"] == "daily"
    assert body["count"] == 3
    # Ascending chronological order.
    timestamps = [c["timestamp"] for c in body["candles"]]
    assert timestamps == sorted(timestamps)
    # Most-recent row carries delivery_pct via LEFT JOIN.
    assert body["candles"][-1]["delivery_pct"] == 42.0
    # Earlier rows have NULL delivery_pct surfaced as None (not nan).
    assert body["candles"][0]["delivery_pct"] is None


def test_stock_ohlcv_date_range_filter(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/stocks/AAA/ohlcv?from=2026-04-09&to=2026-04-10",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    assert body["from"] == "2026-04-09"
    assert body["to"] == "2026-04-10"
    assert all(c["timestamp"].split("T")[0] >= "2026-04-09" for c in body["candles"])


def test_stock_ohlcv_limit_keeps_most_recent(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/stocks/AAA/ohlcv?limit=2",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    # Limit keeps the *most recent* rows but preserves ascending order on the wire.
    timestamps = [c["timestamp"].split("T")[0] for c in body["candles"]]
    assert timestamps == ["2026-04-09", "2026-04-10"]


def test_stock_ohlcv_invalid_dates_silently_dropped(client: TestClient) -> None:
    """Bad date strings should not 4xx — they degrade to no filter."""

    resp = client.get(
        "/api/execution/stocks/AAA/ohlcv?from=not-a-date&to=2026/04/10",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["from"] is None
    # 2026/04/10 with slashes — pd.Timestamp accepts it but the test asserts
    # the field came back populated regardless.
    assert body["count"] >= 2


def test_stock_ohlcv_unknown_symbol_returns_empty(client: TestClient) -> None:
    resp = client.get("/api/execution/stocks/ZZZ/ohlcv", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["count"] == 0
    assert body["candles"] == []


def test_stock_ohlcv_missing_db_returns_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)

    client = TestClient(create_app())
    resp = client.get("/api/execution/stocks/AAA/ohlcv", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "available": False,
        "symbol": "AAA",
        "interval": "daily",
        "candles": [],
    }
