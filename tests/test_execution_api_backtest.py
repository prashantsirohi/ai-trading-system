"""Execution API: backtest profiles + run endpoints."""

from __future__ import annotations

import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from ai_trading_system.ui.execution_api.app import create_app


API_HEADERS = {"x-api-key": "test-api-key"}


@pytest.fixture(autouse=True)
def _api_key(monkeypatch):
    monkeypatch.setenv("EXECUTION_API_KEY", "test-api-key")


@pytest.fixture
def client():
    return TestClient(create_app())


def test_list_profiles_returns_three_starter_profiles(client):
    r = client.get("/api/execution/backtest/profiles", headers=API_HEADERS)
    assert r.status_code == 200
    names = {p["name"] for p in r.json()["profiles"]}
    assert {"aggressive_momentum", "balanced_swing", "positional_trend"} <= names


def test_profile_payload_includes_all_sections(client):
    r = client.get("/api/execution/backtest/profiles", headers=API_HEADERS)
    profile = next(p for p in r.json()["profiles"] if p["name"] == "balanced_swing")
    for section in ("entry", "stop", "exit", "sizing", "constraints"):
        assert section in profile and isinstance(profile[section], dict)
    assert profile["stop"]["method"] == "hybrid"
    assert profile["exit"]["dma_exit_window"] == 20


def test_run_backtest_no_data_returns_status_no_data(client, tmp_path, monkeypatch):
    # Point AI_TRADING_PROJECT_ROOT at empty tmp dir → no pipeline_runs → no_data response
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    r = client.post(
        "/api/execution/backtest/run",
        headers=API_HEADERS,
        json={"profile": "balanced_swing"},
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["status"] == "no_data"
    assert payload["trade_count"] == 0


def test_run_backtest_with_seeded_runs(client, tmp_path, monkeypatch):
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    base = tmp_path / "data" / "pipeline_runs"

    def _row(close=100.0, **over):
        row = {
            "symbol_id": "ACME",
            "exchange": "NSE",
            "close": close,
            "composite_score": 80.0,
            "eligible_rank": 1,
            "is_stage2_uptrend": True,
            "sector_name": "TECH",
            "sector_strength_score": 0.7,
            "sma_11": 99.0,
            "sma_20": 97.0,
            "sma_50": 92.0,
            "sma_200": 80.0,
            "atr_14": 2.0,
            "volume_ratio_20": 2.0,
            "swing_low_20": 94.0,
            "delivery_pct": 60.0,
        }
        row.update(over)
        return row

    for run_date, close in [("2026-03-31", 100.0), ("2026-04-01", 102.0)]:
        rank_dir = base / f"pipeline-{run_date}-aaaaaaaa" / "rank" / "attempt_1"
        rank_dir.mkdir(parents=True)
        pd.DataFrame([_row(close=close)]).to_csv(rank_dir / "ranked_signals.csv", index=False)
    # Day 3: close below 20DMA → engine exit
    rank_dir = base / "pipeline-2026-04-02-bbbbbbbb" / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True)
    pd.DataFrame([_row(close=97.0, sma_20=100.0)]).to_csv(
        rank_dir / "ranked_signals.csv", index=False
    )

    r = client.post(
        "/api/execution/backtest/run",
        headers=API_HEADERS,
        json={"profile": "balanced_swing", "equity": 500_000, "persist": False},
    )
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["status"] == "ok"
    assert payload["profile"] == "balanced_swing"
    assert payload["trade_count"] >= 1
    assert payload["trading_days"] == 3
    assert "trades" in payload and len(payload["trades"]) >= 1
    trade = payload["trades"][0]
    assert trade["entry_reason"] == "entry_confirmed"
    assert trade["stop_price"] is not None


def test_run_backtest_accepts_custom_config_overrides(client, tmp_path, monkeypatch):
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    base = tmp_path / "data" / "pipeline_runs"
    rank_dir = base / "pipeline-2026-04-01-aaaaaaaa" / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "symbol_id": "ACME",
                "exchange": "NSE",
                "close": 100.0,
                "composite_score": 80.0,
                "eligible_rank": 1,
                "is_stage2_uptrend": True,
                "sector_name": "TECH",
                "sector_strength_score": 0.7,
                "sma_11": 99.0,
                "sma_20": 97.0,
                "sma_50": 92.0,
                "sma_200": 80.0,
                "atr_14": 2.0,
                "volume_ratio_20": 2.0,
                "swing_low_20": 94.0,
                "delivery_pct": 60.0,
            }
        ]
    ).to_csv(rank_dir / "ranked_signals.csv", index=False)

    r = client.post(
        "/api/execution/backtest/run",
        headers=API_HEADERS,
        json={
            "profile": "balanced_swing",
            "persist": False,
            "custom_config": {"entry": {"min_volume_ratio": 3.0}},
        },
    )

    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["profile"] == "custom:balanced_swing"
    assert payload["status"] == "ok"
    assert payload["trade_count"] == 0


def test_research_dynamic_returns_sync_quality_and_metadata(client, tmp_path, monkeypatch):
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)

    master = sqlite3.connect(data_dir / "masterdata.db")
    master.execute("CREATE TABLE stock_details (Symbol TEXT PRIMARY KEY, Sector TEXT)")
    master.execute("INSERT INTO stock_details VALUES ('AAA', 'TECH')")
    master.commit()
    master.close()

    conn = duckdb.connect(str(data_dir / "ohlcv.duckdb"))
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            security_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            parquet_file VARCHAR,
            ingestion_version BIGINT,
            ingestion_ts TIMESTAMP
        )
        """
    )
    start = date(2025, 1, 1)
    rows = [
        ("AAA", None, "NSE", start + timedelta(days=i), 100 + i, 101 + i, 99 + i, 100 + i, 1000 + i, None, 1, start + timedelta(days=i))
        for i in range(240)
    ]
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()

    r = client.post(
        "/api/execution/backtest/run",
        headers=API_HEADERS,
        json={
            "profile": "balanced_swing",
            "data_source": "research_dynamic",
            "from_date": "2025-08-10",
            "to_date": "2025-08-28",
            "persist": True,
            "custom_config": {"entry": {"require_sector_positive": False}},
        },
    )

    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["status"] == "ok"
    assert payload["data_source"] == "research_dynamic"
    assert payload["sync"]["status"] == "applied"
    assert payload["sync"]["masterdata"]["status"] == "applied"
    assert payload["data_quality"]["status"] == "ok"
    assert payload["data_quality"]["masterdata_exists"] is True
    assert payload["run_metadata"]["ranking_method_version"] == "research_dynamic_v3_canonical_factor_scoring_stage2_benchmark"
    artifact_dir = tmp_path / payload["artifact_dir"]
    assert (artifact_dir / "metadata.json").exists()


def test_winner_capture_endpoint_syncs_and_returns_defaults(client, tmp_path, monkeypatch):
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)

    conn = duckdb.connect(str(data_dir / "ohlcv.duckdb"))
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            security_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            parquet_file VARCHAR,
            ingestion_version BIGINT,
            ingestion_ts TIMESTAMP
        )
        """
    )
    rows = [
        ("AAA", None, "NSE", date(2025, 1, 1), 100, 100, 100, 100, 1000, None, 1, date(2025, 1, 1)),
        ("AAA", None, "NSE", date(2025, 12, 31), 200, 200, 200, 200, 1000, None, 1, date(2025, 12, 31)),
        ("BBB", None, "NSE", date(2025, 1, 1), 100, 100, 100, 100, 1000, None, 1, date(2025, 1, 1)),
        ("BBB", None, "NSE", date(2025, 12, 31), 150, 150, 150, 150, 1000, None, 1, date(2025, 12, 31)),
    ]
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()

    def _ranked(*_, **__):
        return {
            date(2025, 2, 1): pd.DataFrame(
                [
                    {
                        "symbol_id": "AAA",
                        "eligible_rank": 1,
                        "composite_score_adjusted": 95.0,
                        "close": 120.0,
                    }
                ]
            )
        }

    monkeypatch.setattr(
        "ai_trading_system.research.backtesting.winner_capture.load_research_ranked_by_date",
        _ranked,
    )

    r = client.post(
        "/api/execution/backtest/winner-capture",
        headers=API_HEADERS,
        json={"year": 2025, "persist": False},
    )

    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["status"] == "ok"
    assert payload["top_gainers"] == 50
    assert payload["rank_cutoff"] == 50
    assert payload["sync"]["status"] == "applied"
    assert payload["summary"]["winner_count"] == 2
    assert payload["summary"]["captured_count"] == 1
    assert payload["winners"][0]["symbol_id"] == "AAA"
    assert payload["winners"][0]["captured"] is True
