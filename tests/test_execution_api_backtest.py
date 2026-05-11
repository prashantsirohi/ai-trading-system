"""Execution API: backtest profiles + run endpoints."""

from __future__ import annotations

import os
from pathlib import Path

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
