"""Diagnostics sprint: /factor-coverage status enum (not_wired / partial / ok)."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from tests.research.perf_tracker.conftest import API_HEADERS, insert_perf_rows


def _seed_mixed_coverage(project: Path) -> None:
    today = date(2026, 5, 8)
    rows = []
    for i in range(10):
        rows.append({
            "run_date": today - timedelta(days=i),
            "symbol_id": f"COV{i}",
            "rank_position": i + 1,
            "watchlist_bucket": "CORE_MOMENTUM",
            "fwd_5d_return": 1.0,
            "fwd_20d_return": 1.0,
            "factor_rs":              float(i),  # 100% coverage
            "factor_vol":             float(i),
            "factor_trend":           float(i),
            "factor_prox":            float(i),
            "factor_deliv":           float(i),
            "factor_sector":          float(i),
            # 60% coverage → partial_coverage
            "factor_momentum_accel":  float(i) if i < 6 else None,
        })
    insert_perf_rows(project, rows)


def test_factor_coverage_status_enum(project: Path, api_client: TestClient) -> None:
    _seed_mixed_coverage(project)
    resp = api_client.get(
        "/api/execution/perf-tracker/factor-coverage",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    rows = {r["factor"]: r for r in resp.json()["factors"]}
    assert rows["rs"]["status"] == "ok"
    assert rows["rs"]["coverage_pct"] == 100.0
    assert rows["momentum_accel"]["status"] == "partial_coverage"
    assert rows["momentum_accel"]["coverage_pct"] == 60.0


def test_factor_coverage_not_wired_when_all_null(
    project: Path,
    api_client: TestClient,
) -> None:
    today = date(2026, 5, 8)
    rows = []
    for i in range(5):
        rows.append({
            "run_date": today - timedelta(days=i),
            "symbol_id": f"NW{i}",
            "rank_position": i + 1,
            "watchlist_bucket": "CORE_MOMENTUM",
            "fwd_5d_return": 1.0,
            "fwd_20d_return": 1.0,
            "factor_rs": float(i),
            "factor_momentum_accel": None,
        })
    insert_perf_rows(project, rows)
    resp = api_client.get(
        "/api/execution/perf-tracker/factor-coverage",
        headers=API_HEADERS,
    )
    rows = {r["factor"]: r for r in resp.json()["factors"]}
    assert rows["momentum_accel"]["status"] == "not_wired"
    assert rows["momentum_accel"]["coverage_pct"] == 0.0
