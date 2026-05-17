"""Diagnostics sprint: /drift watch tier + unreliable_coverage tier."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from tests.research.perf_tracker.conftest import API_HEADERS, insert_perf_rows


def test_drift_insufficient_sample_below_1500(project: Path, api_client: TestClient) -> None:
    today = date(2026, 5, 8)
    rows = []
    for i in range(60):  # well below 1500
        rows.append({
            "run_date": today - timedelta(days=i % 5),
            "symbol_id": f"DR{i}",
            "rank_position": i % 10 + 1,
            "watchlist_bucket": "CORE_MOMENTUM",
            "fwd_5d_return": 1.0,
            "fwd_20d_return": 1.0,
            "factor_rs": float(i),
            "factor_vol": float(i),
            "factor_trend": float(i),
            "factor_prox": float(i),
            "factor_deliv": float(i),
            "factor_sector": float(i),
            "factor_momentum_accel": float(i),
        })
    insert_perf_rows(project, rows)
    resp = api_client.get("/api/execution/perf-tracker/drift", headers=API_HEADERS)
    assert resp.status_code == 200
    statuses = {r["status"] for r in resp.json()["factors"]}
    assert "insufficient_sample" in statuses
    assert "warning" not in statuses
    assert "critical" not in statuses


def test_drift_watch_tier_when_sample_in_mid_band(
    project: Path,
    api_client: TestClient,
) -> None:
    """1500 ≤ recent_n < 3000 should cap status at 'watch' even if delta is large."""
    today = date(2026, 5, 8)
    rows = []
    # Recent window (last 30d): 2000 rows with inverted relationship.
    for i in range(2000):
        score = float(i % 100)
        rows.append({
            "run_date": today - timedelta(days=i % 20),
            "symbol_id": f"REC{i}",
            "rank_position": i % 100 + 1,
            "watchlist_bucket": "CORE_MOMENTUM",
            "fwd_5d_return": -score,
            "fwd_10d_return": -score,
            "fwd_20d_return": -score,
            "factor_rs": score,
            "factor_vol": score,
            "factor_trend": score,
            "factor_prox": score,
            "factor_deliv": score,
            "factor_sector": score,
            "factor_momentum_accel": score,
        })
    # Baseline 180d: large positive-correlation sample.
    for i in range(8000):
        score = float(i % 100)
        rows.append({
            "run_date": today - timedelta(days=90 + i % 60),
            "symbol_id": f"BASE{i}",
            "rank_position": i % 100 + 1,
            "watchlist_bucket": "CORE_MOMENTUM",
            "fwd_5d_return": score,
            "fwd_10d_return": score,
            "fwd_20d_return": score,
            "factor_rs": score,
            "factor_vol": score,
            "factor_trend": score,
            "factor_prox": score,
            "factor_deliv": score,
            "factor_sector": score,
            "factor_momentum_accel": score,
        })
    insert_perf_rows(project, rows)
    resp = api_client.get(
        "/api/execution/perf-tracker/drift?recent_window=30&baseline_window=180",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    rs = next(r for r in resp.json()["factors"] if r["factor"] == "rs")
    # ~2000 recent rows → watch band, never warning/critical.
    assert 1500 <= rs["recent_n"] < 3000
    assert rs["status"] == "watch"
    assert rs["alert"] is False


def test_drift_unreliable_when_coverage_below_80(
    project: Path,
    api_client: TestClient,
) -> None:
    today = date(2026, 5, 8)
    rows = []
    for i in range(100):
        rows.append({
            "run_date": today - timedelta(days=i % 20),
            "symbol_id": f"COV{i}",
            "rank_position": i % 50 + 1,
            "watchlist_bucket": "CORE_MOMENTUM",
            "fwd_5d_return": 1.0,
            "fwd_20d_return": 1.0,
            "factor_rs": float(i),
            # 50% coverage → poor_coverage / unreliable_coverage for drift.
            "factor_momentum_accel": float(i) if i < 50 else None,
        })
    insert_perf_rows(project, rows)
    resp = api_client.get("/api/execution/perf-tracker/drift", headers=API_HEADERS)
    assert resp.status_code == 200
    momentum = next(r for r in resp.json()["factors"] if r["factor"] == "momentum_accel")
    assert momentum["status"] == "unreliable_coverage"
