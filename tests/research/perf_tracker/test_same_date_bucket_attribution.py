"""Diagnostics sprint: /buckets/same-date trading_days + small_sample flag."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from tests.research.perf_tracker.conftest import API_HEADERS, insert_perf_rows


def test_small_sample_flag_set_when_trading_days_lt_10(
    project: Path,
    api_client: TestClient,
) -> None:
    today = date(2026, 5, 8)
    rows = []
    # Single bucket on 2 dates × 3 symbols = 6 rows, below both thresholds.
    for d in range(2):
        for s in range(3):
            rows.append({
                "run_date": today - timedelta(days=d),
                "symbol_id": f"S{s}",
                "rank_position": s + 1,
                "watchlist_bucket": "CORE_MOMENTUM",
                "fwd_5d_return": 1.0,
                "fwd_20d_return": 1.0,
            })
    insert_perf_rows(project, rows)
    resp = api_client.get(
        "/api/execution/perf-tracker/buckets/same-date?lookback_days=0",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    cm = next(r for r in body["buckets"] if r["bucket"] == "CORE_MOMENTUM")
    assert cm["trading_days"] == 2
    assert cm["small_sample"] is True
    assert body["control"]["trading_days"] == 2


def test_small_sample_false_when_thresholds_met(
    project: Path,
    api_client: TestClient,
) -> None:
    today = date(2026, 5, 8)
    rows = []
    # 12 dates × 50 symbols = 600 rows → above SAME_DATE_SMALL_SAMPLE_DAYS and ROWS.
    for d in range(12):
        for s in range(50):
            rows.append({
                "run_date": today - timedelta(days=d),
                "symbol_id": f"S{s}",
                "rank_position": s + 1,
                "watchlist_bucket": "CORE_MOMENTUM",
                "fwd_5d_return": 1.0,
                "fwd_20d_return": 1.0,
            })
    insert_perf_rows(project, rows)
    resp = api_client.get(
        "/api/execution/perf-tracker/buckets/same-date?lookback_days=0",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    cm = next(r for r in resp.json()["buckets"] if r["bucket"] == "CORE_MOMENTUM")
    assert cm["trading_days"] == 12
    assert cm["small_sample"] is False
