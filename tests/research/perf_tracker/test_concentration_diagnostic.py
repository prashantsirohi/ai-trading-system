"""Diagnostics sprint: /concentration weak/strong/mixed signal."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from tests.research.perf_tracker.conftest import API_HEADERS, insert_perf_rows


def _seed_with_top10_avg_20d(project: Path, *, top10: float, rest: float) -> None:
    today = date(2026, 5, 8)
    rows = []
    # Top-10 cohort: ranks 1..10 across 30 dates × 1 symbol-per-rank.
    for d in range(30):
        for rank in range(1, 11):
            rows.append({
                "run_date": today - timedelta(days=d),
                "symbol_id": f"T{rank}",
                "rank_position": rank,
                "watchlist_bucket": "CORE_MOMENTUM",
                "fwd_5d_return": top10,
                "fwd_10d_return": top10,
                "fwd_20d_return": top10,
            })
        # Fill ranks 11..200 with `rest` returns.
        for rank in range(11, 201):
            rows.append({
                "run_date": today - timedelta(days=d),
                "symbol_id": f"R{rank}",
                "rank_position": rank,
                "watchlist_bucket": "CORE_MOMENTUM",
                "fwd_5d_return": rest,
                "fwd_10d_return": rest,
                "fwd_20d_return": rest,
            })
    insert_perf_rows(project, rows)


def test_concentration_signal_weak_when_top10_matches_top200(
    project: Path,
    api_client: TestClient,
) -> None:
    # Top-200 avg_20d will be a small weighted mix of 1.0 (10 rows) and 1.0 (190 rows) = 1.0
    # Setting both equal forces delta=0 → weak.
    _seed_with_top10_avg_20d(project, top10=1.0, rest=1.0)
    resp = api_client.get(
        "/api/execution/perf-tracker/concentration?lookback_days=0",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["signal"] == "weak"
    assert "Top-10 does not materially outperform" in body["message"]


def test_concentration_signal_strong_when_gap_large(
    project: Path,
    api_client: TestClient,
) -> None:
    # top10=10, rest=1 → top200 avg ≈ (10*10 + 190*1)/200 = 1.45 → delta ≈ 8.55%
    _seed_with_top10_avg_20d(project, top10=10.0, rest=1.0)
    resp = api_client.get(
        "/api/execution/perf-tracker/concentration?lookback_days=0",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["signal"] == "strong"
    assert body["top10_avg_20d"] == 10.0
