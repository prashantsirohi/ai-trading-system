"""Diagnostics sprint: /factor-ic/conditional multi-horizon + multi-cohort."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from tests.research.perf_tracker.conftest import API_HEADERS, insert_perf_rows


def test_conditional_ic_returns_horizons_per_cohort(
    project: Path,
    api_client: TestClient,
) -> None:
    today = date(2026, 5, 8)
    rows = []
    for i in range(40):
        rank = 1 if i < 20 else 201
        score = float(i)
        rows.append({
            "run_date": today - timedelta(days=i % 5),
            "symbol_id": f"COND{i}",
            "rank_position": rank,
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
        "/api/execution/perf-tracker/factor-ic/conditional?windows=90",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["horizons"] == [5, 10, 20]
    assert set(body["cohorts"]) == {"full_universe", "top_200_only", "rank_201_plus_only"}
    rs = next(r for r in body["factors"] if r["factor"] == "rs")
    # Multi-horizon keys present per cohort.
    for cohort in ("full_universe", "top_200_only", "rank_201_plus_only"):
        for h in (5, 10, 20):
            assert f"ic_{h}d_90w_{cohort}" in rs
            assert f"n_{h}d_90w_{cohort}" in rs
    # Per-cohort sample sizes line up.
    assert rs["n_20d_90w_top_200_only"] == 20
    assert rs["n_20d_90w_rank_201_plus_only"] == 20
    assert rs["n_20d_90w_full_universe"] == 40
    # Monotone synthetic data → IC near 1 in full universe (40 rows ≥ 30 min).
    assert rs["ic_20d_90w_full_universe"] is not None
    assert rs["ic_20d_90w_full_universe"] >= 0.9
