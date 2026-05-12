"""Tests for the Performance Tracker endpoints (Phase 0 feedback loop UI)."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ai_trading_system.research.perf_tracker.schema import open_research_db
from ai_trading_system.ui.execution_api.app import create_app


API_HEADERS = {"x-api-key": "test-api-key"}


def _seed_research_db(project_root: Path) -> None:
    """Populate ``rank_cohort_performance`` with a deterministic toy dataset.

    Layout: 30 daily run_dates × 5 symbols. We assign rank 1..5 such that
    rank 1 has the highest factor scores and the highest forward returns,
    rank 5 the lowest — so cohort ordering is testable and factor IC is
    strongly positive.
    """
    today = date(2026, 5, 8)
    rows = []
    for d in range(30):
        run_date = today - timedelta(days=d)
        for rank in range(1, 6):
            # Higher rank number = worse pick. Forward returns descend with rank.
            base_ret = (6 - rank) * 1.0  # rank 1 → 5%, rank 5 → 1%
            rows.append({
                "run_date": run_date,
                "symbol_id": f"SYM{rank}",
                "exchange": "NSE",
                "rank_position": rank,
                "composite_score": 100 - rank * 10,
                "composite_score_adjusted": 100 - rank * 10,
                "rank_mode": "state_only",
                "watchlist_bucket": "TRIGGERED_TODAY" if rank == 1 else (
                    "CORE_MOMENTUM" if rank <= 3 else "EARLY_STAGE2"
                ),
                "config_id": None,
                "fwd_5d_return":  base_ret,
                "fwd_10d_return": base_ret * 1.5,
                "fwd_20d_return": base_ret * 2.0,
                "fwd_60d_return": None,
                "fwd_5d_matured_at":  run_date,
                "fwd_10d_matured_at": run_date,
                "fwd_20d_matured_at": run_date,
                "fwd_60d_matured_at": None,
                "factor_rs":              1.0 - rank * 0.1,
                "factor_vol":             1.0 - rank * 0.1,
                "factor_trend":           1.0 - rank * 0.1,
                "factor_prox":            1.0 - rank * 0.1,
                "factor_deliv":           1.0 - rank * 0.1,
                "factor_sector":          1.0 - rank * 0.1,
                "factor_momentum_accel":  1.0 - rank * 0.1,
                "sector_name": "Test",
            })

    with open_research_db(project_root=project_root, read_only=False) as con:
        con.executemany(
            """
            INSERT INTO rank_cohort_performance (
                run_date, symbol_id, exchange, rank_position,
                composite_score, composite_score_adjusted, rank_mode,
                watchlist_bucket, config_id,
                fwd_5d_return, fwd_10d_return, fwd_20d_return, fwd_60d_return,
                fwd_5d_matured_at, fwd_10d_matured_at, fwd_20d_matured_at,
                fwd_60d_matured_at,
                factor_rs, factor_vol, factor_trend, factor_prox, factor_deliv,
                factor_sector, factor_momentum_accel, sector_name
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                (
                    r["run_date"], r["symbol_id"], r["exchange"], r["rank_position"],
                    r["composite_score"], r["composite_score_adjusted"], r["rank_mode"],
                    r["watchlist_bucket"], r["config_id"],
                    r["fwd_5d_return"], r["fwd_10d_return"], r["fwd_20d_return"], r["fwd_60d_return"],
                    r["fwd_5d_matured_at"], r["fwd_10d_matured_at"], r["fwd_20d_matured_at"],
                    r["fwd_60d_matured_at"],
                    r["factor_rs"], r["factor_vol"], r["factor_trend"], r["factor_prox"], r["factor_deliv"],
                    r["factor_sector"], r["factor_momentum_accel"], r["sector_name"],
                )
                for r in rows
            ],
        )


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    _seed_research_db(tmp_path)
    return TestClient(create_app())


def test_coverage_returns_date_range_and_counts(client: TestClient) -> None:
    resp = client.get("/api/execution/perf-tracker/coverage", headers=API_HEADERS)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["dates"] == 30
    assert body["rows"] == 150  # 30 dates × 5 symbols
    assert body["first_date"] == "2026-04-09"
    assert body["last_date"] == "2026-05-08"


def test_cohorts_top10_beats_201plus(client: TestClient) -> None:
    """Top-10 cohort avg_20d must dominate the 201+ cohort."""
    resp = client.get("/api/execution/perf-tracker/cohorts", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    cohorts = {row["cohort"]: row for row in body["cohorts"]}

    # All 5 seeded ranks fall in top-10 → 30 dates × 5 symbols = 150 rows.
    assert cohorts["top-10"]["n_total"] == 150
    # 201+ has no seeded data.
    assert cohorts["201+"]["n_total"] == 0
    # top-10 avg_20d should be the mean of (10, 8, 6, 4, 2) = 6.0
    assert cohorts["top-10"]["avg_20d"] == 6.0
    # Hit rate is 100% because every fwd_5d is positive.
    assert cohorts["top-10"]["hitrate_5d"] == 100.0


def test_cohorts_lookback_filter_restricts_rows(client: TestClient) -> None:
    """A short lookback must yield fewer rows than the full window."""
    short = client.get(
        "/api/execution/perf-tracker/cohorts?lookback_days=7", headers=API_HEADERS,
    ).json()
    full = client.get(
        "/api/execution/perf-tracker/cohorts?lookback_days=0", headers=API_HEADERS,
    ).json()
    short_top10 = next(r for r in short["cohorts"] if r["cohort"] == "top-10")
    full_top10 = next(r for r in full["cohorts"] if r["cohort"] == "top-10")
    assert short_top10["n_total"] < full_top10["n_total"]


def test_buckets_orders_by_taxonomy(client: TestClient) -> None:
    resp = client.get("/api/execution/perf-tracker/buckets", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    buckets = body["buckets"]
    # TRIGGERED_TODAY must appear before CORE_MOMENTUM, which precedes EARLY_STAGE2.
    order = [b["bucket"] for b in buckets]
    assert order.index("TRIGGERED_TODAY") < order.index("CORE_MOMENTUM")
    assert order.index("CORE_MOMENTUM") < order.index("EARLY_STAGE2")
    # TRIGGERED_TODAY = rank 1 only, so avg_20d = 10.
    trig = next(b for b in buckets if b["bucket"] == "TRIGGERED_TODAY")
    assert trig["avg_20d"] == 10.0


def test_factor_ic_returns_strong_positive(client: TestClient) -> None:
    """With monotone factors vs returns, Spearman IC must be ≥ 0.9."""
    resp = client.get(
        "/api/execution/perf-tracker/factor-ic?windows=30",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["windows"] == [30]
    for row in body["factors"]:
        assert row["ic_30d"] is not None
        assert row["ic_30d"] >= 0.9


def test_factor_ic_rejects_bad_windows(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/perf-tracker/factor-ic?windows=abc",
        headers=API_HEADERS,
    )
    assert resp.status_code == 400


def test_drift_no_alerts_on_stable_seed(client: TestClient) -> None:
    """Synthetic data is stable across windows → no drift flags."""
    resp = client.get("/api/execution/perf-tracker/drift", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["flagged"] == []
    assert len(body["factors"]) == 7


def test_endpoints_require_api_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", "the-real-key")
    _seed_research_db(tmp_path)
    bare = TestClient(create_app())
    resp = bare.get("/api/execution/perf-tracker/coverage")
    assert resp.status_code == 401
