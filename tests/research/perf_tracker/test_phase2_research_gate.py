"""Phase 2 research gates: multi-horizon IC, overlap, and weight evidence."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ai_trading_system.domains.ranking.factors import TREND_PERSISTENCE_FORMULA
from ai_trading_system.research.perf_tracker.digest import build_digest
from ai_trading_system.research.perf_tracker.schema import open_research_db
from tests.research.perf_tracker.conftest import API_HEADERS, insert_perf_rows


def _seed_research_rows(project: Path, *, n: int = 60) -> None:
    today = date(2026, 5, 8)
    rows = []
    for i in range(n):
        score = float(i)
        rows.append({
            "run_date": today - timedelta(days=i % 20),
            "symbol_id": f"RG{i}",
            "rank_position": i + 1,
            "watchlist_bucket": "CORE_MOMENTUM",
            "fwd_5d_return": score,
            "fwd_10d_return": score * 1.5,
            "fwd_20d_return": score * 2.0,
            "factor_rs": score,
            "factor_vol": score,
            "factor_trend": score,
            "factor_prox": score + (i % 3) * 0.01,
            "factor_deliv": score,
            "factor_sector": score,
            "factor_momentum_accel": score,
            "factor_above_200dma": score,
            "factor_liquidity": score,
            "factor_delivery_trend": score,
        })
    insert_perf_rows(project, rows)


def test_factor_ic_endpoint_measures_5d_10d_20d(
    project: Path,
    api_client: TestClient,
) -> None:
    _seed_research_rows(project)

    resp = api_client.get(
        "/api/execution/perf-tracker/factor-ic?windows=30",
        headers=API_HEADERS,
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["horizons"] == [5, 10, 20]
    rs = next(row for row in body["factors"] if row["factor"] == "rs")
    assert rs["ic_5d_30w"] == pytest.approx(1.0)
    assert rs["ic_10d_30w"] == pytest.approx(1.0)
    assert rs["ic_20d_30w"] == pytest.approx(1.0)
    # Back-compat key stays fwd_20d so drift/UI callers do not change shape.
    assert rs["ic_30d"] == rs["ic_20d_30w"]


def test_digest_reports_overlap_and_weight_evidence(project: Path) -> None:
    _seed_research_rows(project)

    result = build_digest(project_root=project, as_of=date(2026, 5, 30))

    overlap = result.section_data["rs_proximity_overlap"].iloc[0]
    assert overlap["status"] == "high_overlap"
    evidence = result.section_data["weight_activation_evidence"]
    decisions = dict(zip(evidence["factor"], evidence["decision"], strict=True))
    assert decisions["liquidity"] == "eligible_for_backtest"
    assert decisions["delivery_trend"] == "eligible_for_backtest"
    assert decisions["above_200dma"] == "eligible_for_backtest"
    assert decisions["momentum_accel"] == "eligible_for_backtest"
    assert "Regime-specific weights remain locked" in result.markdown


def test_ranked_frame_backfill_maps_phase1_candidate_factors(project: Path) -> None:
    from ai_trading_system.research.perf_tracker.backfill import build_rows_from_ranked_frame

    import pandas as pd

    ranked = pd.DataFrame([{
        "symbol_id": "AAA",
        "exchange": "NSE",
        "composite_score": 90.0,
        "liquidity_score": 0.91,
        "delivery_trend_score": 12.5,
        "above_200dma_score": 88.0,
        "momentum_acceleration_score": 77.0,
    }])

    out = build_rows_from_ranked_frame("2026-05-08", ranked)

    assert out.loc[0, "factor_liquidity"] == pytest.approx(0.91)
    assert out.loc[0, "factor_delivery_trend"] == pytest.approx(12.5)
    assert out.loc[0, "factor_above_200dma"] == pytest.approx(88.0)
    assert out.loc[0, "factor_momentum_accel"] == pytest.approx(77.0)


def test_research_schema_adds_phase1_candidate_factor_columns(project: Path) -> None:
    with open_research_db(project_root=project, read_only=False) as con:
        columns = {row[1] for row in con.execute("PRAGMA table_info('rank_cohort_performance')").fetchall()}

    assert "factor_liquidity" in columns
    assert "factor_delivery_trend" in columns


def test_trend_persistence_formula_is_formally_defined() -> None:
    assert "0.7" in TREND_PERSISTENCE_FORMULA
    assert "adx_14" in TREND_PERSISTENCE_FORMULA
    assert "close > sma_20" in TREND_PERSISTENCE_FORMULA
    assert "close > sma_50" in TREND_PERSISTENCE_FORMULA
