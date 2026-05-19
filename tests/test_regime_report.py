"""Phase 7 — regime-stratified forward-return report tests."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.research.backtesting.regime_report import (
    RegimeReport,
    _aggregate_by_regime,
    _attach_forward_returns,
    _summary_dict,
    write_report,
)


# ── forward-return attachment ─────────────────────────────────────────────


def test_attach_forward_returns_computes_pct_change() -> None:
    """A trivial 4-day series of [100, 110, 121, 133.1] should yield
    fwd_1_return values of 10%, 10%, 10% (the last day's fwd_1 is NaN)."""
    daily = pd.DataFrame(
        {
            "date": [
                pd.Timestamp("2024-01-02").date(),
                pd.Timestamp("2024-01-03").date(),
                pd.Timestamp("2024-01-04").date(),
                pd.Timestamp("2024-01-05").date(),
            ],
            "regime": ["bull"] * 4,
        }
    )
    bench = pd.DataFrame(
        {
            "d": [
                pd.Timestamp("2024-01-02").date(),
                pd.Timestamp("2024-01-03").date(),
                pd.Timestamp("2024-01-04").date(),
                pd.Timestamp("2024-01-05").date(),
            ],
            "close": [100.0, 110.0, 121.0, 133.1],
        }
    )
    out = _attach_forward_returns(daily, bench, horizons=(1,))
    rets = out["fwd_1_return"].astype("Float64").tolist()
    # First three are +10% each; last has no forward day in window → NA
    assert abs(float(rets[0]) - 10.0) < 1e-6
    assert abs(float(rets[1]) - 10.0) < 1e-6
    assert abs(float(rets[2]) - 10.0) < 1e-6
    assert pd.isna(rets[3])


def test_attach_forward_returns_handles_empty_inputs() -> None:
    empty = pd.DataFrame(columns=["date", "regime"])
    bench = pd.DataFrame(columns=["d", "close"])
    out = _attach_forward_returns(empty, bench, horizons=(5,))
    assert out.empty


# ── aggregation ───────────────────────────────────────────────────────────


def test_aggregate_by_regime_emits_per_horizon_row() -> None:
    daily = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=10).date,
            "regime": ["bull"] * 5 + ["risk_off"] * 5,
            "fwd_5_return": [1.0, 2.0, -1.0, 3.0, 0.5, -2.0, -1.5, 0.5, -0.5, 1.0],
        }
    )
    agg = _aggregate_by_regime(daily, horizons=(5,))
    bull_row = agg[agg["regime"] == "bull"].iloc[0]
    risk_off_row = agg[agg["regime"] == "risk_off"].iloc[0]
    assert bull_row["sample_size"] == 5
    assert risk_off_row["sample_size"] == 5
    # Bull mean = (1 + 2 - 1 + 3 + 0.5) / 5 = 1.1
    assert abs(bull_row["mean_return_pct"] - 1.1) < 1e-6
    # Bull win-rate: 4 positive of 5 = 80%
    assert bull_row["win_rate_pct"] == 80.0
    # Risk_off win-rate: 2 positive of 5 = 40%
    assert risk_off_row["win_rate_pct"] == 40.0


def test_aggregate_emits_row_with_zero_sample_when_returns_all_nan() -> None:
    daily = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=3).date,
            "regime": ["bull"] * 3,
            "fwd_5_return": [pd.NA, pd.NA, pd.NA],
        }
    )
    agg = _aggregate_by_regime(daily, horizons=(5,))
    assert len(agg) == 1
    row = agg.iloc[0]
    assert row["sample_size"] == 0
    assert pd.isna(row["mean_return_pct"])


# ── ordering verdict ──────────────────────────────────────────────────────


def _per_regime_horizon_from_means(
    horizon: int, means: dict[str, float]
) -> pd.DataFrame:
    rows = []
    for regime, mean in means.items():
        rows.append(
            {
                "regime": regime,
                "horizon_days": horizon,
                "days_in_regime": 100,
                "pct_of_period": 20.0,
                "sample_size": 50,
                "mean_return_pct": mean,
                "median_return_pct": mean,
                "win_rate_pct": 50.0,
                "max_drawup_pct": mean + 1.0,
                "max_drawdown_pct": mean - 1.0,
            }
        )
    return pd.DataFrame(rows)


def test_summary_detects_monotone_non_decreasing_ordering() -> None:
    means = {
        "risk_off": -2.0,
        "neutral": 0.5,
        "cautious_bull": 1.5,
        "bull": 3.0,
        "strong_bull": 5.0,
    }
    per_regime_horizon = _per_regime_horizon_from_means(20, means)
    summary = _summary_dict(
        pd.DataFrame({"date": []}),
        per_regime_horizon,
        horizons=(20,),
        from_date="2005-01-01",
        to_date="2025-12-31",
        benchmark="UNIV_TOP1000",
    )
    verdict = summary["forward_return_ordering"]["20d"]
    assert verdict["monotone_non_decreasing"] is True


def test_summary_detects_violation_when_risk_off_outperforms_bull() -> None:
    """Real-data scenario: risk_off mean-reverts and outperforms bull."""
    means = {
        "risk_off": 3.4,
        "neutral": 0.7,
        "cautious_bull": 2.6,
        "bull": 3.0,
        "strong_bull": 2.1,
    }
    per_regime_horizon = _per_regime_horizon_from_means(20, means)
    summary = _summary_dict(
        pd.DataFrame({"date": []}),
        per_regime_horizon,
        horizons=(20,),
        from_date="2005-01-01",
        to_date="2025-12-31",
        benchmark="UNIV_TOP1000",
    )
    verdict = summary["forward_return_ordering"]["20d"]
    assert verdict["monotone_non_decreasing"] is False
    # by_rank exposes the regime → mean mapping for diagnosis
    by_rank = {row["regime"]: row["mean_return_pct"] for row in verdict["by_rank"]}
    assert by_rank["risk_off"] == 3.4
    assert by_rank["strong_bull"] == 2.1


# ── write_report ─────────────────────────────────────────────────────────


def test_write_report_creates_summary_csv_daily(tmp_path: Path) -> None:
    daily = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=5).date,
            "regime": ["bull"] * 5,
            "raw_regime": ["bull"] * 5,
            "fwd_5_return": [1.0, 2.0, -1.0, 3.0, 0.5],
        }
    )
    per_regime_horizon = _per_regime_horizon_from_means(
        5, {"bull": 1.1, "risk_off": -0.5}
    )
    summary = {"from_date": "2024-01-02", "to_date": "2024-01-08", "total_days": 5}
    report = RegimeReport(daily=daily, per_regime_horizon=per_regime_horizon, summary=summary)
    paths = write_report(report, out_dir=tmp_path, stem="test_report")
    assert paths["summary"].exists()
    assert paths["csv"].exists()
    assert paths["daily"].exists()
    parsed = json.loads(paths["summary"].read_text(encoding="utf-8"))
    assert parsed["total_days"] == 5
    csv = pd.read_csv(paths["csv"])
    assert set(csv["regime"]) == {"bull", "risk_off"}


def test_summary_dict_lists_only_horizons_present_in_aggregate() -> None:
    """When the aggregate has rows for horizon 5 but caller asks about 5
    and 20, the 20d ordering is still emitted (with empty by_rank)."""
    per_regime_horizon = _per_regime_horizon_from_means(5, {"bull": 1.0})
    summary = _summary_dict(
        pd.DataFrame({"date": []}),
        per_regime_horizon,
        horizons=(5, 20),
        from_date="2024-01-01",
        to_date="2024-12-31",
        benchmark="UNIV_TOP1000",
    )
    assert "5d" in summary["forward_return_ordering"]
    assert "20d" in summary["forward_return_ordering"]
    # 20d has no data — by_rank empty, ordering trivially True (vacuous)
    assert summary["forward_return_ordering"]["20d"]["by_rank"] == []
