from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.fundamentals.trends import compute_fundamental_trends


def _score(symbol: str, score: float, tier: str, *, quality: float = 70, growth: float = 70, valuation: float = 50) -> dict:
    return {
        "symbol": symbol,
        "snapshot_date": "2026-05-07",
        "fundamental_score": score,
        "quality_score": quality,
        "growth_score": growth,
        "balance_sheet_score": 70,
        "valuation_score": valuation,
        "ownership_score": 70,
        "fundamental_tier": tier,
    }


def _raw(symbol: str, *, roce: float = 20, roe: float = 18, opm: float = 20, debt: float = 0.5, pledge: float = 0) -> dict:
    return {
        "symbol": symbol,
        "roce": roce,
        "roe": roe,
        "opm": opm,
        "debt_to_equity": debt,
        "pledged_pct": pledge,
        "sales_growth_3y": 15,
        "profit_growth_3y": 15,
    }


def test_trend_labels_cover_core_cases() -> None:
    current_scores = pd.DataFrame(
        [
            _score("IMP", 76, "A", quality=76),
            _score("DET", 60, "B"),
            _score("TURN", 72, "A"),
            _score("VTRAP", 62, "B", quality=55, growth=52, valuation=75),
            _score("STABLE", 74, "A"),
            _score("NEW", 70, "A"),
        ]
    )
    previous_scores = pd.DataFrame(
        [
            {**_score("IMP", 68, "B", quality=70), "snapshot_date": "2026-04-01"},
            {**_score("DET", 70, "A"), "snapshot_date": "2026-04-01"},
            {**_score("TURN", 50, "C"), "snapshot_date": "2026-04-01"},
            {**_score("VTRAP", 64, "B", quality=65, growth=62, valuation=60), "snapshot_date": "2026-04-01"},
            {**_score("STABLE", 73, "A"), "snapshot_date": "2026-04-01"},
        ]
    )
    current_raw = pd.DataFrame(
        [
            _raw("IMP", roce=24),
            _raw("DET", debt=1.4),
            _raw("TURN"),
            _raw("VTRAP"),
            _raw("STABLE"),
            _raw("NEW"),
        ]
    )
    previous_raw = pd.DataFrame(
        [
            _raw("IMP", roce=22),
            _raw("DET", debt=0.5),
            _raw("TURN"),
            _raw("VTRAP"),
            _raw("STABLE"),
        ]
    )

    trends = compute_fundamental_trends(current_scores, previous_scores, current_raw, previous_raw).set_index("symbol")

    assert trends.loc["IMP", "fundamental_trend_label"] == "IMPROVING"
    assert trends.loc["DET", "fundamental_trend_label"] == "DETERIORATING"
    assert trends.loc["TURN", "fundamental_trend_label"] == "TURNAROUND"
    assert trends.loc["VTRAP", "fundamental_trend_label"] == "VALUE_TRAP_RISK"
    assert trends.loc["STABLE", "fundamental_trend_label"] == "STABLE_GOOD"
    assert trends.loc["NEW", "fundamental_trend_label"] == "INSUFFICIENT_HISTORY"
    assert trends.loc["IMP", "fundamental_score_delta"] == 8
    assert "sales_growth_delta" in trends.columns
    assert "profit_growth_delta" in trends.columns
    assert trends.loc["IMP", "sales_growth_delta"] == trends.loc["IMP", "sales_growth_3y_delta"]
    assert trends.loc["IMP", "profit_growth_delta"] == trends.loc["IMP", "profit_growth_3y_delta"]
