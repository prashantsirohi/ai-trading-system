from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.catalysts.analyzer import analyze_catalysts, apply_catalyst_adjustment
from ai_trading_system.domains.catalysts.collector import select_catalyst_universe
from ai_trading_system.domains.catalysts.contracts import CATALYST_OUTPUT_COLUMNS


def test_select_catalyst_universe_limits_to_final_candidate_sets() -> None:
    ranked = pd.DataFrame(
        [
            {"symbol_id": f"R{i}", "composite_score": 100 - i}
            for i in range(60)
        ]
    )
    watchlist = pd.DataFrame(
        [
            {"symbol": "WATCH", "watchlist_bucket": "ADD_TO_WATCHLIST"},
            {"symbol": "SKIP", "watchlist_bucket": "IGNORE_FOR_NOW"},
        ]
    )
    breakout = pd.DataFrame([{"symbol_id": "BRK.NS", "qualified": True}])
    trends = pd.DataFrame([{"symbol": "NSE:IMP", "fundamental_trend_label": "IMPROVING"}])

    universe = select_catalyst_universe(
        ranked,
        watchlist=watchlist,
        breakout=breakout,
        trends=trends,
        top_n=50,
    )

    assert len([symbol for symbol in universe if symbol.startswith("R")]) == 50
    assert "WATCH" in universe
    assert "SKIP" not in universe
    assert "BRK" in universe
    assert "IMP" in universe


def test_analyze_catalysts_outputs_contract_schema() -> None:
    evidence = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "summary": "Company announced a large order win with strong margin expansion.",
                "source": "announcements.csv",
                "confidence": 0.9,
            },
            {
                "symbol": "OUTSIDE",
                "summary": "Should not be scored",
                "source": "news.csv",
            },
        ]
    )

    scored = analyze_catalysts(["AAA"], evidence)

    assert list(scored.columns) == CATALYST_OUTPUT_COLUMNS
    assert scored.loc[0, "symbol"] == "AAA"
    assert scored.loc[0, "catalyst_type"] == "ORDER_WIN"
    assert scored.loc[0, "catalyst_score"] > 0
    assert scored.loc[0, "evidence_source"] == "announcements.csv"


def test_apply_catalyst_adjustment_uses_catalyst_formula_where_present() -> None:
    watchlist = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "composite_score": 80,
                "breakout_pattern_score": 90,
                "fundamental_score": 70,
                "final_watchlist_score": 78,
            },
            {
                "symbol": "BBB",
                "composite_score": 80,
                "breakout_pattern_score": 90,
                "fundamental_score": 70,
                "final_watchlist_score": 78,
            },
        ]
    )
    catalysts = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "catalyst_score": 100,
                "catalyst_type": "RESULTS_BREAKOUT",
                "catalyst_summary": "Earnings beat",
                "evidence_source": "artifact",
                "confidence": 0.8,
            }
        ]
    )

    adjusted = apply_catalyst_adjustment(watchlist, catalysts).set_index("symbol")

    assert adjusted.loc["AAA", "final_watchlist_score"] == 82.0
    assert adjusted.loc["BBB", "final_watchlist_score"] == 78
