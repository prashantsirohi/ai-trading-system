from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.candidates.builder import build_final_candidates


def _ranked() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 90, "prox_high": 4},
            {"symbol_id": "BBB", "composite_score": 85, "prox_high": 5},
            {"symbol_id": "DDD", "composite_score": 80, "prox_high": 6},
        ]
    )


def test_f4_maps_to_blowout_result_breakout() -> None:
    result, _ = build_final_candidates(
        ranked_signals=_ranked(),
        watchlist_candidates=pd.DataFrame(
            [
                {
                    "symbol": "AAA",
                    "final_watchlist_score": 90,
                    "fundamental_score": 80,
                    "watchlist_bucket": "F4_ACTION_CANDIDATE",
                    "quarterly_result_bucket": "BLOWOUT_RESULT",
                    "valuation_history_bucket": "BELOW_OWN_MEDIAN",
                }
            ]
        ),
        min_candidates=1,
        max_candidates=1,
    )

    assert result.iloc[0]["candidate_group"] == "BLOWOUT_RESULT_BREAKOUT"


def test_f3_maps_to_fund_value_tech_ready() -> None:
    result, _ = build_final_candidates(
        ranked_signals=_ranked(),
        watchlist_candidates=pd.DataFrame(
            [
                {
                    "symbol": "BBB",
                    "final_watchlist_score": 82,
                    "fundamental_score": 78,
                    "watchlist_bucket": "F3_FUND_VALUE_TECH_READY",
                    "quarterly_result_bucket": "GREAT_RESULT",
                    "valuation_history_bucket": "FAIR_VALUE",
                }
            ]
        ),
        min_candidates=1,
        max_candidates=1,
    )

    assert result.iloc[0]["candidate_group"] == "FUND_VALUE_TECH_READY"


def test_d1_maps_to_result_downturn_avoid() -> None:
    result, _ = build_final_candidates(
        ranked_signals=_ranked(),
        watchlist_candidates=pd.DataFrame(
            [
                {
                    "symbol": "DDD",
                    "final_watchlist_score": 40,
                    "fundamental_score": 60,
                    "watchlist_bucket": "D1_RESULT_DOWNTURN",
                    "quarterly_result_bucket": "DETERIORATING",
                    "valuation_history_bucket": "BELOW_OWN_MEDIAN",
                }
            ]
        ),
        min_candidates=1,
        max_candidates=3,
    )

    assert result.set_index("symbol").loc["DDD", "candidate_group"] == "RESULT_DOWNTURN_AVOID"
