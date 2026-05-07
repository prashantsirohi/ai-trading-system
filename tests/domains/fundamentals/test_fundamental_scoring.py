from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.fundamentals.scoring import compute_fundamental_scores


def _base_row(symbol: str, **overrides):
    row = {
        "symbol": symbol,
        "name": symbol,
        "industry_group": "Capital Goods",
        "industry": "Industrial Products",
        "roce": 32,
        "roe": 26,
        "opm": 32,
        "opm_last_year": 27,
        "piotroski_score": 8,
        "sales_growth_3y": 28,
        "sales_growth_5y": 23,
        "profit_growth_3y": 30,
        "profit_growth_5y": 22,
        "yoy_quarterly_profit_growth": 25,
        "debt_to_equity": 0.2,
        "cash_from_operations_last_year": 100,
        "free_cash_flow_last_year": 50,
        "pe": 18,
        "forward_pe": 15,
        "ev_ebitda": 10,
        "peg_3y": 1.0,
        "price_to_sales": 2,
        "price_to_book": 3,
        "pledged_pct": 0,
        "promoter_holding": 55,
        "dii_holding": 10,
        "fii_holding": 12,
        "is_not_sme": 1,
    }
    row.update(overrides)
    return row


def test_fundamental_tiers_and_hard_red_flags() -> None:
    df = pd.DataFrame(
        [
            _base_row("AAA"),
            _base_row(
                "BBB",
                roce=14,
                roe=12,
                opm=18,
                opm_last_year=18,
                sales_growth_3y=10,
                sales_growth_5y=9,
                profit_growth_3y=9,
                profit_growth_5y=8,
                yoy_quarterly_profit_growth=5,
                pe=30,
                ev_ebitda=18,
                promoter_holding=35,
            ),
            _base_row("CCC", pledged_pct=7, debt_to_equity=1.5),
            _base_row("PLEDGE", pledged_pct=12),
            _base_row("SME", is_not_sme=0),
            _base_row("DEBT", debt_to_equity=2.5),
            _base_row("BANK", industry_group="Banks", industry="Private Bank", debt_to_equity=5.0),
            _base_row("MISS", roce=None, roe=None, opm=None, piotroski_score=None),
        ]
    )

    scored = compute_fundamental_scores(df, snapshot_date="2026-05-07").set_index("symbol")

    assert scored.loc["AAA", "fundamental_tier"] == "A"
    assert scored.loc["BBB", "fundamental_tier"] in {"B", "C"}
    assert scored.loc["CCC", "fundamental_tier"] == "C"
    assert scored.loc["PLEDGE", "fundamental_tier"] == "Reject"
    assert bool(scored.loc["PLEDGE", "hard_red_flag"]) is True
    assert "pledged_pct > 10" in scored.loc["PLEDGE", "red_flags"]
    assert scored.loc["SME", "fundamental_tier"] == "Reject"
    assert bool(scored.loc["SME", "hard_red_flag"]) is True
    assert scored.loc["DEBT", "fundamental_tier"] == "Reject"
    assert "debt_to_equity > 2" in scored.loc["DEBT", "red_flags"]
    assert "debt_to_equity > 2" not in scored.loc["BANK", "red_flags"]
    assert pd.notna(scored.loc["MISS", "fundamental_score"])
