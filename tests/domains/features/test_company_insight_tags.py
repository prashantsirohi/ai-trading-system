from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.features.company_insight_tags import compute_company_insight_tags


def _row(symbol: str, report_date: str, **overrides) -> dict:
    row = {
        "symbol": symbol,
        "report_date": report_date,
        "available_at": report_date,
        "sales_cr": 100.0,
        "net_profit_cr": 10.0,
        "operating_profit_cr": 20.0,
        "opm_pct": 20.0,
        "npm_pct": 10.0,
        "sales_qoq_growth": 0.05,
        "sales_yoy_growth": 0.12,
        "profit_qoq_growth": 0.15,
        "profit_yoy_growth": 0.20,
        "operating_profit_qoq_growth": 0.10,
        "operating_profit_yoy_growth": 0.20,
        "opm_qoq_change": 0.5,
        "opm_yoy_change": 1.0,
        "npm_qoq_change": 0.5,
        "npm_yoy_change": 1.0,
        "sales_4q_cagr": 0.10,
        "profit_4q_cagr": 0.10,
        "sales_8q_cagr": 0.12,
        "profit_8q_cagr": 0.12,
        "positive_profit_quarters_4q": 4,
        "sales_growth_positive_quarters_4q": 4,
        "profit_growth_positive_quarters_4q": 4,
        "margin_expansion_quarters_4q": 4,
        "created_at": pd.Timestamp.utcnow(),
    }
    row.update(overrides)
    return row


def test_turnaround_and_great_result_tags_are_detected() -> None:
    rows = [
        _row("TURN", "2025-03-31", sales_yoy_growth=0, net_profit_cr=-5, profit_yoy_growth=0, opm_yoy_change=0),
        _row("TURN", "2025-06-30", sales_yoy_growth=0, net_profit_cr=2, profit_qoq_growth=0, opm_yoy_change=0),
        _row("TURN", "2025-09-30", sales_yoy_growth=0, net_profit_cr=3, profit_qoq_growth=0.5, opm_yoy_change=0),
        _row("TURN", "2025-12-31", sales_yoy_growth=0, net_profit_cr=5, profit_qoq_growth=0.5, opm_yoy_change=0),
        _row("TURN", "2026-03-31", sales_yoy_growth=0.25, net_profit_cr=12, profit_qoq_growth=1.4, profit_yoy_growth=0.8, opm_yoy_change=5.2),
        _row("TURN", "2026-06-30", sales_yoy_growth=0.22, net_profit_cr=15, profit_qoq_growth=0.25, profit_yoy_growth=6.5, opm_yoy_change=4.0),
        _row("BLOW", "2025-03-31", sales_cr=100, net_profit_cr=10),
        _row("BLOW", "2025-06-30", sales_cr=105, net_profit_cr=11),
        _row("BLOW", "2025-09-30", sales_cr=110, net_profit_cr=12),
        _row("BLOW", "2025-12-31", sales_cr=115, net_profit_cr=13),
        _row("BLOW", "2026-03-31", sales_yoy_growth=0.35, profit_yoy_growth=0.80, profit_qoq_growth=0.30, opm_yoy_change=4.5, net_profit_cr=18),
    ]

    tags = compute_company_insight_tags(pd.DataFrame(rows))
    turn_tags = set(tags.loc[tags["symbol"].eq("TURN"), "insight_type"])
    blow_tags = set(tags.loc[tags["symbol"].eq("BLOW"), "insight_type"])

    assert {"turnaround_candidate", "turnaround_confirmed", "loss_to_profit", "margin_recovery", "sales_recovery"}.issubset(turn_tags)
    assert {"great_result", "blowout_result", "margin_expansion_result", "profit_acceleration_result"}.issubset(blow_tags)
    assert tags["evidence_json"].str.contains("Sales").any()


def test_compounder_tags_are_detected_for_steady_growth() -> None:
    rows = []
    for idx in range(9):
        rows.append(
            _row(
                "COMP",
                str(pd.Timestamp("2024-03-31") + pd.DateOffset(months=3 * idx))[:10],
                sales_yoy_growth=0.16,
                profit_yoy_growth=0.18,
                sales_8q_cagr=0.18,
                profit_8q_cagr=0.20,
                opm_pct=22.0 + (idx % 2) * 0.2,
                net_profit_cr=10 + idx,
            )
        )

    tags = compute_company_insight_tags(pd.DataFrame(rows))

    comp_tags = set(tags["insight_type"])
    assert "consistent_compounder" in comp_tags
    assert "quality_growth" in comp_tags
