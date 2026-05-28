from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.fundamentals.presentation_payloads import (
    build_fundamental_sheet_payload,
    build_sector_context_rows,
    valuation_label,
)


def test_sheet_payload_filters_univ_top1000_and_latest_five_years() -> None:
    payload = build_fundamental_sheet_payload(
        universe_valuation=pd.DataFrame(
            [
                {"universe_id": "UNIV_TOP1000_MCAP", "date": "2021-05-26", "index_level_mcap_weight": 900},
                {"universe_id": "UNIV_TOP1000_MCAP", "date": "2021-05-27", "index_level_mcap_weight": 1000},
                {"universe_id": "UNIV_TOP1000_MCAP", "date": "2026-05-27", "index_level_mcap_weight": 1500},
                {"universe_id": "UNIV_TOP500_MCAP", "date": "2026-05-27", "index_level_mcap_weight": 9999},
            ]
        ),
        valuation_cycle=pd.DataFrame(
            [
                {"entity_id": "UNIV_TOP1000_MCAP", "date": "2021-05-26", "pe_ttm": 20},
                {"entity_id": "UNIV_TOP1000_MCAP", "date": "2021-05-27", "pe_ttm": 21},
                {"entity_id": "UNIV_TOP1000_MCAP", "date": "2026-05-27", "pe_ttm": 35},
                {"entity_id": "UNIV_TOP500_MCAP", "date": "2026-05-27", "pe_ttm": 100},
            ]
        ),
    )

    dates = [row["date"] for row in payload["chart_rows"]]
    assert dates == ["2021-05-27", "2026-05-27"]
    assert all(row.get("pe_ttm") != 100 for row in payload["chart_rows"])


def test_sector_context_preserves_absolute_rank_and_filters_quadrant() -> None:
    rows = build_sector_context_rows(
        sector_dashboard=pd.DataFrame(
            [
                {"RS_rank": 2, "Sector": "Pharma", "RS": 0.74, "Momentum": 0.2, "Quadrant": "Leading"},
                {"RS_rank": 5, "Sector": "Auto Components", "RS": 0.66, "Momentum": 0.25, "Quadrant": "Leading"},
                {"RS_rank": 7, "Sector": "Banks", "RS": 0.4, "Momentum": -0.1, "Quadrant": "Lagging"},
                {"RS_rank": 12, "Sector": "Automobiles", "RS": 0.5, "Momentum": 0.12, "Quadrant": "Improving"},
            ]
        ),
        sector_valuation=pd.DataFrame(
            [
                {"universe_id": "UNIV_TOP1000_MCAP", "date": "2026-05-27", "sector_name": "Pharma", "pe_ttm": 30, "pe_avg_5y": 24},
                {"universe_id": "UNIV_TOP1000_MCAP", "date": "2026-05-27", "sector_name": "Automobiles", "pe_ttm": 15, "pe_avg_5y": 18},
            ]
        ),
    )

    assert rows["Rank"].tolist() == [2.0, 5.0, 12.0]
    assert "Banks" not in set(rows["Sector"])
    assert rows.loc[rows["Sector"].eq("Pharma"), "Valuation vs 5Y Avg PE"].iloc[0] == "High premium (+25.0%)"


def test_sector_valuation_label_against_5y_average() -> None:
    assert valuation_label(18, 20).startswith("Below 5Y avg")
    assert valuation_label(20.5, 20).startswith("Near 5Y avg")
    assert valuation_label(23, 20).startswith("Above 5Y avg")
    assert valuation_label(28, 20).startswith("High premium")
