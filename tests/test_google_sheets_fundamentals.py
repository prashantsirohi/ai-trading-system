from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.publish.channels import google_sheets


class _FakeSheet:
    pass


class _FakeManager:
    FORMAT_DATE = {"type": "DATE", "pattern": "yyyy-mm-dd"}
    FORMAT_INT = {"type": "NUMBER", "pattern": "0"}
    FORMAT_DECIMAL_2 = {"type": "NUMBER", "pattern": "0.00"}
    FORMAT_DECIMAL_4 = {"type": "NUMBER", "pattern": "0.0000"}
    FORMAT_PERCENT_1 = {"type": "PERCENT", "pattern": "0.0%"}
    instances = []

    def __init__(self):
        self.last_error = None
        self.written = {}
        _FakeManager.instances.append(self)

    def open_spreadsheet(self):
        return True

    def get_or_create_sheet(self, sheet_name, rows=1000, cols=26):
        return _FakeSheet()

    def write_dataframe(self, frame, sheet_name, include_header=True, clear_sheet=True):
        self.written[sheet_name] = frame.copy()
        return True

    def apply_number_formats(self, sheet_name, formats, header_row=1):
        return True

    def replace_line_charts(self, sheet_name, *, chart_specs):
        return True


def test_publish_fundamental_dashboard_writes_single_valuation_tab(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_SPREADSHEET_ID", "sheet-id")
    monkeypatch.setattr(google_sheets, "GoogleSheetsManager", _FakeManager)
    _FakeManager.instances.clear()

    ok = google_sheets.publish_fundamental_dashboard(
        {
            "fundamental_dashboard_payload": {
                "run_date": "2026-05-07",
                "summary": {"great_results_count": 1, "top_earnings_sector": "IT"},
                "universe": {
                    "pe_ttm": 24.1,
                    "pe_200dma": 22.8,
                    "pe_percentile_5y": 82,
                    "valuation_zone": "expensive",
                },
            },
            "universe_valuation_daily": pd.DataFrame(
                [
                    {"universe_id": "UNIV_TOP1000_MCAP", "date": "2021-05-26", "index_level_mcap_weight": 900, "loss_mcap_pct": 0.04},
                    {"universe_id": "UNIV_TOP1000_MCAP", "date": "2021-05-27", "index_level_mcap_weight": 1000, "loss_mcap_pct": 0.05},
                    {"universe_id": "UNIV_TOP1000_MCAP", "date": "2026-05-27", "index_level_mcap_weight": 1500, "loss_mcap_pct": 0.06},
                    {"universe_id": "UNIV_TOP500_MCAP", "date": "2026-05-27", "index_level_mcap_weight": 1200, "loss_mcap_pct": 0.02},
                ]
            ),
            "valuation_cycle_features": pd.DataFrame(
                [
                    {"entity_id": "UNIV_TOP1000_MCAP", "date": "2021-05-26", "pe_ttm": 20, "pe_200dma": 19, "pe_5y_median": 18, "pe_percentile_5y": 40},
                    {"entity_id": "UNIV_TOP1000_MCAP", "date": "2021-05-27", "pe_ttm": 21, "pe_200dma": 19, "pe_5y_median": 18, "pe_percentile_5y": 50},
                    {"entity_id": "UNIV_TOP1000_MCAP", "date": "2026-05-27", "pe_ttm": 35, "pe_200dma": 33, "pe_5y_median": 28, "pe_percentile_5y": 74},
                    {"entity_id": "UNIV_TOP500_MCAP", "date": "2026-05-27", "pe_ttm": 40, "pe_200dma": 38, "pe_5y_median": 30, "pe_percentile_5y": 90},
                ]
            ),
            "sector_dashboard": pd.DataFrame(
                [
                    {"RS_rank": 2, "Sector": "Pharma", "RS": 0.74, "Momentum": 0.2, "Quadrant": "Leading"},
                    {"RS_rank": 3, "Sector": "Banks", "RS": 0.7, "Momentum": -0.1, "Quadrant": "Lagging"},
                ]
            ),
            "sector_valuation_daily": pd.DataFrame(
                [{"universe_id": "UNIV_TOP1000_MCAP", "date": "2026-05-27", "sector_name": "Pharma", "pe_ttm": 30, "pe_avg_5y": 24}]
            ),
            "great_results_latest": pd.DataFrame(
                [{"symbol": f"AAA{i:03d}", "report_date": "2026-03-31", "sales_yoy_growth": 0.2, "insight_score": 200 - i} for i in range(120)]
            ),
            "great_results": pd.DataFrame(
                [
                    {"symbol": "OLD", "report_date": "2025-12-31", "sales_yoy_growth": 0.2, "insight_score": 99},
                    {"symbol": "OLD", "report_date": "2026-03-31", "sales_yoy_growth": 0.2, "insight_score": 98},
                ]
            ),
            "turnaround_candidates_latest": pd.DataFrame(
                [{"symbol": "BBB", "report_date": "2026-03-31", "insight_score": 81, "evidence_json": '{"note":"PAT loss to profit"}'}]
            ),
            "compounder_candidates_latest": pd.DataFrame([{"symbol": "CCC", "report_date": "2026-03-31", "insight_score": 78}]),
            "sector_earnings_latest": pd.DataFrame([{"sector_name": "IT", "report_date": "2026-03-31", "sector_fundamental_score": 91}]),
            "sector_valuation_latest": pd.DataFrame([{"sector_name": "IT", "date": "2026-05-27", "pe_ttm": 20}]),
        }
    )

    manager = _FakeManager.instances[0]
    assert ok is True
    assert set(manager.written) == {"VALUATION_DASHBOARD"}
    values = manager.written["VALUATION_DASHBOARD"].astype(str).values.flatten().tolist()
    assert "SECTOR CONTEXT - Leading/Improving only; Rank = absolute RS rank across all sectors" in values
    assert "Pharma" in values
    assert "Banks" not in values
    assert "2021-05-26" not in values
    assert "UNIV_TOP1000_MCAP" in values


def test_publish_fundamental_watchlist_writes_tracking_tab(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_SPREADSHEET_ID", "sheet-id")
    monkeypatch.setattr(google_sheets, "GoogleSheetsManager", _FakeManager)
    _FakeManager.instances.clear()

    ok = google_sheets.publish_fundamental_watchlist(
        pd.DataFrame(
            [
                {
                    "symbol": "AAA",
                    "name": "Alpha",
                    "industry_group": "Capital Goods",
                    "watchlist_bucket": "F1_FUNDAMENTAL_WATCH",
                    "final_watchlist_score": 78.5,
                    "quarterly_result_bucket": "GREAT_RESULT",
                    "quarterly_result_score": 88,
                    "valuation_history_bucket": "FAIR_VALUE",
                    "valuation_history_score": 50,
                    "valuation_reason": "Fair versus own valuation history",
                    "composite_score": 82,
                    "sector_strength": 72,
                    "pe_ttm": 22,
                    "ps_ttm": 4,
                    "pb": 5,
                    "watchlist_reason": "Great result + fair value",
                },
                {
                    "symbol": "THERMAX",
                    "name": "Thermax",
                    "industry_group": "Capital Goods",
                    "watchlist_bucket": "D1_RESULT_DOWNTURN",
                    "final_watchlist_score": 64.13,
                    "quarterly_result_bucket": "DETERIORATING",
                    "quarterly_result_score": 47,
                    "valuation_history_bucket": "BELOW_OWN_MEDIAN",
                    "valuation_history_score": 70,
                    "watchlist_reason": "Deteriorating quarterly result",
                }
            ]
        )
    )

    manager = _FakeManager.instances[0]
    assert ok is True
    assert set(manager.written) == {"Fundamental Watchlist"}
    frame = manager.written["Fundamental Watchlist"]
    assert frame.loc[0, "symbol"] == "AAA"
    assert frame.loc[0, "watchlist_bucket"] == "F1_FUNDAMENTAL_WATCH"
    assert "THERMAX" not in frame["symbol"].astype(str).tolist()
    assert "valuation_reason" in frame.columns
