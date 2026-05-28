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

    def apply_number_formats(self, sheet_name, formats):
        return True


def test_publish_fundamental_dashboard_writes_expected_tabs(monkeypatch) -> None:
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
            "universe_valuation_latest": pd.DataFrame([{"universe_id": "UNIV_TOP500", "date": "2026-05-27", "pe_ttm": 24.1}]),
            "valuation_cycle_latest": pd.DataFrame([{"entity_id": "UNIV_TOP500", "date": "2026-05-27", "pe_percentile_5y": 82}]),
        }
    )

    manager = _FakeManager.instances[0]
    assert ok is True
    assert set(manager.written) == {
        "FUNDAMENTAL_SUMMARY",
        "GREAT_RESULTS",
        "TURNAROUNDS",
        "COMPOUNDERS",
        "SECTOR_EARNINGS",
        "SECTOR_VALUATION",
        "UNIVERSE_VALUATION",
        "VALUATION_CYCLE",
    }
    assert "symbol" in manager.written["GREAT_RESULTS"].columns
    assert "pe_ttm" in manager.written["UNIVERSE_VALUATION"].columns
    assert len(manager.written["GREAT_RESULTS"]) == 100
    assert "OLD" not in set(manager.written["GREAT_RESULTS"]["symbol"])
    assert manager.written["TURNAROUNDS"].iloc[0]["evidence"] == "PAT loss to profit"
