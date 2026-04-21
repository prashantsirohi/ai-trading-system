from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.publish.dashboard import publish_dashboard_payload


class _FakeWorksheet:
    def __init__(self, title: str, sheet_id: int):
        self.title = title
        self.id = sheet_id
        self.updates: list[tuple[str, list[list[object]]]] = []

    def update(self, values, range_name=None):
        self.updates.append((range_name or "A1", values))

    def format(self, range_name, cell_format):
        self.updates.append((f"format:{range_name}", [cell_format]))


class _FakeSpreadsheet:
    def __init__(self):
        self.deleted: list[str] = []
        self.batch_requests: list[dict] = []
        self._chart_metadata = {
            "sheets": [
                {
                    "properties": {"sheetId": 777},
                    "charts": [],
                }
            ]
        }

    def del_worksheet(self, worksheet):
        self.deleted.append(worksheet.title)

    def batch_update(self, request):
        self.batch_requests.append(request)

    def fetch_sheet_metadata(self):
        return self._chart_metadata


class _FakeManager:
    last_instance = None

    def __init__(self):
        _FakeManager.last_instance = self
        self.last_error = None
        self.spreadsheet = _FakeSpreadsheet()
        self.sheets: dict[str, _FakeWorksheet] = {}
        self.writes: list[tuple[str, str, pd.DataFrame, bool]] = []

    def open_spreadsheet(self):
        return True

    def get_worksheet(self, sheet_name: str):
        return self.sheets.get(sheet_name)

    def get_or_create_sheet(self, title: str, rows: int = 1000, cols: int = 26):
        _ = rows, cols
        ws = self.sheets.get(title)
        if ws is None:
            ws = _FakeWorksheet(title=title, sheet_id=777)
            self.sheets[title] = ws
        return ws

    def write_dataframe(
        self,
        df: pd.DataFrame,
        sheet_name: str = "Sheet1",
        include_index: bool = False,
        include_header: bool = True,
        start_cell: str = "A1",
        clear_sheet: bool = False,
    ) -> bool:
        _ = include_index, include_header
        self.writes.append((sheet_name, start_cell, df.copy(), clear_sheet))
        return True


def test_publish_dashboard_payload_writes_single_dated_sheet_with_unfiltered_breakouts(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("ai_trading_system.domains.publish.dashboard.GoogleSheetsManager", _FakeManager)
    monkeypatch.setattr(
        "ai_trading_system.domains.publish.dashboard._load_operational_breadth",
        lambda _root: pd.DataFrame(
            [{"Date": "2026-04-07", "PctAbove200": 52.4}, {"Date": "2026-04-08", "PctAbove200": 54.1}]
        ),
    )

    ranked_df = pd.DataFrame(
        [
            {
                "symbol_id": f"S{i:03d}",
                "composite_score": float(200 - i),
                "rel_strength_score": float(100 - i / 2),
                "close": float(100 + i),
            }
            for i in range(30)
        ]
    )
    sector_df = pd.DataFrame(
        [
            {"Sector": "Banks", "RS_rank": 1, "RS": 0.62, "Momentum": 0.12, "Quadrant": "Leading"},
            {"Sector": "Power", "RS_rank": 2, "RS": 0.58, "Momentum": 0.08, "Quadrant": "Weakening"},
        ]
    )
    breakout_df = pd.DataFrame(
        [
            {
                "symbol_id": "X1",
                "taxonomy_family": "high_52w_breakout",
                "breakout_state": "qualified",
                "candidate_tier": "A",
                "breakout_score": 6,
            },
            {
                "symbol_id": "X2",
                "taxonomy_family": "consolidation_breakout",
                "breakout_state": "filtered_by_regime",
                "candidate_tier": "C",
                "breakout_score": 2,
            },
        ]
    )

    payload = {"summary": {"run_date": "2026-04-09", "data_trust_status": "trusted"}}
    result = publish_dashboard_payload(
        payload,
        project_root=tmp_path,
        run_date="2026-04-09",
        ranked_df=ranked_df,
        breakout_df=breakout_df,
        sector_df=sector_df,
    )

    manager = _FakeManager.last_instance
    assert manager is not None
    assert result["sheet_name"] == "2026-04-09"
    assert all(write[0] == "2026-04-09" for write in manager.writes)

    breakout_frames = [write[2] for write in manager.writes if list(write[2].columns) == ["Symbol", "Setup", "State", "Tier", "Score", "TradingView"]]
    assert breakout_frames
    assert len(breakout_frames[0]) == 2
    assert set(breakout_frames[0]["State"]) == {"qualified", "filtered_by_regime"}
    assert breakout_frames[0]["TradingView"].str.startswith("https://www.tradingview.com/chart/?symbol=NSE:").all()

    sector_frames = [write[2] for write in manager.writes if list(write[2].columns) == ["Sector", "Rank", "RS", "Momentum", "Quadrant"]]
    assert sector_frames
    assert set(sector_frames[0]["Quadrant"]) == {"Leading", "Weakening"}

    rank_frames = [write[2] for write in manager.writes if list(write[2].columns) == ["Symbol", "Score", "RS", "Close", "TradingView"]]
    assert rank_frames
    assert len(rank_frames[0]) == 25
    assert rank_frames[0]["TradingView"].str.startswith("https://www.tradingview.com/chart/?symbol=NSE:").all()

    assert manager.spreadsheet.batch_requests
    assert any("addChart" in req for call in manager.spreadsheet.batch_requests for req in call.get("requests", []))
    assert any("updateDimensionProperties" in req for call in manager.spreadsheet.batch_requests for req in call.get("requests", []))
