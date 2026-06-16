from __future__ import annotations

from pathlib import Path
import re

import duckdb
import pandas as pd

from ai_trading_system.domains.publish.dashboard import _load_operational_breadth, publish_dashboard_payload


class _FakeWorksheet:
    def __init__(self, title: str, sheet_id: int):
        self.title = title
        self.id = sheet_id
        self.updates: list[tuple[str, list[list[object]]]] = []

    def update(self, values, range_name=None):
        self.updates.append((range_name or "A1", values))

    def format(self, range_name, cell_format):
        self.updates.append((f"format:{range_name}", [cell_format]))

    def clear(self):
        self.updates.append(("clear", [[]]))


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
    preexisting_titles: set[str] = set()

    def __init__(self):
        _FakeManager.last_instance = self
        self.last_error = None
        self.spreadsheet = _FakeSpreadsheet()
        self.sheets: dict[str, _FakeWorksheet] = {
            title: _FakeWorksheet(title=title, sheet_id=700 + idx)
            for idx, title in enumerate(sorted(_FakeManager.preexisting_titles))
        }
        self.writes: list[tuple[str, str, pd.DataFrame, bool]] = []
        self.hidden_writes: list[tuple[str, pd.DataFrame, int, int]] = []
        self.hidden: set[str] = set()
        self.requests_attempted = 0
        self.rows_written = 0
        self.quota_limited = False
        self.retry_recommended_after_seconds = None

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

    def update_worksheet_values(self, worksheet, values, range_name="A1"):
        self.requests_attempted += 1
        self.rows_written += len(values)
        worksheet.update(values, range_name=range_name)

    def batch_update(self, request):
        self.spreadsheet.batch_update(request)

    def fetch_sheet_metadata(self):
        return self.spreadsheet.fetch_sheet_metadata()

    def write_hidden_data_sheet(self, sheet_name, dataframe, max_rows, max_cols):
        safe = dataframe.iloc[:max_rows, :max_cols].copy() if isinstance(dataframe, pd.DataFrame) else pd.DataFrame()
        self.hidden_writes.append((sheet_name, safe, max_rows, max_cols))
        self.hidden.add(sheet_name)
        self.get_or_create_sheet(sheet_name, rows=max_rows + 5, cols=max_cols)
        return True

    def hide_worksheet(self, sheet_name):
        self.hidden.add(sheet_name)
        return True

    def quota_metadata(self):
        return {
            "google_sheets_quota_limited": self.quota_limited,
            "retry_recommended_after_seconds": self.retry_recommended_after_seconds,
            "sheets_requests_attempted": self.requests_attempted,
            "sheets_rows_written": self.rows_written,
            "google_sheets_error": None,
        }

    def delete_worksheets(self, titles):
        deleted = []
        for title in titles:
            ws = self.sheets.get(title)
            if ws is None:
                continue
            self.spreadsheet.del_worksheet(ws)
            deleted.append(title)
        return {"deleted": deleted, "failed": []}

    def prune_date_named_worksheets(self, keep=0):
        date_titles = sorted([title for title in self.sheets if re.fullmatch(r"\d{4}-\d{2}-\d{2}", title)], reverse=True)
        return self.delete_worksheets(date_titles[keep:])

    def reorder_worksheets(self, ordered_titles):
        self.spreadsheet.batch_update({"requests": [{"reorder": ordered_titles}]})
        return True


def _write_breadth_fixture_db(path: Path, *, start: str, end: str, high_close: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                close DOUBLE
            )
            """
        )
        rows = []
        for date_value in pd.bdate_range(start, end):
            aaa_close = high_close if date_value.date() == pd.Timestamp(end).date() else 100.0
            rows.append(("AAA", "NSE", date_value.to_pydatetime(), aaa_close))
            rows.append(("BBB", "NSE", date_value.to_pydatetime(), 100.0))
        con.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?)", rows)
    finally:
        con.close()


def test_load_operational_breadth_honors_data_root(monkeypatch, tmp_path: Path) -> None:
    external_root = tmp_path / "external-data"
    repo_root = tmp_path / "repo"
    (repo_root / "src" / "ai_trading_system").mkdir(parents=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    _write_breadth_fixture_db(repo_root / "data" / "ohlcv.duckdb", start="2025-01-01", end="2025-10-10", high_close=200.0)
    _write_breadth_fixture_db(external_root / "ohlcv.duckdb", start="2025-01-01", end="2025-10-10", high_close=50.0)
    monkeypatch.setenv("DATA_ROOT", str(external_root))

    breadth = _load_operational_breadth(repo_root)

    assert not breadth.empty
    assert float(breadth.iloc[-1]["PctAbove200"]) == 0.0


def test_load_operational_breadth_starts_at_2020(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    (repo_root / "src" / "ai_trading_system").mkdir(parents=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    _write_breadth_fixture_db(repo_root / "data" / "ohlcv.duckdb", start="2019-01-01", end="2020-02-10", high_close=200.0)
    monkeypatch.delenv("DATA_ROOT", raising=False)

    breadth = _load_operational_breadth(repo_root)

    assert not breadth.empty
    assert breadth["Date"].min() >= "2020-01-01"


def test_publish_dashboard_payload_writes_single_dated_sheet_with_unfiltered_breakouts(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("ai_trading_system.domains.publish.dashboard.GoogleSheetsManager", _FakeManager)
    monkeypatch.setattr(
        "ai_trading_system.domains.publish.dashboard._load_operational_breadth",
        lambda _root: pd.DataFrame(
            [
                {
                    "Date": "2026-04-07",
                    "PctAbove200": 52.4,
                    "New52WHighs": 4,
                    "New52WLows": 0,
                    "HighLowRatio": None,
                    "HighLowRatioSMA10": None,
                    "Advancers": 520,
                    "Decliners": 480,
                    "ADLine": 0,
                    "IndexLevel": 1400,
                    "PEPctile5Y": 72.0,
                    "PEPctile5YSMA20": 72.0,
                },
                {
                    "Date": "2026-04-08",
                    "PctAbove200": 54.1,
                    "New52WHighs": 8,
                    "New52WLows": 2,
                    "HighLowRatio": 4.0,
                    "HighLowRatioSMA10": 4.0,
                    "Advancers": 560,
                    "Decliners": 440,
                    "ADLine": 120,
                    "IndexLevel": 1425,
                    "PEPctile5Y": 74.0,
                    "PEPctile5YSMA20": 73.0,
                },
            ]
        ),
    )

    ranked_df = pd.DataFrame(
        [
            {
                "symbol_id": f"S{i:03d}",
                "sector_name": "Banks",
                "composite_score": float(200 - i),
                "rel_strength_score": float(100 - i / 2),
                "close": float(100 + i),
                "return_5": float(12 - i * 0.2),
                "return_20": float(24 - i * 0.3),
                "delivery_pct": float(65 - i * 0.5),
                "volume_zscore_20": float(3.0 if i < 6 else 1.2),
                "stage2_label": "strong_stage2" if i < 8 else "stage2",
            }
            for i in range(30)
        ]
    )
    prior_ranked_df = ranked_df.copy()
    prior_ranked_df.loc[:, "composite_score"] = prior_ranked_df["composite_score"] - 10
    prior_ranked_df.loc[0, "composite_score"] = 100.0
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
    failed_breakouts_df = pd.DataFrame(
        [
            {
                "symbol_id": "S001",
                "sector_name": "Banks",
                "trigger_run_id": "pipeline-2026-04-02-rank",
                "trigger_level": 115.0,
                "current_close": 101.0,
                "drop_pct": -12.17,
                "trigger_tier": "A",
            }
        ]
    )
    investigator_scores = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "verdict": "HIGH_CONVICTION",
                "final_score": 88,
                "status": "HIGH_CONVICTION",
                "move_tag": "SECTOR_ROTATION",
                "delivery_pct": 64,
                "volume_ratio_20": 3.2,
                "rank_position": 3,
            }
        ]
    )
    investigator_repeat = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "appearance_count_20d": 4,
                "repeat_score": 72,
                "price_progression_pct": 11.5,
                "rank_change_20d": -8,
                "volume_escalation": True,
            }
        ]
    )
    investigator_traps = pd.DataFrame(
        [
            {
                "symbol_id": "TRAP",
                "verdict": "NOISE_TRAP",
                "final_score": 21,
                "drop_reason": "LOW_DELIVERY_NO_REPEAT",
                "delivery_pct": 12,
                "rank_position": 120,
            }
        ]
    )
    pattern_df = pd.DataFrame(
        [
            {
                "symbol_id": "S002",
                "pattern_family": "cup_handle",
                "pattern_state": "ready",
                "pattern_operational_tier": "tier_1",
                "pattern_score": 91.5,
                "breakout_level": 125.0,
                "volume_ratio_20": 2.4,
                "stage2_label": "strong_stage2",
            }
        ]
    )

    payload = {
        "summary": {"run_date": "2026-04-09", "data_trust_status": "trusted"},
        "market_regime_phase": {
            "regime_phase": "base_forming_stage1",
            "phase_label": "Base forming (S1)",
            "phase_emoji": "🟡",
            "driven_by": {
                "market_stage": "MIXED",
                "breadth_velocity_bucket": "positive",
                "s2_pct": 0.20,
            },
        },
    }
    _FakeManager.preexisting_titles = {"DATA", "FILTER", "Publish_Log", "2026-04-08"}
    try:
        result = publish_dashboard_payload(
            payload,
            project_root=tmp_path,
            run_date="2026-04-09",
            ranked_df=ranked_df,
            breakout_df=breakout_df,
            sector_df=sector_df,
            prior_ranked_df=prior_ranked_df,
            failed_breakouts_df=failed_breakouts_df,
            pattern_df=pattern_df,
            investigator_scores_df=investigator_scores,
            investigator_repeat_df=investigator_repeat,
            investigator_trap_df=investigator_traps,
            ranking_feedback={
                "status": "ok",
                "rank_bucket_rows": [
                    {"horizon": "20d", "rank_bucket": "top-10", "avg_return": 4.2},
                    {"horizon": "20d", "rank_bucket": "rank-51-plus", "avg_return": 1.0},
                ],
                "factor_ic_rows": [
                    {"horizon": "20d", "factor": "rs", "ic": 0.12, "rows": 120, "signal": "positive"},
                    {"horizon": "20d", "factor": "vol", "ic": -0.05, "rows": 120, "signal": "negative"},
                ],
                "bucket_rows": [
                    {"horizon": "20d", "bucket": "AVOID_WEAK_CONFIRMATION", "avg_return": -2.0, "win_rate_pct": 30.0, "interpretation": "weak"},
                ],
                "drift_rows": [
                    {"factor": "trend", "status": "warning", "recent_ic": -0.01, "baseline_ic": 0.08},
                ],
                "recommendations": [],
                "warnings": [],
            },
        )
    finally:
        _FakeManager.preexisting_titles = set()

    manager = _FakeManager.last_instance
    assert manager is not None
    assert result["sheet_name"] == "01_Daily_Report"
    assert result["base_sheet_name"] == "2026-04-09"
    assert result["sector_sheet_name"] == "04_Sector_Leadership"
    assert result["breadth_sheet_name"] == "05_Market_Breadth"
    assert result["investigator_sheet_name"] == "_DATA_INVESTIGATOR"
    assert {"DATA", "FILTER", "Publish_Log", "2026-04-08"}.issubset(set(manager.spreadsheet.deleted))

    visible_titles = {"01_Daily_Report", "04_Sector_Leadership", "05_Market_Breadth"}
    visible_updates = {
        title: [update for update in manager.sheets[title].updates if update[0] == "A1"]
        for title in visible_titles
    }
    assert all(len(updates) == 1 for updates in visible_updates.values())
    for updates in visible_updates.values():
        grid = updates[0][1]
        assert len(grid) == 60
        assert all(len(row) == 14 for row in grid)

    daily_grid = visible_updates["01_Daily_Report"][0][1]
    daily_text = "\n".join(str(cell) for row in daily_grid for cell in row if cell != "")
    assert "DAILY SUMMARY" in daily_text
    assert "RANKING FEEDBACK" in daily_text
    assert "BREAKOUTS (all, unfiltered)" in daily_text
    assert "Base forming (S1)" in daily_text
    assert "cup_handle" in daily_text
    assert "filtered_by_regime" in daily_text
    assert "S029" not in daily_text

    assert "EVENTS SUMMARY" not in daily_text

    hidden = {name: frame for name, frame, _max_rows, _max_cols in manager.hidden_writes}
    assert {"_DATA_BREADTH", "_DATA_SECTOR_HISTORY", "_DATA_INVESTIGATOR"}.issubset(hidden)
    assert len(hidden["_DATA_BREADTH"]) <= 250
    assert len(hidden["_DATA_SECTOR_HISTORY"]) <= 500
    assert len(hidden["_DATA_INVESTIGATOR"]) <= 300
    assert "AAA" in set(hidden["_DATA_INVESTIGATOR"]["Symbol"].astype(str))
    assert hidden["_DATA_BREADTH"].iloc[-1]["PEPctile5Y"] == 74.0
    assert hidden["_DATA_BREADTH"].iloc[-1]["PEPctile5YSMA20"] == 73.0

    assert manager.spreadsheet.batch_requests
    chart_requests = [
        req
        for call in manager.spreadsheet.batch_requests
        for req in call.get("requests", [])
        if "addChart" in req
    ]
    assert len(chart_requests) == 2
    chart_specs = [request["addChart"]["chart"]["spec"] for request in chart_requests]
    assert chart_specs[0]["title"] == "Operational Long-Term Breadth (% Above SMA200 and PE 5Y Percentile SMA20)"
    assert len(chart_specs[0]["basicChart"]["series"]) == 2
    assert any("updateDimensionProperties" in req for call in manager.spreadsheet.batch_requests for req in call.get("requests", []))


def test_publish_dashboard_payload_keeps_existing_same_date_sheet(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("ai_trading_system.domains.publish.dashboard.GoogleSheetsManager", _FakeManager)
    monkeypatch.setattr(
        "ai_trading_system.domains.publish.dashboard._load_operational_breadth",
        lambda _root: pd.DataFrame(),
    )
    _FakeManager.preexisting_titles = {"2026-04-09"}
    try:
        result = publish_dashboard_payload(
            {"summary": {"run_date": "2026-04-09"}},
            project_root=tmp_path,
            run_date="2026-04-09",
            ranked_df=pd.DataFrame([{"symbol_id": "AAA", "composite_score": 90.0}]),
            breakout_df=pd.DataFrame(),
            sector_df=pd.DataFrame(),
        )
    finally:
        _FakeManager.preexisting_titles = set()

    manager = _FakeManager.last_instance
    assert manager is not None
    assert result["base_sheet_name"] == "2026-04-09"
    assert result["sheet_name"] == "01_Daily_Report"
    assert manager.spreadsheet.deleted == ["2026-04-09"]
