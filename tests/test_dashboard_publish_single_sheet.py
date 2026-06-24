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
        self.dimensions: dict[str, tuple[int, int]] = {}
        self.requests_attempted = 0
        self.rows_written = 0
        self.quota_limited = False
        self.retry_recommended_after_seconds = None

    def open_spreadsheet(self):
        return True

    def get_worksheet(self, sheet_name: str):
        return self.sheets.get(sheet_name)

    def get_or_create_sheet(self, title: str, rows: int = 1000, cols: int = 26):
        self.dimensions[title] = (rows, cols)
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
                "symbol_id": "LOWPRI",
                "appearance_count_20d": 6,
                "repeat_score": 88,
                "price_progression_pct": 4.0,
                "rank_change_20d": -2,
                "volume_escalation": False,
                "high_priority_repeat": False,
            },
            {
                "symbol_id": "AAA",
                "appearance_count_20d": 4,
                "repeat_score": 72,
                "price_progression_pct": 11.5,
                "rank_change_20d": -8,
                "volume_escalation": True,
                "high_priority_repeat": True,
            }
        ]
    )
    investigator_active = pd.DataFrame(
        [
            {
                "symbol_id": "WATCH",
                "status": "Watchlist",
                "verdict": "WATCH_ONLY",
                "score_current": 80,
                "score_peak": 80,
                "appearance_count_20d": 1,
                "days_since_last_seen": 0,
                "price_vs_first_trigger_pct": 0.0,
                "rank_change_20d": 0,
                "volume_escalation": False,
                "pattern_family": "flag",
                "pattern_score": 30,
                "s1_promotion_state": "S1_BASE_FORMING",
                "trigger_reason": "weekly_momentum",
                "sector": "-",
                "last_seen_date": "2026-04-09",
            },
            {
                "symbol_id": "MED",
                "status": "Active Research",
                "verdict": "MEDIUM_CONVICTION",
                "score_current": 61,
                "score_peak": 61,
                "appearance_count_20d": 1,
                "days_since_last_seen": 0,
                "price_vs_first_trigger_pct": 0.0,
                "rank_change_20d": -21,
                "volume_escalation": True,
                "pattern_family": "",
                "pattern_score": 0,
                "s1_promotion_state": "",
                "trigger_reason": "sector_rotation",
                "sector": "FMCG",
                "last_seen_date": "2026-04-09",
            },
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
                "symbol_id": f"P{i:03d}",
                "pattern_family": "cup_handle",
                "pattern_state": "ready",
                "pattern_operational_tier": "tier_1",
                "pattern_score": 100.0 - i,
                "breakout_level": 125.0 + i,
                "volume_ratio_20": 2.4,
                "stage2_label": "strong_stage2",
            }
            for i in range(30)
        ]
    )
    sector_rotation_df = pd.DataFrame(
        [
            {"date": "2026-04-08", "industry": "Banks", "rs_ratio": 104.2, "rs_momentum": 101.5, "quadrant": "Leading", "alpha_20d": 0.08},
            {"date": "2026-04-08", "industry": "Power", "rs_ratio": 102.1, "rs_momentum": 97.4, "quadrant": "Weakening", "alpha_20d": 0.02},
            {"date": "2026-04-08", "industry": "Chemicals", "rs_ratio": 98.3, "rs_momentum": 103.2, "quadrant": "Improving", "alpha_20d": -0.01},
            {"date": "2026-04-07", "industry": "Banks", "rs_ratio": 103.4, "rs_momentum": 100.8, "quadrant": "Leading", "alpha_20d": 0.06},
            {"date": "2026-04-07", "industry": "Media", "rs_ratio": 101.2, "rs_momentum": 99.1, "quadrant": "Weakening", "alpha_20d": 0.01},
        ]
    )
    industry_rotation_df = pd.DataFrame(
        [
            {
                "date": "2026-04-08",
                "rotation_group_name": f"Industry {i:02d}",
                "parent_sector": "Banks" if i % 2 == 0 else "Power",
                "quadrant": "Leading" if i < 20 else "Improving",
                "rs_ratio": 130.0 - i,
                "rs_momentum": 101.0 + i / 10,
                "alpha_20d": 0.08 - i / 1000,
                "rotation_index": 110.0 + i,
                "benchmark_index": 100.0,
                "constituent_count": 5 + i,
            }
            for i in range(30)
        ]
        + [
            {
                "date": "2026-04-07",
                "rotation_group_name": "Old Industry",
                "parent_sector": "Old",
                "quadrant": "Leading",
                "rs_ratio": 150.0,
                "rs_momentum": 120.0,
                "alpha_20d": 0.12,
                "rotation_index": 125.0,
                "benchmark_index": 100.0,
            }
        ]
    )
    investigator_payload = {
        "decision_queue": [
            {
                "symbol_id": "LOW",
                "decision_verdict": "Watch",
                "decision_reason": "Repeat + price holding",
                "investigator_score": 51,
                "appearance_count_20d": 4,
                "price_vs_first_trigger_pct": 7.1,
                "rank_change_20d": 2,
                "volume_signal": "Rising",
                "action": "Open",
            },
            {
                "symbol_id": "HIGH",
                "decision_verdict": "Investigate",
                "decision_reason": "Sector Rotation",
                "investigator_score": 79,
                "appearance_count_20d": 2,
                "price_vs_first_trigger_pct": 3.6,
                "rank_change_20d": -121,
                "volume_signal": "Flat",
                "action": "Open",
            },
        ]
    }

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
    _FakeManager.preexisting_titles = {"DATA", "FILTER", "Publish_Log", "02_Watchlist_Current", "05_Market_Breadth", "2026-04-08"}
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
            investigator_active_df=investigator_active,
            investigator_trap_df=investigator_traps,
            sector_rotation_df=sector_rotation_df,
            industry_rotation_df=industry_rotation_df,
            investigator_payload=investigator_payload,
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
    assert result["diagnostics_sheet_name"] == "Diagnostics"
    assert result["model_feedback_sheet_name"] == "Model_Feedback"
    assert result["sector_sheet_name"] == "04_Sector_Leadership"
    assert result["industry_rotation_sheet_name"] == "industry rotation"
    assert result["breadth_sheet_name"] == "01_Daily_Report"
    assert result["investigator_sheet_name"] == "investigator"
    assert result["investigator_data_sheet_name"] == "_DATA_INVESTIGATOR"
    assert {"DATA", "FILTER", "Publish_Log", "02_Watchlist_Current", "05_Market_Breadth", "2026-04-08"}.issubset(set(manager.spreadsheet.deleted))

    visible_titles = {"01_Daily_Report", "Diagnostics", "Model_Feedback", "04_Sector_Leadership", "industry rotation", "investigator"}
    visible_updates = {
        title: [update for update in manager.sheets[title].updates if update[0] == "A1"]
        for title in visible_titles
    }
    assert all(len(updates) == 1 for updates in visible_updates.values())
    daily_grid = visible_updates["01_Daily_Report"][0][1]
    diagnostics_grid = visible_updates["Diagnostics"][0][1]
    model_feedback_grid = visible_updates["Model_Feedback"][0][1]
    sector_grid = visible_updates["04_Sector_Leadership"][0][1]
    industry_grid = visible_updates["industry rotation"][0][1]
    investigator_grid = visible_updates["investigator"][0][1]
    assert len(daily_grid) == 140
    assert len(diagnostics_grid) >= 10
    assert len(model_feedback_grid) >= 4
    assert len(sector_grid) == 60
    assert all(len(row) == 26 for row in daily_grid)
    assert all(len(row) == 2 for row in diagnostics_grid)
    assert all(len(row) <= 25 for row in model_feedback_grid)
    assert all(len(row) == 14 for row in sector_grid)
    assert manager.dimensions["01_Daily_Report"] == (140, 26)
    assert manager.dimensions["04_Sector_Leadership"] == (60, 14)
    assert industry_grid[0][:6] == ["Date", "Industry", "Sector", "Quadrant", "RS Ratio", "RS Momentum"]
    industry_text = "\n".join(str(cell) for row in industry_grid for cell in row if cell != "")
    assert "Industry 00" in industry_text
    assert "Industry 29" in industry_text
    assert "Old Industry" not in industry_text
    assert len([row for row in industry_grid[1:] if row and row[1] != ""]) == 30
    assert investigator_grid[0] == [
        "Symbol",
        "Verdict",
        "S1 State",
        "Pattern",
        "Pattern Score",
        "Setup",
        "Sector",
        "Score",
        "Repeat",
        "Price vs First",
        "Rank Change",
        "Volume",
        "Days Stale",
        "Trap Flags",
        "Last Seen",
        "Action",
    ]
    assert investigator_grid[1][0] == "WATCH"
    assert investigator_grid[1][1] == "Watch"
    assert investigator_grid[1][2] == "S1_BASE_FORMING"
    assert investigator_grid[1][9] == "0.0%"
    assert investigator_grid[2][0] == "MED"
    assert investigator_grid[2][11] == "Rising"
    assert not [update for update in manager.sheets["05_Market_Breadth"].updates if update[0] == "A1"]

    daily_text = "\n".join(str(cell) for row in daily_grid for cell in row if cell != "")
    assert "TOP MARKET DECISION BANNER" in daily_text
    assert "DAILY SUMMARY" in daily_text
    assert "CONFIRMED BREAKOUTS" in daily_text
    assert "STUDY WATCHLIST TOP 10" in daily_text
    assert "DIAGNOSTICS" not in daily_text
    assert "MARKET BREADTH SNAPSHOT" in daily_text
    assert "% Above SMA200" in daily_text
    assert "PE 5Y Percentile" in daily_text
    assert "New High / Low" in daily_text
    assert "ACTIVE INVESTIGATOR LIST" not in daily_text
    assert "INVESTIGATOR ACTION QUEUE" not in daily_text
    assert "TOP RANKED" in daily_text
    assert "RANKING FEEDBACK" not in daily_text
    assert "ranked_signals:" not in daily_text
    assert "Missing optional columns" not in daily_text
    assert "Base forming (S1)" in daily_text
    assert "Cup/Handle" in daily_text
    assert "P009" in daily_text
    assert "P024" not in daily_text
    assert "S000" in daily_text
    assert "Reason" in daily_text
    assert "Risk Note" in daily_text
    assert "Watchlist Score" in daily_text

    assert "EVENTS SUMMARY" not in daily_text
    diagnostics_text = "\n".join(str(cell) for row in diagnostics_grid for cell in row if cell != "")
    assert "Missing optional columns" in diagnostics_text
    assert "Rows in ranked_signals" in diagnostics_text
    assert "Rank artifact path" in diagnostics_text
    model_feedback_text = "\n".join(str(cell) for row in model_feedback_grid for cell in row if cell != "")
    assert "RANKING FEEDBACK" in model_feedback_text
    assert "rank_edge" in model_feedback_text
    assert "top-10 vs rank-51-plus" in model_feedback_text
    assert "MARKET MOVES SNAPSHOT" in model_feedback_text

    hidden = {name: frame for name, frame, _max_rows, _max_cols in manager.hidden_writes}
    assert {"_DATA_BREADTH", "_DATA_SECTOR_HISTORY", "_DATA_INVESTIGATOR"}.issubset(hidden)
    assert len(hidden["_DATA_BREADTH"]) <= 250
    assert len(hidden["_DATA_SECTOR_HISTORY"]) <= 500
    assert len(hidden["_DATA_INVESTIGATOR"]) <= 300
    assert "AAA" in set(hidden["_DATA_INVESTIGATOR"]["Symbol"].astype(str))
    assert "MED" in set(hidden["_DATA_INVESTIGATOR"]["Symbol"].astype(str))
    assert "Banks" in set(hidden["_DATA_SECTOR_HISTORY"]["industry"].astype(str))
    assert "Media" in set(hidden["_DATA_SECTOR_HISTORY"]["industry"].astype(str))
    assert hidden["_DATA_SECTOR_HISTORY"]["industry"].value_counts().to_dict()["Banks"] == 1
    assert hidden["_DATA_BREADTH"].iloc[-1]["PEPctile5Y"] == 74.0
    assert hidden["_DATA_BREADTH"].iloc[-1]["PEPctile5YSMA20"] == 73.0

    assert manager.spreadsheet.batch_requests
    chart_requests = [
        req
        for call in manager.spreadsheet.batch_requests
        for req in call.get("requests", [])
        if "addChart" in req
    ]
    assert len(chart_requests) == 1
    chart_specs = [request["addChart"]["chart"]["spec"] for request in chart_requests]
    assert chart_specs[0]["title"] == "Sector Rotation: Relative Strength vs Momentum"
    assert chart_specs[0]["basicChart"]["chartType"] == "SCATTER"
    assert len(chart_specs[0]["basicChart"]["series"]) == 1
    assert chart_specs[0]["basicChart"]["domains"][0]["domain"]["sourceRange"]["sources"][0]["endRowIndex"] == 5
    assert any("updateDimensionProperties" in req for call in manager.spreadsheet.batch_requests for req in call.get("requests", []))


def test_publish_dashboard_payload_writes_full_investigator_active_list(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("ai_trading_system.domains.publish.dashboard.GoogleSheetsManager", _FakeManager)
    monkeypatch.setattr(
        "ai_trading_system.domains.publish.dashboard._load_operational_breadth",
        lambda _root: pd.DataFrame(),
    )
    active = pd.DataFrame(
        [
            {
                "symbol_id": f"ACTIVE{i:03d}",
                "verdict": "WATCH_ONLY",
                "score_current": float(100 - i),
                "final_score": float(100 - i),
                "score_peak": float(105 - i),
                "appearance_count_20d": i,
                "sector_name": f"Sector {i:03d}",
                "last_seen_date": "2026-04-09",
            }
            for i in range(65)
        ]
    )
    active.loc[0, "sector_name"] = ""

    result = publish_dashboard_payload(
        {"summary": {"run_date": "2026-04-09"}},
        project_root=tmp_path,
        run_date="2026-04-09",
        ranked_df=pd.DataFrame(
            [
                {"symbol_id": "AAA", "composite_score": 90.0},
                {"symbol_id": "ACTIVE000", "composite_score": 89.0},
            ]
        ),
        breakout_df=pd.DataFrame(),
        stock_scan_df=pd.DataFrame([{"symbol_id": "ACTIVE000", "sector_name": "Stock Scan Sector"}]),
        sector_df=pd.DataFrame(),
        investigator_active_df=active,
    )

    manager = _FakeManager.last_instance
    assert manager is not None
    investigator_updates = [update for update in manager.sheets["investigator"].updates if update[0] == "A1"]
    assert len(investigator_updates) == 1
    investigator_grid = investigator_updates[0][1]
    assert result["investigator_rows_written"] == 66
    assert len(investigator_grid) == 66
    assert investigator_grid[1][0] == "ACTIVE000"
    assert investigator_grid[1][6] == "Stock Scan Sector"
    assert investigator_grid[1][7] == 100.0
    assert investigator_grid[-1][0] == "ACTIVE064"
    assert investigator_grid[-1][6] == "Sector 064"
    assert investigator_grid[-1][7] == 36.0


def test_publish_dashboard_payload_daily_report_marks_no_breakouts_and_new_entries(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("ai_trading_system.domains.publish.dashboard.GoogleSheetsManager", _FakeManager)
    monkeypatch.setattr(
        "ai_trading_system.domains.publish.dashboard._load_operational_breadth",
        lambda _root: pd.DataFrame(),
    )

    result = publish_dashboard_payload(
        {
            "summary": {
                "run_date": "2026-04-09",
                "data_trust_status": "degraded",
                "allowed_exposure": 0.15,
            },
            "market_regime_phase": {
                "phase_label": "Bear / Stage 4",
                "driven_by": {"breadth_velocity_bucket": "very_negative"},
            },
        },
        project_root=tmp_path,
        run_date="2026-04-09",
        ranked_df=pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "sector": "Banks",
                    "composite_score": 88,
                    "close": 100,
                    "sma_50": 95,
                    "sma_200": 80,
                    "sma50_slope_20d_pct": 2,
                    "near_52w_high_pct": 8,
                    "volume_ratio_20": 1.6,
                    "delivery_pct": 60,
                }
            ]
        ),
        breakout_df=pd.DataFrame([{"symbol_id": "AAA", "qualified": False, "breakout_state": "watchlist", "breakout_score": 80, "breakout_level": 103}]),
        pattern_df=pd.DataFrame([{"symbol_id": "AAA", "pattern_family": "VCP", "pattern_score": 85, "breakout_level": 103}]),
        sector_df=pd.DataFrame([{"Sector": "Banks", "Quadrant": "Leading"}]),
        watchlist_df=pd.DataFrame([{"symbol_id": "AAA", "days_on_watchlist": 1}]),
        prior_watchlist_df=pd.DataFrame([{"symbol_id": "BBB"}]),
    )

    manager = _FakeManager.last_instance
    assert manager is not None
    daily_grid = [update for update in manager.sheets["01_Daily_Report"].updates if update[0] == "A1"][0][1]
    daily_text = "\n".join(str(cell) for row in daily_grid for cell in row if cell != "")
    assert result["daily_report"]["confirmed_breakout_rows"] == 0
    assert "No confirmed / trade-qualified breakouts today." in daily_text
    assert "No trade-qualified breakouts today." in daily_text
    assert "NEW" in daily_text
    assert "Watchlist Score" in daily_text


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
