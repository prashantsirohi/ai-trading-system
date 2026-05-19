from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.publish.decision_bundle import build_publish_decision_bundle
from ai_trading_system.domains.publish.channels.google_sheets import publish_watchlist_candidates


class _FakeSheetManager:
    last_instance = None

    def __init__(self):
        _FakeSheetManager.last_instance = self
        self.last_error = None
        self.frame = pd.DataFrame()

    def open_spreadsheet(self):
        return True

    def get_or_create_sheet(self, sheet_name, rows=1000, cols=26):
        return _FakeWorksheet()

    def get_worksheet(self, sheet_name):
        return _FakeWorksheet()

    def write_dataframe(self, df, sheet_name="Sheet1", include_index=False, include_header=True, start_cell="A1", clear_sheet=False):
        self.frame = df.copy()
        self.sheet_name = sheet_name
        self.clear_sheet = clear_sheet
        return True

    def append_rows(self, df, sheet_name="Sheet1", include_header=False):
        self.frame = df.copy()
        self.sheet_name = sheet_name
        return True

    def apply_number_formats(self, sheet_name, formats):
        return True


class _FakeWorksheet:
    def clear(self):
        return None


def test_decision_bundle_uses_watchlist_for_daily_and_telegram() -> None:
    watchlist = pd.DataFrame(
        [
            {
                "rank": 1,
                "symbol_id": "MSPL",
                "sector": "Industrial",
                "sector_status": "LEADING",
                "stage": "STAGE_2",
                "momentum_tags": "WEEKLY_GAINER,DELIVERY_ACCUMULATION,TOP_RANKED",
                "setup_label": "FLAG_BREAKOUT",
                "watchlist_score": 92.7,
                "composite_score": 63.28,
                "action": "Study",
                "watchlist_reason": "delivery accumulation + leading sector + weekly gainer",
            }
        ]
    )

    bundle = build_publish_decision_bundle(
        run_date="2026-05-07",
        ranked_signals=pd.DataFrame([{"symbol_id": "MSPL", "composite_score": 63.28}]),
        breakout_scan=pd.DataFrame(),
        pattern_scan=pd.DataFrame([{"symbol_id": "MSPL", "pattern_family": "flag", "breakout_level": 36.36}]),
        sector_dashboard=pd.DataFrame([{"Sector": "Industrial", "RS_rank": 4, "RS": 0.64, "Momentum": 0.06, "Quadrant": "Leading"}]),
        event_frame=pd.DataFrame(),
        breadth_frame=pd.DataFrame([{"Date": "2026-05-07", "PctAbove200": 49.32}]),
        watchlist_frame=watchlist,
        trust_status="trusted",
    )

    assert bundle.watchlist_candidates.iloc[0]["Symbol"] == "MSPL"
    assert bundle.watchlist_candidates.iloc[0]["Watchlist Score"] == 92.7
    assert bundle.watchlist_candidates.iloc[0]["Composite Score"] == 63.28
    assert "MSPL" in bundle.telegram_digest
    assert "No qualified breakouts today" in bundle.telegram_digest
    assert "trigger:" not in bundle.telegram_digest


def test_decision_bundle_pattern_sort_and_event_summary_hide_hashes_from_telegram() -> None:
    patterns = pd.DataFrame(
        [
            {"symbol_id": "WATCH1", "pattern_family": "flag", "pattern_state": "watchlist", "pattern_operational_tier": "tier_1", "volume_ratio_20": 9.0, "stage2_label": "stage2", "pattern_score": 100},
            {"symbol_id": "CONF2", "pattern_family": "ipo_base", "pattern_state": "confirmed", "pattern_operational_tier": "tier_2", "volume_ratio_20": 1.0, "stage2_label": "stage2", "pattern_score": 100},
            {"symbol_id": "CONF1", "pattern_family": "ipo_base", "pattern_state": "confirmed", "pattern_operational_tier": "tier_1", "volume_ratio_20": 2.0, "stage2_label": "stage1_to_stage2", "pattern_score": 100},
        ]
    )
    events = pd.DataFrame(
        [
            {"symbol": "CONF1", "category": "volume_shock", "tier": "A", "materiality_label": "high", "event_hash": "secret-hash"},
            {"symbol": "LOW1", "category": "volume_shock", "tier": "C", "materiality_label": "low", "event_hash": "low-hash"},
        ]
    )
    ranked = pd.DataFrame([{"symbol_id": "CONF1", "composite_score": 80.45}])

    bundle = build_publish_decision_bundle(
        run_date="2026-05-07",
        ranked_signals=ranked,
        breakout_scan=pd.DataFrame(),
        pattern_scan=patterns,
        sector_dashboard=pd.DataFrame(),
        event_frame=events,
        breadth_frame=pd.DataFrame(),
        watchlist_frame=pd.DataFrame(),
        trust_status="trusted",
    )

    assert bundle.pattern_setups["Symbol"].tolist()[:3] == ["CONF1", "CONF2", "WATCH1"]
    assert bundle.event_summary["total_events"] == 2
    assert bundle.event_summary["material_events"] == 1
    assert bundle.event_summary["low_info_events"] == 1
    assert bundle.event_summary["overlap_symbols"] == ["CONF1"]
    assert "CONF1" in bundle.telegram_digest
    assert "secret-hash" not in bundle.telegram_digest


def test_watchlist_current_sheet_uses_bundle_contract(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_SPREADSHEET_ID", "sheet")
    monkeypatch.setattr("ai_trading_system.domains.publish.channels.google_sheets.GoogleSheetsManager", _FakeSheetManager)
    bundle = build_publish_decision_bundle(
        run_date="2026-05-07",
        ranked_signals=pd.DataFrame([{"symbol_id": "AAA", "composite_score": 81.2, "close": 100.0}]),
        breakout_scan=pd.DataFrame(),
        pattern_scan=pd.DataFrame([{"symbol_id": "AAA", "pattern_family": "darvas", "breakout_level": 110.0}]),
        sector_dashboard=pd.DataFrame(),
        event_frame=pd.DataFrame(),
        breadth_frame=pd.DataFrame(),
        watchlist_frame=pd.DataFrame(
            [
                {
                    "rank": 1,
                    "symbol_id": "AAA",
                    "sector": "Pharma",
                    "sector_status": "LEADING",
                    "stage": "STAGE_2",
                    "setup_label": "DARVAS_BREAKOUT",
                    "watchlist_score": 86.8,
                    "composite_score": 81.2,
                    "action": "Study",
                    "watchlist_reason": "leading Pharma + top ranked",
                }
            ]
        ),
        trust_status="trusted",
    )

    assert publish_watchlist_candidates(pd.DataFrame(), decision_bundle=bundle) is True
    manager = _FakeSheetManager.last_instance
    assert manager.sheet_name == "Watchlist Current"
    assert list(manager.frame.columns) == [
        "Status",
        "Priority",
        "Symbol",
        "Sector",
        "Sector Status",
        "Stage",
        "Watchlist Score",
        "Composite Score",
        "Previous Rank",
        "Rank Change",
        "Days On List",
        "New Entry",
        "Tags",
        "Setup",
        "Trigger Price",
        "Current Close",
        "Entry Zone",
        "Stop Zone",
        "Reason",
        "Event Catalyst",
        "LLM Catalyst",
        "Risk Flag",
        "Last Seen",
        "Added Date",
    ]
    assert manager.frame.iloc[0]["Symbol"] == "AAA"
    assert manager.frame.iloc[0]["LLM Catalyst"] == ""
