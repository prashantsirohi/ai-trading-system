from __future__ import annotations

from pathlib import Path

import pandas as pd


def test_resolve_sector_drilldown_rank_source_prefers_full_latest_artifact() -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    latest_rank_frames = {
        "ranked_signals": pd.DataFrame(
            [
                {"symbol_id": "UJJIVANSFB", "composite_score": 79.0},
                {"symbol_id": "AUBANK", "composite_score": 55.0},
                {"symbol_id": "AXISBANK", "composite_score": 54.0},
            ]
        )
    }
    session_rank_df = pd.DataFrame([{"symbol_id": "UJJIVANSFB", "composite_score": 79.0}])
    dashboard_payload = {
        "ranked_signals": [
            {"symbol_id": "UJJIVANSFB", "composite_score": 79.0},
        ]
    }

    resolved = research_app.resolve_sector_drilldown_rank_source(
        latest_rank_frames,
        session_rank_df,
        dashboard_payload,
    )

    assert resolved["symbol_id"].tolist() == ["UJJIVANSFB", "AUBANK", "AXISBANK"]


def test_resolve_drilldown_label_kind_distinguishes_sector_and_industry_group(monkeypatch) -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    lookup = pd.DataFrame(
        [
            {"symbol_id": "NAM-INDIA", "company_name": "Nippon Life Ind.", "sector_name": "Finance", "industry_group": "Capital Markets"},
            {"symbol_id": "ABB", "company_name": "ABB Ltd", "sector_name": "Industrial", "industry_group": "Electrical Equipment"},
        ]
    )
    monkeypatch.setattr(research_app, "load_symbol_sector_details", lambda: lookup.copy())

    assert research_app.resolve_drilldown_label_kind("Capital Markets") == "Industry Group"
    assert research_app.resolve_drilldown_label_kind("Industrial") == "Sector"


def test_build_sector_universe_frame_adds_overall_and_sector_ranks(
    monkeypatch,
    tmp_path: Path,
) -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    monkeypatch.setattr(research_app, "MASTER_DB", str(tmp_path / "masterdata.db"))

    lookup = pd.DataFrame(
        [
            {"symbol_id": "ABB", "company_name": "ABB Ltd", "sector_name": "Industrial", "industry_group": "Electrical Equipment"},
            {"symbol_id": "ACC", "company_name": "ACC Ltd", "sector_name": "Industrial", "industry_group": "Cement"},
            {"symbol_id": "XYZ", "company_name": "XYZ Ltd", "sector_name": "Industrial", "industry_group": "Capital Goods"},
        ]
    )
    monkeypatch.setattr(research_app, "load_symbol_sector_details", lambda: lookup.copy())

    rank_df = pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 90.0},
            {"symbol_id": "ABB", "composite_score": 80.0},
            {"symbol_id": "BBB", "composite_score": 70.0},
            {"symbol_id": "ACC", "composite_score": 60.0},
        ]
    )

    frame, used_fallback = research_app.build_sector_universe_frame(rank_df, "Industrial")

    assert used_fallback is False
    assert frame["symbol_id"].tolist() == ["ABB", "ACC", "XYZ"]

    abb = frame.loc[frame["symbol_id"] == "ABB"].iloc[0]
    acc = frame.loc[frame["symbol_id"] == "ACC"].iloc[0]
    xyz = frame.loc[frame["symbol_id"] == "XYZ"].iloc[0]

    assert abb["overall_rank"] == 2
    assert abb["sector_rank"] == 1
    assert abb["ranked_flag"] == "Yes"

    assert acc["overall_rank"] == 4
    assert acc["sector_rank"] == 2
    assert acc["ranked_flag"] == "Yes"

    assert pd.isna(xyz["overall_rank"])
    assert pd.isna(xyz["sector_rank"])
    assert xyz["ranked_flag"] == "No"
