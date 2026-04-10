from __future__ import annotations

import pandas as pd

from ui.research.dashboard_helpers import build_value_sparkline_payload, prepare_sector_rotation_frame


def test_prepare_sector_rotation_frame_normalizes_lowercase_aliases() -> None:
    sector_df = pd.DataFrame(
        [
            {
                "sector": "Banks",
                "rs": "0.62",
                "rs_20": "0.58",
                "momentum": "0.07",
                "rs_rank": "1",
                "quadrant": "Leading",
            },
            {
                "sector": "Energy",
                "rs": "0.54",
                "rs_20": "0.56",
                "momentum": "-0.02",
                "rs_rank": "2",
                "quadrant": "Weakening",
            },
        ]
    )
    stock_scan_df = pd.DataFrame(
        [
            {"Sector": "Banks", "Category": "BUY"},
            {"Sector": "Banks", "Category": "STRONG BUY"},
            {"Sector": "Energy", "Category": "SELL"},
        ]
    )

    prepared = prepare_sector_rotation_frame(sector_df, stock_scan_df=stock_scan_df)

    assert not prepared.empty
    for col in ("Sector", "RS", "RS_20", "Momentum", "RS_rank", "Quadrant", "rs_change_20", "breadth_buy_pct"):
        assert col in prepared.columns
    banks_row = prepared[prepared["Sector"] == "Banks"].iloc[0]
    assert float(banks_row["breadth_buy_pct"]) == 100.0


def test_prepare_sector_rotation_frame_sorts_by_rank_when_present() -> None:
    sector_df = pd.DataFrame(
        [
            {"Sector": "Energy", "RS": 0.4, "RS_rank": 2},
            {"Sector": "Banks", "RS": 0.3, "RS_rank": 1},
        ]
    )

    prepared = prepare_sector_rotation_frame(sector_df)

    assert prepared.iloc[0]["Sector"] == "Banks"
    assert prepared.iloc[1]["Sector"] == "Energy"


def test_prepare_sector_rotation_frame_drops_invalid_sector_labels() -> None:
    sector_df = pd.DataFrame(
        [
            {"Sector": None, "RS": 0.4, "RS_rank": 2},
            {"Sector": "  ", "RS": 0.3, "RS_rank": 1},
            {"Sector": "nan", "RS": 0.2, "RS_rank": 3},
            {"Sector": "Banks", "RS": 0.5, "RS_rank": 1},
        ]
    )

    prepared = prepare_sector_rotation_frame(sector_df)

    assert len(prepared) == 1
    assert prepared.iloc[0]["Sector"] == "Banks"


def test_build_value_sparkline_payload_tracks_sector_rs_trend() -> None:
    history_df = pd.DataFrame(
        [
            {"run_id": "pipeline-2026-04-01-aaa", "sector_name": "Industrial", "rs_value": 0.42, "run_order": 0},
            {"run_id": "pipeline-2026-04-02-bbb", "sector_name": "Industrial", "rs_value": 0.47, "run_order": 1},
            {"run_id": "pipeline-2026-04-03-ccc", "sector_name": "Industrial", "rs_value": 0.51, "run_order": 2},
        ]
    )

    payload = build_value_sparkline_payload(
        history_df,
        key_col="sector_name",
        value_col="rs_value",
        max_points=10,
    )

    industrial = payload["Industrial"]
    assert industrial["sparkline"] == [0.42, 0.47, 0.51]
    assert industrial["trend"] == "Improving"
    assert industrial["delta_value"] == 0.09
