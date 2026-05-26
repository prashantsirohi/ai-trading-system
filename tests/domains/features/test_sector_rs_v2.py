from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.features.sector_rs import (
    _align_sector_component,
    _build_sector_labels,
    _compute_sector_breadth_above_ma,
    _compute_sector_ew_return_vs_universe_rank,
    _write_empty_outputs,
)


def test_sector_ew_return_vs_universe_rank_favors_outperforming_sector() -> None:
    dates = pd.date_range("2024-01-01", periods=8, freq="D")
    close_df = pd.DataFrame(
        {
            "A1": [100, 102, 104, 107, 111, 116, 122, 129],
            "A2": [100, 101, 103, 106, 110, 115, 121, 128],
            "B1": [100, 99, 98, 97, 96, 95, 94, 93],
            "B2": [100, 100, 99, 99, 98, 98, 97, 97],
        },
        index=dates,
    )
    sector_labels = _build_sector_labels(
        close_df,
        {"A1": "Sector A", "A2": "Sector A", "B1": "Sector B", "B2": "Sector B"},
    )
    returns = close_df.pct_change(fill_method=None)
    ew_index = (1 + returns.mean(axis=1)).cumprod() * 100

    component, sector_ew_index = _compute_sector_ew_return_vs_universe_rank(
        returns=returns,
        ew_index=ew_index,
        lookbacks=[3],
        sector_labels=sector_labels,
    )

    latest = component.dropna(how="all").iloc[-1]
    assert latest["Sector A"] > latest["Sector B"]
    assert list(sector_ew_index.columns) == ["Sector A", "Sector B"]


def test_sector_breadth_above_ma_favors_sector_with_more_stocks_above_ma() -> None:
    dates = pd.date_range("2024-01-01", periods=220, freq="D")
    base = pd.Series(range(220), index=dates)
    close_df = pd.DataFrame(
        {
            "A1": 100 + base * 0.50,
            "A2": 100 + base * 0.45,
            "B1": 200 - base * 0.40,
            "B2": 200 - base * 0.35,
        },
        index=dates,
    )
    sector_labels = _build_sector_labels(
        close_df,
        {"A1": "Sector A", "A2": "Sector A", "B1": "Sector B", "B2": "Sector B"},
    )

    breadth = _compute_sector_breadth_above_ma(close_df, sector_labels)

    latest = breadth.iloc[-1]
    assert latest["Sector A"] > latest["Sector B"]


def test_sector_rs_v2_composite_math_and_alignment() -> None:
    dates = pd.date_range("2024-01-01", periods=2, freq="D")
    member = pd.DataFrame(
        {"Sector A": [0.60, 0.80], "Sector B": [0.40, 0.20]},
        index=dates,
    )
    ew = pd.DataFrame(
        {"Sector B": [0.50, 0.30], "Sector A": [0.70, 0.90]},
        index=dates,
    )
    breadth = pd.DataFrame(
        {"Sector A": [0.20, 1.00], "Sector B": [0.80, 0.00]},
        index=dates,
    )

    final = (
        0.50 * _align_sector_component(member, member)
        + 0.30 * _align_sector_component(ew, member)
        + 0.20 * _align_sector_component(breadth, member)
    )
    expected = 0.50 * member + 0.30 * ew[member.columns] + 0.20 * breadth

    pd.testing.assert_frame_equal(final, expected)
    assert final.shape == member.shape
    assert final.index.equals(member.index)
    assert final.columns.equals(member.columns)


def test_write_empty_outputs_writes_all_sector_rs_v2_artifacts(tmp_path) -> None:
    _write_empty_outputs(feature_store_dir=str(tmp_path))

    output_dir = tmp_path / "all_symbols"
    expected_files = {
        "sector_rs.parquet",
        "stock_vs_sector.parquet",
        "ew_index.parquet",
        "sector_member_rs_breadth.parquet",
        "sector_ew_return_vs_universe.parquet",
        "sector_breadth_above_ma.parquet",
        "sector_ew_index.parquet",
    }
    assert {path.name for path in output_dir.glob("*.parquet")} == expected_files

    ew_index = pd.read_parquet(output_dir / "ew_index.parquet")
    assert list(ew_index.columns) == ["timestamp", "ew_index"]
