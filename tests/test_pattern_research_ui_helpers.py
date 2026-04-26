from __future__ import annotations

import pandas as pd


def test_build_pattern_chart_index_matches_chart_files_to_events() -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    events_df = pd.DataFrame(
        [
            {
                "event_id": "AAA-cup_handle-2024-05-01",
                "symbol_id": "AAA",
                "pattern_type": "cup_handle",
                "breakout_date": "2024-05-01",
            },
            {
                "event_id": "BBB-round_bottom-2024-06-01",
                "symbol_id": "BBB",
                "pattern_type": "round_bottom",
                "breakout_date": "2024-06-01",
            },
        ]
    )
    chart_paths = [
        "/tmp/bundle/charts/AAA-cup_handle-2024-05-01.html",
        "/tmp/bundle/charts/BBB-round_bottom-2024-06-01.html",
    ]

    out = research_app.build_pattern_chart_index(events_df, chart_paths)

    assert out["event_id"].tolist() == [
        "AAA-cup_handle-2024-05-01",
        "BBB-round_bottom-2024-06-01",
    ]
    assert out["chart_label"].tolist() == [
        "AAA | cup_handle | 2024-05-01",
        "BBB | round_bottom | 2024-06-01",
    ]
    assert out["chart_exists"].tolist() == [True, True]


def test_build_pattern_chart_index_includes_events_without_precomputed_chart() -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    events_df = pd.DataFrame(
        [
            {
                "event_id": "AAA-cup_handle-2024-05-01",
                "symbol_id": "AAA",
                "pattern_type": "cup_handle",
                "breakout_date": "2024-05-01",
            }
        ]
    )

    out = research_app.build_pattern_chart_index(events_df, [])

    assert out["event_id"].tolist() == ["AAA-cup_handle-2024-05-01"]
    assert out["chart_path"].tolist() == [""]
    assert out["chart_exists"].tolist() == [False]


def test_build_pattern_browser_rows_merges_events_trades_and_chart_paths() -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    events_df = pd.DataFrame(
        [
            {
                "event_id": "A1",
                "symbol_id": "AAA",
                "pattern_type": "cup_handle",
                "breakout_date": "2024-05-01",
                "cup_depth_pct": 24.5,
                "width_bars": 42,
                "handle_depth_pct": 6.0,
                "breakout_volume_ratio": 2.1,
                "volume_dry_up": True,
            },
        ]
    )
    trades_df = pd.DataFrame(
        [
            {
                "event_id": "A1",
                "net_return": 0.125,
                "r_multiple": 1.8,
                "exit_reason": "target",
            }
        ]
    )
    chart_index_df = pd.DataFrame(
        [
            {
                "event_id": "A1",
                "symbol_id": "AAA",
                "pattern_type": "cup_handle",
                "breakout_date": "2024-05-01",
                "breakout_year": "2024",
                "chart_path": "/tmp/A1.html",
                "chart_label": "AAA | cup_handle | 2024-05-01",
            }
        ]
    )

    browser_df = research_app.build_pattern_browser_rows(
        events_df,
        trades_df,
        chart_index_df,
    )

    assert browser_df["event_id"].tolist() == ["A1"]
    assert browser_df["breakout_year"].tolist() == ["2024"]
    assert browser_df["chart_path"].tolist() == ["/tmp/A1.html"]
    assert "breakout 2024-05-01" in browser_df.iloc[0]["comment"]
    assert "depth 24.5%" in browser_df.iloc[0]["comment"]
    assert "net 12.5%" in browser_df.iloc[0]["comment"]


def test_coerce_pattern_config_converts_lists_back_to_tuples() -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    config = research_app._coerce_pattern_config(
        {
            "exchange": "NSE",
            "symbols": ["AAA", "BBB"],
            "event_horizons": [5, 10, 20, 40],
        }
    )

    assert config.symbols == ("AAA", "BBB")
    assert config.event_horizons == (5, 10, 20, 40)


def test_pattern_stock_button_label_is_compact_and_marks_selection() -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    label = research_app._pattern_stock_button_label(
        pd.Series(
            {
                "symbol_id": "AAA",
            }
        ),
        selected=True,
    )

    assert label == "AAA *"


def test_pattern_quality_badge_distinguishes_strong_borderline_and_failed() -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    strong = research_app._pattern_quality_badge(
        pd.Series(
            {
                "pattern_type": "round_bottom",
                "net_return": 0.08,
                "breakout_volume_ratio": 2.0,
                "cup_depth_pct": 22.0,
                "width_bars": 38,
                "symmetry_ratio": 1.0,
                "volume_dry_up": True,
            }
        )
    )
    borderline = research_app._pattern_quality_badge(
        pd.Series(
            {
                "pattern_type": "cup_handle",
                "net_return": 0.03,
                "breakout_volume_ratio": 1.4,
                "cup_depth_pct": 22.0,
                "width_bars": 38,
                "handle_depth_pct": 10.0,
                "volume_dry_up": False,
            }
        )
    )
    failed = research_app._pattern_quality_badge(
        pd.Series(
            {
                "pattern_type": "cup_handle",
                "net_return": -0.07,
                "exit_reason": "stop",
            }
        )
    )

    assert strong == "STRONG"
    assert borderline == "BORDERLINE"
    assert failed == "FAILED"


def test_pattern_quality_badge_html_renders_colored_pill() -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    html = research_app._pattern_quality_badge_html(
        pd.Series(
            {
                "pattern_type": "cup_handle",
                "net_return": -0.02,
                "exit_reason": "stop",
            }
        )
    )

    assert "FAILED" in html
    assert "border-radius:999px" in html
    assert "background:#7f1d1d" in html


def test_build_pattern_overlay_option_map_orders_confirmed_first() -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    options = research_app._build_pattern_overlay_option_map(
        pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "signal_id": "watch-1",
                    "pattern_family": "flag",
                    "pattern_state": "watchlist",
                    "signal_date": "2024-06-01",
                    "pattern_score": 65.0,
                },
                {
                    "symbol_id": "AAA",
                    "signal_id": "confirm-1",
                    "pattern_family": "cup_handle",
                    "pattern_state": "confirmed",
                    "signal_date": "2024-06-02",
                    "pattern_score": 82.0,
                },
            ]
        ),
        "AAA",
    )

    labels = list(options.keys())
    assert labels[0] == "None"
    assert "cup handle" in labels[1]
    assert options[labels[1]] == "confirm-1"


def test_plot_candlestick_with_features_adds_pattern_overlay_annotation() -> None:
    import ai_trading_system.interfaces.streamlit.research.app as research_app

    ohlcv = pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0, 103.0],
            "high": [101.0, 102.0, 103.0, 104.0],
            "low": [99.0, 100.0, 101.0, 102.0],
            "close": [100.0, 101.0, 102.0, 103.0],
            "volume": [1000.0, 1100.0, 1200.0, 1300.0],
        },
        index=pd.to_datetime(["2024-06-01", "2024-06-02", "2024-06-03", "2024-06-04"]),
    )
    pattern_row = pd.Series(
        {
            "pattern_family": "cup_handle",
            "pattern_state": "confirmed",
            "pattern_score": 88.0,
            "breakout_level": 104.0,
            "invalidation_price": 99.0,
            "pivot_dates": "[\"2024-06-01\", \"2024-06-02\", \"2024-06-03\", \"2024-06-04\"]",
            "pivot_prices": "[100.0, 101.0, 102.0, 103.0]",
            "pivot_labels": "[\"left\", \"trough\", \"right\", \"handle\"]",
        }
    )

    fig = research_app.plot_candlestick_with_features(ohlcv, {}, "AAA", pattern_row=pattern_row)

    assert len(fig.data) >= 4
    assert any("Score 88.0" in str(annotation.text) for annotation in fig.layout.annotations)
