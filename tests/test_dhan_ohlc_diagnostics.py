from __future__ import annotations

from pathlib import Path

import pandas as pd

from collectors import dhan_ohlc_diagnostics


def test_build_fix_strategy_returns_actionable_steps() -> None:
    steps = dhan_ohlc_diagnostics.build_fix_strategy(
        {
            "db_vs_dhan_mismatch": 5,
            "dhan_vs_reference_mismatch": 3,
            "possible_one_day_shift": 2,
            "possible_scale_issue": 1,
            "missing_in_reference": 4,
            "missing_in_dhan": 0,
        }
    )
    assert any("Rebuild suspect window" in step for step in steps)
    assert any("Demote Dhan" in step for step in steps)
    assert any("timestamp parsing path" in step for step in steps)
    assert any("scale-ratio guardrails" in step for step in steps)


def test_shift_and_scale_helpers_detect_signatures() -> None:
    left = pd.DataFrame(
        [
            {"trade_date": "2026-04-01", "close": 100.0},
            {"trade_date": "2026-04-02", "close": 100.0},
        ]
    )
    right = pd.DataFrame(
        [
            {"trade_date": "2026-04-02", "close": 100.0},
            {"trade_date": "2026-04-03", "close": 100.0},
        ]
    )
    shift = dhan_ohlc_diagnostics._shift_score(
        dhan_ohlc_diagnostics._date_set(left),
        dhan_ohlc_diagnostics._date_set(right),
        offset_days=1,
    )
    assert shift == 1.0

    scaled = pd.DataFrame(
        [
            {"trade_date": "2026-04-01", "close": 1000.0},
            {"trade_date": "2026-04-02", "close": 1000.0},
        ]
    )
    ratio = dhan_ohlc_diagnostics._scale_ratio(left, scaled, field="close")
    assert ratio == 0.1


def test_normalization_uses_ist_dates_for_epoch_timestamps() -> None:
    dhan_epoch = pd.DataFrame(
        [
            {"timestamp": 1774981800, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
            {"timestamp": 1775068200, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 11},
        ]
    )
    db_dates = pd.DataFrame(
        [
            {"timestamp": "2026-04-01", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
            {"timestamp": "2026-04-02", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 11},
        ]
    )
    normalized_dhan = dhan_ohlc_diagnostics._normalize_trade_frame(dhan_epoch)
    normalized_db = dhan_ohlc_diagnostics._normalize_trade_frame(db_dates)

    assert normalized_dhan["trade_date"].tolist() == ["2026-04-01", "2026-04-02"]
    assert normalized_db["trade_date"].tolist() == ["2026-04-01", "2026-04-02"]
    shift = dhan_ohlc_diagnostics._shift_score(
        dhan_ohlc_diagnostics._date_set(normalized_dhan),
        dhan_ohlc_diagnostics._date_set(normalized_db),
        offset_days=1,
    )
    assert shift < 0.6


def test_run_diagnostics_produces_issue_tags_and_report(tmp_path: Path, monkeypatch) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def get_symbols_from_masterdb(self, exchanges=None):
            return [
                {"symbol_id": "AAA", "security_id": "101", "exchange": "NSE"},
                {"symbol_id": "BBB", "security_id": "102", "exchange": "NSE"},
            ]

    def fake_fetch_dhan(_collector, symbol_info, from_date, to_date):
        symbol = symbol_info["symbol_id"]
        if symbol == "AAA":
            return pd.DataFrame(
                [
                    {"timestamp": "2026-04-01", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
                    {"timestamp": "2026-04-02", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 11},
                ]
            )
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    def fake_load_db(_db_path, symbol_id, exchange, from_date, to_date):
        if symbol_id == "AAA":
            return pd.DataFrame(
                [
                    {"timestamp": "2026-04-02", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
                    {"timestamp": "2026-04-03", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 11},
                ]
            )
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    def fake_reference_map(project_root, symbols, from_date, to_date):
        return (
            {
                "AAA": pd.DataFrame(
                    [
                        {"trade_date": "2026-04-01", "open": 1000, "high": 1001, "low": 999, "close": 1000, "volume": 10},
                        {"trade_date": "2026-04-02", "open": 1000, "high": 1001, "low": 999, "close": 1000, "volume": 11},
                    ]
                )
            },
            {"nse_dates": [], "yfinance_dates": ["2026-04-01", "2026-04-02"], "missing_dates_after_fallback": []},
        )

    monkeypatch.setattr(dhan_ohlc_diagnostics, "DhanCollector", DummyCollector)
    monkeypatch.setattr(dhan_ohlc_diagnostics, "_fetch_dhan_window", fake_fetch_dhan)
    monkeypatch.setattr(dhan_ohlc_diagnostics, "_load_db_window", fake_load_db)
    monkeypatch.setattr(dhan_ohlc_diagnostics, "_build_reference_map", fake_reference_map)

    report = dhan_ohlc_diagnostics.run_diagnostics(
        project_root=tmp_path,
        from_date="2026-04-01",
        to_date="2026-04-03",
        symbol_limit=2,
    )

    by_symbol = {row["symbol_id"]: row for row in report["symbols"]}
    aaa = by_symbol["AAA"]
    bbb = by_symbol["BBB"]

    assert "db_vs_dhan_mismatch" in aaa["issue_tags"]
    assert "dhan_vs_reference_mismatch" in aaa["issue_tags"]
    assert "possible_one_day_shift" in aaa["issue_tags"]
    assert "possible_scale_issue" in aaa["issue_tags"]
    assert "missing_in_dhan" in bbb["issue_tags"]
    assert "missing_in_reference" in bbb["issue_tags"]

    assert Path(report["report_path"]).exists()
    assert Path(report["report_dir"]).exists()
