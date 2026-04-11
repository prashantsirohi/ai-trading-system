from __future__ import annotations

import json
from pathlib import Path

from ui.research.data_access import (
    load_latest_rank_frames,
    load_pattern_backtest_bundle,
    list_pattern_backtest_bundles,
)


def test_list_pattern_backtest_bundles_reads_recent_summary_files(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports" / "research"
    bundle_a = reports_dir / "pattern_backtests" / "pattern_backtest_20260101_010101"
    bundle_b = reports_dir / "pattern_backtests" / "pattern_backtest_20260102_020202"
    bundle_a.mkdir(parents=True, exist_ok=True)
    bundle_b.mkdir(parents=True, exist_ok=True)

    (bundle_a / "pattern_events.csv").write_text("event_id\nE1\n", encoding="utf-8")
    (bundle_a / "pattern_trades.csv").write_text("event_id\nT1\n", encoding="utf-8")
    (bundle_b / "pattern_events.csv").write_text("event_id\nE1\nE2\n", encoding="utf-8")
    (bundle_b / "pattern_trades.csv").write_text("event_id\nT1\nT2\n", encoding="utf-8")

    (bundle_a / "summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-01-01T01:01:01Z",
                "from_date": "2020-01-01",
                "to_date": "2025-12-31",
                "artifacts": {
                    "pattern_events": str(bundle_a / "pattern_events.csv"),
                    "pattern_trades": str(bundle_a / "pattern_trades.csv"),
                    "charts": [str(bundle_a / "charts" / "a.html")],
                },
            }
        ),
        encoding="utf-8",
    )
    (bundle_b / "summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-01-02T02:02:02Z",
                "from_date": "2021-01-01",
                "to_date": "2025-12-31",
                "artifacts": {
                    "pattern_events": str(bundle_b / "pattern_events.csv"),
                    "pattern_trades": str(bundle_b / "pattern_trades.csv"),
                    "charts": [str(bundle_b / "charts" / "b1.html"), str(bundle_b / "charts" / "b2.html")],
                },
            }
        ),
        encoding="utf-8",
    )

    bundles = list_pattern_backtest_bundles(str(reports_dir), max_bundles=10)

    assert len(bundles) == 2
    assert bundles.iloc[0]["bundle_name"] == "pattern_backtest_20260102_020202"
    assert int(bundles.iloc[0]["event_count"]) == 2
    assert int(bundles.iloc[0]["trade_count"]) == 2
    assert int(bundles.iloc[0]["chart_count"]) == 2


def test_load_pattern_backtest_bundle_reads_csv_and_chart_artifacts(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "reports" / "research" / "pattern_backtests" / "pattern_backtest_20260101_010101"
    charts_dir = bundle_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    summary_csv = bundle_dir / "summary.csv"
    yearly_csv = bundle_dir / "yearly_breakdown.csv"
    events_csv = bundle_dir / "pattern_events.csv"
    trades_csv = bundle_dir / "pattern_trades.csv"
    chart_path = charts_dir / "sample_chart.html"

    summary_csv.write_text("pattern_type,signal_count\ncup_handle,4\n", encoding="utf-8")
    yearly_csv.write_text("breakout_year,pattern_type,signal_count\n2025,cup_handle,4\n", encoding="utf-8")
    events_csv.write_text("event_id,symbol_id\nE1,AAA\n", encoding="utf-8")
    trades_csv.write_text("event_id,net_return\nE1,0.12\n", encoding="utf-8")
    chart_path.write_text("<html><body>chart</body></html>", encoding="utf-8")

    (bundle_dir / "summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-01-01T01:01:01Z",
                "artifacts": {
                    "summary_csv": str(summary_csv),
                    "yearly_breakdown_csv": str(yearly_csv),
                    "pattern_events": str(events_csv),
                    "pattern_trades": str(trades_csv),
                    "charts": [str(chart_path)],
                },
            }
        ),
        encoding="utf-8",
    )

    bundle = load_pattern_backtest_bundle(str(bundle_dir))

    assert bundle["summary_json"]["generated_at"] == "2026-01-01T01:01:01Z"
    assert bundle["summary_df"]["pattern_type"].tolist() == ["cup_handle"]
    assert bundle["events_df"]["symbol_id"].tolist() == ["AAA"]
    assert bundle["trades_df"]["event_id"].tolist() == ["E1"]
    assert bundle["yearly_df"]["breakout_year"].astype(int).tolist() == [2025]
    assert bundle["chart_paths"] == [str(chart_path)]


def test_load_latest_rank_frames_includes_pattern_scan(tmp_path: Path) -> None:
    rank_dir = tmp_path / "data" / "pipeline_runs" / "pipeline-2026-04-10-demo" / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    (rank_dir / "dashboard_payload.json").write_text("{}", encoding="utf-8")
    (rank_dir / "ranked_signals.csv").write_text("symbol_id,composite_score\nAAA,90\n", encoding="utf-8")
    (rank_dir / "breakout_scan.csv").write_text("symbol_id\nAAA\n", encoding="utf-8")
    (rank_dir / "pattern_scan.csv").write_text("signal_id,symbol_id,pattern_family\nP1,AAA,cup_handle\n", encoding="utf-8")
    (rank_dir / "stock_scan.csv").write_text("Symbol\nAAA\n", encoding="utf-8")
    (rank_dir / "sector_dashboard.csv").write_text("Sector\nTech\n", encoding="utf-8")

    frames = load_latest_rank_frames(str(tmp_path))

    assert "pattern_scan" in frames
    assert frames["pattern_scan"]["pattern_family"].tolist() == ["cup_handle"]
