from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.publish.channels.quantstats import (
    _parse_run_date,
    build_dashboard_strategy_returns,
    publish_dashboard_quantstats_tearsheet,
)


def _write_ranked(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_build_dashboard_strategy_returns_from_rank_snapshots(tmp_path: Path) -> None:
    run1 = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "pipeline-2026-03-01-aaaa1111"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )
    run2 = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "pipeline-2026-03-02-bbbb2222"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )
    run3 = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "pipeline-2026-03-03-cccc3333"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )
    run3 = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "pipeline-2026-03-03-cccc3333"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )
    run3 = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "pipeline-2026-03-03-cccc3333"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )
    run3 = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "pipeline-2026-03-03-cccc3333"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )

    _write_ranked(
        run1,
        [
            {"symbol_id": "AAA", "close": 100.0, "composite_score": 90.0},
            {"symbol_id": "BBB", "close": 50.0, "composite_score": 80.0},
        ],
    )
    _write_ranked(
        run2,
        [
            {"symbol_id": "AAA", "close": 110.0, "composite_score": 95.0},
            {"symbol_id": "BBB", "close": 45.0, "composite_score": 70.0},
        ],
    )
    _write_ranked(
        run3,
        [
            {"symbol_id": "AAA", "close": 99.0, "composite_score": 91.0},
            {"symbol_id": "BBB", "close": 49.5, "composite_score": 75.0},
        ],
    )

    returns, detail_df = build_dashboard_strategy_returns(
        [run1, run2, run3],
        top_n=2,
        min_overlap=2,
    )

    assert len(returns) == 2
    assert len(detail_df) == 2
    assert returns.index[0] == pd.Timestamp("2026-03-02")
    assert returns.index[1] == pd.Timestamp("2026-03-03")
    assert abs(float(returns.iloc[0])) < 1e-12
    assert abs(float(returns.iloc[1])) < 1e-12
    assert int(detail_df.iloc[0]["overlap_count"]) == 2
    assert int(detail_df.iloc[1]["overlap_count"]) == 2


def test_publish_dashboard_quantstats_tearsheet_writes_outputs(
    tmp_path: Path, monkeypatch
) -> None:
    run1 = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "pipeline-2026-03-01-aaaa1111"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )
    run2 = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "pipeline-2026-03-02-bbbb2222"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )
    run3 = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "pipeline-2026-03-03-cccc3333"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )
    _write_ranked(
        run1,
        [
            {"symbol_id": "AAA", "close": 100.0, "composite_score": 90.0},
            {"symbol_id": "BBB", "close": 50.0, "composite_score": 80.0},
        ],
    )
    _write_ranked(
        run2,
        [
            {"symbol_id": "AAA", "close": 105.0, "composite_score": 92.0},
            {"symbol_id": "BBB", "close": 55.0, "composite_score": 81.0},
        ],
    )
    _write_ranked(
        run3,
        [
            {"symbol_id": "AAA", "close": 104.0, "composite_score": 91.0},
            {"symbol_id": "BBB", "close": 56.0, "composite_score": 82.0},
        ],
    )
    _write_ranked(
        run3,
        [
            {"symbol_id": "AAA", "close": 104.0, "composite_score": 91.0},
            {"symbol_id": "BBB", "close": 56.0, "composite_score": 82.0},
        ],
    )
    _write_ranked(
        run3,
        [
            {"symbol_id": "AAA", "close": 104.0, "composite_score": 91.0},
            {"symbol_id": "BBB", "close": 56.0, "composite_score": 82.0},
        ],
    )
    _write_ranked(
        run3,
        [
            {"symbol_id": "AAA", "close": 103.0, "composite_score": 89.0},
            {"symbol_id": "BBB", "close": 56.0, "composite_score": 82.0},
        ],
    )

    def _fake_render(returns_series_csv, output_html, title):
        _ = returns_series_csv
        _ = title
        output = Path(output_html)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("<html>stub tearsheet</html>", encoding="utf-8")
        return (0, "", "")

    monkeypatch.setattr(
        "ai_trading_system.domains.publish.channels.quantstats.HAS_QUANTSTATS",
        True,
    )
    monkeypatch.setattr(
        "ai_trading_system.domains.publish.channels.quantstats._render_quantstats_tearsheet_subprocess",
        _fake_render,
    )

    latest_sector_df = pd.DataFrame(
        [
            {"sector": "Banks", "rs": 0.6, "rs_20": 0.55, "momentum": 0.08, "rs_rank": 1, "quadrant": "Leading"},
            {"sector": "Energy", "rs": 0.5, "rs_20": 0.52, "momentum": -0.01, "rs_rank": 2, "quadrant": "Weakening"},
        ]
    )

    result = publish_dashboard_quantstats_tearsheet(
        project_root=tmp_path,
        run_id="pipeline-2026-03-02-bbbb2222",
        run_date="2026-03-02",
        top_n=2,
        min_overlap=1,
        max_runs=10,
        latest_sector_df=latest_sector_df,
    )

    assert result["ok"] is True
    tearsheet_path = Path(str(result["tearsheet_path"]))
    assert tearsheet_path.exists()
    html = tearsheet_path.read_text(encoding="utf-8")
    assert "Sector Rotation Heatmap" in html
    assert "Top Ranked Stocks" in html
    assert "Breakout Candidates" in html
    assert "Market Breadth (% Above SMA200)" in html
    assert Path(str(result["returns_path"])).exists()
    assert Path(str(result["metadata_path"])).exists()
    assert result.get("quantstats_core_path") is None


def test_publish_dashboard_quantstats_tearsheet_can_write_optional_core_html(
    tmp_path: Path, monkeypatch
) -> None:
    run1 = tmp_path / "data" / "pipeline_runs" / "pipeline-2026-03-01-aaaa1111" / "rank" / "attempt_1" / "ranked_signals.csv"
    run2 = tmp_path / "data" / "pipeline_runs" / "pipeline-2026-03-02-bbbb2222" / "rank" / "attempt_1" / "ranked_signals.csv"
    run3 = tmp_path / "data" / "pipeline_runs" / "pipeline-2026-03-03-cccc3333" / "rank" / "attempt_1" / "ranked_signals.csv"
    _write_ranked(run1, [{"symbol_id": "AAA", "close": 100.0, "composite_score": 90.0}, {"symbol_id": "BBB", "close": 50.0, "composite_score": 80.0}])
    _write_ranked(run2, [{"symbol_id": "AAA", "close": 105.0, "composite_score": 92.0}, {"symbol_id": "BBB", "close": 55.0, "composite_score": 81.0}])
    _write_ranked(run3, [{"symbol_id": "AAA", "close": 104.0, "composite_score": 91.0}, {"symbol_id": "BBB", "close": 56.0, "composite_score": 82.0}])

    def _fake_render(returns_series_csv, output_html, title):
        _ = returns_series_csv
        _ = title
        output = Path(output_html)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("<html>stub core</html>", encoding="utf-8")
        return (0, "", "")

    monkeypatch.setattr("ai_trading_system.domains.publish.channels.quantstats.HAS_QUANTSTATS", True)
    monkeypatch.setattr("ai_trading_system.domains.publish.channels.quantstats._render_quantstats_tearsheet_subprocess", _fake_render)

    result = publish_dashboard_quantstats_tearsheet(
        project_root=tmp_path,
        run_id="pipeline-2026-03-02-bbbb2222",
        run_date="2026-03-02",
        top_n=2,
        min_overlap=1,
        max_runs=10,
        write_core_quantstats_html=True,
    )

    assert result["ok"] is True
    assert Path(str(result["quantstats_core_path"])).exists()


def test_build_dashboard_strategy_returns_handles_mixed_run_id_date_parsing(tmp_path: Path) -> None:
    manual = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "manual-run-a"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )
    pipeline = (
        tmp_path
        / "data"
        / "pipeline_runs"
        / "pipeline-2026-03-02-bbbb2222"
        / "rank"
        / "attempt_1"
        / "ranked_signals.csv"
    )

    _write_ranked(
        manual,
        [
            {"symbol_id": "AAA", "close": 100.0, "composite_score": 90.0},
            {"symbol_id": "BBB", "close": 50.0, "composite_score": 80.0},
        ],
    )
    _write_ranked(
        pipeline,
        [
            {"symbol_id": "AAA", "close": 101.0, "composite_score": 91.0},
            {"symbol_id": "BBB", "close": 49.0, "composite_score": 79.0},
        ],
    )

    returns, detail_df = build_dashboard_strategy_returns(
        [manual, pipeline],
        top_n=2,
        min_overlap=1,
    )

    assert len(detail_df) == 1
    assert len(returns) == 1


def test_parse_run_date_fallback_from_mtime_is_timezone_naive() -> None:
    parsed = _parse_run_date("manual-run-a", 1_710_000_000.0)
    assert parsed.tzinfo is None
    assert parsed == parsed.normalize()
