from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.research.symbol_report.cli import main as cli_main
from ai_trading_system.research.symbol_report.dataset import build_symbol_report
from ai_trading_system.research.symbol_report.loaders import (
    latest_rank_attempts_by_date,
    load_artifact_timeline,
    load_feature_history,
    load_ohlcv,
    load_weekly_stage_history,
)


def _paths(tmp_path: Path):
    paths = get_domain_paths(project_root=tmp_path, data_domain="operational")
    paths.root_dir.mkdir(parents=True, exist_ok=True)
    paths.feature_store_dir.mkdir(parents=True, exist_ok=True)
    paths.pipeline_runs_dir.mkdir(parents=True, exist_ok=True)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)
    return paths


def _write_ohlcv_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                provider VARCHAR,
                validation_status VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog VALUES
            ('AAA', 'NSE', '2025-01-01', 10, 11, 9, 10.5, 1000, 'nse', 'trusted'),
            ('AAA', 'NSE', '2025-01-02', 10.5, 12, 10, 11.5, 1200, 'nse', 'trusted'),
            ('AAA', 'NSE', '2025-01-03', 11.5, 13, 11, 12.5, 1500, 'nse', 'trusted'),
            ('BBB', 'NSE', '2025-01-02', 20, 21, 19, 20.5, 800, 'nse', 'trusted')
            """
        )
        conn.execute(
            """
            CREATE TABLE weekly_stage_snapshot (
                symbol VARCHAR,
                week_end_date DATE,
                stage_label VARCHAR,
                stage_confidence DOUBLE,
                stage_transition VARCHAR,
                bars_in_stage INTEGER,
                stage_entry_date DATE,
                ma10w DOUBLE,
                ma30w DOUBLE,
                ma40w DOUBLE,
                ma30w_slope_4w DOUBLE,
                weekly_rs_score DOUBLE,
                weekly_volume_ratio DOUBLE,
                support_level DOUBLE,
                resistance_level DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO weekly_stage_snapshot VALUES
            ('AAA', '2025-01-01', 'S1', 0.70, 'NONE', 3, '2024-12-18', 10, 10, 10, 0.0, 50, 0.8, 9, 12),
            ('AAA', '2025-01-03', 'S2', 0.90, 'S1_TO_S2', 1, '2025-01-03', 11, 10.5, 10.2, 0.02, 80, 1.4, 10, 13)
            """
        )
    finally:
        conn.close()


def _write_features(feature_store: Path) -> None:
    rsi_dir = feature_store / "rsi" / "NSE"
    sma_dir = feature_store / "sma" / "NSE"
    rsi_dir.mkdir(parents=True)
    sma_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "timestamp": "2025-01-01", "close": 10.5, "rsi_14": 45.0},
            {"symbol_id": "AAA", "exchange": "NSE", "timestamp": "2025-01-02", "close": 11.5, "rsi_14": 55.0},
        ]
    ).to_parquet(rsi_dir / "AAA.parquet", index=False)
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "timestamp": "2025-01-02", "close": 11.5, "sma_20": 10.8, "sma_50": 10.2, "sma_200": 9.5},
            {"symbol_id": "AAA", "exchange": "NSE", "timestamp": "2025-01-03", "close": 12.5, "sma_20": 11.1, "sma_50": 10.4, "sma_200": 9.7},
        ]
    ).to_parquet(sma_dir / "AAA.parquet", index=False)


def _write_csv(path: Path, rows: list[dict], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=columns).to_csv(path, index=False)


def _write_artifacts(pipeline_runs: Path) -> None:
    older = pipeline_runs / "pipeline-2025-01-02-old" / "rank" / "attempt_1"
    newer = pipeline_runs / "pipeline-2025-01-02-new" / "rank" / "attempt_2"
    empty_sidecar = pipeline_runs / "pipeline-2025-01-03-new" / "rank" / "attempt_1"

    _write_csv(
        older / "ranked_signals.csv",
        [{"symbol_id": "AAA", "exchange": "NSE", "composite_score": 50, "eligible_rank": True}],
    )
    _write_csv(
        newer / "ranked_signals.csv",
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "composite_score": 70,
                "composite_score_adjusted": 75,
                "eligible_rank": True,
                "rel_strength_score": 80,
                "trend_score_score": 65,
                "weekly_stage_label": "S2",
            }
        ],
    )
    _write_csv(
        newer / "pattern_scan.csv",
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "pattern_family": "darvas_box",
                "pattern_state": "confirmed",
                "signal_date": "2025-01-02",
                "breakout_level": 12.0,
                "invalidation_price": 10.0,
                "setup_quality": 77.0,
                "pattern_priority_rank": 1,
            }
        ],
    )
    _write_csv(
        newer / "stock_scan.csv",
        [{"symbol_id": "AAA", "exchange": "NSE", "category": "BUY", "why": "Strong", "score": 0.8}],
    )
    (newer / "breakout_scan.csv").write_text("", encoding="utf-8")

    _write_csv(
        empty_sidecar / "ranked_signals.csv",
        [{"symbol_id": "BBB", "exchange": "NSE", "composite_score": 40}],
    )
    _write_csv(
        empty_sidecar / "pattern_scan.csv",
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "pattern_family": "inside_week_breakout",
                "pattern_state": "watchlist",
                "signal_date": "2025-01-03",
                "breakout_level": 13.0,
                "pattern_priority_rank": 2,
            }
        ],
    )

    os.utime(older / "ranked_signals.csv", (100, 100))
    os.utime(newer / "ranked_signals.csv", (200, 200))


def test_loaders_filter_ohlcv_features_and_stage_history(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    _write_ohlcv_db(paths.ohlcv_db_path)
    _write_features(paths.feature_store_dir)

    ohlcv = load_ohlcv(
        paths.ohlcv_db_path,
        symbol="AAA",
        exchange="NSE",
        from_date="2025-01-02",
        to_date="2025-01-03",
    )
    assert ohlcv["close"].tolist() == [11.5, 12.5]

    features = load_feature_history(
        paths.feature_store_dir,
        symbol="AAA",
        exchange="NSE",
        from_date="2025-01-01",
        to_date="2025-01-03",
    )
    assert {"rsi_14", "sma_20", "sma_50", "sma_200"}.issubset(features.columns)
    assert len(features) == 3

    stages = load_weekly_stage_history(
        paths.ohlcv_db_path,
        symbol="AAA",
        from_date="2025-01-01",
        to_date="2025-01-03",
    )
    assert stages["stage_transition"].tolist() == ["NONE", "S1_TO_S2"]


def test_artifact_walker_chooses_latest_attempt_and_skips_empty_csv(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    _write_artifacts(paths.pipeline_runs_dir)

    attempts = latest_rank_attempts_by_date(
        paths.pipeline_runs_dir,
        from_date="2025-01-01",
        to_date="2025-01-03",
    )
    assert attempts[pd.Timestamp("2025-01-02").date()].parent.name == "rank"
    assert attempts[pd.Timestamp("2025-01-02").date()].parent.parent.name == "pipeline-2025-01-02-new"

    timeline = load_artifact_timeline(
        paths.pipeline_runs_dir,
        symbol="AAA",
        from_date="2025-01-01",
        to_date="2025-01-03",
    )
    row = timeline[timeline["run_date"] == "2025-01-02"].iloc[0]
    assert bool(row["ranked_emitted"]) is True
    assert row["pattern_family"] == "darvas_box"
    assert row["stock_category"] == "BUY"

    gap = timeline[timeline["run_date"] == "2025-01-03"].iloc[0]
    assert bool(gap["ranked_emitted"]) is False
    assert gap["pattern_family"] == "inside_week_breakout"


def test_build_symbol_report_marks_captured_and_not_emitted(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    _write_ohlcv_db(paths.ohlcv_db_path)
    _write_features(paths.feature_store_dir)
    _write_artifacts(paths.pipeline_runs_dir)

    report = build_symbol_report(
        paths,
        symbol="AAA",
        exchange="NSE",
        from_date="2025-01-01",
        to_date="2025-01-03",
    )

    assert "rsi_14" in report.price_features.columns
    assert report.artifacts["pattern_emitted"].sum() == 2
    statuses = dict(zip(report.diagnostics["run_date"], report.diagnostics["diagnostic_status"]))
    assert statuses["2025-01-02"] == "captured"
    assert statuses["2025-01-03"] == "not_emitted"


def test_cli_generates_html_and_missing_symbol_errors(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    paths = _paths(tmp_path)
    _write_ohlcv_db(paths.ohlcv_db_path)
    _write_features(paths.feature_store_dir)
    _write_artifacts(paths.pipeline_runs_dir)

    output = tmp_path / "report.html"
    assert cli_main(
        [
            "--project-root",
            str(tmp_path),
            "--symbol",
            "AAA",
            "--from-date",
            "2025-01-01",
            "--to-date",
            "2025-01-03",
            "--output",
            str(output),
        ]
    ) == 0
    assert output.exists()
    html = output.read_text(encoding="utf-8")
    assert "AAA Diagnostic Report" in html
    assert "Plotly.newPlot" in html
    assert str(output) in capsys.readouterr().out

    with pytest.raises(SystemExit) as exc:
        cli_main(["--project-root", str(tmp_path), "--symbol", "MISSING"])
    assert exc.value.code == 2
