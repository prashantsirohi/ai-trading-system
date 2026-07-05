"""Winner-validation research report tests."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.backtesting import winner_validation
from ai_trading_system.research.backtesting.winner_validation import (
    StudyWindow,
    WinnerValidationConfig,
    _active_proxy_distribution,
    _capture_rates,
    _factor_summary_by_group,
    _load_winners,
    _pattern_summaries,
    _rally_quartile_summary,
    _year_window,
    run_winner_validation_report,
)


CREATE_CATALOG = """
CREATE TABLE _catalog (
    symbol_id VARCHAR,
    security_id VARCHAR,
    exchange VARCHAR,
    timestamp TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    parquet_file VARCHAR,
    ingestion_version BIGINT,
    ingestion_ts TIMESTAMP
)
"""

CREATE_DELIVERY = """
CREATE TABLE _delivery (
    symbol_id VARCHAR,
    exchange VARCHAR,
    timestamp DATE,
    delivery_pct DOUBLE,
    volume BIGINT,
    delivery_qty BIGINT
)
"""


def _insert_series(
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    *,
    start: date,
    days: int,
    start_close: float,
    step: float,
    volume: int = 1000,
) -> None:
    rows = []
    for idx in range(days):
        d = start + timedelta(days=idx)
        close = start_close + step * idx
        rows.append((symbol, d.isoformat(), close, close * 1.02, close * 0.98, close, volume + idx, d.isoformat()))
    conn.executemany(
        """
        INSERT INTO _catalog VALUES (?, NULL, 'NSE', ?, ?, ?, ?, ?, ?, NULL, 1, ?)
        """,
        rows,
    )
    conn.executemany(
        "INSERT INTO _delivery VALUES (?, 'NSE', ?, ?, ?, ?)",
        [(symbol, (start + timedelta(days=idx)).isoformat(), 50.0 + idx % 20, volume + idx, int((volume + idx) * 0.5)) for idx in range(days)],
    )


def _fixture_db(tmp_path: Path) -> Path:
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    conn = duckdb.connect(str(paths.ohlcv_db_path))
    conn.execute(CREATE_CATALOG)
    conn.execute(CREATE_DELIVERY)
    history_start = date(2024, 11, 1)
    _insert_series(conn, "AAA", start=history_start, days=61, start_close=8.0, step=0.02)
    _insert_series(conn, "BBB", start=history_start, days=61, start_close=8.0, step=0.03)
    _insert_series(conn, "CCC", start=history_start, days=61, start_close=19.0, step=0.01)
    start = date(2025, 1, 1)
    _insert_series(conn, "AAA", start=start, days=40, start_close=10.0, step=0.25)
    _insert_series(conn, "BBB", start=start, days=40, start_close=10.0, step=0.80)
    _insert_series(conn, "CCC", start=start, days=40, start_close=20.0, step=0.05)
    _insert_series(conn, "NIFTY50", start=start, days=40, start_close=1000.0, step=20.0)
    conn.executemany(
        """
        INSERT INTO _catalog VALUES ('JUMP', NULL, 'NSE', ?, ?, ?, ?, ?, 1000, NULL, 1, ?)
        """,
        [
            ("2025-01-01", 10.0, 10.0, 10.0, 10.0, "2025-01-01"),
            ("2025-01-02", 60.0, 60.0, 60.0, 60.0, "2025-01-02"),
            ("2025-02-09", 70.0, 70.0, 70.0, 70.0, "2025-02-09"),
        ],
    )
    conn.close()
    return paths.ohlcv_db_path


def test_load_winners_excludes_discontinuities_and_benchmarks(tmp_path: Path) -> None:
    db_path = _fixture_db(tmp_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        winners = _load_winners(
            conn,
            StudyWindow(2025, date(2025, 1, 1), date(2025, 2, 28), "2025"),
            exchange="NSE",
            top_n=10,
            min_days=30,
        )
    finally:
        conn.close()

    assert "BBB" in set(winners["symbol_id"])
    assert "JUMP" not in set(winners["symbol_id"])
    assert "NIFTY50" not in set(winners["symbol_id"])
    assert winners.iloc[0]["symbol_id"] == "BBB"


def test_year_window_uses_partial_latest_date(tmp_path: Path) -> None:
    db_path = _fixture_db(tmp_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        window = _year_window(conn, 2025, "NSE")
    finally:
        conn.close()

    assert window.end == date(2025, 2, 9)
    assert window.label == "2025 YTD through 2025-02-09"


def test_report_writes_expected_artifacts_and_excludes_head_shoulders_from_positive_presence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _fixture_db(tmp_path)

    def _fake_patterns(_con, symbol_id, signal_date, **_kwargs):
        if symbol_id == "BBB":
            return f"flag:confirmed:{signal_date.isoformat()};head_shoulders:confirmed:{signal_date.isoformat()}"
        if symbol_id == "AAA":
            return f"head_shoulders:confirmed:{signal_date.isoformat()}"
        return ""

    monkeypatch.setattr(winner_validation, "_pattern_labels", _fake_patterns)
    out_dir = tmp_path / "reports_out"
    summary = run_winner_validation_report(
        WinnerValidationConfig(
            years=(2025,),
            top_n=3,
            project_root=tmp_path,
            output_dir=out_dir,
            full_year_min_days=30,
            partial_year_min_days=30,
        )
    )

    assert summary["winner_count"] == 3
    assert (out_dir / "per_winner_multi_year.csv").exists()
    assert (out_dir / "factor_gap_summary.csv").exists()
    assert (out_dir / "pattern_mix.csv").exists()
    assert (out_dir / "pattern_timing_summary.csv").exists()
    assert (out_dir / "winner_validation_summary.json").exists()
    assert (out_dir / "winner_study_summary.md").exists()
    assert summary["pattern_presence"]["positive_pattern_symbols"] == 1
    rows = pd.read_csv(out_dir / "per_winner_multi_year.csv")
    assert "active_technical_proxy_score" in rows.columns
    assert "active_technical_proxy_pctile" in rows.columns
    assert "technical_score_current_active" not in rows.columns


def test_distribution_capture_and_quartile_helpers() -> None:
    frame = pd.DataFrame(
        {
            "active_technical_proxy_pctile": [10.0, 35.0, 55.0, 85.0],
            "low_to_high_rally_pct": [400.0, 200.0, 100.0, 50.0],
        }
    )

    dist = _active_proxy_distribution(frame)
    capture = _capture_rates(frame)
    quartiles = _rally_quartile_summary(frame)

    assert dist["above_50_count"] == 2
    assert capture["pctile_ge_70"]["captured_count"] == 1
    assert quartiles["top"]["median_active_technical_proxy_pctile"] == 10.0
    assert quartiles["bottom"]["median_active_technical_proxy_pctile"] == 85.0


def test_factor_summary_supports_grouped_ic() -> None:
    frame = pd.DataFrame(
        {
            "regime_bucket": ["bull", "bull", "bull", "bull"],
            "relative_strength": [1.0, 2.0, 3.0, 4.0],
            "relative_strength_pctile": [25.0, 50.0, 75.0, 100.0],
            "low_to_high_rally_pct": [10.0, 20.0, 30.0, 40.0],
        }
    )
    for factor in winner_validation.FACTOR_COLUMNS:
        if factor == "relative_strength":
            continue
        frame[factor] = [1.0, 1.0, 1.0, 1.0]
        frame[f"{factor}_pctile"] = [50.0, 50.0, 50.0, 50.0]

    rows = _factor_summary_by_group(frame, "regime_bucket")
    rs = next(row for row in rows if row["factor"] == "relative_strength")

    assert rs["regime_bucket"] == "bull"
    assert rs["active_proxy_weight"] == winner_validation.DEFAULT_FACTOR_WEIGHTS["relative_strength"]
    assert rs["live_ranker_weight"] == winner_validation.DEFAULT_FACTOR_WEIGHTS["relative_strength"]
    assert rs["top_quartile_hit_rate"] == 50.0
    assert rs["ic_vs_rally"] == 1.0


def test_pattern_timing_buckets_and_head_shoulders_diagnostic_only() -> None:
    frame = pd.DataFrame(
        [
            {
                "year": 2025,
                "symbol_id": "AAA",
                "signal_date": "2025-03-31",
                "patterns_near_low": "flag:confirmed:2025-03-30;head_shoulders:confirmed:2025-03-20",
            },
            {
                "year": 2025,
                "symbol_id": "BBB",
                "signal_date": "2025-03-31",
                "patterns_near_low": "darvas_box:confirmed:2025-02-10",
            },
        ]
    )

    mix, timing, presence = _pattern_summaries(frame)

    assert presence["positive_pattern_symbols"] == 2
    assert "head_shoulders" in set(mix["pattern_family"])
    timing_counts = dict(zip(timing["age_bucket"], timing["count"]))
    assert timing_counts["0-5"] == 1
    assert timing_counts["41-60"] == 1
    assert timing_counts["6-20"] == 0


def test_cli_smoke(monkeypatch, capsys) -> None:
    def _fake_run(config):
        assert config.years == (2025,)
        assert config.top_n == 2
        return {"artifact_dir": "/tmp/winner-validation"}

    monkeypatch.setattr(winner_validation, "run_winner_validation_report", _fake_run)
    winner_validation.main(["--years", "2025", "--top-n", "2"])

    assert "Wrote winner validation artifacts to /tmp/winner-validation" in capsys.readouterr().out
