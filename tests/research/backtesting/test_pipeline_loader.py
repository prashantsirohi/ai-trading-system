"""pipeline_runs loader + CLI smoke tests."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.research.backtesting import (
    discover_runs,
    load_ranked_by_date,
)


def _seed_run(base: Path, run_date: str, suffix: str, rows: list[dict]) -> Path:
    run_dir = base / f"pipeline-{run_date}-{suffix}"
    rank_dir = run_dir / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True)
    csv_path = rank_dir / "ranked_signals.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    return csv_path


def test_discover_runs_returns_chronological(tmp_path: Path) -> None:
    base = tmp_path / "pipeline_runs"
    base.mkdir()
    _seed_run(base, "2026-03-31", "aaaa1111", [{"symbol_id": "X"}])
    _seed_run(base, "2026-04-01", "bbbb2222", [{"symbol_id": "Y"}])
    runs = discover_runs(base)
    assert [d for d, _ in runs] == [date(2026, 3, 31), date(2026, 4, 1)]


def test_discover_runs_skips_non_matching_dirs(tmp_path: Path) -> None:
    base = tmp_path / "pipeline_runs"
    base.mkdir()
    (base / "scratch").mkdir()
    (base / "pipeline-not-a-date-xxx").mkdir()
    _seed_run(base, "2026-04-01", "bbbb2222", [{"symbol_id": "Y"}])
    runs = discover_runs(base)
    assert len(runs) == 1


def test_load_ranked_by_date_respects_window_and_symbol_filter(tmp_path: Path) -> None:
    base = tmp_path / "pipeline_runs"
    base.mkdir()
    _seed_run(base, "2026-03-31", "a", [{"symbol_id": "ACME"}, {"symbol_id": "OTHER"}])
    _seed_run(base, "2026-04-01", "b", [{"symbol_id": "ACME"}])
    _seed_run(base, "2026-05-01", "c", [{"symbol_id": "ACME"}])

    by_date = load_ranked_by_date(
        base,
        from_date=date(2026, 4, 1),
        to_date=date(2026, 4, 30),
        symbols=["ACME"],
    )
    assert list(by_date.keys()) == [date(2026, 4, 1)]
    assert list(by_date[date(2026, 4, 1)]["symbol_id"]) == ["ACME"]


def test_discover_picks_latest_attempt(tmp_path: Path) -> None:
    base = tmp_path / "pipeline_runs"
    run_dir = base / "pipeline-2026-04-01-abc12345"
    (run_dir / "rank" / "attempt_1").mkdir(parents=True)
    (run_dir / "rank" / "attempt_3").mkdir(parents=True)
    pd.DataFrame([{"symbol_id": "OLD"}]).to_csv(
        run_dir / "rank" / "attempt_1" / "ranked_signals.csv", index=False
    )
    pd.DataFrame([{"symbol_id": "NEW"}]).to_csv(
        run_dir / "rank" / "attempt_3" / "ranked_signals.csv", index=False
    )
    by_date = load_ranked_by_date(base)
    assert list(by_date[date(2026, 4, 1)]["symbol_id"]) == ["NEW"]


def test_cli_runs_end_to_end(tmp_path: Path) -> None:
    base = tmp_path / "pipeline_runs"
    base.mkdir()
    full_row = {
        "symbol_id": "ACME",
        "exchange": "NSE",
        "close": 100.0,
        "composite_score": 80.0,
        "eligible_rank": 1,
        "is_stage2_uptrend": True,
        "sector_name": "TECH",
        "sector_strength_score": 0.7,
        "sma_11": 99.0,
        "sma_20": 97.0,
        "sma_50": 92.0,
        "sma_200": 80.0,
        "atr_14": 2.0,
        "volume_ratio_20": 2.0,
        "swing_low_20": 94.0,
        "delivery_pct": 60.0,
    }
    _seed_run(base, "2026-03-31", "aaaa1111", [full_row])
    _seed_run(base, "2026-04-01", "bbbb2222", [{**full_row, "close": 102.0}])
    _seed_run(
        base,
        "2026-04-02",
        "cccc3333",
        [{**full_row, "close": 97.0, "sma_20": 100.0}],  # trigger 20DMA exit
    )

    out_dir = tmp_path / "out"
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "ai_trading_system.research.backtesting",
            "--risk-profile",
            "balanced_swing",
            "--pipeline-runs-dir",
            str(base),
            "--out",
            str(out_dir),
            "--equity",
            "500000",
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    summary = json.loads(proc.stdout)
    assert summary["profile"] == "balanced_swing"
    assert summary["trade_count"] >= 1
    assert summary["trading_days"] == 3
    assert Path(summary["trades_path"]).exists()
