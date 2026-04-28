"""Integration tests for the weekly stage gate in eligibility + ranker wiring."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.domains.ranking.eligibility import apply_rank_eligibility


# ── eligibility.py unit tests ────────────────────────────────────────────────

def _frame(rows):
    """Build a minimal candidates frame."""
    return pd.DataFrame(rows)


def test_gate_off_by_default_passes_all():
    df = _frame([
        {"symbol_id": "A", "close": 100.0, "weekly_stage_label": "S4", "weekly_stage_confidence": 0.9},
        {"symbol_id": "B", "close": 100.0, "weekly_stage_label": "S2", "weekly_stage_confidence": 0.9},
    ])
    out = apply_rank_eligibility(df)
    assert out["eligible_rank"].all()


def test_gate_blocks_non_s2_with_snapshot():
    df = _frame([
        {"symbol_id": "A", "close": 100.0, "weekly_stage_label": "S4", "weekly_stage_confidence": 0.9},
        {"symbol_id": "B", "close": 100.0, "weekly_stage_label": "S2", "weekly_stage_confidence": 0.9},
        {"symbol_id": "C", "close": 100.0, "weekly_stage_label": "S1", "weekly_stage_confidence": 0.8},
    ])
    out = apply_rank_eligibility(df, weekly_stage_gate_enabled=True)
    result = out.set_index("symbol_id")
    assert result.loc["B", "eligible_rank"] is True or bool(result.loc["B", "eligible_rank"])
    assert not result.loc["A", "eligible_rank"]
    assert not result.loc["C", "eligible_rank"]
    # Rejection reason tagged correctly
    assert any("weekly_stage:S4" in r for r in result.loc["A", "rejection_reasons"])
    assert any("weekly_stage:S1" in r for r in result.loc["C", "rejection_reasons"])


def test_gate_passes_no_snapshot_symbols():
    """Symbols with no snapshot (NaN label) pass through — backfill catch-up."""
    df = _frame([
        {"symbol_id": "A", "close": 100.0, "weekly_stage_label": None,  "weekly_stage_confidence": None},
        {"symbol_id": "B", "close": 100.0, "weekly_stage_label": "S4",  "weekly_stage_confidence": 0.9},
    ])
    out = apply_rank_eligibility(df, weekly_stage_gate_enabled=True)
    result = out.set_index("symbol_id")
    assert bool(result.loc["A", "eligible_rank"])   # no snapshot → pass
    assert not result.loc["B", "eligible_rank"]     # S4 → blocked


def test_gate_blocks_s2_below_confidence():
    df = _frame([
        # S2 label but confidence too low (0.4 < 0.6 default)
        {"symbol_id": "A", "close": 100.0, "weekly_stage_label": "S2", "weekly_stage_confidence": 0.4},
    ])
    out = apply_rank_eligibility(df, weekly_stage_gate_enabled=True)
    assert not out.iloc[0]["eligible_rank"]
    assert any("weekly_stage:S2" in r for r in out.iloc[0]["rejection_reasons"])


def test_gate_allows_s2_above_confidence():
    df = _frame([
        {"symbol_id": "A", "close": 100.0, "weekly_stage_label": "S2", "weekly_stage_confidence": 0.8},
    ])
    out = apply_rank_eligibility(df, weekly_stage_gate_enabled=True)
    assert bool(out.iloc[0]["eligible_rank"])


def test_gate_stacks_with_price_filter():
    """Both gates apply independently; both reasons appear."""
    df = _frame([
        {"symbol_id": "A", "close": 10.0,  "weekly_stage_label": "S4", "weekly_stage_confidence": 0.9},
    ])
    out = apply_rank_eligibility(df, weekly_stage_gate_enabled=True)
    reasons = out.iloc[0]["rejection_reasons"]
    assert "min_price" in reasons
    assert any("weekly_stage:S4" in r for r in reasons)


def test_gate_no_column_does_not_crash():
    """If weekly_stage_label column is absent the gate is silently skipped."""
    df = _frame([{"symbol_id": "A", "close": 100.0}])
    out = apply_rank_eligibility(df, weekly_stage_gate_enabled=True)
    assert bool(out.iloc[0]["eligible_rank"])


# ── _apply_weekly_stage_gate unit test (no real DB needed) ───────────────────

def test_apply_weekly_stage_gate_joins_columns(tmp_path: Path):
    """Verify the ranker method joins snapshot columns and logs counts."""
    from ai_trading_system.domains.ranking.stage_classifier import StageResult
    from ai_trading_system.domains.ranking.stage_store import write_snapshots
    import pandas as pd

    db = tmp_path / "ohlcv.duckdb"
    pr = tmp_path / "p"
    write_snapshots(
        [
            StageResult("RELIANCE", pd.Timestamp("2026-04-25"), "S2", 0.9, "NONE",
                        100, 95, 90, 0.01, 70.0, 1.2, 88.0, 110.0),
            StageResult("INFY",     pd.Timestamp("2026-04-25"), "S4", 0.8, "NONE",
                        80,  90, 95, -0.02, 40.0, 0.8, 70.0,  90.0),
        ],
        ohlcv_db_path=db, parquet_root=pr, run_id="t1",
    )

    from ai_trading_system.domains.ranking.ranker import StockRanker
    ranker = StockRanker.__new__(StockRanker)
    ranker.ohlcv_db_path = str(db)

    candidates = pd.DataFrame({
        "symbol_id": ["RELIANCE", "INFY", "TCS"],
        "close": [1400.0, 1300.0, 2900.0],
    })
    result = ranker._apply_weekly_stage_gate(candidates, "2026-04-27")

    assert "weekly_stage_label" in result.columns
    assert "weekly_stage_confidence" in result.columns

    idx = result.set_index("symbol_id")
    assert idx.loc["RELIANCE", "weekly_stage_label"] == "S2"
    assert idx.loc["INFY",     "weekly_stage_label"] == "S4"
    assert pd.isna(idx.loc["TCS", "weekly_stage_label"])  # no snapshot → NaN
