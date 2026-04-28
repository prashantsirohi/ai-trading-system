"""Unit tests for market_stage.py breadth-based regime classifier."""
from __future__ import annotations

import pytest
import pandas as pd
import duckdb
from pathlib import Path

from ai_trading_system.domains.ranking.market_stage import get_market_stage


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_snapshot_db(tmp_path: Path, rows: list[dict]) -> str:
    """Create a minimal DuckDB with weekly_stage_snapshot populated."""
    db_path = str(tmp_path / "ohlcv.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("""
        CREATE TABLE weekly_stage_snapshot (
            symbol VARCHAR,
            week_end_date DATE,
            stage_label VARCHAR,
            stage_confidence DOUBLE,
            stage_transition VARCHAR,
            ma10w DOUBLE, ma30w DOUBLE, ma40w DOUBLE,
            ma30w_slope_4w DOUBLE, weekly_rs_score DOUBLE,
            weekly_volume_ratio DOUBLE, support_level DOUBLE,
            resistance_level DOUBLE, created_at TIMESTAMP, run_id VARCHAR
        )
    """)
    if rows:
        df = pd.DataFrame(rows)
        conn.execute("INSERT INTO weekly_stage_snapshot SELECT * FROM df")
    conn.close()
    return db_path


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_fallback_on_missing_table(tmp_path):
    """Empty DB (no table) returns MIXED fallback."""
    db = str(tmp_path / "empty.duckdb")
    duckdb.connect(db).close()
    result = get_market_stage(db)
    assert result["market_stage"] == "MIXED"
    assert result["method"] == "fallback_default"
    assert result["classified_symbols"] == 0


def test_fallback_on_insufficient_symbols(tmp_path):
    """Fewer than min_classified_symbols → MIXED fallback."""
    rows = [
        {"symbol": f"SYM{i}", "week_end_date": "2026-04-25",
         "stage_label": "S4", "stage_confidence": 0.9,
         "stage_transition": "NONE",
         "ma10w": 100, "ma30w": 100, "ma40w": 100,
         "ma30w_slope_4w": 0.0, "weekly_rs_score": 50.0,
         "weekly_volume_ratio": 1.0, "support_level": 90.0,
         "resistance_level": 110.0, "created_at": "2026-04-25 00:00:00",
         "run_id": "t1"}
        for i in range(10)   # only 10, well below 200
    ]
    db = _make_snapshot_db(tmp_path, rows)
    result = get_market_stage(db, min_classified_symbols=200)
    assert result["market_stage"] == "MIXED"
    assert result["method"] == "fallback_default"


def test_s4_bear_market(tmp_path):
    """56% S4 symbols → market_stage=S4."""
    n = 400
    s4_n = 225   # 56.25%
    rows = []
    for i in range(s4_n):
        rows.append({"symbol": f"S4_{i}", "week_end_date": "2026-04-25",
                     "stage_label": "S4", "stage_confidence": 0.9,
                     "stage_transition": "NONE",
                     "ma10w": 100, "ma30w": 110, "ma40w": 108,
                     "ma30w_slope_4w": -0.01, "weekly_rs_score": 30.0,
                     "weekly_volume_ratio": 0.8, "support_level": 90.0,
                     "resistance_level": 105.0,
                     "created_at": "2026-04-25 00:00:00", "run_id": "t1"})
    for i in range(n - s4_n):
        rows.append({"symbol": f"S2_{i}", "week_end_date": "2026-04-25",
                     "stage_label": "S2", "stage_confidence": 0.8,
                     "stage_transition": "NONE",
                     "ma10w": 100, "ma30w": 90, "ma40w": 88,
                     "ma30w_slope_4w": 0.01, "weekly_rs_score": 60.0,
                     "weekly_volume_ratio": 1.2, "support_level": 85.0,
                     "resistance_level": 105.0,
                     "created_at": "2026-04-25 00:00:00", "run_id": "t1"})
    db = _make_snapshot_db(tmp_path, rows)
    result = get_market_stage(db, min_classified_symbols=200)
    assert result["market_stage"] == "S4"
    assert result["method"] == "breadth"
    assert result["s4_pct"] > 0.40


def test_s2_bull_market(tmp_path):
    """45% S2 symbols → market_stage=S2."""
    n = 500
    s2_n = 230   # 46%
    rows = []
    for i in range(s2_n):
        rows.append({"symbol": f"S2_{i}", "week_end_date": "2026-04-25",
                     "stage_label": "S2", "stage_confidence": 0.8,
                     "stage_transition": "NONE",
                     "ma10w": 100, "ma30w": 90, "ma40w": 88,
                     "ma30w_slope_4w": 0.01, "weekly_rs_score": 65.0,
                     "weekly_volume_ratio": 1.2, "support_level": 85.0,
                     "resistance_level": 105.0,
                     "created_at": "2026-04-25 00:00:00", "run_id": "t1"})
    for i in range(n - s2_n):
        rows.append({"symbol": f"S1_{i}", "week_end_date": "2026-04-25",
                     "stage_label": "S1", "stage_confidence": 0.7,
                     "stage_transition": "NONE",
                     "ma10w": 100, "ma30w": 100, "ma40w": 100,
                     "ma30w_slope_4w": 0.001, "weekly_rs_score": 50.0,
                     "weekly_volume_ratio": 0.9, "support_level": 95.0,
                     "resistance_level": 105.0,
                     "created_at": "2026-04-25 00:00:00", "run_id": "t1"})
    db = _make_snapshot_db(tmp_path, rows)
    result = get_market_stage(db, min_classified_symbols=200)
    assert result["market_stage"] == "S2"
    assert result["s2_pct"] > 0.40


def test_mixed_market(tmp_path):
    """Roughly equal distribution → MIXED."""
    n_each = 100
    stages = ["S1", "S2", "S3", "S4"]
    rows = []
    for st in stages:
        for i in range(n_each):
            rows.append({"symbol": f"{st}_{i}", "week_end_date": "2026-04-25",
                         "stage_label": st, "stage_confidence": 0.7,
                         "stage_transition": "NONE",
                         "ma10w": 100, "ma30w": 100, "ma40w": 100,
                         "ma30w_slope_4w": 0.0, "weekly_rs_score": 50.0,
                         "weekly_volume_ratio": 1.0, "support_level": 95.0,
                         "resistance_level": 105.0,
                         "created_at": "2026-04-25 00:00:00", "run_id": "t1"})
    db = _make_snapshot_db(tmp_path, rows)
    result = get_market_stage(db, min_classified_symbols=200)
    assert result["market_stage"] == "MIXED"
    assert result["s2_pct"] == pytest.approx(0.25, abs=0.01)


def test_asof_filters_correctly(tmp_path):
    """asof cuts off more-recent rows; older snapshot determines regime."""
    rows = []
    # Old snapshot (2026-01-10): 50% S2 → bull
    for i in range(250):
        rows.append({"symbol": f"SYM_{i}", "week_end_date": "2026-01-10",
                     "stage_label": "S2" if i < 250 else "S4",
                     "stage_confidence": 0.8, "stage_transition": "NONE",
                     "ma10w": 100, "ma30w": 90, "ma40w": 88,
                     "ma30w_slope_4w": 0.01, "weekly_rs_score": 65.0,
                     "weekly_volume_ratio": 1.2, "support_level": 85.0,
                     "resistance_level": 105.0,
                     "created_at": "2026-01-10 00:00:00", "run_id": "r1"})
    # Newer snapshot (2026-04-25): same symbols now mostly S4 → bear
    for i in range(250):
        rows.append({"symbol": f"SYM_{i}", "week_end_date": "2026-04-25",
                     "stage_label": "S4", "stage_confidence": 0.9,
                     "stage_transition": "NONE",
                     "ma10w": 100, "ma30w": 110, "ma40w": 108,
                     "ma30w_slope_4w": -0.01, "weekly_rs_score": 30.0,
                     "weekly_volume_ratio": 0.8, "support_level": 90.0,
                     "resistance_level": 105.0,
                     "created_at": "2026-04-25 00:00:00", "run_id": "r2"})
    db = _make_snapshot_db(tmp_path, rows)
    # asof=old date → uses 2026-01-10 rows → S2 bull
    result_old = get_market_stage(db, asof="2026-01-15", min_classified_symbols=100)
    assert result_old["market_stage"] == "S2"
    # asof=latest → uses 2026-04-25 rows → S4 bear
    result_new = get_market_stage(db, asof="2026-04-28", min_classified_symbols=100)
    assert result_new["market_stage"] == "S4"


def test_undefined_labels_excluded(tmp_path):
    """UNDEFINED labels are excluded from the breadth count."""
    rows = []
    # 220 UNDEFINED (should be ignored)
    for i in range(220):
        rows.append({"symbol": f"U_{i}", "week_end_date": "2026-04-25",
                     "stage_label": "UNDEFINED", "stage_confidence": 0.0,
                     "stage_transition": "NONE",
                     "ma10w": 100, "ma30w": 100, "ma40w": 100,
                     "ma30w_slope_4w": 0.0, "weekly_rs_score": 50.0,
                     "weekly_volume_ratio": 1.0, "support_level": 95.0,
                     "resistance_level": 105.0,
                     "created_at": "2026-04-25 00:00:00", "run_id": "t1"})
    # 220 S4 (should trigger bear)
    for i in range(220):
        rows.append({"symbol": f"S4_{i}", "week_end_date": "2026-04-25",
                     "stage_label": "S4", "stage_confidence": 0.9,
                     "stage_transition": "NONE",
                     "ma10w": 100, "ma30w": 110, "ma40w": 108,
                     "ma30w_slope_4w": -0.01, "weekly_rs_score": 30.0,
                     "weekly_volume_ratio": 0.8, "support_level": 90.0,
                     "resistance_level": 105.0,
                     "created_at": "2026-04-25 00:00:00", "run_id": "t1"})
    # 100 S1 and S2 each
    for st in ["S1", "S2"]:
        for i in range(100):
            rows.append({"symbol": f"{st}_{i}", "week_end_date": "2026-04-25",
                         "stage_label": st, "stage_confidence": 0.7,
                         "stage_transition": "NONE",
                         "ma10w": 100, "ma30w": 100, "ma40w": 100,
                         "ma30w_slope_4w": 0.0, "weekly_rs_score": 50.0,
                         "weekly_volume_ratio": 1.0, "support_level": 95.0,
                         "resistance_level": 105.0,
                         "created_at": "2026-04-25 00:00:00", "run_id": "t1"})
    db = _make_snapshot_db(tmp_path, rows)
    # Total classified (non-UNDEFINED): 220+100+100 = 420
    # S4% = 220/420 ≈ 52% → S4
    result = get_market_stage(db, min_classified_symbols=200)
    assert result["market_stage"] == "S4"
    assert result["classified_symbols"] == 420


def test_returns_all_pct_keys(tmp_path):
    """Result dict always contains expected keys."""
    rows = [
        {"symbol": f"SYM_{i}", "week_end_date": "2026-04-25",
         "stage_label": "S2", "stage_confidence": 0.8,
         "stage_transition": "NONE",
         "ma10w": 100, "ma30w": 90, "ma40w": 88,
         "ma30w_slope_4w": 0.01, "weekly_rs_score": 65.0,
         "weekly_volume_ratio": 1.2, "support_level": 85.0,
         "resistance_level": 105.0,
         "created_at": "2026-04-25 00:00:00", "run_id": "t1"}
        for i in range(300)
    ]
    db = _make_snapshot_db(tmp_path, rows)
    result = get_market_stage(db, min_classified_symbols=200)
    for key in ["market_stage", "method", "s2_pct", "s4_pct", "s1_pct", "s3_pct",
                "classified_symbols", "asof"]:
        assert key in result, f"Missing key: {key}"
