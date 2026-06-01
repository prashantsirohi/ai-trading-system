"""Tests for the v2 panel fast-path that reads from rank_cohort_performance."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.research.perf_tracker.schema import (
    RANK_COHORT_ALTER_DDLS,
    RANK_COHORT_DDL,
    RANK_COHORT_INDEX_DDL,
)
from ai_trading_system.research.ranking_optimisation import data_v2 as data_v2_module
from ai_trading_system.research.ranking_optimisation.data_v2 import (
    PRODUCTION_FACTOR_COLUMNS,
    load_live_factor_panel,
)


def _make_research_db_with_rows(
    tmp_path: Path,
    *,
    run_date: str,
    n: int = 100,
    factor_scale: float = 1.0,
    include_horizon_columns: tuple[int, ...] = (5, 10, 20, 60),
) -> Path:
    db_dir = tmp_path / "data"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "research.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(RANK_COHORT_DDL)
    con.execute(RANK_COHORT_INDEX_DDL)
    for stmt in RANK_COHORT_ALTER_DDLS:
        con.execute(stmt)
    rows = []
    for i in range(n):
        # forward_return stored as percent — fast path divides by 100 on read.
        rows.append(
            {
                "run_date": run_date,
                "symbol_id": f"SYM{i:03d}",
                "exchange": "NSE",
                "rank_position": i + 1,
                "composite_score": 50.0 + i * 0.1,
                "composite_score_adjusted": 50.0 + i * 0.1,
                "rank_mode": "default",
                "watchlist_bucket": None,
                "config_id": None,
                "fwd_5d_return":  (1.0 * i) if 5 in include_horizon_columns else None,
                "fwd_10d_return": (1.5 * i) if 10 in include_horizon_columns else None,
                "fwd_20d_return": (2.0 * i) if 20 in include_horizon_columns else None,
                "fwd_60d_return": (3.0 * i) if 60 in include_horizon_columns else None,
                "fwd_5d_matured_at": None,
                "fwd_10d_matured_at": None,
                "fwd_20d_matured_at": None,
                "fwd_60d_matured_at": None,
                "factor_rs":             10.0 * factor_scale + i,
                "factor_vol":            20.0 * factor_scale + i,
                "factor_trend":          30.0 * factor_scale + i,
                "factor_prox":           40.0 * factor_scale + i,
                "factor_deliv":          50.0 * factor_scale + i,
                "factor_sector":         60.0 * factor_scale + i,
                "factor_momentum_accel": 70.0 * factor_scale + i,
                "factor_above_200dma":   80.0 * factor_scale + i,
                "factor_liquidity":      None,
                "factor_delivery_trend": None,
                "sector_name": "IT",
            }
        )
    df = pd.DataFrame(rows)
    con.register("incoming", df)
    con.execute(
        "INSERT INTO rank_cohort_performance BY NAME "
        "SELECT *, CURRENT_TIMESTAMP AS inserted_at FROM incoming"
    )
    con.unregister("incoming")
    con.close()
    return db_path


def test_load_live_factor_panel_uses_table_when_rows_present(tmp_path: Path, monkeypatch):
    """Fast path: when the table has matching rows, do NOT invoke the slow loader."""
    _make_research_db_with_rows(tmp_path, run_date="2023-03-31", n=120)

    call_counter = {"loader_calls": 0}

    def fake_slow_loader(*args, **kwargs):
        call_counter["loader_calls"] += 1
        return {}

    monkeypatch.setattr(data_v2_module, "load_research_ranked_by_date", fake_slow_loader)

    panel = load_live_factor_panel(
        "2023-03-31",
        horizon_days=20,
        project_root=tmp_path,
        degenerate_var_floor=0.5,
    )

    assert panel.n == 120
    assert call_counter["loader_calls"] == 0, "slow loader was invoked despite fast path having data"
    # All 8 production factor score columns present.
    for col in PRODUCTION_FACTOR_COLUMNS:
        assert col in panel.df.columns
    # Forward return was scaled from percent to fraction (÷100).
    # row 50 had fwd_20d_return=100.0 percent → 1.0 fraction.
    fr_row_50 = panel.df.loc[panel.df["symbol_id"] == "SYM050", "forward_return"].iloc[0]
    assert fr_row_50 == pytest.approx(1.0)


def test_load_live_factor_panel_falls_back_to_live_when_table_empty(tmp_path: Path, monkeypatch):
    """If the table is empty, fall back to the slow loader."""
    # Create the table but with no rows.
    db_dir = tmp_path / "data"
    db_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_dir / "research.duckdb"))
    con.execute(RANK_COHORT_DDL)
    con.execute(RANK_COHORT_INDEX_DDL)
    for stmt in RANK_COHORT_ALTER_DDLS:
        con.execute(stmt)
    con.close()

    call_counter = {"loader_calls": 0}

    def fake_slow_loader(*args, **kwargs):
        call_counter["loader_calls"] += 1
        return {}  # empty → caller returns empty panel

    monkeypatch.setattr(data_v2_module, "load_research_ranked_by_date", fake_slow_loader)

    panel = load_live_factor_panel(
        "2023-03-31",
        horizon_days=20,
        project_root=tmp_path,
    )

    assert call_counter["loader_calls"] == 1, "slow loader should have been invoked on empty table"
    assert panel.n == 0


def test_load_live_factor_panel_falls_back_when_horizon_not_in_table(tmp_path: Path, monkeypatch):
    """Horizons other than 5/10/20/60 must skip the fast path."""
    _make_research_db_with_rows(tmp_path, run_date="2023-03-31", n=120)

    call_counter = {"loader_calls": 0}

    def fake_slow_loader(*args, **kwargs):
        call_counter["loader_calls"] += 1
        return {}

    monkeypatch.setattr(data_v2_module, "load_research_ranked_by_date", fake_slow_loader)

    panel = load_live_factor_panel(
        "2023-03-31",
        horizon_days=30,  # not in {5,10,20,60}
        project_root=tmp_path,
    )

    assert call_counter["loader_calls"] == 1, "fast path should be skipped for non-standard horizon"
    assert panel.n == 0


def test_load_live_factor_panel_falls_back_when_horizon_column_null(tmp_path: Path, monkeypatch):
    """If table rows exist but the requested horizon's column is all-null, fall back."""
    # Build a table with all fwd_20d_return NULL (only fwd_5d populated).
    _make_research_db_with_rows(
        tmp_path,
        run_date="2023-03-31",
        n=120,
        include_horizon_columns=(5,),
    )

    call_counter = {"loader_calls": 0}

    def fake_slow_loader(*args, **kwargs):
        call_counter["loader_calls"] += 1
        return {}

    monkeypatch.setattr(data_v2_module, "load_research_ranked_by_date", fake_slow_loader)

    panel = load_live_factor_panel(
        "2023-03-31",
        horizon_days=20,
        project_root=tmp_path,
    )

    assert call_counter["loader_calls"] == 1, "fast path should fall back when horizon column is null"
    assert panel.n == 0


def test_load_panel_from_table_aliases_factor_columns(tmp_path: Path, monkeypatch):
    """Read path must map factor_* → PRODUCTION_FACTOR_COLUMNS names so fitness reads correctly."""
    _make_research_db_with_rows(tmp_path, run_date="2023-03-31", n=120)

    monkeypatch.setattr(
        data_v2_module,
        "load_research_ranked_by_date",
        lambda *a, **kw: {},
    )

    panel = load_live_factor_panel(
        "2023-03-31",
        horizon_days=20,
        project_root=tmp_path,
        degenerate_var_floor=0.5,
    )

    # Columns must use PRODUCTION names, not factor_* names.
    assert "rel_strength_score" in panel.df.columns
    assert "above_200dma_score" in panel.df.columns
    assert "factor_rs" not in panel.df.columns
    assert "factor_above_200dma" not in panel.df.columns


def test_load_live_factor_panel_prefer_table_false_skips_fast_path(tmp_path: Path, monkeypatch):
    """When prefer_table=False, the fast path is bypassed even if rows exist."""
    _make_research_db_with_rows(tmp_path, run_date="2023-03-31", n=120)

    call_counter = {"loader_calls": 0}

    def fake_slow_loader(*args, **kwargs):
        call_counter["loader_calls"] += 1
        return {}

    monkeypatch.setattr(data_v2_module, "load_research_ranked_by_date", fake_slow_loader)

    panel = load_live_factor_panel(
        "2023-03-31",
        horizon_days=20,
        project_root=tmp_path,
        prefer_table=False,
    )

    assert call_counter["loader_calls"] == 1
    assert panel.n == 0
