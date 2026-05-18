"""Hermetic tests for historical_backfill — synthetic DuckDB via tmp_path."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.research.perf_tracker import (
    backfill as backfill_module,
    forward_returns as forward_returns_module,
    historical_backfill as historical_module,
)
from ai_trading_system.research.perf_tracker.backfill import build_rows_from_ranked_frame
from ai_trading_system.research.perf_tracker.forward_returns import compute_forward_returns
from ai_trading_system.research.perf_tracker.historical_backfill import run_historical_backfill
from ai_trading_system.research.perf_tracker.schema import (
    RANK_COHORT_ALTER_DDLS,
    RANK_COHORT_DDL,
    RANK_COHORT_INDEX_DDL,
)


def _make_synthetic_ranked_frame(n: int = 50) -> pd.DataFrame:
    """A ranked-signals-shaped DataFrame for one trading date."""
    return pd.DataFrame(
        {
            "symbol_id":                   [f"SYM{i:03d}" for i in range(n)],
            "exchange":                    ["NSE"] * n,
            "composite_score":             [50.0 + i / 2.0 for i in range(n)],
            "composite_score_adjusted":    [50.0 + i / 2.0 for i in range(n)],
            "rank_mode":                   ["default"] * n,
            "sector_name":                 ["IT"] * n,
            "rel_strength_score":          [10.0 + i for i in range(n)],
            "vol_intensity_score":         [20.0 + i for i in range(n)],
            "trend_score_score":           [30.0 + i for i in range(n)],
            "prox_high_score":             [40.0 + i for i in range(n)],
            "delivery_pct_score":          [50.0 + i for i in range(n)],
            "sector_strength_score":       [60.0 + i for i in range(n)],
            "momentum_acceleration_score": [70.0 + i for i in range(n)],
            "above_200dma_score":          [80.0 + i for i in range(n)],
        }
    )


def _make_research_db_with_table(tmp_path: Path) -> Path:
    research_dir = tmp_path / "data"
    research_dir.mkdir(parents=True, exist_ok=True)
    db_path = research_dir / "research.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(RANK_COHORT_DDL)
    con.execute(RANK_COHORT_INDEX_DDL)
    for stmt in RANK_COHORT_ALTER_DDLS:
        con.execute(stmt)
    con.close()
    return db_path


def _make_research_ohlcv_db(tmp_path: Path, dates: list[date], symbols: list[str]) -> Path:
    """Build a minimal research_ohlcv.duckdb _catalog used by enumerate_trading_dates
    and compute_forward_returns."""
    db_dir = tmp_path / "data" / "research"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "research_ohlcv.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            exchange  VARCHAR,
            timestamp TIMESTAMP,
            close     DOUBLE,
            volume    BIGINT,
            high      DOUBLE,
            low       DOUBLE,
            open      DOUBLE
        )
        """
    )
    rows = [
        (sym, "NSE", pd.Timestamp(d), 100.0 + i, 1000, 110.0, 90.0, 100.0)
        for i, d in enumerate(dates)
        for sym in symbols
    ]
    con.register("incoming", pd.DataFrame(rows, columns=["symbol_id","exchange","timestamp","close","volume","high","low","open"]))
    con.execute("INSERT INTO _catalog SELECT * FROM incoming")
    con.unregister("incoming")
    con.close()
    return db_path


# ---------- build_rows_from_ranked_frame ------------------------------------


def test_build_rows_from_ranked_frame_maps_production_columns():
    ranked = _make_synthetic_ranked_frame(n=5)
    out = build_rows_from_ranked_frame("2024-03-15", ranked)
    assert len(out) == 5
    # Column mapping applied.
    assert "factor_rs" in out.columns
    assert "factor_above_200dma" in out.columns
    # Required schema columns present.
    for col in ("run_date", "symbol_id", "exchange", "rank_position",
                "watchlist_bucket", "config_id"):
        assert col in out.columns
    # Re-rank by score: best row gets rank_position=1.
    top = out.sort_values("composite_score", ascending=False).iloc[0]
    assert int(top["rank_position"]) == 1


def test_build_rows_from_ranked_frame_re_ranks_input_order_agnostic():
    ranked = _make_synthetic_ranked_frame(n=10)
    # Shuffle deliberately.
    ranked = ranked.sample(frac=1, random_state=42).reset_index(drop=True)
    out = build_rows_from_ranked_frame("2024-03-15", ranked)
    # First row (after re-rank) should be the highest composite_score.
    assert out.iloc[0]["composite_score"] == ranked["composite_score"].max()


def test_build_rows_from_ranked_frame_attaches_buckets_when_given():
    ranked = _make_synthetic_ranked_frame(n=3)
    buckets = pd.DataFrame(
        {"symbol_id": ["SYM000", "SYM001"], "watchlist_bucket": ["CORE_MOMENTUM", "EARLY_STAGE2"]}
    )
    out = build_rows_from_ranked_frame("2024-03-15", ranked, buckets=buckets)
    buckets_map = dict(zip(out["symbol_id"], out["watchlist_bucket"]))
    assert buckets_map["SYM000"] == "CORE_MOMENTUM"
    assert buckets_map["SYM001"] == "EARLY_STAGE2"
    assert pd.isna(buckets_map["SYM002"])


def test_build_rows_from_ranked_frame_handles_missing_factor_columns_gracefully():
    ranked = _make_synthetic_ranked_frame(n=3).drop(columns=["above_200dma_score"])
    out = build_rows_from_ranked_frame("2024-03-15", ranked)
    # Missing column becomes NA but row is built.
    assert out["factor_above_200dma"].isna().all()


# ---------- compute_forward_returns ohlcv_db_path param ---------------------


def test_compute_forward_returns_accepts_ohlcv_db_path(tmp_path: Path):
    dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4),
             date(2024, 1, 5), date(2024, 1, 8), date(2024, 1, 9)]
    ohlcv_db = _make_research_ohlcv_db(tmp_path, dates=dates, symbols=["AAA"])
    rows = pd.DataFrame(
        {"run_date": [str(dates[0])], "symbol_id": ["AAA"], "exchange": ["NSE"]}
    )
    enriched = compute_forward_returns(
        rows,
        ohlcv_db_path=ohlcv_db,
        horizons=(5,),
    )
    # 5-bar forward close was added 5 rows after dates[0] in the synthetic data.
    # close at idx 0 was 100.0, at idx 5 was 105.0 → return = 5%.
    assert "fwd_5d_return" in enriched.columns
    assert enriched["fwd_5d_return"].iloc[0] == pytest.approx(5.0)


# ---------- run_historical_backfill end-to-end (monkeypatched) --------------


def test_run_historical_backfill_inserts_rows(tmp_path: Path, monkeypatch):
    # Prepare the destination research.duckdb (rank_cohort_performance table).
    _make_research_db_with_table(tmp_path)
    # Prepare a minimal research OHLCV calendar for the date enumerator.
    trading_days = [date(2024, 3, 1), date(2024, 3, 4), date(2024, 3, 5)]
    _make_research_ohlcv_db(tmp_path, dates=trading_days, symbols=["AAA", "BBB"])

    def fake_loader(project_root, *, from_date, to_date, exchange, benchmark_symbol):
        # Return synthetic ranked frames for the trading_days in range.
        return {
            d: _make_synthetic_ranked_frame(n=5)
            for d in trading_days if from_date <= d <= to_date
        }

    def fake_forward(rows, *, project_root=None, horizons=None, ohlcv_db_path=None):
        out = rows.copy()
        for h in (5, 10, 20, 60):
            out[f"fwd_{h}d_return"] = 1.0 * h  # known values for assertion
            out[f"fwd_{h}d_matured_at"] = pd.NaT
        return out

    monkeypatch.setattr(historical_module, "load_research_ranked_by_date", fake_loader)
    monkeypatch.setattr(historical_module, "compute_forward_returns", fake_forward)

    result = run_historical_backfill(
        from_date=date(2024, 3, 1),
        to_date=date(2024, 3, 5),
        project_root=tmp_path,
        frequency="daily",
    )

    assert result["dates_processed"] == 3
    assert result["rows_upserted"] == 3 * 5

    db_path = tmp_path / "data" / "research.duckdb"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        total = con.execute("SELECT COUNT(*) FROM rank_cohort_performance").fetchone()[0]
        sample = con.execute(
            "SELECT factor_rs, factor_above_200dma, fwd_20d_return "
            "FROM rank_cohort_performance ORDER BY run_date, rank_position LIMIT 1"
        ).fetchone()
    finally:
        con.close()
    assert total == 15
    assert sample[0] is not None  # factor_rs populated
    assert sample[1] is not None  # factor_above_200dma populated
    assert sample[2] == pytest.approx(20.0)  # fwd_20d_return from fake


def test_historical_backfill_idempotent_on_rerun(tmp_path: Path, monkeypatch):
    _make_research_db_with_table(tmp_path)
    trading_days = [date(2024, 3, 1), date(2024, 3, 4)]
    _make_research_ohlcv_db(tmp_path, dates=trading_days, symbols=["AAA"])

    def fake_loader(project_root, *, from_date, to_date, exchange, benchmark_symbol):
        return {d: _make_synthetic_ranked_frame(n=3) for d in trading_days}

    def fake_forward(rows, *, project_root=None, horizons=None, ohlcv_db_path=None):
        out = rows.copy()
        for h in (5, 10, 20, 60):
            out[f"fwd_{h}d_return"] = 0.0
            out[f"fwd_{h}d_matured_at"] = pd.NaT
        return out

    monkeypatch.setattr(historical_module, "load_research_ranked_by_date", fake_loader)
    monkeypatch.setattr(historical_module, "compute_forward_returns", fake_forward)

    run_historical_backfill(
        from_date=date(2024, 3, 1), to_date=date(2024, 3, 5),
        project_root=tmp_path, frequency="daily",
    )
    run_historical_backfill(
        from_date=date(2024, 3, 1), to_date=date(2024, 3, 5),
        project_root=tmp_path, frequency="daily",
    )

    con = duckdb.connect(str(tmp_path / "data" / "research.duckdb"), read_only=True)
    try:
        total = con.execute("SELECT COUNT(*) FROM rank_cohort_performance").fetchone()[0]
    finally:
        con.close()
    assert total == 2 * 3  # 2 dates × 3 symbols, NOT 12 (no duplicates)


def test_historical_backfill_rejects_unknown_frequency(tmp_path: Path):
    _make_research_db_with_table(tmp_path)
    _make_research_ohlcv_db(tmp_path, dates=[date(2024, 3, 1)], symbols=["AAA"])
    with pytest.raises(ValueError, match="frequency="):
        run_historical_backfill(
            from_date=date(2024, 3, 1), to_date=date(2024, 3, 5),
            project_root=tmp_path, frequency="hourly",
        )


def test_historical_backfill_no_data_returns_zero_counts(tmp_path: Path, monkeypatch):
    _make_research_db_with_table(tmp_path)
    # An empty OHLCV catalog → no trading days → empty result.
    db_dir = tmp_path / "data" / "research"
    db_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_dir / "research_ohlcv.duckdb"))
    con.execute("CREATE TABLE _catalog(symbol_id VARCHAR, exchange VARCHAR, timestamp TIMESTAMP, close DOUBLE, volume BIGINT, high DOUBLE, low DOUBLE, open DOUBLE)")
    con.close()

    result = run_historical_backfill(
        from_date=date(2024, 3, 1), to_date=date(2024, 3, 5),
        project_root=tmp_path, frequency="daily",
    )
    assert result == {"dates_processed": 0, "rows_upserted": 0}
