"""Research dynamic backtest loader tests."""

from __future__ import annotations

from datetime import date, timedelta
import sqlite3

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.backtesting.research_loader import (
    RANKING_METHOD_VERSION,
    load_research_ranked_by_date,
    validate_research_dynamic_data,
)


def test_research_loader_computes_engine_columns(tmp_path):
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    conn = duckdb.connect(str(paths.ohlcv_db_path))
    conn.execute(
        """
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
    )
    start = date(2025, 1, 1)
    rows = []
    for i in range(240):
        d = start + timedelta(days=i)
        rows.append(("AAA", None, "NSE", d, 100 + i, 101 + i, 99 + i, 100 + i, 1000 + i, None, 1, d))
        rows.append(("BBB", None, "NSE", d, 200 - i * 0.1, 201 - i * 0.1, 199 - i * 0.1, 200 - i * 0.1, 900 + i, None, 1, d))
        rows.append(("NIFTY50", None, "NSE", d, 1000 + i * 0.2, 1001 + i * 0.2, 999 + i * 0.2, 1000 + i * 0.2, 5000 + i, None, 1, d))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.execute(
        """
        CREATE TABLE weekly_stage_snapshot (
            symbol VARCHAR,
            week_end_date DATE,
            stage_label VARCHAR,
            stage_confidence DOUBLE,
            stage_transition VARCHAR,
            bars_in_stage INTEGER,
            stage_entry_date DATE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO weekly_stage_snapshot VALUES
            ('AAA', '2025-08-22', 'S2', 0.9, 'S1_TO_S2', 3, '2025-08-15'),
            ('BBB', '2025-08-22', 'S4', 0.9, 'NONE', 10, '2025-08-01')
        """
    )
    conn.close()
    master = sqlite3.connect(paths.root_dir / "masterdata.db")
    master.execute("CREATE TABLE stock_details (Symbol TEXT PRIMARY KEY, Sector TEXT)")
    master.execute("INSERT INTO stock_details VALUES ('AAA', 'TECH')")
    master.execute("INSERT INTO stock_details VALUES ('BBB', 'BANKS')")
    master.commit()
    master.close()

    ranked = load_research_ranked_by_date(
        tmp_path,
        from_date=start + timedelta(days=220),
        to_date=start + timedelta(days=239),
    )

    assert ranked
    frame = ranked[start + timedelta(days=239)]
    for column in [
        "sma_11",
        "sma_200",
        "ema_20",
        "atr_14",
        "volume_ratio_20",
        "swing_low_20",
        "return_50",
        "drawdown_from_recent_high_pct",
        "sma50_rising_20d",
        "below_ema20_days_20",
    ]:
        assert column in frame.columns
    for column in [
        "rel_strength_score",
        "trend_score_score",
        "prox_high_score",
        "sector_strength_score",
        "composite_score_adjusted",
        "rs_vs_nifty_score",
    ]:
        assert column in frame.columns
        assert frame[column].notna().all()
    assert "AAA" in set(frame["symbol_id"])
    assert "NIFTY50" not in set(frame["symbol_id"])
    assert int(frame.loc[frame["symbol_id"] == "AAA", "eligible_rank"].iloc[0]) == 1
    assert frame.loc[frame["symbol_id"] == "AAA", "sector_name"].iloc[0] == "TECH"
    assert frame.loc[frame["symbol_id"] == "AAA", "weekly_stage_label"].iloc[0] == "S2"
    assert frame.loc[frame["symbol_id"] == "AAA", "stage2_freshness_bonus"].iloc[0] == 4.0
    assert frame.loc[frame["symbol_id"] == "AAA", "stage2_transition_bonus"].iloc[0] == 5.0
    assert RANKING_METHOD_VERSION == "research_dynamic_v3_canonical_factor_scoring_stage2_benchmark"

    quality = validate_research_dynamic_data(
        tmp_path,
        from_date=start + timedelta(days=220),
        to_date=start + timedelta(days=239),
    )
    assert quality["status"] == "ok"
    assert quality["row_count"] == 720
    assert quality["symbol_count"] == 3
    assert quality["masterdata_exists"] is True


def test_no_lookahead_truncation_invariance(tmp_path):
    """Feature values for date D must be identical whether the loader sees data
    up to D or up to D+30. Locks the no-lookahead property.
    """
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    conn = duckdb.connect(str(paths.ohlcv_db_path))
    conn.execute(
        """
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
    )
    start = date(2025, 1, 1)
    rows = []
    for i in range(300):
        d = start + timedelta(days=i)
        rows.append(("AAA", None, "NSE", d, 100 + i * 0.5, 102 + i * 0.5, 99 + i * 0.5, 101 + i * 0.5, 1000 + i, None, 1, d))
        rows.append(("BBB", None, "NSE", d, 200 - i * 0.1, 201 - i * 0.1, 199 - i * 0.1, 200 - i * 0.1, 900 + i, None, 1, d))
        rows.append(("NIFTY50", None, "NSE", d, 1000 + i * 0.2, 1001 + i * 0.2, 999 + i * 0.2, 1000 + i * 0.2, 5000 + i, None, 1, d))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()
    master = sqlite3.connect(paths.root_dir / "masterdata.db")
    master.execute("CREATE TABLE stock_details (Symbol TEXT PRIMARY KEY, Sector TEXT)")
    master.execute("INSERT INTO stock_details VALUES ('AAA', 'TECH')")
    master.execute("INSERT INTO stock_details VALUES ('BBB', 'BANKS')")
    master.commit()
    master.close()

    cutoff = start + timedelta(days=230)
    # Loader run 1: data up to cutoff.
    ranked_short = load_research_ranked_by_date(
        tmp_path, from_date=cutoff, to_date=cutoff
    )
    # Loader run 2: data up to cutoff + 30 days.
    ranked_long = load_research_ranked_by_date(
        tmp_path, from_date=cutoff, to_date=cutoff + timedelta(days=30)
    )

    assert cutoff in ranked_short
    assert cutoff in ranked_long
    short_frame = ranked_short[cutoff].set_index("symbol_id").sort_index()
    long_frame = ranked_long[cutoff].set_index("symbol_id").sort_index()

    # Spot-check the numeric feature columns that drive ranking decisions.
    for col in [
        "close",
        "sma_20",
        "sma_50",
        "sma_200",
        "ema_20",
        "atr_14",
        "volume_ratio_20",
        "high_52w",
        "return_20",
        "return_60",
        "composite_score",
        "composite_score_adjusted",
        "eligible_rank",
    ]:
        if col not in short_frame.columns:
            continue
        # NaN-safe equality.
        for sym in short_frame.index:
            s = short_frame.at[sym, col]
            l = long_frame.at[sym, col]
            if pd.isna(s) and pd.isna(l):
                continue
            assert s == l, f"lookahead leak in column={col} symbol={sym}: short={s} long={l}"
