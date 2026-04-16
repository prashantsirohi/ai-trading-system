"""
Batch feature computation for all symbols using DuckDB parallel export.
Features are computed into temp tables, then exported as partitioned Parquet
via DuckDB's COPY command (much faster than per-symbol pandas writes).

Usage:
    python compute_features_batch.py                    # All features
    python compute_features_batch.py --features rsi    # Single feature
    python compute_features_batch.py --dry-run        # Show what would run
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import List

from core.bootstrap import ensure_project_root_on_path

project_root = str(ensure_project_root_on_path(__file__))

from utils.env import load_project_env
from core.logging import logger

load_project_env(project_root)

import duckdb

DB_PATH = os.path.join(project_root, "data", "ohlcv.duckdb")
FEATURE_DIR = os.path.join(project_root, "data", "feature_store")
os.makedirs(FEATURE_DIR, exist_ok=True)


def create_temp_feature_table(conn, table_name: str, columns_sql: str):
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} ({columns_sql})")


def export_feature(conn, table_name: str, feature_name: str, exchange: str) -> int:
    """Export temp table to partitioned Parquet via DuckDB COPY."""
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if row_count == 0:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        return 0

    feat_dir = os.path.join(FEATURE_DIR, feature_name, exchange)
    if os.path.exists(feat_dir):
        import shutil

        shutil.rmtree(feat_dir)
    os.makedirs(feat_dir, exist_ok=True)

    try:
        conn.execute(f"""
            COPY {table_name}
            TO '{feat_dir}'
            (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE, OVERWRITE TRUE)
        """)
    except Exception as e:
        logger.warning(f"  COPY failed, falling back to pandas export: {e}")
        df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        feat_dir = os.path.join(FEATURE_DIR, feature_name, exchange)
        os.makedirs(feat_dir, exist_ok=True)
        for sym_id in df["symbol_id"].unique():
            sym_df = df[df["symbol_id"] == sym_id]
            out_path = os.path.join(feat_dir, f"{sym_id}.parquet")
            sym_df.to_parquet(out_path, index=False)
            row_count = len(df)

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    return row_count


def batch_rsi(conn, exchange="NSE", period=14):
    tbl = f"temp_feat_rsi"
    logger.info(f"  RSI({period})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, rsi DOUBLE",
    )
    conn.execute(f"""
        WITH prices AS (
            SELECT symbol_id, exchange, timestamp, close,
                   LAG(close) OVER w AS prev_close
            FROM _catalog
            WHERE exchange = '{exchange}'
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        ),
        gl AS (
            SELECT symbol_id, exchange, timestamp, close,
                   CASE WHEN close - prev_close > 0 THEN close - prev_close ELSE 0 END AS gain,
                   CASE WHEN prev_close - close > 0 THEN prev_close - close ELSE 0 END AS loss
            FROM prices WHERE prev_close IS NOT NULL
        ),
        sm AS (
            SELECT symbol_id, exchange, timestamp, close,
                   AVG(gain) OVER w AS ag,
                   AVG(loss) OVER w AS al
            FROM gl
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
        )
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               CASE WHEN al = 0 THEN 100
                    ELSE ROUND(100 - (100 / (1 + ag / NULLIF(al, 0))), 4)
               END AS rsi
        FROM sm WHERE ag IS NOT NULL AND al IS NOT NULL
        ORDER BY symbol_id, timestamp
    """)
    conn.commit()
    rows = export_feature(conn, tbl, "rsi", exchange)
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_sma(conn, exchange="NSE", periods=[20, 50, 200]):
    tbl = f"temp_feat_sma"
    logger.info(f"  SMA({periods})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, period INT, sma_value DOUBLE",
    )

    for p in periods:
        conn.execute(f"""
            INSERT INTO {tbl}
            SELECT symbol_id, exchange, timestamp, close, {p} AS period,
                   ROUND(AVG(close) OVER w, 4) AS sma_value
            FROM _catalog
            WHERE exchange = '{exchange}'
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {p - 1} PRECEDING AND CURRENT ROW)
            QUALIFY COUNT(*) OVER (PARTITION BY symbol_id) >= {p}
            ORDER BY symbol_id, timestamp
        """)

    conn.commit()
    rows = export_feature(conn, tbl, "sma", exchange)
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_ema(conn, exchange="NSE", periods=[12, 26, 50, 200]):
    tbl = f"temp_feat_ema"
    logger.info(f"  EMA({periods})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, ema_period INT, ema_value DOUBLE",
    )

    for p in periods:
        alpha = 2.0 / (p + 1)
        conn.execute(f"""
            INSERT INTO {tbl}
            SELECT symbol_id, exchange, timestamp, close, {p} AS ema_period,
                   ROUND(EXPONENTIAL_MOVING_AVERAGE(close, {alpha}) OVER w, 4) AS ema_value
            FROM _catalog
            WHERE exchange = '{exchange}'
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp) >= {p}
            ORDER BY symbol_id, timestamp
        """)

    conn.commit()
    rows = export_feature(conn, tbl, "ema", exchange)
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_macd(conn, exchange="NSE", fast=12, slow=26, signal=9):
    tbl = f"temp_feat_macd"
    logger.info(f"  MACD({fast},{slow},{signal})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, "
        "macd_line DOUBLE, macd_signal DOUBLE, macd_histogram DOUBLE",
    )

    alpha_fast = 2.0 / (fast + 1)
    alpha_slow = 2.0 / (slow + 1)

    conn.execute(f"""
        WITH ema_data AS (
            SELECT symbol_id, exchange, timestamp, close,
                   EXPONENTIAL_MOVING_AVERAGE(close, {alpha_fast}) OVER w AS ef,
                   EXPONENTIAL_MOVING_AVERAGE(close, {alpha_slow}) OVER w AS es
            FROM _catalog
            WHERE exchange = '{exchange}'
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        ),
        macd_data AS (
            SELECT symbol_id, exchange, timestamp, close,
                   ef - es AS ml,
                   EXPONENTIAL_MOVING_AVERAGE(ef - es, {2.0 / (signal + 1)}) OVER w AS sl
            FROM ema_data
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp) >= {slow}
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        )
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               ROUND(ml, 4) AS macd_line,
               ROUND(sl, 4) AS macd_signal,
               ROUND(ml - sl, 4) AS macd_histogram
        FROM macd_data
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(conn, tbl, "macd", exchange)
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_atr(conn, exchange="NSE", period=14):
    tbl = f"temp_feat_atr"
    logger.info(f"  ATR({period})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, atr_value DOUBLE, atr_period INT",
    )

    conn.execute(f"""
        WITH hlc AS (
            SELECT symbol_id, exchange, timestamp, high, low, close,
                   LAG(close) OVER w AS prev_close
            FROM _catalog
            WHERE exchange = '{exchange}'
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        ),
        tr_data AS (
            SELECT symbol_id, exchange, timestamp, close,
                   GREATEST(high, prev_close) - LEAST(low, prev_close) AS tr
            FROM hlc WHERE prev_close IS NOT NULL
        )
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               ROUND(AVG(tr) OVER w, 4) AS atr_value, {period} AS atr_period
        FROM tr_data
        WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(conn, tbl, "atr", exchange)
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_adx(conn, exchange="NSE", period=14):
    tbl = f"temp_feat_adx"
    logger.info(f"  ADX({period})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, "
        "adx_plus DOUBLE, adx_minus DOUBLE, adx_value DOUBLE, adx_period INT",
    )

    conn.execute(f"""
        WITH hlc AS (
            SELECT symbol_id, exchange, timestamp, high, low, close,
                   LAG(close) OVER w AS prev_close
            FROM _catalog WHERE exchange = '{exchange}'
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        ),
        tr_hilo AS (
            SELECT symbol_id, exchange, timestamp, close, high, low,
                   GREATEST(high, prev_close) - LEAST(low, prev_close) AS tr,
                   high - LAG(low) OVER w AS pdm,
                   LAG(high) OVER w - low AS mdm
            FROM hlc WHERE prev_close IS NOT NULL
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        ),
        smoothed AS (
            SELECT symbol_id, exchange, timestamp, close,
                   AVG(tr) OVER w AS atr_val,
                   AVG(CASE WHEN pdm > mdm AND pdm > 0 THEN pdm ELSE 0 END) OVER w AS pdi,
                   AVG(CASE WHEN mdm > pdm AND mdm > 0 THEN mdm ELSE 0 END) OVER w AS mdi
            FROM tr_hilo
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
        ),
        dx_data AS (
            SELECT symbol_id, exchange, timestamp, close, atr_val, pdi, mdi,
                   CASE WHEN atr_val > 0 THEN 100 * pdi / atr_val ELSE 0 END AS pdx,
                   CASE WHEN atr_val > 0 THEN 100 * mdi / atr_val ELSE 0 END AS mdx,
                   CASE WHEN atr_val > 0 THEN ABS(100 * (pdi - mdi) / atr_val) ELSE 0 END AS dx_val
            FROM smoothed WHERE atr_val > 0
        )
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               ROUND(pdx, 4) AS adx_plus,
               ROUND(mdx, 4) AS adx_minus,
               ROUND(AVG(dx_val) OVER w, 4) AS adx_value,
               {period} AS adx_period
        FROM dx_data
        WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(conn, tbl, "adx", exchange)
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_bollinger_bands(conn, exchange="NSE", period=20, std_dev=2):
    tbl = f"temp_feat_bb"
    logger.info(f"  BB({period},{std_dev})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, "
        "bb_middle DOUBLE, bb_upper DOUBLE, bb_lower DOUBLE, bb_period INT, bb_std INT",
    )

    conn.execute(f"""
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               ROUND(AVG(close) OVER w, 4) AS bb_middle,
               ROUND(AVG(close) OVER w + {std_dev} * STDDEV(close) OVER w, 4) AS bb_upper,
               ROUND(AVG(close) OVER w - {std_dev} * STDDEV(close) OVER w, 4) AS bb_lower,
               {period} AS bb_period, {std_dev} AS bb_std
        FROM _catalog
        WHERE exchange = '{exchange}'
        WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
        QUALIFY COUNT(*) OVER (PARTITION BY symbol_id) >= {period}
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(conn, tbl, "bb", exchange)
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_roc(conn, exchange="NSE", periods=[1, 3, 5, 10, 20]):
    tbl = f"temp_feat_roc"
    logger.info(f"  ROC({periods})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, "
        "roc_period INT, roc_value DOUBLE",
    )

    for p in periods:
        conn.execute(f"""
            INSERT INTO {tbl}
            SELECT symbol_id, exchange, timestamp, close, {p} AS roc_period,
                   ROUND(CASE WHEN LAG(close, {p}) OVER w > 0
                       THEN 100 * (close - LAG(close, {p}) OVER w) / LAG(close, {p}) OVER w
                       ELSE 0 END, 4) AS roc_value
            FROM _catalog
            WHERE exchange = '{exchange}'
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        """)
        conn.commit()

    rows = export_feature(conn, tbl, "roc", exchange)
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_supertrend(conn, exchange="NSE", period=10, multiplier=3):
    tbl = f"temp_feat_supertrend"
    logger.info(f"  Supertrend({period},{multiplier})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, "
        "atr_value DOUBLE, st_upper DOUBLE, st_lower DOUBLE, st_signal INT, "
        "st_period INT, st_multiplier INT",
    )

    conn.execute(f"""
        WITH hlc AS (
            SELECT symbol_id, exchange, timestamp, high, low, close,
                   AVG(high - low) OVER w AS avg_tr,
                   (high + low) / 2 AS hl2
            FROM _catalog
            WHERE exchange = '{exchange}'
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
        ),
        bands AS (
            SELECT symbol_id, exchange, timestamp, high, low, close, avg_tr,
                   hl2 + {multiplier} * avg_tr AS ub,
                   hl2 - {multiplier} * avg_tr AS lb,
                   ROW_NUMBER() OVER w AS rn
            FROM hlc WHERE avg_tr IS NOT NULL
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        ),
        st_data AS (
            SELECT symbol_id, exchange, timestamp, close, avg_tr, ub, lb,
                   CASE WHEN close <= ub THEN ub ELSE lb END AS final_band
            FROM bands WHERE rn > {period}
        )
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               ROUND(avg_tr, 4) AS atr_value,
               ROUND(ub, 4) AS st_upper,
               ROUND(lb, 4) AS st_lower,
               CASE WHEN close > final_band THEN 1 ELSE -1 END AS st_signal,
               {period} AS st_period, {multiplier} AS st_multiplier
        FROM st_data
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(conn, tbl, "supertrend", exchange)
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def register_features(conn, feature_name: str, exchange: str, rows_computed: int):
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS _feat_id_seq START 1
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _feature_registry (
            feature_id BIGINT PRIMARY KEY DEFAULT nextval('_feat_id_seq'),
            feature_name TEXT NOT NULL,
            exchange TEXT,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            rows_computed BIGINT,
            status TEXT DEFAULT 'completed'
        )
    """)
    conn.execute(
        "INSERT INTO _feature_registry (feature_name, exchange, rows_computed, status) VALUES (?, ?, ?, ?)",
        (feature_name, exchange, rows_computed, "completed"),
    )
    conn.commit()


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--features", nargs="+", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    all_features = {
        "rsi": batch_rsi,
        "sma": batch_sma,
        "ema": batch_ema,
        "macd": batch_macd,
        "atr": batch_atr,
        "adx": batch_adx,
        "bb": batch_bollinger_bands,
        "roc": batch_roc,
        "supertrend": batch_supertrend,
    }

    features_to_run = {
        k: v
        for k, v in all_features.items()
        if args.features is None or k in args.features
    }

    conn = duckdb.connect(DB_PATH)
    n_syms = conn.execute(
        "SELECT COUNT(DISTINCT symbol_id) FROM _catalog WHERE exchange = 'NSE'"
    ).fetchone()[0]
    logger.info(f"Computing features for {n_syms} NSE symbols...")

    if args.dry_run:
        logger.info(f"[DRY RUN] Would compute: {list(features_to_run.keys())}")
        conn.close()
        return

    t0 = time.time()
    total_rows = 0

    for feat_name, feat_fn in features_to_run.items():
        try:
            rows = feat_fn(conn)
            register_features(conn, feat_name, "NSE", rows)
            total_rows += rows
        except Exception as e:
            import traceback

            logger.error(f"  ERROR {feat_name}: {e}")
            logger.error(traceback.format_exc())

    conn.close()

    elapsed = time.time() - t0
    logger.info(f"\nAll features computed in {elapsed:.1f}s")
    logger.info(f"Total rows written: {total_rows:,}")


if __name__ == "__main__":
    main()
