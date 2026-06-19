"""
Batch feature computation for all symbols using DuckDB parallel export.
Features are computed into temp tables, then exported as partitioned Parquet
via DuckDB's COPY command (much faster than per-symbol pandas writes).

Usage:
    python compute_features_batch.py                    # All features
    python compute_features_batch.py --features rsi    # Single feature
    python compute_features_batch.py --dry-run        # Show what would run
"""

import time
import pandas as pd
from pathlib import Path
from typing import Any, Iterable
import os

from ai_trading_system.platform.utils.bootstrap import ensure_project_root_on_path


def _resolve_project_root(anchor: str) -> str:
    env_root = os.getenv("AI_TRADING_PROJECT_ROOT")
    if env_root:
        return str(ensure_project_root_on_path(env_root))
    anchor_path = os.path.abspath(anchor)
    parts = anchor_path.split(os.sep)
    if "src" in parts:
        src_idx = parts.index("src")
        root = os.sep.join(parts[:src_idx]) or os.sep
        return str(ensure_project_root_on_path(root))
    return str(ensure_project_root_on_path(anchor))


project_root = _resolve_project_root(__file__)

from ai_trading_system.platform.utils.env import load_project_env
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.platform.logging.logger import logger
from ai_trading_system.domains.features.repository import ensure_feature_catalog_source

load_project_env(project_root)

import duckdb

_PATHS = get_domain_paths(project_root=project_root, data_domain="operational")
DB_PATH = str(_PATHS.ohlcv_db_path)
FEATURE_DIR = str(_PATHS.feature_store_dir)
DEFAULT_FEATURE_TYPES = [
    "rsi",
    "adx",
    "sma",
    "ema",
    "macd",
    "atr",
    "bb",
    "roc",
    "supertrend",
]
BATCH_SOURCE_TABLE = "_feature_batch_source"
os.makedirs(FEATURE_DIR, exist_ok=True)


def create_temp_feature_table(conn, table_name: str, columns_sql: str):
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} ({columns_sql})")


def _duckdb_literal(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _normalize_symbols(symbols: Iterable[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    normalized = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    return normalized


def prepare_feature_batch_source(
    conn,
    *,
    exchange: str,
    symbols: Iterable[str] | None = None,
    source_table: str = BATCH_SOURCE_TABLE,
) -> int:
    """Create a temporary source table for one exchange and optional symbols."""
    normalized_symbols = _normalize_symbols(symbols)
    conn.execute(f"DROP TABLE IF EXISTS {source_table}")
    params: list[Any] = [str(exchange)]
    symbol_filter = ""
    if normalized_symbols is not None:
        if not normalized_symbols:
            symbol_filter = " AND FALSE"
        else:
            placeholders = ",".join("?" for _ in normalized_symbols)
            symbol_filter = f" AND symbol_id IN ({placeholders})"
            params.extend(normalized_symbols)
    conn.execute(
        f"""
        CREATE TEMP TABLE {source_table} AS
        SELECT *
        FROM _catalog_feature_source
        WHERE exchange = ?
          AND timestamp IS NOT NULL
          {symbol_filter}
        """,
        params,
    )
    return int(conn.execute(f"SELECT COUNT(DISTINCT symbol_id) FROM {source_table}").fetchone()[0] or 0)


def export_feature(
    conn,
    table_name: str,
    feature_name: str,
    exchange: str,
    *,
    feature_store_dir: str | Path = FEATURE_DIR,
    replace_exchange: bool = True,
) -> int:
    """Export temp table to parquet while preserving symbol-file compatibility."""
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if row_count == 0:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        return 0

    feat_dir = Path(feature_store_dir) / feature_name / exchange
    if replace_exchange and feat_dir.exists():
        import shutil

        shutil.rmtree(feat_dir)
    feat_dir.mkdir(parents=True, exist_ok=True)

    try:
        parts_dir = feat_dir / "_duckdb_parts"
        parts_dir.mkdir(parents=True, exist_ok=True)
        conn.execute(f"""
            COPY {table_name}
            TO '{_duckdb_literal(parts_dir)}'
            (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE, OVERWRITE TRUE)
        """)
    except Exception as e:
        logger.warning(f"  COPY failed; continuing with symbol-file export only: {e}")

    df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
    if not df.empty and "symbol_id" in df.columns:
        for sym_id, sym_df in df.groupby("symbol_id", sort=True):
            safe_symbol = str(sym_id).strip().upper()
            if not safe_symbol:
                continue
            out_path = feat_dir / f"{safe_symbol}.parquet"
            sym_df.sort_values("timestamp").to_parquet(out_path, index=False)

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    return row_count


def batch_rsi(
    conn,
    exchange="NSE",
    period=14,
    *,
    source_table: str = "_catalog_feature_source",
    feature_store_dir: str | Path = FEATURE_DIR,
    replace_exchange: bool = True,
):
    tbl = f"temp_feat_rsi"
    logger.info(f"  RSI({period})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        f"symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, rsi_{period} DOUBLE",
    )
    conn.execute(f"""
        WITH prices AS (
            SELECT symbol_id, exchange, timestamp, close,
                   LAG(close) OVER w AS prev_close
            FROM {source_table}
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
               END AS rsi_{period}
        FROM sm WHERE ag IS NOT NULL AND al IS NOT NULL
        ORDER BY symbol_id, timestamp
    """)
    conn.commit()
    rows = export_feature(
        conn,
        tbl,
        "rsi",
        exchange,
        feature_store_dir=feature_store_dir,
        replace_exchange=replace_exchange,
    )
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_sma(
    conn,
    exchange="NSE",
    periods=[20, 50, 200],
    *,
    source_table: str = "_catalog_feature_source",
    feature_store_dir: str | Path = FEATURE_DIR,
    replace_exchange: bool = True,
):
    tbl = f"temp_feat_sma"
    logger.info(f"  SMA({periods})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE",
    )
    for p in periods:
        conn.execute(f"ALTER TABLE {tbl} ADD COLUMN sma_{int(p)} DOUBLE")

    sma_cols = ",\n                   ".join(
        f"ROUND(AVG(close) OVER w{int(p)}, 4) AS sma_{int(p)}" for p in periods
    )
    window_defs = ", ".join(
        f"w{int(p)} AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {int(p) - 1} PRECEDING AND CURRENT ROW)"
        for p in periods
    )
    conn.execute(f"""
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               {sma_cols}
        FROM {source_table}
        WINDOW {window_defs}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp) >= {max(int(p) for p in periods)}
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(
        conn,
        tbl,
        "sma",
        exchange,
        feature_store_dir=feature_store_dir,
        replace_exchange=replace_exchange,
    )
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_ema(
    conn,
    exchange="NSE",
    periods=[12, 26, 50, 200],
    *,
    source_table: str = "_catalog_feature_source",
    feature_store_dir: str | Path = FEATURE_DIR,
    replace_exchange: bool = True,
):
    tbl = f"temp_feat_ema"
    logger.info(f"  EMA({periods})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE",
    )
    for p in periods:
        conn.execute(f"ALTER TABLE {tbl} ADD COLUMN ema_{int(p)} DOUBLE")

    ema_cols = ",\n                   ".join(
        f"""ROUND(
                       CASE
                           WHEN LAG(close) OVER w IS NULL THEN close
                           ELSE LAG(close) OVER w + {2.0 / (int(p) + 1)} * (close - LAG(close) OVER w)
                       END,
                       4
                   ) AS ema_{int(p)}"""
        for p in periods
    )
    conn.execute(f"""
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               {ema_cols}
        FROM {source_table}
        WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp) >= {min(int(p) for p in periods)}
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(
        conn,
        tbl,
        "ema",
        exchange,
        feature_store_dir=feature_store_dir,
        replace_exchange=replace_exchange,
    )
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_macd(
    conn,
    exchange="NSE",
    fast=12,
    slow=26,
    signal=9,
    *,
    source_table: str = "_catalog_feature_source",
    feature_store_dir: str | Path = FEATURE_DIR,
    replace_exchange: bool = True,
):
    tbl = f"temp_feat_macd"
    logger.info(f"  MACD({fast},{slow},{signal})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, "
        f"macd_line DOUBLE, macd_signal_{signal} DOUBLE, macd_histogram DOUBLE",
    )

    conn.execute(f"""
        WITH prices AS (
            SELECT symbol_id, exchange, timestamp, close,
                   LAG(close) OVER w AS prev_close
            FROM {source_table}
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        ),
        ema_fast AS (
            SELECT symbol_id, exchange, timestamp, close,
                   CASE WHEN prev_close IS NULL THEN close
                        ELSE prev_close + {2.0 / (fast + 1)} * (close - prev_close)
                   END AS ema_f
            FROM prices
        ),
        ema_slow AS (
            SELECT symbol_id, exchange, timestamp, close,
                   CASE WHEN prev_close IS NULL THEN close
                        ELSE prev_close + {2.0 / (slow + 1)} * (close - prev_close)
                   END AS ema_s
            FROM prices
        ),
        macd_line AS (
            SELECT
                f.symbol_id, f.exchange, f.timestamp, f.close,
                f.ema_f - s.ema_s AS ml
            FROM ema_fast f
            JOIN ema_slow s USING (symbol_id, exchange, timestamp)
        ),
        macd_data AS (
            SELECT symbol_id, exchange, timestamp, close,
                   ml,
                   CASE WHEN LAG(ml) OVER w IS NULL THEN ml
                        ELSE LAG(ml) OVER w + {2.0 / (signal + 1)} * (ml - LAG(ml) OVER w)
                   END AS sl
            FROM macd_line
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp) >= {slow}
        )
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               ROUND(ml, 4) AS macd_line,
               ROUND(sl, 4) AS macd_signal_{signal},
               ROUND(ml - sl, 4) AS macd_histogram
        FROM macd_data
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(
        conn,
        tbl,
        "macd",
        exchange,
        feature_store_dir=feature_store_dir,
        replace_exchange=replace_exchange,
    )
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_atr(
    conn,
    exchange="NSE",
    period=14,
    *,
    source_table: str = "_catalog_feature_source",
    feature_store_dir: str | Path = FEATURE_DIR,
    replace_exchange: bool = True,
):
    tbl = f"temp_feat_atr"
    logger.info(f"  ATR({period})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        f"symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, atr_{period} DOUBLE",
    )

    conn.execute(f"""
        WITH hlc AS (
            SELECT symbol_id, exchange, timestamp, high, low, close,
                   LAG(close) OVER w AS prev_close
            FROM {source_table}
            WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        ),
        tr_data AS (
            SELECT symbol_id, exchange, timestamp, close,
                   GREATEST(high, prev_close) - LEAST(low, prev_close) AS tr
            FROM hlc WHERE prev_close IS NOT NULL
        )
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               ROUND(AVG(tr) OVER w, 4) AS atr_{period}
        FROM tr_data
        WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(
        conn,
        tbl,
        "atr",
        exchange,
        feature_store_dir=feature_store_dir,
        replace_exchange=replace_exchange,
    )
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_adx(
    conn,
    exchange="NSE",
    period=14,
    *,
    source_table: str = "_catalog_feature_source",
    feature_store_dir: str | Path = FEATURE_DIR,
    replace_exchange: bool = True,
):
    tbl = f"temp_feat_adx"
    logger.info(f"  ADX({period})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, "
        f"plus_di_{period} DOUBLE, minus_di_{period} DOUBLE, adx_{period} DOUBLE",
    )

    conn.execute(f"""
        WITH hlc AS (
            SELECT symbol_id, exchange, timestamp, high, low, close,
                   LAG(close) OVER w AS prev_close
            FROM {source_table}
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
               ROUND(pdx, 4) AS plus_di_{period},
               ROUND(mdx, 4) AS minus_di_{period},
               ROUND(AVG(dx_val) OVER w, 4) AS adx_{period}
        FROM dx_data
        WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(
        conn,
        tbl,
        "adx",
        exchange,
        feature_store_dir=feature_store_dir,
        replace_exchange=replace_exchange,
    )
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_bollinger_bands(
    conn,
    exchange="NSE",
    period=20,
    std_dev=2,
    *,
    source_table: str = "_catalog_feature_source",
    feature_store_dir: str | Path = FEATURE_DIR,
    replace_exchange: bool = True,
):
    tbl = f"temp_feat_bb"
    logger.info(f"  BB({period},{std_dev})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, "
        f"bb_middle_{period} DOUBLE, bb_upper_{period}_{int(std_dev)}sd DOUBLE, bb_lower_{period}_{int(std_dev)}sd DOUBLE",
    )

    conn.execute(f"""
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               ROUND(AVG(close) OVER w, 4) AS bb_middle_{period},
               ROUND(AVG(close) OVER w + {std_dev} * STDDEV(close) OVER w, 4) AS bb_upper_{period}_{int(std_dev)}sd,
               ROUND(AVG(close) OVER w - {std_dev} * STDDEV(close) OVER w, 4) AS bb_lower_{period}_{int(std_dev)}sd
        FROM {source_table}
        WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
        QUALIFY COUNT(*) OVER (PARTITION BY symbol_id) >= {period}
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(
        conn,
        tbl,
        "bb",
        exchange,
        feature_store_dir=feature_store_dir,
        replace_exchange=replace_exchange,
    )
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_roc(
    conn,
    exchange="NSE",
    periods=[1, 3, 5, 10, 20],
    *,
    source_table: str = "_catalog_feature_source",
    feature_store_dir: str | Path = FEATURE_DIR,
    replace_exchange: bool = True,
):
    tbl = f"temp_feat_roc"
    logger.info(f"  ROC({periods})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE",
    )
    for p in periods:
        conn.execute(f"ALTER TABLE {tbl} ADD COLUMN roc_{int(p)} DOUBLE")

    roc_cols = ",\n               ".join(
        f"""ROUND(CASE WHEN LAG(close, {int(p)}) OVER w > 0
                       THEN 100 * (close - LAG(close, {int(p)}) OVER w) / LAG(close, {int(p)}) OVER w
                       ELSE NULL END, 4) AS roc_{int(p)}"""
        for p in periods
    )
    conn.execute(f"""
        INSERT INTO {tbl}
        SELECT symbol_id, exchange, timestamp, close,
               {roc_cols}
        FROM {source_table}
        WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp)
        ORDER BY symbol_id, timestamp
    """)
    conn.commit()

    rows = export_feature(
        conn,
        tbl,
        "roc",
        exchange,
        feature_store_dir=feature_store_dir,
        replace_exchange=replace_exchange,
    )
    logger.info(f"    -> {rows:,} rows in {time.time() - t0:.1f}s")
    return rows


def batch_supertrend(
    conn,
    exchange="NSE",
    period=10,
    multiplier=3,
    *,
    source_table: str = "_catalog_feature_source",
    feature_store_dir: str | Path = FEATURE_DIR,
    replace_exchange: bool = True,
):
    tbl = f"temp_feat_supertrend"
    logger.info(f"  Supertrend({period},{multiplier})...")
    t0 = time.time()
    create_temp_feature_table(
        conn,
        tbl,
        "symbol_id TEXT, exchange TEXT, timestamp TIMESTAMP, close DOUBLE, "
        f"supertrend_{period}_{int(multiplier)} DOUBLE, supertrend_dir_{period}_{int(multiplier)} INT",
    )

    conn.execute(f"""
        WITH hlc AS (
            SELECT symbol_id, exchange, timestamp, high, low, close,
                   AVG(high - low) OVER w AS avg_tr,
                   (high + low) / 2 AS hl2
            FROM {source_table}
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
               ROUND(final_band, 4) AS supertrend_{period}_{int(multiplier)},
               CASE WHEN close > final_band THEN 1 ELSE -1 END AS supertrend_dir_{period}_{int(multiplier)}
        FROM st_data
        ORDER BY symbol_id, timestamp
    """)

    conn.commit()
    rows = export_feature(
        conn,
        tbl,
        "supertrend",
        exchange,
        feature_store_dir=feature_store_dir,
        replace_exchange=replace_exchange,
    )
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


BATCH_FEATURE_FUNCTIONS = {
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


def run_batch_feature_computation(
    *,
    project_root: str | Path | None = None,
    data_domain: str = "operational",
    symbols: Iterable[str] | None = None,
    exchanges: Iterable[str] | None = None,
    feature_types: Iterable[str] | None = None,
    full_rebuild: bool = False,
    incremental: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compute feature groups with set-based DuckDB SQL and compatible parquet output."""
    root = project_root or globals().get("project_root")
    paths = get_domain_paths(project_root=root, data_domain=data_domain)
    normalized_symbols = _normalize_symbols(symbols)
    selected_exchanges = [str(exchange).strip().upper() for exchange in (exchanges or ["NSE"]) if str(exchange).strip()]
    selected_features = [str(feature).strip().lower() for feature in (feature_types or DEFAULT_FEATURE_TYPES)]
    unknown = sorted(set(selected_features) - set(BATCH_FEATURE_FUNCTIONS))
    if unknown:
        raise ValueError(f"Unsupported batch feature type(s): {unknown}")

    conn = duckdb.connect(str(paths.ohlcv_db_path))
    rows_by_type = {feature_name: 0 for feature_name in selected_features}
    symbols_by_exchange: dict[str, int] = {}
    started = time.time()
    try:
        ensure_feature_catalog_source(conn)
        replace_exchange = normalized_symbols is None
        for exchange in selected_exchanges:
            symbol_count = prepare_feature_batch_source(
                conn,
                exchange=exchange,
                symbols=normalized_symbols,
                source_table=BATCH_SOURCE_TABLE,
            )
            symbols_by_exchange[exchange] = symbol_count
            logger.info(
                "DuckDB batch feature source ready: exchange=%s symbols=%s features=%s replace_exchange=%s",
                exchange,
                symbol_count,
                selected_features,
                replace_exchange,
            )
            if dry_run:
                continue
            for feature_name in selected_features:
                feature_fn = BATCH_FEATURE_FUNCTIONS[feature_name]
                rows = int(
                    feature_fn(
                        conn,
                        exchange=exchange,
                        source_table=BATCH_SOURCE_TABLE,
                        feature_store_dir=paths.feature_store_dir,
                        replace_exchange=replace_exchange,
                    )
                    or 0
                )
                register_features(conn, feature_name, exchange, rows)
                rows_by_type[feature_name] += rows
    finally:
        conn.close()

    total_rows = int(sum(rows_by_type.values()))
    return {
        "mode": "duckdb_batch",
        "data_domain": data_domain,
        "exchanges": selected_exchanges,
        "feature_types": selected_features,
        "feature_result": rows_by_type,
        "feature_rows_by_type": rows_by_type,
        "rows_written_total": total_rows,
        "symbols_targeted": int(sum(symbols_by_exchange.values())),
        "symbols_by_exchange": symbols_by_exchange,
        "full_rebuild": bool(full_rebuild),
        "incremental": bool(incremental),
        "dry_run": bool(dry_run),
        "feature_store_dir": str(paths.feature_store_dir),
        "elapsed_seconds": round(time.time() - started, 3),
    }


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--features", nargs="+", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--data-domain", default="operational", choices=["operational", "research"])
    args = parser.parse_args()

    summary = run_batch_feature_computation(
        project_root=project_root,
        data_domain=args.data_domain,
        feature_types=args.features,
        full_rebuild=True,
        dry_run=args.dry_run,
    )
    logger.info("DuckDB batch feature summary: %s", summary)


if __name__ == "__main__":
    main()
