"""Phase 1A quant feature expansion.

These features are persisted for observability and downstream research only.
They are not rank factors until Phase 2 validation promotes them.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.domains.features.repository import init_feature_registry, register_feature
from ai_trading_system.platform.logging.logger import logger


BENCHMARK_ALIASES: tuple[str, ...] = (
    "NIFTY_500",
    "NIFTY500",
    "NIFTY 500",
    "NIFTY50",
    "NIFTY_50",
    "^NSEI",
)
INDEX_LIKE_SYMBOLS: frozenset[str] = frozenset(
    {
        "NIFTY",
        "NIFTY50",
        "NIFTY_50",
        "NIFTY 50",
        "NIFTY500",
        "NIFTY_500",
        "NIFTY 500",
        "NIFTYBANK",
        "BANKNIFTY",
        "FINNIFTY",
        "MIDCPNIFTY",
        "^NSEI",
    }
)

PHASE1_SYMBOL_COLUMNS: tuple[str, ...] = (
    "realized_vol_20",
    "realized_vol_60",
    "beta_to_nifty_60",
    "beta_to_nifty_60_obs",
    "max_drawdown_63",
    "max_drawdown_126",
    "atr_pct",
    "avg_value_traded_20",
    "liquidity_score",
    "delivery_pct_latest",
    "delivery_pct_5d_avg",
    "delivery_pct_20d_avg",
    "delivery_pct_change_5d",
    "delivery_pct_vs_20d",
    "delivery_trend_score",
)

PHASE1_BREADTH_COLUMNS: tuple[str, ...] = (
    "breadth_score",
    "breadth_velocity_score",
    "breadth_velocity_bucket",
    "pct_above_200dma",
    "pct_at_52w_high",
    "advance_decline_ratio",
    "universe_count",
    "eligible_200dma_count",
    "advance_count",
    "decline_count",
)


@dataclass(frozen=True)
class Phase1RefreshResult:
    symbol_rows: int
    breadth_rows: int
    latest_date: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "status": "completed",
            "symbol_rows": self.symbol_rows,
            "breadth_rows": self.breadth_rows,
            "latest_date": self.latest_date,
        }


def refresh_phase1_features(
    *,
    ohlcv_db_path: str | Path,
    as_of: str,
    exchange: str = "NSE",
) -> Phase1RefreshResult:
    """Compute and persist Phase 1 symbol and market-breadth features."""
    db_path = str(ohlcv_db_path)
    conn = duckdb.connect(db_path)
    try:
        _create_phase1_catalog_view(conn)
        symbol_features = compute_phase1_symbol_features(conn, as_of=as_of, exchange=exchange)
        breadth_features = compute_phase1_market_breadth(conn, as_of=as_of, exchange=exchange)
        symbol_rows = _replace_symbol_features(conn, symbol_features)
        breadth_rows = _replace_breadth_features(conn, breadth_features)
        latest = None
        if not symbol_features.empty:
            latest = str(pd.to_datetime(symbol_features["timestamp"]).dt.date.max())
        elif not breadth_features.empty:
            latest = str(pd.to_datetime(breadth_features["timestamp"]).dt.date.max())
        conn.commit()
    finally:
        conn.close()

    if symbol_rows:
        init_feature_registry(db_path)
        register_feature(
            db_path,
            "phase1_symbol_features",
            exchange=exchange,
            rows_computed=symbol_rows,
            lookback_days=260,
            feature_file="duckdb:feat_phase1_symbol_features",
            note="Phase 1A persisted symbol risk/liquidity/delivery features",
        )
    if breadth_rows:
        init_feature_registry(db_path)
        register_feature(
            db_path,
            "phase1_market_breadth",
            exchange=exchange,
            rows_computed=breadth_rows,
            lookback_days=260,
            feature_file="duckdb:feat_phase1_market_breadth",
            note="Phase 1A persisted market breadth velocity features",
        )
    return Phase1RefreshResult(symbol_rows=symbol_rows, breadth_rows=breadth_rows, latest_date=latest)


def _catalog_columns(conn: duckdb.DuckDBPyConnection) -> set[str]:
    return {row[1] for row in conn.execute("PRAGMA table_info('_catalog')").fetchall()}


def _create_phase1_catalog_view(conn: duckdb.DuckDBPyConnection) -> None:
    columns = _catalog_columns(conn)

    def price_expr(adjusted_col: str, raw_col: str) -> str:
        if adjusted_col in columns:
            return f"COALESCE({adjusted_col}, {raw_col}) AS {raw_col}"
        return raw_col

    exclude_cols = [c for c in ("open", "high", "low", "close") if c in columns]
    exclude_sql = f"* EXCLUDE ({', '.join(exclude_cols)})," if exclude_cols else "* ,"
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW _catalog_feature_source AS
        SELECT
            {exclude_sql}
            {price_expr("adjusted_open", "open")},
            {price_expr("adjusted_high", "high")},
            {price_expr("adjusted_low", "low")},
            {price_expr("adjusted_close", "close")}
        FROM _catalog
        """
    )


def _equity_predicate(conn: duckdb.DuckDBPyConnection) -> str:
    columns = _catalog_columns(conn)
    if "instrument_type" in columns:
        return "LOWER(COALESCE(instrument_type, '')) IN ('equity', 'eq')"
    quoted = ", ".join("'" + symbol.replace("'", "''") + "'" for symbol in sorted(INDEX_LIKE_SYMBOLS))
    predicate = f"UPPER(symbol_id) NOT IN ({quoted})"
    if "is_benchmark" in columns:
        predicate += " AND COALESCE(is_benchmark, FALSE) = FALSE"
    return predicate


def _load_price_history(conn: duckdb.DuckDBPyConnection, *, as_of: str, exchange: str) -> pd.DataFrame:
    equity_predicate = _equity_predicate(conn)
    return conn.execute(
        f"""
        SELECT symbol_id, exchange, CAST(timestamp AS DATE) AS trade_date,
               timestamp, open, high, low, close, volume
        FROM _catalog_feature_source
        WHERE exchange = ?
          AND timestamp <= CAST(? AS TIMESTAMP)
          AND close IS NOT NULL
          AND close > 0
          AND {equity_predicate}
        ORDER BY symbol_id, trade_date
        """,
        [exchange, as_of],
    ).fetchdf()


def _load_benchmark_returns(
    conn: duckdb.DuckDBPyConnection,
    *,
    as_of: str,
    exchange: str,
) -> pd.Series:
    for alias in BENCHMARK_ALIASES:
        frame = conn.execute(
            """
            SELECT CAST(timestamp AS DATE) AS trade_date, close
            FROM _catalog_feature_source
            WHERE exchange = ?
              AND symbol_id = ?
              AND timestamp <= CAST(? AS TIMESTAMP)
              AND close IS NOT NULL
              AND close > 0
            ORDER BY trade_date
            """,
            [exchange, alias, as_of],
        ).fetchdf()
        if len(frame) < 41:
            continue
        frame.loc[:, "trade_date"] = pd.to_datetime(frame["trade_date"])
        returns = pd.to_numeric(frame["close"], errors="coerce").pct_change()
        returns.index = frame["trade_date"]
        returns = returns.dropna()
        if len(returns) >= 40:
            returns.name = "benchmark_return"
            return returns
    return pd.Series(dtype=float, name="benchmark_return")


def compute_phase1_symbol_features(
    conn: duckdb.DuckDBPyConnection,
    *,
    as_of: str,
    exchange: str = "NSE",
) -> pd.DataFrame:
    _create_phase1_catalog_view(conn)
    history = _load_price_history(conn, as_of=as_of, exchange=exchange)
    if history.empty:
        return pd.DataFrame(columns=["symbol_id", "exchange", "timestamp", *PHASE1_SYMBOL_COLUMNS])

    history.loc[:, "trade_date"] = pd.to_datetime(history["trade_date"])
    for column in ("open", "high", "low", "close", "volume"):
        history.loc[:, column] = pd.to_numeric(history[column], errors="coerce")
    history = history.sort_values(["symbol_id", "exchange", "trade_date"], kind="stable")

    benchmark_returns = _load_benchmark_returns(conn, as_of=as_of, exchange=exchange)
    rows: list[dict[str, object]] = []
    for (symbol, exc), group in history.groupby(["symbol_id", "exchange"], sort=False):
        group = group.dropna(subset=["close"]).copy()
        if group.empty:
            continue
        close = group["close"]
        daily_return = close.pct_change()
        high = group["high"]
        low = group["low"]
        prev_close = close.shift(1)
        true_range = pd.concat(
            [(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()],
            axis=1,
        ).max(axis=1)
        latest_close = float(close.iloc[-1])
        atr_14 = true_range.rolling(14, min_periods=10).mean().iloc[-1]
        avg_value = (close * group["volume"]).rolling(20, min_periods=15).mean().iloc[-1]

        beta = np.nan
        beta_obs = 0
        if not benchmark_returns.empty:
            stock_returns = daily_return.copy()
            stock_returns.index = group["trade_date"]
            aligned = pd.concat(
                [stock_returns.rename("stock_return"), benchmark_returns],
                axis=1,
                join="inner",
            ).dropna().tail(60)
            beta_obs = int(len(aligned))
            bench_var = aligned["benchmark_return"].var(ddof=1) if beta_obs >= 40 else np.nan
            if beta_obs >= 40 and pd.notna(bench_var) and bench_var > 0:
                beta = float(aligned["stock_return"].cov(aligned["benchmark_return"]) / bench_var)

        rows.append(
            {
                "symbol_id": symbol,
                "exchange": exc,
                "timestamp": group["timestamp"].iloc[-1],
                "realized_vol_20": _realized_vol(daily_return, 20, 15),
                "realized_vol_60": _realized_vol(daily_return, 60, 40),
                "beta_to_nifty_60": beta,
                "beta_to_nifty_60_obs": beta_obs,
                "max_drawdown_63": _max_drawdown(close, 63, 50),
                "max_drawdown_126": _max_drawdown(close, 126, 100),
                "atr_pct": float(atr_14 / latest_close * 100.0) if pd.notna(atr_14) and latest_close > 0 else np.nan,
                "avg_value_traded_20": float(avg_value) if pd.notna(avg_value) else np.nan,
            }
        )

    features = pd.DataFrame(rows)
    if features.empty:
        return features
    features.loc[:, "liquidity_score"] = (
        pd.to_numeric(features["avg_value_traded_20"], errors="coerce").rank(pct=True, method="average")
    )
    delivery = _compute_delivery_trends(conn, as_of=as_of, exchange=exchange)
    if not delivery.empty:
        features = features.merge(delivery, on=["symbol_id", "exchange"], how="left")
    for column in PHASE1_SYMBOL_COLUMNS:
        if column not in features.columns:
            features.loc[:, column] = np.nan
    return features[["symbol_id", "exchange", "timestamp", *PHASE1_SYMBOL_COLUMNS]]


def _realized_vol(returns: pd.Series, window: int, min_obs: int) -> float:
    sample = returns.dropna().tail(window)
    if len(sample) < min_obs:
        return np.nan
    return float(sample.std(ddof=1) * np.sqrt(252.0) * 100.0)


def _max_drawdown(close: pd.Series, window: int, min_obs: int) -> float:
    sample = pd.to_numeric(close, errors="coerce").dropna().tail(window)
    if len(sample) < min_obs:
        return np.nan
    running_high = sample.cummax().replace(0, np.nan)
    drawdown = sample / running_high - 1.0
    return float(drawdown.min() * 100.0)


def _delivery_date_column(conn: duckdb.DuckDBPyConnection) -> str | None:
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info('_delivery')").fetchall()}
    except Exception:
        return None
    if "timestamp" in columns:
        return "timestamp"
    if "date" in columns:
        return "date"
    return None


def _compute_delivery_trends(
    conn: duckdb.DuckDBPyConnection,
    *,
    as_of: str,
    exchange: str,
) -> pd.DataFrame:
    date_column = _delivery_date_column(conn)
    if date_column is None:
        return pd.DataFrame()
    try:
        delivery = conn.execute(
            f"""
            SELECT symbol_id, exchange, CAST({date_column} AS DATE) AS trade_date, delivery_pct
            FROM _delivery
            WHERE exchange = ?
              AND CAST({date_column} AS DATE) <= CAST(? AS DATE)
              AND delivery_pct IS NOT NULL
            ORDER BY symbol_id, trade_date
            """,
            [exchange, as_of],
        ).fetchdf()
    except Exception as exc:
        logger.warning("Phase 1 delivery trends unavailable: %s", exc)
        return pd.DataFrame()
    if delivery.empty:
        return pd.DataFrame()
    delivery.loc[:, "delivery_pct"] = pd.to_numeric(delivery["delivery_pct"], errors="coerce")
    rows = []
    for (symbol, exc), group in delivery.groupby(["symbol_id", "exchange"], sort=False):
        pct = group["delivery_pct"].dropna()
        if pct.empty:
            continue
        latest = float(pct.iloc[-1])
        avg5 = float(pct.tail(5).mean()) if len(pct) >= 3 else np.nan
        avg20 = float(pct.tail(20).mean()) if len(pct) >= 10 else np.nan
        change5 = latest - float(pct.iloc[-6]) if len(pct) >= 6 else np.nan
        vs20 = latest - avg20 if pd.notna(avg20) else np.nan
        rows.append(
            {
                "symbol_id": symbol,
                "exchange": exc,
                "delivery_pct_latest": latest,
                "delivery_pct_5d_avg": avg5,
                "delivery_pct_20d_avg": avg20,
                "delivery_pct_change_5d": change5,
                "delivery_pct_vs_20d": vs20,
                "delivery_trend_score": np.nanmean([change5, vs20]) if pd.notna(change5) or pd.notna(vs20) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def compute_phase1_market_breadth(
    conn: duckdb.DuckDBPyConnection,
    *,
    as_of: str,
    exchange: str = "NSE",
) -> pd.DataFrame:
    history = _load_price_history(conn, as_of=as_of, exchange=exchange)
    if history.empty:
        return pd.DataFrame(columns=["symbol_id", "exchange", "timestamp", *PHASE1_BREADTH_COLUMNS])
    history.loc[:, "trade_date"] = pd.to_datetime(history["trade_date"])
    history.loc[:, "close"] = pd.to_numeric(history["close"], errors="coerce")
    history = history.sort_values(["symbol_id", "trade_date"], kind="stable")
    grouped = history.groupby("symbol_id", sort=False)
    history.loc[:, "sma_200"] = grouped["close"].transform(lambda s: s.rolling(200, min_periods=200).mean())
    history.loc[:, "high_252"] = grouped["close"].transform(lambda s: s.rolling(252, min_periods=200).max())
    history.loc[:, "prev_close"] = grouped["close"].shift(1)
    rows = []
    for date, day in history.groupby("trade_date", sort=True):
        eligible = day[day["sma_200"].notna()].copy()
        if eligible.empty:
            continue
        advances = int((day["close"] > day["prev_close"]).sum())
        declines = int((day["close"] < day["prev_close"]).sum())
        pct_above = float((eligible["close"] > eligible["sma_200"]).mean())
        pct_high = float((eligible["close"] >= eligible["high_252"]).mean())
        breadth_score = pct_above * 70.0 + pct_high * 30.0
        rows.append(
            {
                "symbol_id": "__MARKET__",
                "exchange": exchange,
                "timestamp": pd.Timestamp(date),
                "breadth_score": breadth_score,
                "pct_above_200dma": pct_above,
                "pct_at_52w_high": pct_high,
                "advance_decline_ratio": float(advances / declines) if declines > 0 else float(advances) if advances else 0.0,
                "universe_count": int(len(day)),
                "eligible_200dma_count": int(len(eligible)),
                "advance_count": advances,
                "decline_count": declines,
            }
        )
    breadth = pd.DataFrame(rows)
    if breadth.empty:
        return breadth
    breadth.loc[:, "breadth_velocity_score"] = breadth["breadth_score"].diff(5).fillna(0.0)
    breadth.loc[:, "breadth_velocity_bucket"] = pd.cut(
        breadth["breadth_velocity_score"],
        bins=[-np.inf, -5.0, 5.0, np.inf],
        labels=["negative", "neutral", "positive"],
    ).astype(str)
    return breadth[["symbol_id", "exchange", "timestamp", *PHASE1_BREADTH_COLUMNS]]


def _replace_symbol_features(conn: duckdb.DuckDBPyConnection, frame: pd.DataFrame) -> int:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feat_phase1_symbol_features (
            symbol_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            date DATE,
            realized_vol_20 DOUBLE,
            realized_vol_60 DOUBLE,
            beta_to_nifty_60 DOUBLE,
            beta_to_nifty_60_obs DOUBLE,
            max_drawdown_63 DOUBLE,
            max_drawdown_126 DOUBLE,
            atr_pct DOUBLE,
            avg_value_traded_20 DOUBLE,
            liquidity_score DOUBLE,
            delivery_pct_latest DOUBLE,
            delivery_pct_5d_avg DOUBLE,
            delivery_pct_20d_avg DOUBLE,
            delivery_pct_change_5d DOUBLE,
            delivery_pct_vs_20d DOUBLE,
            delivery_trend_score DOUBLE,
            PRIMARY KEY (symbol_id, exchange, timestamp)
        )
        """
    )
    if frame.empty:
        return 0
    frame = frame.copy()
    frame.loc[:, "date"] = pd.to_datetime(frame["timestamp"]).dt.date
    min_date = frame["date"].min()
    max_date = frame["date"].max()
    conn.execute(
        "DELETE FROM feat_phase1_symbol_features WHERE date BETWEEN ? AND ?",
        [min_date, max_date],
    )
    conn.execute("INSERT INTO feat_phase1_symbol_features BY NAME SELECT * FROM frame")
    return int(len(frame))


def _replace_breadth_features(conn: duckdb.DuckDBPyConnection, frame: pd.DataFrame) -> int:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feat_phase1_market_breadth (
            symbol_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            date DATE,
            breadth_score DOUBLE,
            breadth_velocity_score DOUBLE,
            breadth_velocity_bucket VARCHAR,
            pct_above_200dma DOUBLE,
            pct_at_52w_high DOUBLE,
            advance_decline_ratio DOUBLE,
            universe_count DOUBLE,
            eligible_200dma_count DOUBLE,
            advance_count DOUBLE,
            decline_count DOUBLE,
            PRIMARY KEY (symbol_id, exchange, timestamp)
        )
        """
    )
    if frame.empty:
        return 0
    frame = frame.copy()
    frame.loc[:, "date"] = pd.to_datetime(frame["timestamp"]).dt.date
    min_date = frame["date"].min()
    max_date = frame["date"].max()
    conn.execute(
        "DELETE FROM feat_phase1_market_breadth WHERE exchange = ? AND date BETWEEN ? AND ?",
        [str(frame["exchange"].iloc[0]), min_date, max_date],
    )
    conn.execute("INSERT INTO feat_phase1_market_breadth BY NAME SELECT * FROM frame")
    return int(len(frame))
