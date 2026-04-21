"""Two-tier pattern scan cache backed by control_plane.duckdb."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


class PatternCacheStore:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pattern_cache (
                    symbol_id VARCHAR NOT NULL,
                    exchange VARCHAR NOT NULL DEFAULT 'NSE',
                    pattern_family VARCHAR NOT NULL,
                    pattern_state VARCHAR NOT NULL,
                    stage2_score DOUBLE,
                    stage2_label VARCHAR,
                    signal_date DATE NOT NULL,
                    breakout_level DOUBLE,
                    watchlist_trigger_level DOUBLE,
                    invalidation_price DOUBLE,
                    pattern_score DOUBLE,
                    setup_quality DOUBLE,
                    width_bars INTEGER,
                    scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    scan_run_id VARCHAR,
                    payload_json VARCHAR,
                    PRIMARY KEY (symbol_id, exchange, pattern_family, pattern_state, signal_date)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pattern_cache_signal_date
                ON pattern_cache (signal_date)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pattern_cache_stage2
                ON pattern_cache (stage2_score, pattern_state)
                """
            )

    def read_cached_signals(
        self,
        *,
        signal_date: str,
        exchange: str = "NSE",
        min_pattern_score: float = 0.0,
    ) -> pd.DataFrame:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM pattern_cache
                WHERE exchange = ?
                  AND signal_date = CAST(? AS DATE)
                  AND COALESCE(pattern_score, 0) >= ?
                ORDER BY symbol_id, pattern_family, pattern_state
                """,
                [str(exchange), str(signal_date), float(min_pattern_score)],
            ).fetchall()
        if not rows:
            return pd.DataFrame()
        payloads = [json.loads(row[0]) for row in rows if row[0]]
        return pd.DataFrame(payloads)

    def write_signals(
        self,
        signals_df: pd.DataFrame,
        *,
        scan_run_id: str,
        replace_date: str | None = None,
        replace_run_scope: str | None = None,
    ) -> int:
        if signals_df.empty:
            if replace_date is not None or replace_run_scope is not None:
                with self._connect() as conn:
                    if replace_date is not None:
                        conn.execute("DELETE FROM pattern_cache WHERE signal_date = CAST(? AS DATE)", [str(replace_date)])
                    if replace_run_scope is not None:
                        conn.execute("DELETE FROM pattern_cache WHERE scan_run_id LIKE ?", [f"{str(replace_run_scope)}%"])
            return 0
        output = signals_df.copy()
        if "exchange" not in output.columns:
            output.loc[:, "exchange"] = "NSE"
        else:
            output.loc[:, "exchange"] = output["exchange"].fillna("NSE").replace("", "NSE")
        if "signal_date" not in output.columns:
            raise ValueError("signals_df must include signal_date")
        output.loc[:, "scan_run_id"] = str(scan_run_id)
        output.loc[:, "payload_json"] = output.apply(
            lambda row: json.dumps({key: self._jsonable(value) for key, value in row.items()}, sort_keys=True),
            axis=1,
        )
        cols = [
            "symbol_id",
            "exchange",
            "pattern_family",
            "pattern_state",
            "stage2_score",
            "stage2_label",
            "signal_date",
            "breakout_level",
            "watchlist_trigger_level",
            "invalidation_price",
            "pattern_score",
            "setup_quality",
            "width_bars",
            "scan_run_id",
            "payload_json",
        ]
        for col in cols:
            if col not in output.columns:
                output.loc[:, col] = None
        with self._connect() as conn:
            if replace_date is not None:
                conn.execute("DELETE FROM pattern_cache WHERE signal_date = CAST(? AS DATE)", [str(replace_date)])
            if replace_run_scope is not None:
                conn.execute("DELETE FROM pattern_cache WHERE scan_run_id LIKE ?", [f"{str(replace_run_scope)}%"])
            relation = conn.from_df(output[cols])
            conn.register("pattern_cache_stage", relation)
            conn.execute(
                """
                INSERT OR REPLACE INTO pattern_cache
                (symbol_id, exchange, pattern_family, pattern_state, stage2_score, stage2_label, signal_date,
                 breakout_level, watchlist_trigger_level, invalidation_price, pattern_score, setup_quality,
                 width_bars, scan_run_id, payload_json)
                SELECT
                    symbol_id,
                    exchange,
                    pattern_family,
                    pattern_state,
                    CAST(stage2_score AS DOUBLE),
                    stage2_label,
                    CAST(signal_date AS DATE),
                    CAST(breakout_level AS DOUBLE),
                    CAST(watchlist_trigger_level AS DOUBLE),
                    CAST(invalidation_price AS DOUBLE),
                    CAST(pattern_score AS DOUBLE),
                    CAST(setup_quality AS DOUBLE),
                    CAST(width_bars AS INTEGER),
                    scan_run_id,
                    payload_json
                FROM pattern_cache_stage
                """
            )
            conn.unregister("pattern_cache_stage")
        return int(len(output))

    def symbols_needing_rescan(
        self,
        all_symbols: list[str],
        *,
        ohlcv_db_path: str | Path,
        min_price_change_pct: float = 1.0,
        min_volume_ratio: float = 1.3,
        as_of_date: str | None = None,
    ) -> list[str]:
        symbols = [str(symbol).strip().upper() for symbol in all_symbols if str(symbol).strip()]
        if not symbols:
            return []
        placeholders = ",".join("?" for _ in symbols)
        date_filter = "AND CAST(timestamp AS DATE) <= CAST(? AS DATE)" if as_of_date is not None else ""
        params: list[Any] = list(symbols)
        if as_of_date is not None:
            params.append(str(as_of_date))
        params.extend([float(min_price_change_pct), float(min_volume_ratio)])
        conn = duckdb.connect(str(ohlcv_db_path), read_only=True)
        try:
            rows = conn.execute(
                f"""
                WITH scoped AS (
                    SELECT
                        symbol_id,
                        timestamp,
                        close,
                        volume,
                        LAG(close) OVER (PARTITION BY symbol_id ORDER BY timestamp) AS prev_close,
                        AVG(volume) OVER (
                            PARTITION BY symbol_id
                            ORDER BY timestamp
                            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                        ) AS prev20_volume,
                        ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) AS rn
                    FROM _catalog
                    WHERE symbol_id IN ({placeholders})
                    {date_filter}
                )
                SELECT symbol_id
                FROM scoped
                WHERE rn = 1
                  AND (
                        ABS(((close - prev_close) / NULLIF(prev_close, 0)) * 100.0) >= ?
                        OR COALESCE(volume / NULLIF(prev20_volume, 0), 0.0) >= ?
                  )
                ORDER BY symbol_id
                """,
                params,
            ).fetchall()
        finally:
            conn.close()
        return [str(row[0]) for row in rows]

    def latest_full_scan_date(self, exchange: str = "NSE") -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(signal_date)
                FROM pattern_cache
                WHERE exchange = ?
                  AND scan_run_id LIKE 'full:%'
                """,
                [str(exchange)],
            ).fetchone()
        return row[0].isoformat() if row and row[0] is not None else None

    def latest_cached_signal_date(self, *, as_of_date: str | None = None, exchange: str = "NSE") -> str | None:
        if as_of_date is None:
            sql = "SELECT MAX(signal_date) FROM pattern_cache WHERE exchange = ?"
            params = [str(exchange)]
        else:
            sql = """
                SELECT MAX(signal_date)
                FROM pattern_cache
                WHERE exchange = ?
                  AND signal_date < CAST(? AS DATE)
            """
            params = [str(exchange), str(as_of_date)]
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return row[0].isoformat() if row and row[0] is not None else None

    @staticmethod
    def _jsonable(value: Any) -> Any:
        if isinstance(value, (pd.Timestamp,)):
            return value.isoformat()
        if pd.isna(value):
            return None
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return value
        return value
