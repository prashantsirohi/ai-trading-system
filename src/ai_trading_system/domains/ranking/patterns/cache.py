"""Daily pattern lifecycle snapshot cache backed by control_plane.duckdb."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

ACTIVE_LIFECYCLE_STATES = ("watchlist", "confirmed")


class PatternCacheStore:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            exists = bool(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_name = 'pattern_cache'
                    """
                ).fetchone()[0]
            )
            if not exists:
                self._create_schema(conn)
                return

            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info('pattern_cache')").fetchall()}
            if "as_of_date" not in columns or "pattern_lifecycle_state" not in columns:
                for index_name in (
                    "idx_pattern_cache_signal_date",
                    "idx_pattern_cache_stage2",
                    "idx_pattern_cache_lifecycle",
                    "idx_pattern_cache_as_of_date",
                ):
                    conn.execute(f"DROP INDEX IF EXISTS {index_name}")
                try:
                    conn.execute("ALTER TABLE pattern_cache RENAME TO pattern_cache_legacy")
                    self._create_schema(conn)
                    conn.execute(
                        """
                        INSERT INTO pattern_cache (
                            symbol_id,
                            exchange,
                            pattern_family,
                            pattern_state,
                            pattern_lifecycle_state,
                            stage2_score,
                            stage2_label,
                            signal_date,
                            as_of_date,
                            fresh_signal_date,
                            first_seen_date,
                            last_seen_date,
                            invalidated_date,
                            expired_date,
                            carry_forward_bars,
                            breakout_level,
                            watchlist_trigger_level,
                            invalidation_price,
                            pattern_score,
                            setup_quality,
                            width_bars,
                            scanned_at,
                            scan_run_id,
                            payload_json
                        )
                        SELECT
                            symbol_id,
                            COALESCE(exchange, 'NSE'),
                            pattern_family,
                            pattern_state,
                            pattern_state,
                            stage2_score,
                            stage2_label,
                            CAST(signal_date AS DATE),
                            CAST(signal_date AS DATE),
                            CAST(signal_date AS DATE),
                            CAST(signal_date AS DATE),
                            CAST(signal_date AS DATE),
                            NULL,
                            NULL,
                            0,
                            breakout_level,
                            watchlist_trigger_level,
                            invalidation_price,
                            pattern_score,
                            setup_quality,
                            width_bars,
                            scanned_at,
                            scan_run_id,
                            payload_json
                        FROM pattern_cache_legacy
                        """
                    )
                    conn.execute("DROP TABLE pattern_cache_legacy")
                except duckdb.Error:
                    self._ensure_column(conn, "pattern_lifecycle_state", "VARCHAR")
                    self._ensure_column(conn, "fresh_signal_date", "DATE")
                    self._ensure_column(conn, "first_seen_date", "DATE")
                    self._ensure_column(conn, "last_seen_date", "DATE")
                    self._ensure_column(conn, "invalidated_date", "DATE")
                    self._ensure_column(conn, "expired_date", "DATE")
                    self._ensure_column(conn, "carry_forward_bars", "INTEGER")
                    self._ensure_column(conn, "as_of_date", "DATE")
                    conn.execute(
                        """
                        UPDATE pattern_cache
                        SET
                            pattern_lifecycle_state = COALESCE(pattern_lifecycle_state, pattern_state),
                            fresh_signal_date = COALESCE(fresh_signal_date, signal_date),
                            first_seen_date = COALESCE(first_seen_date, signal_date),
                            last_seen_date = COALESCE(last_seen_date, signal_date),
                            carry_forward_bars = COALESCE(carry_forward_bars, 0),
                            as_of_date = COALESCE(as_of_date, signal_date)
                        """
                    )
                    self._ensure_indexes(conn)
                return

            self._ensure_column(conn, "fresh_signal_date", "DATE")
            self._ensure_column(conn, "pattern_lifecycle_state", "VARCHAR")
            self._ensure_column(conn, "first_seen_date", "DATE")
            self._ensure_column(conn, "last_seen_date", "DATE")
            self._ensure_column(conn, "invalidated_date", "DATE")
            self._ensure_column(conn, "expired_date", "DATE")
            self._ensure_column(conn, "carry_forward_bars", "INTEGER")
            self._ensure_column(conn, "as_of_date", "DATE")
            self._ensure_indexes(conn)

    def _create_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pattern_cache (
                symbol_id VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL DEFAULT 'NSE',
                pattern_family VARCHAR NOT NULL,
                pattern_state VARCHAR NOT NULL,
                pattern_lifecycle_state VARCHAR NOT NULL,
                stage2_score DOUBLE,
                stage2_label VARCHAR,
                signal_date DATE NOT NULL,
                as_of_date DATE NOT NULL,
                fresh_signal_date DATE,
                first_seen_date DATE,
                last_seen_date DATE,
                invalidated_date DATE,
                expired_date DATE,
                carry_forward_bars INTEGER DEFAULT 0,
                breakout_level DOUBLE,
                watchlist_trigger_level DOUBLE,
                invalidation_price DOUBLE,
                pattern_score DOUBLE,
                setup_quality DOUBLE,
                width_bars INTEGER,
                scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                scan_run_id VARCHAR,
                payload_json VARCHAR,
                PRIMARY KEY (symbol_id, exchange, pattern_family, as_of_date)
            )
            """
        )
        self._ensure_indexes(conn)

    def _ensure_indexes(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pattern_cache_as_of_date
            ON pattern_cache (as_of_date)
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
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pattern_cache_lifecycle
            ON pattern_cache (pattern_lifecycle_state, as_of_date)
            """
        )

    def _ensure_column(self, conn: duckdb.DuckDBPyConnection, name: str, type_sql: str) -> None:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info('pattern_cache')").fetchall()}
        if name not in columns:
            conn.execute(f"ALTER TABLE pattern_cache ADD COLUMN {name} {type_sql}")

    def read_snapshot(
        self,
        *,
        as_of_date: str,
        exchange: str = "NSE",
        min_pattern_score: float = 0.0,
        lifecycle_states: tuple[str, ...] | list[str] | set[str] | None = None,
    ) -> pd.DataFrame:
        where = [
            "exchange = ?",
            "as_of_date = CAST(? AS DATE)",
            "COALESCE(pattern_score, 0) >= ?",
        ]
        params: list[Any] = [str(exchange), str(as_of_date), float(min_pattern_score)]
        if lifecycle_states:
            states = [str(value) for value in lifecycle_states if str(value).strip()]
            if states:
                placeholders = ",".join("?" for _ in states)
                where.append(f"pattern_lifecycle_state IN ({placeholders})")
                params.extend(states)

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT payload_json
                FROM pattern_cache
                WHERE {' AND '.join(where)}
                ORDER BY symbol_id, pattern_family
                """,
                params,
            ).fetchall()
        if not rows:
            return pd.DataFrame()
        payloads = [json.loads(row[0]) for row in rows if row[0]]
        return pd.DataFrame(payloads)

    def read_cached_signals(
        self,
        *,
        signal_date: str,
        exchange: str = "NSE",
        min_pattern_score: float = 0.0,
    ) -> pd.DataFrame:
        return self.read_snapshot(
            as_of_date=signal_date,
            exchange=exchange,
            min_pattern_score=min_pattern_score,
        )

    def write_signals(
        self,
        signals_df: pd.DataFrame,
        *,
        scan_run_id: str,
        replace_date: str | None = None,
        replace_run_scope: str | None = None,
        as_of_date: str | None = None,
    ) -> int:
        snapshot_date = str(as_of_date or replace_date or "")
        if signals_df.empty:
            if snapshot_date or replace_run_scope is not None:
                with self._connect() as conn:
                    if snapshot_date:
                        conn.execute(
                            "DELETE FROM pattern_cache WHERE as_of_date = CAST(? AS DATE)",
                            [snapshot_date],
                        )
                    if replace_run_scope is not None:
                        conn.execute(
                            "DELETE FROM pattern_cache WHERE scan_run_id LIKE ?",
                            [f"{str(replace_run_scope)}%"],
                        )
            return 0

        output = signals_df.copy()
        if "exchange" not in output.columns:
            output.loc[:, "exchange"] = "NSE"
        else:
            output.loc[:, "exchange"] = output["exchange"].fillna("NSE").replace("", "NSE")
        if "signal_date" not in output.columns:
            raise ValueError("signals_df must include signal_date")

        if not snapshot_date:
            if "as_of_date" in output.columns:
                snapshot_date = str(output["as_of_date"].dropna().astype(str).iloc[0])
            else:
                snapshot_date = str(output["signal_date"].dropna().astype(str).iloc[0])

        output.loc[:, "scan_run_id"] = str(scan_run_id)
        output.loc[:, "as_of_date"] = str(snapshot_date)
        output.loc[:, "fresh_signal_date"] = output.get("fresh_signal_date", output["signal_date"])
        output.loc[:, "pattern_lifecycle_state"] = output.get(
            "pattern_lifecycle_state",
            output.get("pattern_state", pd.Series("watchlist", index=output.index)),
        )
        output.loc[:, "first_seen_date"] = output.get("first_seen_date", output["fresh_signal_date"])
        output.loc[:, "last_seen_date"] = output.get("last_seen_date", output["as_of_date"])
        output.loc[:, "invalidated_date"] = output.get("invalidated_date", pd.Series(None, index=output.index))
        output.loc[:, "expired_date"] = output.get("expired_date", pd.Series(None, index=output.index))
        output.loc[:, "carry_forward_bars"] = pd.to_numeric(
            output.get("carry_forward_bars", pd.Series(0, index=output.index)),
            errors="coerce",
        ).fillna(0).astype(int)
        output.loc[:, "payload_json"] = output.apply(
            lambda row: json.dumps({key: self._jsonable(value) for key, value in row.items()}, sort_keys=True),
            axis=1,
        )

        cols = [
            "symbol_id",
            "exchange",
            "pattern_family",
            "pattern_state",
            "pattern_lifecycle_state",
            "stage2_score",
            "stage2_label",
            "signal_date",
            "as_of_date",
            "fresh_signal_date",
            "first_seen_date",
            "last_seen_date",
            "invalidated_date",
            "expired_date",
            "carry_forward_bars",
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
            if snapshot_date:
                conn.execute(
                    "DELETE FROM pattern_cache WHERE as_of_date = CAST(? AS DATE)",
                    [snapshot_date],
                )
            if replace_run_scope is not None:
                conn.execute(
                    "DELETE FROM pattern_cache WHERE scan_run_id LIKE ?",
                    [f"{str(replace_run_scope)}%"],
                )
            relation = conn.from_df(output[cols])
            conn.register("pattern_cache_stage", relation)
            conn.execute(
                """
                INSERT OR REPLACE INTO pattern_cache (
                    symbol_id,
                    exchange,
                    pattern_family,
                    pattern_state,
                    pattern_lifecycle_state,
                    stage2_score,
                    stage2_label,
                    signal_date,
                    as_of_date,
                    fresh_signal_date,
                    first_seen_date,
                    last_seen_date,
                    invalidated_date,
                    expired_date,
                    carry_forward_bars,
                    breakout_level,
                    watchlist_trigger_level,
                    invalidation_price,
                    pattern_score,
                    setup_quality,
                    width_bars,
                    scan_run_id,
                    payload_json
                )
                SELECT
                    symbol_id,
                    exchange,
                    pattern_family,
                    pattern_state,
                    pattern_lifecycle_state,
                    CAST(stage2_score AS DOUBLE),
                    stage2_label,
                    CAST(signal_date AS DATE),
                    CAST(as_of_date AS DATE),
                    CAST(fresh_signal_date AS DATE),
                    CAST(first_seen_date AS DATE),
                    CAST(last_seen_date AS DATE),
                    CAST(invalidated_date AS DATE),
                    CAST(expired_date AS DATE),
                    CAST(carry_forward_bars AS INTEGER),
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

    def latest_snapshot_date(self, *, as_of_date: str | None = None, exchange: str = "NSE") -> str | None:
        if as_of_date is None:
            sql = "SELECT MAX(as_of_date) FROM pattern_cache WHERE exchange = ?"
            params = [str(exchange)]
        else:
            sql = """
                SELECT MAX(as_of_date)
                FROM pattern_cache
                WHERE exchange = ?
                  AND as_of_date < CAST(? AS DATE)
            """
            params = [str(exchange), str(as_of_date)]
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return row[0].isoformat() if row and row[0] is not None else None

    def latest_full_scan_date(self, exchange: str = "NSE") -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(as_of_date)
                FROM pattern_cache
                WHERE exchange = ?
                  AND (
                        scan_run_id LIKE 'full:%'
                        OR scan_run_id LIKE 'weekly_full_%'
                  )
                """,
                [str(exchange)],
            ).fetchone()
        return row[0].isoformat() if row and row[0] is not None else None

    def latest_cached_signal_date(self, *, as_of_date: str | None = None, exchange: str = "NSE") -> str | None:
        return self.latest_snapshot_date(as_of_date=as_of_date, exchange=exchange)

    def load_latest_active_signals_before(self, *, as_of_date: str, exchange: str = "NSE") -> pd.DataFrame:
        latest_snapshot_date = self.latest_snapshot_date(as_of_date=as_of_date, exchange=exchange)
        if latest_snapshot_date is None:
            return pd.DataFrame()
        return self.read_snapshot(
            as_of_date=latest_snapshot_date,
            exchange=exchange,
            lifecycle_states=ACTIVE_LIFECYCLE_STATES,
        )

    @staticmethod
    def _jsonable(value: Any) -> Any:
        if isinstance(value, (pd.Timestamp,)):
            return value.isoformat()
        if hasattr(value, "isoformat") and callable(value.isoformat):
            try:
                return value.isoformat()
            except Exception:
                pass
        if pd.isna(value):
            return None
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return value
        return value
