"""Shared fixtures for the read-only MCP interface.

The heart of this file is ``connection_guard``: an autouse fixture that
replaces ``duckdb.connect`` and ``sqlite3.connect`` for the duration of every
test in this package and fails any call that would open a writable handle.

This is the enforcement for invariant I1. A checksum comparison after the fact
would only catch a write that actually happened; intercepting the constructor
catches the *ability* to write, including on code paths a test never exercises
hard enough to mutate anything.

Fixture construction legitimately needs to write, so store builders run inside
``connection_guard.paused()``. They depend on the guard fixture explicitly, so
pytest resolves it first and the ordering is deterministic.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import duckdb
import pytest

from ai_trading_system.interfaces.mcp.context import McpContext, McpProfile


class ReadOnlyViolation(AssertionError):
    """A guarded call tried to open a writable database handle."""


@dataclass
class ConnectionGuard:
    """Records every database connection and rejects writable ones."""

    calls: list[tuple[str, str, bool]] = field(default_factory=list)
    _paused: bool = False

    @contextmanager
    def paused(self) -> Iterator["ConnectionGuard"]:
        """Temporarily allow writable handles, for building fixture stores."""

        previous = self._paused
        self._paused = True
        try:
            yield self
        finally:
            self._paused = previous

    def record(self, driver: str, database: str, read_only: bool) -> None:
        self.calls.append((driver, database, read_only))

    def check_duckdb(self, database: Any, read_only: Any) -> None:
        if self._paused:
            return
        if str(database) == ":memory:":
            return
        if read_only is not True:
            raise ReadOnlyViolation(
                f"duckdb.connect({database!r}) opened without read_only=True. "
                "The MCP interface may only open DuckDB stores read-only "
                "(':memory:' is allowed for Parquet scans)."
            )

    def check_sqlite(self, database: Any, uri: Any) -> None:
        if self._paused:
            return
        target = str(database)
        if target == ":memory:":
            return
        if not (uri and target.startswith("file:") and "mode=ro" in target):
            raise ReadOnlyViolation(
                f"sqlite3.connect({database!r}, uri={uri!r}) opened without a "
                "read-only URI. The MCP interface must use "
                "sqlite3.connect(f'file:{path}?mode=ro', uri=True)."
            )

    def databases(self) -> list[str]:
        return [database for _, database, _ in self.calls]


@pytest.fixture(autouse=True)
def connection_guard(monkeypatch: pytest.MonkeyPatch) -> Iterator[ConnectionGuard]:
    guard = ConnectionGuard()

    real_duckdb_connect = duckdb.connect
    real_sqlite_connect = sqlite3.connect

    def guarded_duckdb_connect(
        database: Any = ":memory:", read_only: bool = False, **kwargs: Any
    ):
        guard.check_duckdb(database, read_only)
        guard.record("duckdb", str(database), bool(read_only))
        return real_duckdb_connect(database, read_only=read_only, **kwargs)

    def guarded_sqlite_connect(database: Any, *args: Any, **kwargs: Any):
        uri = kwargs.get("uri", False)
        guard.check_sqlite(database, uri)
        guard.record("sqlite3", str(database), bool(uri))
        return real_sqlite_connect(database, *args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", guarded_duckdb_connect)
    monkeypatch.setattr(sqlite3, "connect", guarded_sqlite_connect)
    yield guard


# ---------------------------------------------------------------------------
# Fixture stores
# ---------------------------------------------------------------------------

TRADING_DAYS = [
    "2026-01-05",
    "2026-01-06",
    "2026-01-07",
    "2026-01-08",
    "2026-01-09",
]

# A 2:1 split on 2026-01-08 makes raw and adjusted OHLC diverge, so a test can
# tell which basis a tool returned.
_CATALOG_ROWS = [
    # symbol, exchange, day, raw close, adjusted close
    ("AAA", "NSE", "2026-01-05", 200.0, 100.0),
    ("AAA", "NSE", "2026-01-06", 210.0, 105.0),
    ("AAA", "NSE", "2026-01-07", 220.0, 110.0),
    ("AAA", "NSE", "2026-01-08", 115.0, 115.0),
    ("AAA", "NSE", "2026-01-09", 120.0, 120.0),
    ("AAA", "BSE", "2026-01-08", 114.5, 114.5),
    ("AAA", "BSE", "2026-01-09", 119.5, 119.5),
    ("BBB", "NSE", "2026-01-08", 50.0, 50.0),
    ("BBB", "NSE", "2026-01-09", 52.0, 52.0),
]


def _build_ohlcv_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR, security_id VARCHAR, exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT,
                adjusted_open DOUBLE, adjusted_high DOUBLE, adjusted_low DOUBLE,
                adjusted_close DOUBLE,
                provider VARCHAR, validation_status VARCHAR
            )
            """
        )
        for symbol, exchange, day, raw, adjusted in _CATALOG_ROWS:
            conn.execute(
                """
                INSERT INTO _catalog VALUES (
                    ?, ?, ?, CAST(? AS TIMESTAMP),
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    'nse_bhavcopy', 'validated'
                )
                """,
                [
                    symbol, f"SEC{symbol}", exchange, day,
                    raw, raw * 1.01, raw * 0.99, raw, 100000,
                    adjusted, adjusted * 1.01, adjusted * 0.99, adjusted,
                ],
            )

        conn.execute(
            """
            CREATE TABLE _delivery (
                symbol_id VARCHAR, exchange VARCHAR, timestamp DATE,
                delivery_pct DOUBLE, volume BIGINT, delivery_qty BIGINT
            )
            """
        )
        for symbol, exchange, day, _raw, _adj in _CATALOG_ROWS:
            conn.execute(
                "INSERT INTO _delivery VALUES (?, ?, CAST(? AS DATE), ?, ?, ?)",
                [symbol, exchange, day, 45.5, 100000, 45500],
            )

        conn.execute(
            """
            CREATE TABLE feat_phase1_symbol_features (
                symbol_id VARCHAR, exchange VARCHAR, timestamp TIMESTAMP,
                date DATE,
                realized_vol_20 DOUBLE, realized_vol_60 DOUBLE,
                beta_to_nifty_60 DOUBLE, max_drawdown_63 DOUBLE,
                atr_pct DOUBLE, avg_value_traded_20 DOUBLE,
                liquidity_score DOUBLE, delivery_pct_20d_avg DOUBLE,
                delivery_trend_score DOUBLE,
                PRIMARY KEY (symbol_id, exchange, timestamp)
            )
            """
        )
        for index, day in enumerate(TRADING_DAYS):
            conn.execute(
                """
                INSERT INTO feat_phase1_symbol_features VALUES (
                    'AAA', 'NSE', CAST(? AS TIMESTAMP), CAST(? AS DATE),
                    ?, 0.28, 1.12, -0.18, 2.4, 8500.0, 0.81, 44.0, 0.62
                )
                """,
                [day, day, 0.30 + index * 0.01],
            )

        # Legacy weekly stage store: deliberately stale relative to the
        # governed store, matching production.
        conn.execute(
            """
            CREATE TABLE weekly_stage_snapshot (
                symbol VARCHAR, week_end_date DATE, stage_label VARCHAR,
                stage_confidence DOUBLE, stage_transition VARCHAR,
                bars_in_stage INTEGER, stage_entry_date DATE,
                ma10w DOUBLE, ma30w DOUBLE, ma40w DOUBLE, ma30w_slope_4w DOUBLE,
                weekly_rs_score DOUBLE, weekly_volume_ratio DOUBLE,
                support_level DOUBLE, resistance_level DOUBLE,
                created_at TIMESTAMP, run_id VARCHAR,
                PRIMARY KEY (symbol, week_end_date)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO weekly_stage_snapshot VALUES (
                'AAA', CAST('2025-12-26' AS DATE), 'S2', 0.82, 'S1_TO_S2',
                4, CAST('2025-11-28' AS DATE),
                108.0, 100.0, 98.0, 0.012, 71.0, 1.35, 92.0, 126.0,
                CAST('2025-12-27 00:00:00' AS TIMESTAMP), 'run-legacy'
            )
            """
        )
    finally:
        conn.close()


def _build_control_plane_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE decision_model_deployment (
                decision_domain VARCHAR, model_version VARCHAR, config_hash VARCHAR,
                environment VARCHAR, status VARCHAR,
                effective_from DATE, effective_to DATE
            )
            """
        )
        for domain in ("rank", "stage", "stage1", "pattern"):
            conn.execute(
                """
                INSERT INTO decision_model_deployment VALUES (
                    ?, 'v1', 'cfg1', 'production', 'approved',
                    CAST('2025-01-01' AS DATE), NULL
                )
                """,
                [domain],
            )

        conn.execute(
            """
            CREATE TABLE rank_history (
                symbol_id VARCHAR, exchange VARCHAR, trade_date DATE,
                universe_id VARCHAR, rank_position INTEGER, rank_percentile DOUBLE,
                composite_score DOUBLE, rs_score DOUBLE, volume_score DOUBLE,
                trend_score DOUBLE, proximity_score DOUBLE, sector_score DOUBLE,
                rank_model_version VARCHAR, rank_formula_name VARCHAR,
                rank_config_hash VARCHAR, pipeline_run_id VARCHAR
            )
            """
        )
        for index, day in enumerate(TRADING_DAYS):
            conn.execute(
                """
                INSERT INTO rank_history VALUES (
                    'AAA', 'NSE', CAST(? AS DATE), 'NSE_OPERATIONAL', ?, 0.98,
                    ?, 80.0, 40.0, 70.0, 65.0, 72.0,
                    'v1', 'weighted_sum', 'cfg1', 'run-1'
                )
                """,
                [day, 12 - index, 70.0 + index],
            )
            conn.execute(
                """
                INSERT INTO rank_history VALUES (
                    'BBB', 'NSE', CAST(? AS DATE), 'NSE_OPERATIONAL', ?, 0.55,
                    ?, 40.0, 20.0, 35.0, 30.0, 33.0,
                    'v1', 'weighted_sum', 'cfg1', 'run-1'
                )
                """,
                [day, 120 + index, 41.0 + index],
            )

        conn.execute(
            """
            CREATE TABLE rank_universe_history AS
            SELECT *,
                   composite_score AS composite_score_adjusted,
                   NULL::DOUBLE AS momentum_acceleration_score,
                   NULL::DOUBLE AS delivery_score,
                   0.9::DOUBLE AS rank_confidence,
                   TRUE AS rank_eligible,
                   '[]'::VARCHAR AS rejection_reasons,
                   0.8::DOUBLE AS liquidity_score,
                   10000000::DOUBLE AS avg_value_traded_20,
                   55.0::DOUBLE AS delivery_pct_20d_avg,
                   0.5::DOUBLE AS delivery_trend_score,
                   'profile_C_cash_only'::VARCHAR AS selection_policy,
                   60.0::DOUBLE AS effective_min_score,
                   20::INTEGER AS effective_top_n,
                   'neutral'::VARCHAR AS market_regime,
                   trade_date AS regime_as_of,
                   0::INTEGER AS regime_age_days,
                   'ALIGNED'::VARCHAR AS regime_freshness_status,
                   'rank-regime-freshness-v1'::VARCHAR AS regime_freshness_policy_version
            FROM rank_history
            """
        )
        conn.execute(
            """INSERT INTO rank_universe_history
               SELECT 'CCC','NSE',CAST('2026-01-09' AS DATE),'NSE_OPERATIONAL',300,0.2,
                      25.0,20.0,10.0,20.0,15.0,18.0,'v1','weighted_sum','cfg1','run-1',
                      25.0,NULL,NULL,0.5,TRUE,'[]',0.4,2000000,35.0,-0.1,
                      'profile_C_cash_only',60.0,20,'neutral',CAST('2026-01-09' AS DATE),0,
                      'ALIGNED','rank-regime-freshness-v1'"""
        )

        # ROTATOR drops out of the ranked universe before the latest session,
        # which is ordinary for a top-N cross-section. Observed on the live
        # store, where several symbols were last ranked one session back.
        for day in TRADING_DAYS[:-1]:
            conn.execute(
                """
                INSERT INTO rank_history VALUES (
                    'ROTATOR', 'NSE', CAST(? AS DATE), 'NSE_OPERATIONAL', 44, 0.70,
                    58.0, 55.0, 30.0, 50.0, 45.0, 48.0,
                    'v1', 'weighted_sum', 'cfg1', 'run-1'
                )
                """,
                [day],
            )

        conn.execute(
            """
            CREATE TABLE stage_history (
                symbol_id VARCHAR, exchange VARCHAR, trade_date DATE,
                stage_family VARCHAR, stage_label VARCHAR, stage_confidence DOUBLE,
                stage_input_complete BOOLEAN, close DOUBLE,
                sma_50 DOUBLE, sma_200 DOUBLE, stage_reason VARCHAR,
                stage_model_version VARCHAR, stage_config_hash VARCHAR,
                pipeline_run_id VARCHAR
            )
            """
        )
        for day in TRADING_DAYS:
            conn.execute(
                """
                INSERT INTO stage_history VALUES (
                    'AAA', 'NSE', CAST(? AS DATE), 'BROAD_STAGE', 'S2', 0.77,
                    TRUE, 115.0, 108.0, 96.0, 'above rising 200dma',
                    'v1', 'cfg1', 'run-1'
                )
                """,
                [day],
            )
            conn.execute(
                """
                INSERT INTO stage_history VALUES (
                    'BBB', 'NSE', CAST(? AS DATE), 'BROAD_STAGE', 'S4', 0.61,
                    TRUE, 50.0, 55.0, 62.0, 'below falling 200dma',
                    'v1', 'cfg1', 'run-1'
                )
                """,
                [day],
            )

        conn.execute(
            """
            CREATE TABLE pattern_history (
                symbol_id VARCHAR, exchange VARCHAR, trade_date DATE,
                pattern_family VARCHAR, pattern_state VARCHAR, pattern_score DOUBLE,
                pivot_price DOUBLE, distance_to_pivot_pct DOUBLE,
                pattern_model_version VARCHAR, pattern_config_hash VARCHAR
            )
            """
        )
        for day in TRADING_DAYS:
            conn.execute(
                """
                INSERT INTO pattern_history VALUES (
                    'AAA', 'NSE', CAST(? AS DATE), 'cup_handle', 'forming', 68.0,
                    126.0, 8.7, 'v1', 'cfg1'
                )
                """,
                [day],
            )

        # Governed Phase-3B weekly stage history: the store with live coverage.
        conn.execute(
            """
            CREATE TABLE weekly_stock_stage_history (
                observation_id VARCHAR, exchange VARCHAR, symbol_id VARCHAR,
                sector_id VARCHAR, sector_name VARCHAR, as_of DATE,
                source_week_start DATE, source_week_end DATE,
                stage_status VARCHAR, effective_stage VARCHAR,
                classifier_version VARCHAR, source_artifact_hash VARCHAR,
                observation_json VARCHAR, run_id VARCHAR, stage_attempt INTEGER,
                created_at TIMESTAMP
            )
            """
        )
        governed = [
            ("2026-01-02", "stage_1_basing"),
            ("2026-01-09", "transition_1_to_2"),
        ]
        for index, (as_of, stage) in enumerate(governed):
            conn.execute(
                """
                INSERT INTO weekly_stock_stage_history VALUES (
                    ?, 'NSE', 'AAA', 'SEC-CAP', 'Capital Goods', CAST(? AS DATE),
                    CAST(? AS DATE), CAST(? AS DATE),
                    'locked', ?, 'weekly-stage-v2', 'hash', ?, 'run-1', 1,
                    CAST(? AS TIMESTAMP)
                )
                """,
                [
                    f"obs-{index}", as_of, as_of, as_of, stage,
                    json.dumps({
                        "exchange": "NSE",
                        "symbol_id": "AAA",
                        "sector_id": "SEC-CAP",
                        "sector_name": "Capital Goods",
                        "as_of": as_of,
                        "source_week_start": as_of,
                        "source_week_end": as_of,
                        "stage_status": "locked",
                        "effective_stage": stage,
                        "classifier_version": "weekly-stage-v2",
                        "run_id": "run-1",
                    }),
                    as_of,
                ],
            )
        conn.execute(
            """
            CREATE TABLE stage_observation_governance (
                governance_event_id VARCHAR, observation_scope VARCHAR,
                observation_id VARCHAR, supersedes_observation_id VARCHAR,
                authoritative BOOLEAN, correction_authority VARCHAR,
                recorded_at TIMESTAMP
            )
            """
        )
        for index, (as_of, _stage) in enumerate(governed):
            conn.execute(
                """
                INSERT INTO stage_observation_governance VALUES (
                    ?, 'STOCK', ?, NULL, TRUE, 'original_observation',
                    CAST(? AS TIMESTAMP)
                )
                """,
                [f"gov-{index}", f"obs-{index}", as_of],
            )
    finally:
        conn.close()


def _build_master_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE symbols (
                symbol_id TEXT PRIMARY KEY, security_id TEXT, symbol_name TEXT,
                exchange TEXT, instrument_type TEXT, isin TEXT,
                lot_size INTEGER, tick_size REAL, freeze_quantity INTEGER,
                sector TEXT, industry TEXT, nse_symbol TEXT, bse_symbol TEXT,
                mcap REAL, last_updated TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    "AAA", "SECAAA", "Alpha Industries Ltd", "NSE", "EQUITY",
                    "INE000A01001", 1, 0.05, 0, "Capital Goods", "Defence",
                    "AAA", "500001", 125000.0, "2026-01-09",
                ),
                (
                    "BBB", "SECBBB", "Beta Metals Ltd", "NSE", "EQUITY",
                    "INE000B01002", 1, 0.05, 0, "Metals", "Steel",
                    "BBB", "500002", 42000.0, "2026-01-09",
                ),
                (
                    "CCC", "SECCCC", "Gamma Corp Ltd", "BSE", "EQUITY",
                    "INE000C01003", 1, 0.05, 0, "Chemicals", "Specialty",
                    None, "500003", 9000.0, "2026-01-09",
                ),
            ],
        )

        conn.execute(
            """
            CREATE TABLE stock_details (
                Security_id TEXT, Name TEXT, Symbol TEXT,
                "Industry Group" TEXT, Industry TEXT, MCAP REAL,
                Sector TEXT, exchange TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO stock_details VALUES (?,?,?,?,?,?,?,?)",
            [
                ("SECAAA", "Alpha Industries Ltd", "AAA", "Capital Goods",
                 "Defence", 125000.0, "Capital Goods", "NSE"),
                ("SECBBB", "Beta Metals Ltd", "BBB", "Metals",
                 "Steel", 42000.0, "Metals", "NSE"),
            ],
        )
        conn.execute(
            "CREATE TABLE sector_mapping (industry TEXT PRIMARY KEY, "
            "system_sector TEXT NOT NULL, last_updated TEXT)"
        )
        conn.executemany(
            "INSERT INTO sector_mapping VALUES (?,?,?)",
            [
                ("Defence", "Capital Goods", "2026-01-09"),
                ("Steel", "Metals", "2026-01-09"),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _build_screener_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE screener_company_snapshot (
                symbol TEXT, as_of_date TEXT, face_value REAL,
                market_cap_cr REAL, source TEXT, sync_batch_id TEXT,
                synced_at TEXT, PRIMARY KEY (symbol, as_of_date, source)
            )
            """
        )
        conn.executemany(
            "INSERT INTO screener_company_snapshot VALUES (?,?,?,?,?,?,?)",
            [
                ("AAA", "2025-11-15", 2.0, 118000.0, "screener", "b1", "2025-11-15"),
                ("AAA", "2026-02-14", 2.0, 125000.0, "screener", "b2", "2026-02-14"),
            ],
        )

        conn.execute(
            """
            CREATE TABLE screener_metric_catalog (
                metric_id TEXT PRIMARY KEY, metric_name TEXT, category TEXT,
                statement_type TEXT, unit TEXT, scale TEXT,
                higher_is_better INTEGER, source TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO screener_metric_catalog VALUES (?,?,?,?,?,?,?,?)",
            [
                ("sales", "Sales", "income", "profit_loss", "cr", "1", 1, "screener"),
                ("net_profit", "Net Profit", "income", "profit_loss", "cr", "1", 1, "screener"),
            ],
        )

        conn.execute(
            """
            CREATE TABLE screener_financials (
                symbol TEXT, period_type TEXT, report_date TEXT,
                statement_basis TEXT, metric_id TEXT, value REAL,
                available_at TEXT, source TEXT, sync_batch_id TEXT, synced_at TEXT,
                PRIMARY KEY (symbol, period_type, report_date, statement_basis,
                             metric_id, available_at)
            )
            """
        )
        # available_at is the publication date and is deliberately later than
        # report_date: a quarter ending 2025-12-31 is not knowable on Jan 5.
        conn.executemany(
            "INSERT INTO screener_financials VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                ("AAA", "quarterly", "2025-09-30", "standalone", "sales",
                 900.0, "2025-11-15", "screener", "b1", "2025-11-15"),
                ("AAA", "quarterly", "2025-09-30", "standalone", "net_profit",
                 120.0, "2025-11-15", "screener", "b1", "2025-11-15"),
                ("AAA", "quarterly", "2025-12-31", "standalone", "sales",
                 1000.0, "2026-02-14", "screener", "b2", "2026-02-14"),
                ("AAA", "quarterly", "2025-12-31", "standalone", "net_profit",
                 150.0, "2026-02-14", "screener", "b2", "2026-02-14"),
                ("AAA", "quarterly", "2025-12-31", "consolidated", "sales",
                 1400.0, "2026-02-14", "screener", "b2", "2026-02-14"),
            ],
        )

        conn.execute(
            """
            CREATE TABLE screener_market_valuation (
                symbol TEXT, date TEXT, statement_basis TEXT, price REAL,
                market_cap_cr REAL, pe REAL, pb REAL, ev_ebitda REAL,
                dividend_yield REAL, source TEXT, sync_batch_id TEXT,
                synced_at TEXT,
                PRIMARY KEY (symbol, date, statement_basis, source)
            )
            """
        )
        conn.executemany(
            "INSERT INTO screener_market_valuation VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("AAA", "2025-12-31", "standalone", 110.0, 118000.0, 28.0, 5.1,
                 18.0, 0.7, "screener", "b1", "2025-11-15"),
                ("AAA", "2026-01-09", "standalone", 120.0, 125000.0, 30.5, 5.4,
                 19.2, 0.6, "screener", "b2", "2026-02-14"),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _build_fundamentals_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        # snapshot_date is VARCHAR here and DATE in fundamental_snapshot below:
        # the live stores disagree on the type, so both are exercised.
        conn.execute(
            """
            CREATE TABLE fundamental_scores (
                snapshot_date VARCHAR, symbol VARCHAR, name VARCHAR,
                industry_group VARCHAR, industry VARCHAR,
                quality_score DOUBLE, growth_score DOUBLE,
                balance_sheet_score DOUBLE, valuation_score DOUBLE,
                ownership_score DOUBLE, fundamental_score DOUBLE,
                fundamental_tier VARCHAR, red_flags VARCHAR,
                hard_red_flag BOOLEAN, screener_snapshot_date DATE
            )
            """
        )
        conn.executemany(
            "INSERT INTO fundamental_scores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("2025-11-15", "AAA", "Alpha Industries Ltd", "Capital Goods",
                 "Defence", 70.0, 62.0, 74.0, 40.0, 66.0, 64.2, "B", "", False,
                 "2025-11-15"),
                ("2026-02-14", "AAA", "Alpha Industries Ltd", "Capital Goods",
                 "Defence", 78.0, 71.0, 80.0, 44.0, 69.0, 70.8, "A", "", False,
                 "2026-02-14"),
            ],
        )

        conn.execute(
            """
            CREATE TABLE fundamental_snapshot (
                snapshot_date DATE, symbol VARCHAR, name VARCHAR,
                industry_group VARCHAR, industry VARCHAR, current_price DOUBLE,
                market_cap DOUBLE, pe DOUBLE, roce DOUBLE, roe DOUBLE,
                debt_to_equity DOUBLE, promoter_holding DOUBLE,
                pledged_pct DOUBLE, piotroski_score DOUBLE, opm DOUBLE,
                price_to_book DOUBLE
            )
            """
        )
        conn.executemany(
            "INSERT INTO fundamental_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("2025-11-15", "AAA", "Alpha Industries Ltd", "Capital Goods",
                 "Defence", 110.0, 118000.0, 28.0, 24.0, 19.0, 0.1, 51.0, 0.0,
                 7.0, 21.0, 5.1),
                ("2026-02-14", "AAA", "Alpha Industries Ltd", "Capital Goods",
                 "Defence", 120.0, 125000.0, 30.5, 26.0, 20.5, 0.1, 51.0, 0.0,
                 8.0, 22.5, 5.4),
            ],
        )

        conn.execute(
            """
            CREATE TABLE company_growth_features (
                symbol VARCHAR, report_date VARCHAR, statement_basis VARCHAR,
                available_at VARCHAR, sales_cr DOUBLE, net_profit_cr DOUBLE,
                opm_pct DOUBLE, npm_pct DOUBLE,
                sales_qoq_growth DOUBLE, sales_yoy_growth DOUBLE,
                profit_qoq_growth DOUBLE, profit_yoy_growth DOUBLE,
                sales_4q_cagr DOUBLE, profit_4q_cagr DOUBLE
            )
            """
        )
        conn.executemany(
            "INSERT INTO company_growth_features VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("AAA", "2025-09-30", "standalone", "2025-11-15", 900.0, 120.0,
                 20.0, 13.3, 4.0, 15.0, 6.0, 18.0, 14.0, 17.0),
                ("AAA", "2025-12-31", "standalone", "2026-02-14", 1000.0, 150.0,
                 22.5, 15.0, 11.1, 19.0, 25.0, 22.0, 16.0, 21.0),
            ],
        )
        conn.execute(
            """CREATE TABLE fundamental_thesis_classification (
                classification_id VARCHAR, symbol_id VARCHAR, exchange VARCHAR,
                as_of DATE, source_data_hash VARCHAR, statement_basis VARCHAR,
                source_report_date DATE, source_available_at DATE,
                primary_thesis VARCHAR, secondary_theses_json VARCHAR,
                classification_status VARCHAR, evaluations_json VARCHAR,
                evidence_json VARCHAR, taxonomy_version VARCHAR, rule_version VARCHAR,
                semantic_payload_hash VARCHAR, created_at TIMESTAMP
            )"""
        )
        evaluations = json.dumps([
            {"family": family, "passed": family == "QUALITY_COMPOUNDER", "observed": {}, "required": {}, "blockers": [], "warnings": []}
            for family in (
                "QUALITY_COMPOUNDER", "HIGH_GROWTH_EMERGING", "EARNINGS_ACCELERATION",
                "UNDERVALUED_QUALITY", "CASHFLOW_BALANCE_SHEET_INFLECTION",
                "TURNAROUND_CYCLICAL_RECOVERY", "CAPITAL_RETURN_INCOME",
            )
        ])
        conn.execute(
            """INSERT INTO fundamental_thesis_classification VALUES (
                'fc1','AAA','NSE',CAST('2025-11-15' AS DATE),'hash-old','standalone',
                CAST('2025-09-30' AS DATE),CAST('2025-11-15' AS DATE),
                'UNDERVALUED_QUALITY','[]','QUALIFIED',?, '{}','taxonomy-v1','rules-v1',
                'payload-old',CAST('2025-11-15' AS TIMESTAMP))""", [evaluations]
        )
        conn.execute(
            """INSERT INTO fundamental_thesis_classification VALUES (
                'fc2','AAA','NSE',CAST('2026-01-05' AS DATE),'hash-new','standalone',
                CAST('2025-12-31' AS DATE),CAST('2026-01-05' AS DATE),
                'QUALITY_COMPOUNDER','[\"UNDERVALUED_QUALITY\"]','QUALIFIED',?, '{}','taxonomy-v1','rules-v1',
                'payload-new',CAST('2026-01-05' AS TIMESTAMP))""", [evaluations]
        )
        conn.execute(
            """CREATE TABLE fundamental_thesis_projection (
                projection_id VARCHAR, symbol_id VARCHAR, exchange VARCHAR, as_of DATE,
                source_data_hash VARCHAR, primary_thesis VARCHAR, secondary_theses_json VARCHAR,
                structural_stage VARCHAR, admission_eligible BOOLEAN,
                admission_blockers_json VARCHAR, daily_context_json VARCHAR,
                taxonomy_version VARCHAR, rule_version VARCHAR, admission_version VARCHAR,
                semantic_payload_hash VARCHAR, created_at TIMESTAMP
            )"""
        )
        for day in ("2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"):
            conn.execute(
                """INSERT INTO fundamental_thesis_projection VALUES (
                    ?, 'AAA','NSE',CAST(? AS DATE),'hash-new','QUALITY_COMPOUNDER',
                    '[\"UNDERVALUED_QUALITY\"]','stage_2_advancing',TRUE,'[]','{}',
                    'taxonomy-v1','rules-v1','admission-v1',?,CAST(? AS TIMESTAMP))""",
                [f"fp-{day}", day, f"projection-{day}", day],
            )
    finally:
        conn.close()


def _build_feature_store(root: Path) -> None:
    import pandas as pd

    families = {
        "rsi": {"rsi_14": [55.0, 58.0, 61.0, 64.0, 67.0]},
        "sma": {
            "sma_20": [95.0, 97.0, 99.0, 101.0, 103.0],
            "sma_50": [90.0, 91.0, 92.0, 93.0, 94.0],
            "sma_200": [80.0, 80.5, 81.0, 81.5, 82.0],
        },
        "adx": {
            "plus_di_14": [25.0, 26.0, 27.0, 28.0, 29.0],
            "minus_di_14": [15.0, 14.5, 14.0, 13.5, 13.0],
            "adx_14": [22.0, 24.0, 26.0, 28.0, 30.0],
        },
    }
    for family, columns in families.items():
        directory = root / family / "NSE"
        directory.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame(
            {
                "symbol_id": ["AAA"] * len(TRADING_DAYS),
                "exchange": ["NSE"] * len(TRADING_DAYS),
                "timestamp": pd.to_datetime(TRADING_DAYS),
                "close": [100.0, 105.0, 110.0, 115.0, 120.0],
                **columns,
            }
        )
        frame.to_parquet(directory / "AAA.parquet", index=False)


@pytest.fixture
def data_root(connection_guard: ConnectionGuard, tmp_path: Path) -> Path:
    """Build a complete miniature ``$DATA_ROOT`` and return it."""

    root = tmp_path / "data"
    root.mkdir(parents=True, exist_ok=True)
    (root / "fundamentals").mkdir(parents=True, exist_ok=True)

    with connection_guard.paused():
        _build_ohlcv_db(root / "ohlcv.duckdb")
        _build_control_plane_db(root / "control_plane.duckdb")
        _build_master_db(root / "masterdata.db")
        _build_screener_db(root / "fundamentals" / "screener_financials.db")
        _build_fundamentals_db(root / "fundamentals.duckdb")
        _build_feature_store(root / "feature_store")

    return root


@pytest.fixture
def ctx(data_root: Path, monkeypatch: pytest.MonkeyPatch) -> McpContext:
    """An ``McpContext`` bound to the fixture stores."""

    monkeypatch.setenv("DATA_ROOT", str(data_root))
    monkeypatch.setenv("DATA_DOMAIN", "operational")
    return McpContext.from_env(McpProfile.FIXTURE)
