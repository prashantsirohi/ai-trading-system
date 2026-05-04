from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.ingest import daily_update_runner


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: D401
        return cls(2026, 4, 7)


def test_business_dates_exclude_nse_holidays(tmp_path: Path) -> None:
    masterdb = tmp_path / "masterdata.db"
    conn = sqlite3.connect(masterdb)
    try:
        conn.execute("CREATE TABLE nse_holidays (date TEXT)")
        conn.execute("INSERT INTO nse_holidays (date) VALUES ('2026-04-03')")
        conn.commit()
    finally:
        conn.close()

    dates = daily_update_runner._business_dates(
        "2026-04-01",
        "2026-04-07",
        masterdb_path=str(masterdb),
    )
    assert dates == ["2026-04-01", "2026-04-02", "2026-04-06", "2026-04-07"]


def test_normalize_bhavcopy_frame_strips_series_values() -> None:
    raw = pd.DataFrame(
        [
            {
                "SYMBOL": "AAA",
                "SERIES": " EQ ",
                "OPEN_PRICE": 10.0,
                "HIGH_PRICE": 11.0,
                "LOW_PRICE": 9.5,
                "CLOSE_PRICE": 10.5,
                "TTL_TRD_QNTY": 1000,
            }
        ]
    )
    frame = daily_update_runner._normalize_bhavcopy_frame(
        raw,
        "2026-04-07",
        {"AAA": {"symbol_id": "AAA", "security_id": "101", "exchange": "NSE"}},
    )
    assert len(frame) == 1
    assert frame.iloc[0]["symbol_id"] == "AAA"


def test_normalize_bhavcopy_frame_prefers_isin_mapping_over_symbol_text() -> None:
    raw = pd.DataFrame(
        [
            {
                "SYMBOL": "WRONGTOKEN",
                "ISIN": "INE000A01011",
                "SERIES": "EQ",
                "OPEN_PRICE": 10.0,
                "HIGH_PRICE": 11.0,
                "LOW_PRICE": 9.5,
                "CLOSE_PRICE": 10.5,
                "TTL_TRD_QNTY": 1000,
            }
        ]
    )
    frame = daily_update_runner._normalize_bhavcopy_frame(
        raw,
        "2026-04-07",
        {"AAA": {"symbol_id": "AAA", "security_id": "101", "exchange": "NSE", "isin": "INE000A01011"}},
        isin_map={"INE000A01011": {"symbol_id": "AAA", "security_id": "101", "exchange": "NSE", "isin": "INE000A01011"}},
    )
    assert len(frame) == 1
    assert frame.iloc[0]["symbol_id"] == "AAA"
    assert frame.iloc[0]["isin"] == "INE000A01011"


def test_quarantine_housekeeping_downgrades_noncritical_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog_quarantine (
                symbol_id VARCHAR,
                security_id VARCHAR,
                exchange VARCHAR,
                trade_date DATE,
                reason VARCHAR,
                status VARCHAR,
                source_run_id VARCHAR,
                repair_batch_id VARCHAR,
                note VARCHAR,
                created_at TIMESTAMP,
                resolved_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog_quarantine
            (symbol_id, security_id, exchange, trade_date, reason, status, source_run_id, repair_batch_id, note, created_at, resolved_at)
            VALUES
            ('AAA', '1', 'NSE', '2026-04-03', 'provider_unavailable', 'active', NULL, NULL, NULL, CURRENT_TIMESTAMP, NULL),
            ('BBB', '2', 'NSE', '2026-04-02', 'repair_source_unavailable', 'active', NULL, NULL, NULL, CURRENT_TIMESTAMP, NULL),
            ('CCC', '3', 'NSE', '2026-04-02', 'provider_unavailable', 'active', NULL, NULL, NULL, CURRENT_TIMESTAMP, NULL)
            """
        )
    finally:
        conn.close()

    masterdb = tmp_path / "masterdata.db"
    mconn = sqlite3.connect(masterdb)
    try:
        mconn.execute("CREATE TABLE nse_holidays (date TEXT)")
        mconn.execute("INSERT INTO nse_holidays (date) VALUES ('2026-04-03')")
        mconn.commit()
    finally:
        mconn.close()

    stats = daily_update_runner._downgrade_noncritical_quarantine_rows(
        db_path=str(db_path),
        masterdb_path=str(masterdb),
        from_date="2026-04-01",
        to_date="2026-04-07",
        run_id="pipeline-2026-04-07-test",
    )

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT symbol_id, reason, status
            FROM _catalog_quarantine
            ORDER BY symbol_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert stats["repair_rows_observed"] == 1
    assert stats["non_trading_provider_rows_observed"] == 1
    assert stats["stale_provider_rows_observed"] == 0
    assert rows == [
        ("AAA", "provider_unavailable", "observed"),
        ("BBB", "repair_source_unavailable", "observed"),
        ("CCC", "provider_unavailable", "active"),
    ]


def test_quarantine_housekeeping_downgrades_stale_symbol_provider_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog_quarantine (
                symbol_id VARCHAR,
                security_id VARCHAR,
                exchange VARCHAR,
                trade_date DATE,
                reason VARCHAR,
                status VARCHAR,
                source_run_id VARCHAR,
                repair_batch_id VARCHAR,
                note VARCHAR,
                created_at TIMESTAMP,
                resolved_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog_quarantine
            (symbol_id, security_id, exchange, trade_date, reason, status, source_run_id, repair_batch_id, note, created_at, resolved_at)
            VALUES
            ('AAA', '1', 'NSE', '2026-04-17', 'provider_unavailable', 'active', NULL, NULL, NULL, CURRENT_TIMESTAMP, NULL),
            ('BBB', '2', 'NSE', '2026-04-17', 'provider_unavailable', 'active', NULL, NULL, NULL, CURRENT_TIMESTAMP, NULL)
            """
        )
    finally:
        conn.close()

    stats = daily_update_runner._downgrade_noncritical_quarantine_rows(
        db_path=str(db_path),
        masterdb_path=None,
        from_date="2026-04-13",
        to_date="2026-04-20",
        run_id="pipeline-2026-04-20-test",
        stale_symbol_ids=["AAA"],
    )

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT symbol_id, reason, status
            FROM _catalog_quarantine
            ORDER BY symbol_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert stats["stale_provider_rows_observed"] == 1
    assert rows == [
        ("AAA", "provider_unavailable", "observed"),
        ("BBB", "provider_unavailable", "active"),
    ]


def test_daily_update_runner_uses_nse_then_yfinance_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured_frames = []

    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"

        def get_symbols_from_masterdb(self, exchanges=None):
            return [
                {
                    "symbol_id": "AAA",
                    "security_id": "101",
                    "symbol_name": "AAA Ltd",
                    "industry_group": "Test Group",
                    "industry": "Test Industry",
                    "exchange": "NSE",
                }
            ]

        def _get_last_dates(self, exchanges=None):
            return {"AAA": "2026-04-03"}

        def _upsert_ohlcv(self, dfs):
            captured_frames.extend(dfs)
            return sum(len(df) for df in dfs)

    def fake_fetch_nse_bhavcopy_rows(*, raw_dir, trade_dates, security_map, isin_map=None):
        assert trade_dates == ["2026-04-06"]
        assert set(security_map) == {"AAA"}
        return pd.DataFrame(), [], ["2026-04-06"]

    def fake_fetch_yfinance_rows(*, symbol_rows, trade_dates, batch_size=100):
        assert [row["symbol_id"] for row in symbol_rows] == ["AAA"]
        assert trade_dates == ["2026-04-06"]
        return pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "security_id": "101",
                    "exchange": "NSE",
                    "timestamp": pd.Timestamp("2026-04-06"),
                    "open": 104.0,
                    "high": 106.0,
                    "low": 103.0,
                    "close": 105.0,
                    "volume": 1200,
                }
            ]
        )

    monkeypatch.setattr(daily_update_runner, "project_root", str(tmp_path))
    monkeypatch.setattr(daily_update_runner, "datetime", _FixedDateTime)
    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)
    monkeypatch.setattr(daily_update_runner, "_fetch_nse_bhavcopy_rows", fake_fetch_nse_bhavcopy_rows)
    monkeypatch.setattr(daily_update_runner, "_fetch_yfinance_rows", fake_fetch_yfinance_rows)
    monkeypatch.setattr(daily_update_runner, "_load_historically_trusted_symbols", lambda _db_path: {"AAA"})

    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=50,
        bulk=False,
        nse_primary=True,
        nse_allow_yfinance_fallback=True,
        data_domain="operational",
    )

    assert result["symbols_updated"] == 1
    assert result["symbols_errors"] == 0
    assert result["updated_symbols"] == ["AAA"]
    assert result["providers_used"] == ["yfinance"]
    assert result["nse_bhavcopy_dates"] == []
    assert result["yfinance_fallback_dates"] == ["2026-04-06"]
    assert result["unresolved_dates"] == []
    assert result["rows_written"] == 1
    assert len(captured_frames) == 1
    written = captured_frames[0].reset_index()
    assert written["timestamp"].dt.date.astype(str).tolist() == ["2026-04-06"]


def test_daily_update_runner_canary_blocks_when_dates_remain_unresolved(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"

        def get_symbols_from_masterdb(self, exchanges=None):
            return [
                {
                    "symbol_id": "AAA",
                    "security_id": "101",
                    "symbol_name": "AAA Ltd",
                    "industry_group": "Test Group",
                    "industry": "Test Industry",
                    "exchange": "NSE",
                }
            ]

        def _get_last_dates(self, exchanges=None):
            return {"AAA": "2026-04-03"}

        def _upsert_ohlcv(self, dfs):
            return sum(len(df) for df in dfs)

    def fake_fetch_nse_bhavcopy_rows(*, raw_dir, trade_dates, security_map, isin_map=None):
        return pd.DataFrame(), [], ["2026-04-06"]

    def fake_fetch_yfinance_rows(*, symbol_rows, trade_dates, batch_size=100):
        return pd.DataFrame()

    monkeypatch.setattr(daily_update_runner, "project_root", str(tmp_path))
    monkeypatch.setattr(daily_update_runner, "datetime", _FixedDateTime)
    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)
    monkeypatch.setattr(daily_update_runner, "_fetch_nse_bhavcopy_rows", fake_fetch_nse_bhavcopy_rows)
    monkeypatch.setattr(daily_update_runner, "_fetch_yfinance_rows", fake_fetch_yfinance_rows)
    monkeypatch.setattr(daily_update_runner, "_load_historically_trusted_symbols", lambda _db_path: {"AAA"})

    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=50,
        bulk=False,
        nse_primary=True,
        nse_allow_yfinance_fallback=True,
        canary_mode=True,
        canary_symbol_limit=5,
        data_domain="operational",
    )

    assert result["canary_mode"] is True
    assert result["canary_symbol_limit"] == 5
    assert result["canary_blocked"] is True
    assert result["canary_status"] == "blocked"
    assert result["unresolved_dates"] == ["2026-04-06"]


def test_daily_update_runner_bulk_canary_blocks_on_error(monkeypatch, tmp_path: Path) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"
            self.use_api = True
            self.dhan = object()
            self.client_id = "cid"
            self.api_key = "key"
            self.access_token = "token"
            self.token_manager = type(
                "TM",
                (),
                {
                    "ensure_valid_token": staticmethod(lambda hours_before_expiry=1: "token"),
                    "client_id": "cid",
                    "api_key": "key",
                },
            )()

        def _init_dhan_client(self):
            self.dhan = object()

        def _ensure_valid_token(self):
            return True

        def run_daily_update_bulk(self, **kwargs):
            return {"error": "bulk api failed", "symbols_errors": 0}

    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)
    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=25,
        bulk=True,
        canary_mode=True,
        canary_symbol_limit=3,
        data_domain="operational",
    )
    assert result["canary_mode"] is True
    assert result["canary_symbol_limit"] == 3
    assert result["canary_blocked"] is True
    assert result["canary_status"] == "blocked"


def test_daily_update_runner_bulk_canary_blocks_on_degraded_trust(monkeypatch, tmp_path: Path) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"
            self.use_api = True
            self.dhan = object()
            self.client_id = "cid"
            self.api_key = "key"
            self.access_token = "token"
            self.token_manager = type(
                "TM",
                (),
                {
                    "ensure_valid_token": staticmethod(lambda hours_before_expiry=1: "token"),
                    "client_id": "cid",
                    "api_key": "key",
                },
            )()

        def _init_dhan_client(self):
            self.dhan = object()

        def _ensure_valid_token(self):
            return True

        def run_daily_update_bulk(self, **kwargs):
            return {
                "symbols_updated": 5,
                "symbols_errors": 0,
                "trust_summary": {"status": "degraded"},
            }

    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)
    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=25,
        bulk=True,
        canary_mode=True,
        canary_symbol_limit=3,
        data_domain="operational",
    )
    assert result["canary_blocked"] is True
    assert result["canary_status"] == "blocked"


def test_daily_update_runner_bulk_canary_blocks_on_validator_unresolved_dates(monkeypatch, tmp_path: Path) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"
            self.use_api = True
            self.dhan = object()
            self.client_id = "cid"
            self.api_key = "key"
            self.access_token = "token"
            self.token_manager = type(
                "TM",
                (),
                {
                    "ensure_valid_token": staticmethod(lambda hours_before_expiry=1: "token"),
                    "client_id": "cid",
                    "api_key": "key",
                },
            )()

        def _init_dhan_client(self):
            self.dhan = object()

        def _ensure_valid_token(self):
            return True

        def run_daily_update_bulk(self, **kwargs):
            return {
                "symbols_updated": 5,
                "symbols_errors": 0,
                "validator_status": "ok",
                "validator_unresolved_dates": ["2026-04-17"],
            }

    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)
    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=25,
        bulk=True,
        canary_mode=True,
        canary_symbol_limit=3,
        data_domain="operational",
    )
    assert result["canary_blocked"] is True
    assert result["canary_status"] == "blocked"


def test_daily_update_runner_bulk_canary_passes_when_clean(monkeypatch, tmp_path: Path) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"
            self.use_api = True
            self.dhan = object()
            self.client_id = "cid"
            self.api_key = "key"
            self.access_token = "token"
            self.token_manager = type(
                "TM",
                (),
                {
                    "ensure_valid_token": staticmethod(lambda hours_before_expiry=1: "token"),
                    "client_id": "cid",
                    "api_key": "key",
                },
            )()

        def _init_dhan_client(self):
            self.dhan = object()

        def _ensure_valid_token(self):
            return True

        def run_daily_update_bulk(self, **kwargs):
            return {
                "symbols_updated": 5,
                "symbols_errors": 0,
                "trust_summary": {"status": "trusted"},
                "validator_status": "ok",
                "unresolved_dates": [],
            }

    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)
    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=25,
        bulk=True,
        canary_mode=True,
        canary_symbol_limit=3,
        data_domain="operational",
    )
    assert result["canary_blocked"] is False
    assert result["canary_status"] == "passed"


def test_daily_update_runner_quarantines_only_impacted_symbol_dates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"

        def get_symbols_from_masterdb(self, exchanges=None):
            return [
                {
                    "symbol_id": "AAA",
                    "security_id": "101",
                    "symbol_name": "AAA Ltd",
                    "industry_group": "Test Group",
                    "industry": "Test Industry",
                    "exchange": "NSE",
                },
                {
                    "symbol_id": "BBB",
                    "security_id": "102",
                    "symbol_name": "BBB Ltd",
                    "industry_group": "Test Group",
                    "industry": "Test Industry",
                    "exchange": "NSE",
                },
            ]

        def _get_last_dates(self, exchanges=None):
            return {"AAA": "2026-04-03", "BBB": "2026-04-03"}

        def _upsert_ohlcv(self, dfs):
            return sum(len(df) for df in dfs)

    def fake_fetch_nse_bhavcopy_rows(*, raw_dir, trade_dates, security_map, isin_map=None):
        return pd.DataFrame(), [], ["2026-04-06"]

    def fake_fetch_yfinance_rows(*, symbol_rows, trade_dates, batch_size=100):
        return pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "security_id": "101",
                    "exchange": "NSE",
                    "timestamp": pd.Timestamp("2026-04-06"),
                    "open": 104.0,
                    "high": 106.0,
                    "low": 103.0,
                    "close": 105.0,
                    "volume": 1200,
                }
            ]
        )

    monkeypatch.setattr(daily_update_runner, "project_root", str(tmp_path))
    monkeypatch.setattr(daily_update_runner, "datetime", _FixedDateTime)
    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)
    monkeypatch.setattr(daily_update_runner, "_fetch_nse_bhavcopy_rows", fake_fetch_nse_bhavcopy_rows)
    monkeypatch.setattr(daily_update_runner, "_fetch_yfinance_rows", fake_fetch_yfinance_rows)

    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=50,
        bulk=False,
        nse_primary=True,
        nse_allow_yfinance_fallback=True,
        data_domain="operational",
    )

    assert result["symbols_updated"] == 1
    assert result["unresolved_dates"] == ["2026-04-06"]
    assert result["quarantined_row_count"] == 1
    assert result["unresolved_symbol_date_count"] == 1
    assert result["unresolved_symbol_date_count_all"] == 1

    conn = duckdb.connect(str(tmp_path / "ohlcv.duckdb"), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT symbol_id, trade_date, status
            FROM _catalog_quarantine
            ORDER BY symbol_id
            """
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("BBB", datetime(2026, 4, 6).date(), "active")]


def test_daily_update_runner_applies_stale_grace_to_unresolved_dates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"

        def get_symbols_from_masterdb(self, exchanges=None):
            return [
                {
                    "symbol_id": "AAA",
                    "security_id": "101",
                    "symbol_name": "AAA Ltd",
                    "industry_group": "Test Group",
                    "industry": "Test Industry",
                    "exchange": "NSE",
                }
            ]

        def _get_last_dates(self, exchanges=None):
            # The fixed run date in this test is 2026-04-07, so target_end_date is 2026-04-06.
            # The symbol is stale beyond grace, but the runner should still attempt a catch-up fetch.
            return {"AAA": "2026-04-01"}

        def _upsert_ohlcv(self, dfs):
            return sum(len(df) for df in dfs)

    monkeypatch.setattr(daily_update_runner, "project_root", str(tmp_path))
    monkeypatch.setattr(daily_update_runner, "datetime", _FixedDateTime)
    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)
    captured: dict[str, object] = {}

    def fake_fetch_nse_bhavcopy_rows(*, trade_dates, **kwargs):
        captured["trade_dates"] = list(trade_dates)
        return pd.DataFrame(), [], list(trade_dates)

    monkeypatch.setattr(daily_update_runner, "_fetch_nse_bhavcopy_rows", fake_fetch_nse_bhavcopy_rows)
    monkeypatch.setattr(
        daily_update_runner,
        "_fetch_yfinance_rows",
        lambda **kwargs: pd.DataFrame(),
    )

    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=50,
        bulk=False,
        nse_primary=True,
        stale_missing_symbol_grace_days=1,
        data_domain="operational",
    )

    assert result["symbols_updated"] == 0
    assert captured["trade_dates"] == ["2026-04-02", "2026-04-03", "2026-04-06"]
    assert result["unresolved_dates"] == ["2026-04-02", "2026-04-03", "2026-04-06"]
    assert result["stale_missing_symbol_count"] == 1
    assert result["stale_missing_symbols"] == ["AAA"]


def test_daily_update_runner_counts_trading_gap_not_calendar_gap(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"

        def get_symbols_from_masterdb(self, exchanges=None):
            return [
                {
                    "symbol_id": "AAA",
                    "security_id": "101",
                    "symbol_name": "AAA Ltd",
                    "industry_group": "Test Group",
                    "industry": "Test Industry",
                    "exchange": "NSE",
                }
            ]

        def _get_last_dates(self, exchanges=None):
            return {"AAA": "2026-04-30"}

        def _upsert_ohlcv(self, dfs):
            return sum(len(df) for df in dfs)

    masterdb = sqlite3.connect(tmp_path / "masterdata.db")
    try:
        masterdb.execute("CREATE TABLE nse_holidays (date TEXT)")
        masterdb.execute("INSERT INTO nse_holidays (date) VALUES ('2026-05-01')")
        masterdb.commit()
    finally:
        masterdb.close()

    captured: dict[str, object] = {}

    def fake_fetch_nse_bhavcopy_rows(*, trade_dates, **kwargs):
        captured["trade_dates"] = list(trade_dates)
        return pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "security_id": "101",
                    "exchange": "NSE",
                    "timestamp": pd.Timestamp("2026-05-04"),
                    "open": 104.0,
                    "high": 106.0,
                    "low": 103.0,
                    "close": 105.0,
                    "volume": 1200,
                }
            ]
        ), ["2026-05-04"], []

    monkeypatch.setattr(daily_update_runner, "project_root", str(tmp_path))
    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)
    monkeypatch.setattr(daily_update_runner, "_fetch_nse_bhavcopy_rows", fake_fetch_nse_bhavcopy_rows)
    monkeypatch.setattr(daily_update_runner, "_fetch_yfinance_rows", lambda **kwargs: pd.DataFrame())

    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=50,
        bulk=False,
        nse_primary=True,
        data_domain="operational",
        target_end_date="2026-05-04",
    )

    assert captured["trade_dates"] == ["2026-05-04"]
    assert result["stale_missing_symbol_count"] == 0
    assert result["symbols_updated"] == 1
    assert result["nse_bhavcopy_dates"] == ["2026-05-04"]


def test_daily_update_runner_honors_explicit_target_end_date(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"

        def get_symbols_from_masterdb(self, exchanges=None):
            return [
                {
                    "symbol_id": "AAA",
                    "security_id": "101",
                    "symbol_name": "AAA Ltd",
                    "industry_group": "Test Group",
                    "industry": "Test Industry",
                    "exchange": "NSE",
                }
            ]

        def _get_last_dates(self, exchanges=None):
            return {}

        def _upsert_ohlcv(self, dfs):
            return sum(len(df) for df in dfs)

    captured: dict[str, object] = {}

    def fake_fetch_nse_bhavcopy_rows(*, trade_dates, **kwargs):
        captured["trade_dates"] = list(trade_dates)
        return pd.DataFrame(), [], list(trade_dates)

    monkeypatch.setattr(daily_update_runner, "project_root", str(tmp_path))
    monkeypatch.setattr(daily_update_runner, "datetime", _FixedDateTime)
    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)
    monkeypatch.setattr(daily_update_runner, "_fetch_nse_bhavcopy_rows", fake_fetch_nse_bhavcopy_rows)
    monkeypatch.setattr(daily_update_runner, "_fetch_yfinance_rows", lambda **kwargs: pd.DataFrame())

    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=50,
        bulk=False,
        nse_primary=True,
        data_domain="operational",
        target_end_date="2026-04-07",
    )

    assert captured["trade_dates"][-1] == "2026-04-07"
    assert "2026-04-07" in captured["trade_dates"]
    assert result["target_end_date"] == "2026-04-07"


def test_daily_update_runner_dhan_historical_mode_calls_collector_daily_update(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"
            self.use_api = True
            self.dhan = object()
            self.client_id = "cid"
            self.api_key = "key"
            self.access_token = "token"
            self.token_manager = type(
                "TM",
                (),
                {
                    "ensure_valid_token": staticmethod(lambda hours_before_expiry=1: "token"),
                    "client_id": "cid",
                    "api_key": "key",
                },
            )()
            self.called = False

        def _init_dhan_client(self):
            self.dhan = object()

        def _ensure_valid_token(self):
            return True

        def run_daily_update(
            self,
            exchanges=None,
            batch_size=700,
            max_concurrent=10,
            days_history=7,
            symbol_limit=None,
            compute_features=False,
            full_rebuild=False,
            feature_tail_bars=252,
        ):
            self.called = True
            return {
                "symbols_updated": 1,
                "symbols_errors": 0,
                "updated_symbols": ["AAA"],
                "duration_sec": 0.1,
            }

    holder = {}

    def collector_factory(*args, **kwargs):
        instance = DummyCollector(*args, **kwargs)
        holder["collector"] = instance
        return instance

    def fail_nse(*args, **kwargs):
        raise AssertionError("NSE/yfinance path should not run in dhan_historical_daily mode")

    monkeypatch.setattr(daily_update_runner, "project_root", str(tmp_path))
    monkeypatch.setattr(daily_update_runner, "datetime", _FixedDateTime)
    monkeypatch.setattr(daily_update_runner, "DhanCollector", collector_factory)
    monkeypatch.setattr(daily_update_runner, "_run_nse_yfinance_daily_update", fail_nse)

    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=50,
        bulk=False,
        dhan_historical_daily=True,
        data_domain="operational",
    )

    assert holder["collector"].called is True
    assert result["ohlc_source_mode"] == "dhan_historical_daily"
    assert result["symbols_updated"] == 1


def test_daily_update_runner_defaults_to_dhan_primary(monkeypatch, tmp_path: Path) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.db_path = str(tmp_path / "ohlcv.duckdb")
            self.masterdb_path = str(tmp_path / "masterdata.db")
            self.feature_store_dir = str(tmp_path / "feature_store")
            self.data_domain = "operational"
            self.use_api = True
            self.dhan = object()
            self.client_id = "cid"
            self.api_key = "key"
            self.access_token = "token"
            self.token_manager = type(
                "TM",
                (),
                {
                    "ensure_valid_token": staticmethod(lambda hours_before_expiry=1: "token"),
                    "client_id": "cid",
                    "api_key": "key",
                },
            )()

        def _init_dhan_client(self):
            self.dhan = object()

        def _ensure_valid_token(self):
            return True

    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)

    called = {"dhan_primary": False}

    def fake_dhan_primary(**kwargs):
        called["dhan_primary"] = True
        return {
            "symbols_updated": 1,
            "symbols_errors": 0,
            "updated_symbols": ["AAA"],
            "trust_summary": {"status": "trusted"},
            "validator_status": "ok",
            "duration_sec": 0.1,
        }

    monkeypatch.setattr(daily_update_runner, "_run_dhan_primary_daily_update", fake_dhan_primary)
    monkeypatch.setattr(
        daily_update_runner,
        "_run_nse_yfinance_daily_update",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("NSE primary path should not run by default")),
    )

    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=25,
        bulk=False,
        data_domain="operational",
    )

    assert called["dhan_primary"] is True
    assert result["symbols_updated"] == 1
