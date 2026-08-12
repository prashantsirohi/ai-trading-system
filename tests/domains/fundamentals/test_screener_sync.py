from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ai_trading_system.domains.fundamentals import screener_sync
from ai_trading_system.domains.fundamentals.screener_client import ScreenerFetchResult, ScreenerRateLimitError
from ai_trading_system.domains.fundamentals.screener_store import ScreenerFinancialsStore
from ai_trading_system.domains.fundamentals.screener_sync import build_parser


def test_screener_sync_defaults_follow_data_root(monkeypatch, tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    monkeypatch.setenv("DATA_ROOT", str(data_root))

    args = build_parser().parse_args(["--statement-basis", "standalone"])

    assert Path(args.db_path) == data_root / "fundamentals" / "screener_financials.db"
    assert Path(args.exports_dir) == data_root / "fundamentals" / "exports"
    assert Path(args.master_db_path) == data_root / "masterdata.db"


def test_screener_sync_requires_explicit_statement_basis() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args([])


def test_screener_sync_reports_per_symbol_failures(monkeypatch, tmp_path: Path) -> None:
    class FakeStore:
        def __init__(self, db_path, **_kwargs):
            self.db_path = Path(db_path)

        def get_synced_symbols(self, **_kwargs):
            return set()

        def begin_batch(self, *_args, **_kwargs):
            return None

        def record_error(self, *_args, **_kwargs):
            return None

        def record_sync_result(self, *_args, **_kwargs):
            return None

        def finish_batch(self, *_args, **_kwargs):
            return None

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, symbol, **_kwargs):
            raise RuntimeError(f"download blocked for {symbol}")

        def excel_path(self, symbol, **_kwargs):
            return tmp_path / f"{symbol}_screener.xlsx"

    monkeypatch.setattr(screener_sync, "ScreenerFinancialsStore", FakeStore)
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])
    monkeypatch.setattr(screener_sync, "DEFAULT_RETRY_BACKOFF_SEC", 0.0)
    messages: list[str] = []

    result = screener_sync.run_sync(
        statement_basis="standalone",
        db_path=tmp_path / "screener_financials.db",
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        allow_download=True,
        refresh_readmodels=False,
        progress=messages.append,
    )

    assert result["failed"] == 1
    assert any("[1/1] AAA: download+parse started" in message for message in messages)
    assert any("AAA: failed error=RuntimeError: download blocked for AAA" in message for message in messages)
    assert any("Inspect failures with:" in message for message in messages)


@pytest.mark.parametrize(
    ("as_of_date", "expected_report_date"),
    [
        ("2026-01-15", "2025-12-31"),
        ("2026-04-15", "2026-03-31"),
        ("2026-07-15", "2026-06-30"),
        ("2026-10-15", "2026-09-30"),
    ],
)
def test_expected_quarterly_report_date(as_of_date: str, expected_report_date: str) -> None:
    assert screener_sync.expected_quarterly_report_date(as_of_date) == expected_report_date


def test_symbols_missing_quarterly_report_date_selects_only_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "screener_financials.db"
    store = ScreenerFinancialsStore(db_path)
    store.save_company_financials(
        "AAA",
        _company_data("2025-12-31"),
        sync_batch_id="batch-ok",
        as_of_date="2026-01-15",
    )

    missing = screener_sync._symbols_missing_quarterly_report_date(
        db_path,
        ["AAA", "BBB"],
        report_date="2025-12-31",
        statement_basis="standalone",
    )

    assert missing == ["BBB"]


def test_missing_current_results_reparses_local_export_when_expected_quarter_present(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, symbol, **kwargs):
            calls.append((symbol, kwargs))
            return _fetch_result(tmp_path, symbol, _company_data("2025-12-31"))

    calls: list[tuple[str, dict]] = []
    db_path = tmp_path / "screener_financials.db"
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])

    result = screener_sync.run_sync(
        statement_basis="standalone",
        db_path=db_path,
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        missing_current_results=True,
        as_of_date="2026-01-15",
        refresh_readmodels=False,
    )

    assert result["succeeded"] == 1
    assert result["failed"] == 0
    assert result["expected_report_date"] == "2025-12-31"
    assert calls == [
        (
            "AAA",
            {"statement_basis": "standalone", "force_download": False, "allow_download": False},
        )
    ]
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM screener_financials
            WHERE symbol = 'AAA'
              AND period_type = 'quarterly'
              AND report_date = '2025-12-31'
            """
        ).fetchone()
    assert row[0] > 0


def test_missing_current_results_download_forces_fresh_export_when_allowed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, symbol, **kwargs):
            calls.append((symbol, kwargs))
            return _fetch_result(tmp_path, symbol, _company_data("2025-12-31"))

    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])

    result = screener_sync.run_sync(
        statement_basis="standalone",
        db_path=tmp_path / "screener_financials.db",
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        allow_download=True,
        missing_current_results=True,
        as_of_date="2026-01-15",
        refresh_readmodels=False,
    )

    assert result["succeeded"] == 1
    assert calls == [
        (
            "AAA",
            {"statement_basis": "standalone", "force_download": True, "allow_download": True},
        )
    ]


def test_missing_current_results_skips_stale_export_without_expected_quarter(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, _symbol, **_kwargs):
            calls.append(_symbol)
            return _fetch_result(tmp_path, _symbol, _company_data("2025-09-30"))

    calls: list[str] = []
    db_path = tmp_path / "screener_financials.db"
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])

    result = screener_sync.run_sync(
        statement_basis="standalone",
        db_path=db_path,
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        missing_current_results=True,
        as_of_date="2026-01-15",
        refresh_readmodels=False,
    )

    assert result["succeeded"] == 0
    assert result["skipped"] == 1
    assert result["failed"] == 0
    assert calls == ["AAA"]
    with sqlite3.connect(db_path) as conn:
        error = conn.execute("SELECT error FROM screener_sync_error WHERE symbol = 'AAA'").fetchone()
    assert error is None


def test_sync_retries_transient_symbol_failure(monkeypatch, tmp_path: Path) -> None:
    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, symbol, **_kwargs):
            calls.append(symbol)
            if len(calls) == 1:
                raise TimeoutError("temporary browser timeout")
            return _fetch_result(tmp_path, symbol, _company_data("2025-12-31"))

    calls: list[str] = []
    messages: list[str] = []
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])
    monkeypatch.setattr(screener_sync, "DEFAULT_RETRY_BACKOFF_SEC", 0.0)

    result = screener_sync.run_sync(
        statement_basis="standalone",
        db_path=tmp_path / "screener_financials.db",
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        missing_current_results=True,
        as_of_date="2026-01-15",
        refresh_readmodels=False,
        progress=messages.append,
    )

    assert result["succeeded"] == 1
    assert result["failed"] == 0
    assert calls == ["AAA", "AAA"]
    assert any("attempt 1/3 failed error=TimeoutError" in message for message in messages)
    assert any("retry attempt 2/3" in message for message in messages)


def test_sync_honors_retry_after_for_http_429(monkeypatch, tmp_path: Path) -> None:
    class FakeClient:
        def fetch_company_data(self, symbol, **_kwargs):
            calls.append(symbol)
            if len(calls) == 1:
                raise ScreenerRateLimitError(429, "https://example.test", retry_after=7)
            return _fetch_result(tmp_path, symbol, _company_data("2025-12-31"))

    class FakeStore:
        def save_company_financials(self, *_args, **_kwargs):
            return None

        def record_sync_result(self, *_args, **_kwargs):
            return None

    calls: list[str] = []
    delays: list[float] = []
    monkeypatch.setattr(screener_sync.time, "sleep", delays.append)

    detected = screener_sync._sync_symbol_with_retries(
        client=FakeClient(),
        store=FakeStore(),
        symbol="AAA",
        statement_basis="standalone",
        force_download=True,
        allow_download=True,
        expected_report_date=None,
        sync_batch_id="batch",
        progress=None,
        label="AAA",
    )

    assert detected == "standalone"
    assert calls == ["AAA", "AAA"]
    assert delays == [7.0]


def test_sync_records_failure_after_three_attempts(monkeypatch, tmp_path: Path) -> None:
    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, symbol, **_kwargs):
            calls.append(symbol)
            raise TimeoutError("browser timeout")

        def excel_path(self, symbol, **_kwargs):
            return tmp_path / f"{symbol}_screener.xlsx"

    calls: list[str] = []
    db_path = tmp_path / "screener_financials.db"
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])
    monkeypatch.setattr(screener_sync, "DEFAULT_RETRY_BACKOFF_SEC", 0.0)

    result = screener_sync.run_sync(
        statement_basis="standalone",
        db_path=db_path,
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        missing_current_results=True,
        as_of_date="2026-01-15",
        refresh_readmodels=False,
    )

    assert result["succeeded"] == 0
    assert result["failed"] == 1
    assert calls == ["AAA", "AAA", "AAA"]
    with sqlite3.connect(db_path) as conn:
        error = conn.execute("SELECT error FROM screener_sync_error WHERE symbol = 'AAA'").fetchone()[0]
    assert error == "browser timeout"


def test_default_sync_still_skips_already_synced_symbols(monkeypatch, tmp_path: Path) -> None:
    class FakeStore:
        def __init__(self, _db_path, **_kwargs):
            pass

        def get_synced_symbols(self, **_kwargs):
            return {"AAA"}

        def begin_batch(self, *_args, **_kwargs):
            return None

        def save_company_financials(self, symbol, *_args, **_kwargs):
            saved.append(symbol)

        def record_sync_result(self, *_args, **_kwargs):
            return None

        def finish_batch(self, *_args, **_kwargs):
            return None

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, symbol, **_kwargs):
            fetched.append(symbol)
            return _fetch_result(tmp_path, symbol, _company_data("2025-12-31"))

        def excel_path(self, symbol, **_kwargs):
            return tmp_path / f"{symbol}_screener.xlsx"

    fetched: list[str] = []
    saved: list[str] = []
    monkeypatch.setattr(screener_sync, "ScreenerFinancialsStore", FakeStore)
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA", "BBB"])

    result = screener_sync.run_sync(
        statement_basis="standalone",
        db_path=tmp_path / "screener_financials.db",
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        refresh_readmodels=False,
    )

    assert result["succeeded"] == 1
    assert fetched == ["BBB"]
    assert saved == ["BBB"]


def test_sync_can_be_limited_to_requested_symbols(monkeypatch, tmp_path: Path) -> None:
    class FakeStore:
        def __init__(self, _db_path, **_kwargs):
            pass

        def get_synced_symbols(self, **_kwargs):
            return set()

        def begin_batch(self, *_args, **_kwargs):
            return None

        def save_company_financials(self, symbol, *_args, **_kwargs):
            saved.append(symbol)

        def record_sync_result(self, *_args, **_kwargs):
            return None

        def finish_batch(self, *_args, **_kwargs):
            return None

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, symbol, **_kwargs):
            fetched.append(symbol)
            return _fetch_result(tmp_path, symbol, _company_data("2025-12-31"))

        def excel_path(self, symbol, **_kwargs):
            return tmp_path / f"{symbol}_screener.xlsx"

    fetched: list[str] = []
    saved: list[str] = []
    monkeypatch.setattr(screener_sync, "ScreenerFinancialsStore", FakeStore)
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA", "BBB", "CCC"])

    result = screener_sync.run_sync(
        statement_basis="standalone",
        db_path=tmp_path / "screener_financials.db",
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        refresh_readmodels=False,
        symbols=["bbb", "BBB", "missing"],
    )

    assert result["succeeded"] == 1
    assert fetched == ["BBB"]
    assert saved == ["BBB"]


def test_consolidated_request_stores_detected_standalone_and_is_resumable(monkeypatch, tmp_path: Path) -> None:
    class FakeClient:
        def __init__(self, *, exports_dir, **_kwargs):
            self.exports_dir = Path(exports_dir)

        def excel_path(self, symbol, *, statement_basis):
            suffix = "_consolidated" if statement_basis == "consolidated" else ""
            return self.exports_dir / f"{symbol}{suffix}_screener.xlsx"

        def fetch_company_data(self, symbol, **_kwargs):
            fetched.append(symbol)
            return ScreenerFetchResult(
                data=_company_data("2025-12-31"),
                export_path=self.excel_path(symbol, statement_basis="standalone"),
                requested_basis="consolidated",
                detected_basis="standalone",
            )

    fetched: list[str] = []
    db_path = tmp_path / "screener_financials.db"
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])

    first = screener_sync.run_sync(
        statement_basis="consolidated",
        db_path=db_path,
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        refresh_readmodels=False,
    )
    second = screener_sync.run_sync(
        statement_basis="consolidated",
        db_path=db_path,
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        refresh_readmodels=False,
    )

    assert first["succeeded"] == 1
    assert first["detected_standalone"] == 1
    assert second["total"] == 0
    assert fetched == ["AAA"]
    with sqlite3.connect(db_path) as conn:
        bases = conn.execute("SELECT DISTINCT statement_basis FROM screener_financials").fetchall()
        audit = conn.execute(
            "SELECT requested_basis, detected_basis, status FROM screener_sync_result ORDER BY created_at LIMIT 1"
        ).fetchone()
    assert bases == [("standalone",)]
    assert audit == ("consolidated", "standalone", "succeeded")


def test_export_only_symbol_discovery_strips_consolidated_suffix(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    exports.mkdir()
    (exports / "RELIANCE_screener.xlsx").touch()
    (exports / "RELIANCE_consolidated_screener.xlsx").touch()

    assert screener_sync._load_symbols(tmp_path / "missing-master.db", exports_dir=exports) == ["RELIANCE"]


def _company_data(report_date: str) -> dict:
    return {
        "metadata": {"face_value": 10, "market_cap_cr": 1200},
        "profit_loss": {
            "Sales": {"2025-03-31": 1000},
            "Operating profit": {"2025-03-31": 200},
            "Net profit": {"2025-03-31": 100},
        },
        "quarters": {
            "Sales": {report_date: 300},
            "Operating profit": {report_date: 80},
            "Net profit": {report_date: 50},
        },
        "balance_sheet": {
            "Equity Share Capital": {"2025-03-31": 100},
            "Reserves": {"2025-03-31": 900},
            "Borrowings": {"2025-03-31": 100},
            "Cash & Bank": {"2025-03-31": 50},
        },
        "cash_flow": {"Cash from Operating Activity": {"2025-03-31": 180}},
        "derived": {"Adjusted Equity Shares in Cr": {"2025-03-31": 10}},
    }


def _fetch_result(tmp_path: Path, symbol: str, data: dict, *, basis: str = "standalone") -> ScreenerFetchResult:
    return ScreenerFetchResult(
        data=data,
        export_path=tmp_path / f"{symbol}_screener.xlsx",
        requested_basis=basis,
        detected_basis=basis,
    )
