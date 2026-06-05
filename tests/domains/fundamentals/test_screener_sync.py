from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ai_trading_system.domains.fundamentals import screener_sync
from ai_trading_system.domains.fundamentals.screener_store import ScreenerFinancialsStore
from ai_trading_system.domains.fundamentals.screener_sync import build_parser


def test_screener_sync_defaults_follow_data_root(monkeypatch, tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    monkeypatch.setenv("DATA_ROOT", str(data_root))

    args = build_parser().parse_args([])

    assert Path(args.db_path) == data_root / "fundamentals" / "screener_financials.db"
    assert Path(args.exports_dir) == data_root / "fundamentals" / "exports"
    assert Path(args.master_db_path) == data_root / "masterdata.db"


def test_screener_sync_reports_per_symbol_failures(monkeypatch, tmp_path: Path) -> None:
    class FakeStore:
        def __init__(self, db_path):
            self.db_path = Path(db_path)

        def get_synced_symbols(self):
            return set()

        def begin_batch(self, *_args, **_kwargs):
            return None

        def record_error(self, *_args, **_kwargs):
            return None

        def finish_batch(self, *_args, **_kwargs):
            return None

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, symbol, **_kwargs):
            raise RuntimeError(f"download blocked for {symbol}")

    monkeypatch.setattr(screener_sync, "ScreenerFinancialsStore", FakeStore)
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])
    monkeypatch.setattr(screener_sync, "DEFAULT_RETRY_BACKOFF_SEC", 0.0)
    messages: list[str] = []

    result = screener_sync.run_sync(
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
            return _company_data("2025-12-31")

    calls: list[tuple[str, dict]] = []
    db_path = tmp_path / "screener_financials.db"
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])

    result = screener_sync.run_sync(
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
    assert calls == [("AAA", {"force_download": False, "allow_download": False})]
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
            return _company_data("2025-12-31")

    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])

    result = screener_sync.run_sync(
        db_path=tmp_path / "screener_financials.db",
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        allow_download=True,
        missing_current_results=True,
        as_of_date="2026-01-15",
        refresh_readmodels=False,
    )

    assert result["succeeded"] == 1
    assert calls == [("AAA", {"force_download": True, "allow_download": True})]


def test_missing_current_results_skips_stale_export_without_expected_quarter(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, _symbol, **_kwargs):
            calls.append(_symbol)
            return _company_data("2025-09-30")

    calls: list[str] = []
    db_path = tmp_path / "screener_financials.db"
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])

    result = screener_sync.run_sync(
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
            return _company_data("2025-12-31")

    calls: list[str] = []
    messages: list[str] = []
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])
    monkeypatch.setattr(screener_sync, "DEFAULT_RETRY_BACKOFF_SEC", 0.0)

    result = screener_sync.run_sync(
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


def test_sync_records_failure_after_three_attempts(monkeypatch, tmp_path: Path) -> None:
    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, symbol, **_kwargs):
            calls.append(symbol)
            raise TimeoutError("browser timeout")

    calls: list[str] = []
    db_path = tmp_path / "screener_financials.db"
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA"])
    monkeypatch.setattr(screener_sync, "DEFAULT_RETRY_BACKOFF_SEC", 0.0)

    result = screener_sync.run_sync(
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
        def __init__(self, _db_path):
            pass

        def get_synced_symbols(self):
            return {"AAA"}

        def begin_batch(self, *_args, **_kwargs):
            return None

        def save_company_financials(self, symbol, *_args, **_kwargs):
            saved.append(symbol)

        def finish_batch(self, *_args, **_kwargs):
            return None

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def fetch_company_data(self, symbol, **_kwargs):
            fetched.append(symbol)
            return _company_data("2025-12-31")

    fetched: list[str] = []
    saved: list[str] = []
    monkeypatch.setattr(screener_sync, "ScreenerFinancialsStore", FakeStore)
    monkeypatch.setattr(screener_sync, "ScreenerClient", FakeClient)
    monkeypatch.setattr(screener_sync, "_load_symbols", lambda *_args, **_kwargs: ["AAA", "BBB"])

    result = screener_sync.run_sync(
        db_path=tmp_path / "screener_financials.db",
        master_db_path=tmp_path / "masterdata.db",
        exports_dir=tmp_path / "exports",
        refresh_readmodels=False,
    )

    assert result["succeeded"] == 1
    assert fetched == ["BBB"]
    assert saved == ["BBB"]


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
