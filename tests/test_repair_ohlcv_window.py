from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

from ai_trading_system.domains.ingest import repair as repair_ohlcv_window
from ai_trading_system.domains.ingest.repair import (
    _backup_current_rows,
    _build_comparison_results,
    _compare_trade_frames,
    _delete_window_rows,
    _fetch_symbol_frames,
    _load_verified_trade_dates,
    _validate_repair_price_continuity,
)
from ai_trading_system.domains.ingest.price_continuity import BulkRawPriceBasisShiftError


def _init_catalog(db_path: Path, rows: list[tuple]) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _catalog (
                symbol_id VARCHAR,
                security_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )
        conn.execute("DELETE FROM _catalog")
        for row in rows:
            conn.execute("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)
    finally:
        conn.close()


def test_compare_trade_frames_detects_only_changed_dates() -> None:
    db_frame = pd.DataFrame(
        [
            {"timestamp": "2026-04-01", "open": 100.0, "high": 110.0, "low": 95.0, "close": 105.0, "volume": 1000},
            {"timestamp": "2026-04-02", "open": 106.0, "high": 111.0, "low": 101.0, "close": 107.0, "volume": 1100},
        ]
    )
    api_frame = pd.DataFrame(
        [
            {"timestamp": "2026-04-01", "open": 100.0, "high": 110.0, "low": 95.0, "close": 105.0, "volume": 1000},
            {"timestamp": "2026-04-02", "open": 206.0, "high": 211.0, "low": 201.0, "close": 207.0, "volume": 2100},
        ]
    )

    result = _compare_trade_frames("ABC", "1", db_frame, api_frame)

    assert result.symbol_id == "ABC"
    assert result.mismatch_dates == ["2026-04-02"]
    assert result.mismatches[0]["fields"]["close"] == {"db": 107.0, "api": 207.0}


def test_backup_current_rows_writes_only_target_symbols(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    _init_catalog(
        db_path,
        [
            ("AAA", "1", "NSE", "2026-04-01 00:00:00", 10.0, 11.0, 9.0, 10.5, 100),
            ("BBB", "2", "NSE", "2026-04-01 00:00:00", 20.0, 21.0, 19.0, 20.5, 200),
        ],
    )

    backup_path = _backup_current_rows(
        db_path=db_path,
        report_dir=report_dir,
        symbol_ids=["AAA"],
        exchange="NSE",
        from_date="2026-04-01",
        to_date="2026-04-01",
    )

    assert backup_path.exists()
    backed_up = pd.read_parquet(backup_path)
    assert list(backed_up["symbol_id"]) == ["AAA"]


def test_build_comparison_results_ignores_symbols_with_no_api_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog(
        db_path,
        [
            ("AAA", "1", "NSE", "2026-04-01 00:00:00", 10.0, 11.0, 9.0, 10.5, 100),
        ],
    )

    results = _build_comparison_results(
        db_path=db_path,
        available_symbols=[{"symbol_id": "AAA", "security_id": "1"}],
        api_frame_map={},
        exchange="NSE",
        from_date="2026-04-01",
        to_date="2026-04-01",
    )

    assert len(results) == 1
    assert results[0].db_rows == 1
    assert results[0].api_rows == 0
    assert results[0].mismatch_dates == []


def test_delete_window_rows_removes_stale_rows_for_target_symbols(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog(
        db_path,
        [
            ("AAA", "1", "NSE", "2026-04-01 00:00:00", 10.0, 11.0, 9.0, 10.5, 100),
            ("AAA", "1", "NSE", "2026-04-02 00:00:00", 11.0, 12.0, 10.0, 11.5, 110),
            ("BBB", "2", "NSE", "2026-04-01 00:00:00", 20.0, 21.0, 19.0, 20.5, 200),
        ],
    )

    deleted = _delete_window_rows(
        db_path=db_path,
        symbol_ids=["AAA"],
        exchange="NSE",
        from_date="2026-04-01",
        to_date="2026-04-02",
    )

    assert deleted == 2

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        remaining = conn.execute(
            "SELECT symbol_id, CAST(timestamp AS DATE) AS trade_date FROM _catalog ORDER BY symbol_id, trade_date"
        ).fetchall()
    finally:
        conn.close()

    assert remaining == [("BBB", pd.Timestamp("2026-04-01").date())]


def test_historical_repair_does_not_use_yfinance_fallback_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        repair_ohlcv_window,
        "get_domain_paths",
        lambda **_: SimpleNamespace(raw_dir=tmp_path / "raw", master_db_path=tmp_path / "master.db"),
    )
    monkeypatch.setattr(repair_ohlcv_window, "_business_dates", lambda *_, **__: ["2022-08-08"])
    monkeypatch.setattr(
        repair_ohlcv_window,
        "_fetch_nse_bhavcopy_rows",
        lambda **_: (pd.DataFrame(), [], ["2022-08-08"]),
    )

    def fail_yfinance(**_: object) -> pd.DataFrame:
        raise AssertionError("Historical repair should not silently use Yahoo candles.")

    monkeypatch.setattr(repair_ohlcv_window, "_fetch_yfinance_rows", fail_yfinance)

    with pytest.raises(RuntimeError, match="Repair stopped before rewriting OHLC rows"):
        _fetch_symbol_frames(
            project_root=tmp_path,
            symbols=[{"symbol_id": "AAA", "security_id": "1", "exchange": "NSE"}],
            from_date="2022-08-08",
            to_date="2022-08-08",
        )


def test_fetch_symbol_frames_uses_verified_trade_dates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        repair_ohlcv_window,
        "get_domain_paths",
        lambda **_: SimpleNamespace(raw_dir=tmp_path / "raw", master_db_path=tmp_path / "master.db"),
    )

    def fail_business_dates(*_: object, **__: object) -> list[str]:
        raise AssertionError("Verified manifests must bypass the incomplete holiday calendar.")

    captured: list[str] = []

    def fetch_rows(**kwargs: object) -> tuple[pd.DataFrame, list[str], list[str]]:
        captured.extend(kwargs["trade_dates"])  # type: ignore[arg-type]
        return pd.DataFrame(), [], []

    monkeypatch.setattr(repair_ohlcv_window, "_business_dates", fail_business_dates)
    monkeypatch.setattr(repair_ohlcv_window, "_fetch_nse_bhavcopy_rows", fetch_rows)

    frames = _fetch_symbol_frames(
        project_root=tmp_path,
        symbols=[{"symbol_id": "AAA", "security_id": "1", "exchange": "NSE"}],
        from_date="2006-01-01",
        to_date="2006-01-31",
        verified_trade_dates=["2006-01-03", "2006-01-02", "2006-01-03"],
    )

    assert frames == []
    assert captured == ["2006-01-02", "2006-01-03"]


def test_load_verified_trade_dates_validates_range(tmp_path: Path) -> None:
    manifest = tmp_path / "dates.txt"
    manifest.write_text("# verified\n2006-01-03\n2006-01-02\n2006-01-03\n", encoding="utf-8")

    assert _load_verified_trade_dates(
        manifest,
        from_date="2006-01-01",
        to_date="2006-01-31",
    ) == ["2006-01-02", "2006-01-03"]

    manifest.write_text("2005-12-30\n", encoding="utf-8")
    with pytest.raises(ValueError, match="outside repair window"):
        _load_verified_trade_dates(
            manifest,
            from_date="2006-01-01",
            to_date="2006-01-31",
        )


def test_repair_window_requires_symbols_for_repair(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="No symbols available for repair window"):
        repair_ohlcv_window.repair_window(
            project_root=tmp_path,
            from_date="2026-03-31",
            to_date="2026-04-06",
            apply_changes=False,
        )


def test_repair_continuity_gate_rejects_broad_boundary_shift_before_write(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    prior_rows = [
        (
            f"SYM{index:02d}",
            str(index),
            "NSE",
            "2026-01-01 00:00:00",
            10.0,
            10.0,
            10.0,
            10.0,
            100,
        )
        for index in range(10)
    ]
    _init_catalog(db_path, prior_rows)
    api_frame_map: dict[str, pd.DataFrame] = {}
    for index in range(10):
        symbol = f"SYM{index:02d}"
        frame = pd.DataFrame(
            [{"timestamp": "2026-01-02", "close": 100.0}]
        ).set_index("timestamp")
        api_frame_map[symbol] = frame

    with pytest.raises(BulkRawPriceBasisShiftError, match=r"2026-01-02 \(10 symbols\)"):
        _validate_repair_price_continuity(
            db_path=db_path,
            api_frame_map=api_frame_map,
            symbol_ids=api_frame_map,
            exchange="NSE",
            from_date="2026-01-02",
            to_date="2026-01-02",
        )

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM _catalog").fetchone() == (10,)
    finally:
        conn.close()
