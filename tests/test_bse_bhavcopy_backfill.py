from __future__ import annotations

import io
import sqlite3
import zipfile
from pathlib import Path

import pandas as pd
import duckdb

from ai_trading_system.domains.ingest.bse_bhavcopy_backfill import (
    _load_bse_symbols,
    _normalize_bse_bhavcopy_frame,
    _partition_source_anomalies,
    update_bse_bhavcopy_incremental,
)
from ai_trading_system.domains.ingest.daily_update_runner import (
    _merge_bse_ingest_result,
    _rows_to_symbol_frames,
)
from ai_trading_system.domains.ingest.providers.bse import BSECollector


def _symbols() -> list[dict[str, object]]:
    return [
        {
            "symbol_id": "SPICEJET",
            "security_id": "500285",
            "exchange": "BSE",
            "isin": "INE285B01017",
        }
    ]


def test_bse_collector_orders_urls_by_format_era(tmp_path: Path) -> None:
    collector = BSECollector(data_dir=str(tmp_path))

    legacy = collector._candidate_bhavcopy_urls("2023-09-22")
    current = collector._candidate_bhavcopy_urls("2026-08-07")

    assert legacy[0].endswith("/EQ220923_CSV.ZIP")
    assert current[0].endswith("/BhavCopy_BSE_CM_0_0_0_20260807_F_0000.CSV")
    assert current[1].endswith("/BSE_EQ_BHAVCOPY_07082026_T0.ZIP")


def test_bse_collector_reads_legacy_zip_and_rejects_html(tmp_path: Path) -> None:
    collector = BSECollector(data_dir=str(tmp_path))
    csv_bytes = (
        b"SC_CODE,SC_NAME,SC_GROUP,OPEN,HIGH,LOW,CLOSE,NO_OF_SHRS\n"
        b"500285,SPICEJET,B,50,52,49,51,1000\n"
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("EQ220923.CSV", csv_bytes)

    class Response:
        url = "https://example.test/EQ220923_CSV.ZIP"
        content = buffer.getvalue()

    class HtmlResponse:
        url = "https://example.test/missing.zip"
        content = b"<!DOCTYPE html><html>not a bhavcopy</html>"

    frame = collector._read_bhavcopy_response(Response())  # type: ignore[arg-type]

    assert len(frame) == 1
    assert int(frame.iloc[0]["SC_CODE"]) == 500285
    assert collector._read_bhavcopy_response(HtmlResponse()).empty  # type: ignore[arg-type]


def test_normalize_bse_legacy_frame_uses_security_code_identity() -> None:
    raw = pd.DataFrame(
        [
            {
                "SC_CODE": 500285,
                "SC_NAME": "SPICEJET",
                "SC_GROUP": " B ",
                "OPEN": 50.0,
                "HIGH": 52.0,
                "LOW": 49.0,
                "CLOSE": 51.0,
                "NO_OF_SHRS": 1000,
            },
            {
                "SC_CODE": 500002,
                "SC_NAME": "OUTSIDE",
                "SC_GROUP": "A",
                "OPEN": 10.0,
                "HIGH": 11.0,
                "LOW": 9.0,
                "CLOSE": 10.5,
                "NO_OF_SHRS": 500,
            },
        ]
    )

    output = _normalize_bse_bhavcopy_frame(raw, "2023-09-22", _symbols())

    assert len(output) == 1
    assert output.iloc[0]["symbol_id"] == "SPICEJET"
    assert output.iloc[0]["security_id"] == "500285"
    assert output.iloc[0]["exchange"] == "BSE"
    assert output.iloc[0]["series"] == "B"
    assert int(output.iloc[0]["volume"]) == 1000


def test_normalize_bse_udiff_frame_validates_date() -> None:
    raw = pd.DataFrame(
        [
            {
                "TradDt": "2026-08-07",
                "FinInstrmId": 500285,
                "ISIN": "INE285B01017",
                "SctySrs": "B",
                "OpnPric": 50.0,
                "HghPric": 52.0,
                "LwPric": 49.0,
                "ClsPric": 51.0,
                "TtlTradgVol": 1000,
            }
        ]
    )

    output = _normalize_bse_bhavcopy_frame(raw, "2026-08-07", _symbols())

    assert len(output) == 1
    assert output.iloc[0]["isin"] == "INE285B01017"
    assert output.iloc[0]["timestamp"] == pd.Timestamp("2026-08-07")


def test_load_bse_symbols_is_parameterized_and_exchange_scoped(tmp_path: Path) -> None:
    db_path = tmp_path / "masterdata.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """CREATE TABLE symbols (
                symbol_id TEXT, security_id TEXT, symbol_name TEXT, exchange TEXT,
                instrument_type TEXT, isin TEXT, bse_symbol TEXT
            )"""
        )
        conn.executemany(
            "INSERT INTO symbols VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("SPICEJET", "500285", "SpiceJet", "BSE", "EQ", "INE285B01017", "SPICEJET"),
                ("SPICEJET", "11446", "SpiceJet", "NSE", "EQ", "INE285B01017", None),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    rows = _load_bse_symbols(db_path, ["spicejet"])

    assert len(rows) == 1
    assert rows[0]["exchange"] == "BSE"
    assert rows[0]["security_id"] == "500285"


def test_symbol_frames_preserve_bse_series_lineage() -> None:
    rows = pd.DataFrame(
        [
            {
                "symbol_id": "SPICEJET",
                "security_id": "500285",
                "exchange": "BSE",
                "timestamp": pd.Timestamp("2026-08-07"),
                "open": 50.0,
                "high": 52.0,
                "low": 49.0,
                "close": 51.0,
                "volume": 1000,
                "provider": "bse_bhavcopy",
                "provider_priority": 1,
                "validation_status": "trusted_primary",
                "validated_against": None,
                "ingest_run_id": "run-1",
                "repair_batch_id": "run-1",
                "series": "B",
                "trading_segment": "bse_cash",
            }
        ]
    )

    frames = _rows_to_symbol_frames(rows)

    assert len(frames) == 1
    assert frames[0].iloc[0]["series"] == "B"
    assert frames[0].iloc[0]["trading_segment"] == "bse_cash"


def test_partition_source_anomalies_excludes_inconsistent_official_candle() -> None:
    rows = pd.DataFrame(
        [
            {"symbol_id": "GOOD", "open": 8.0, "high": 8.2, "low": 7.9, "close": 8.1, "volume": 10},
            {"symbol_id": "BAD", "open": 8.0, "high": 8.0, "low": 8.0, "close": 8.15, "volume": 50},
        ]
    )

    valid, anomalies = _partition_source_anomalies(rows)

    assert list(valid["symbol_id"]) == ["GOOD"]
    assert list(anomalies["symbol_id"]) == ["BAD"]


def test_incremental_bse_update_is_noop_when_catalog_is_current(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    master = sqlite3.connect(data_dir / "masterdata.db")
    try:
        master.execute(
            """CREATE TABLE symbols (
                symbol_id TEXT, security_id TEXT, symbol_name TEXT, exchange TEXT,
                instrument_type TEXT, isin TEXT, bse_symbol TEXT
            )"""
        )
        master.execute(
            "INSERT INTO symbols VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("SPICEJET", "500285", "SpiceJet", "BSE", "EQ", "INE285B01017", "SPICEJET"),
        )
        master.commit()
    finally:
        master.close()
    catalog = duckdb.connect(str(data_dir / "ohlcv.duckdb"))
    try:
        catalog.execute(
            """CREATE TABLE _catalog (
                symbol_id VARCHAR, exchange VARCHAR, timestamp TIMESTAMP
            )"""
        )
        catalog.execute("INSERT INTO _catalog VALUES ('SPICEJET', 'BSE', '2026-08-10')")
    finally:
        catalog.close()

    result = update_bse_bhavcopy_incremental(
        project_root=tmp_path,
        target_end_date="2026-08-10",
        run_id="run-1",
    )

    assert result["status"] == "up_to_date"
    assert result["rows_written"] == 0
    assert result["updated_symbols"] == []


def test_merge_bse_result_updates_ingest_contract_and_surfaces_missing_session() -> None:
    nse = {
        "updated_symbols": ["AAA"],
        "symbols_updated": 1,
        "rows_written": 1,
        "providers_used": ["nse_bhavcopy"],
        "nse_bhavcopy_dates": ["2026-08-10"],
        "yfinance_fallback_dates": [],
        "unresolved_dates": [],
        "unresolved_dates_all": [],
        "symbols_errors": 0,
    }
    bse = {
        "status": "updated",
        "rows_written": 2,
        "updated_symbols": ["SPICEJET"],
        "source_sessions": ["2026-08-07"],
        "missing_weekdays": ["2026-08-10"],
    }

    result = _merge_bse_ingest_result(nse, bse)

    assert result["nse_updated_symbols"] == ["AAA"]
    assert result["bse_updated_symbols"] == ["SPICEJET"]
    assert result["updated_symbols"] == ["AAA", "SPICEJET"]
    assert result["rows_written"] == 3
    assert result["providers_used"] == ["nse_bhavcopy", "bse_bhavcopy"]
    assert result["bse_unresolved_dates"] == ["2026-08-10"]
    assert result["unresolved_dates"] == ["2026-08-10"]
