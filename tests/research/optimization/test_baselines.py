"""Benchmark buy-hold resolution from _index_catalog and _catalog fallback."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.optimization.baselines import benchmark_buyhold_return
from ai_trading_system.research.optimization.recipe import Benchmark


def _seed_index_catalog(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS _index_catalog (
            index_code VARCHAR NOT NULL, date DATE NOT NULL,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE NOT NULL,
            volume BIGINT, value DOUBLE, provider VARCHAR, ingest_run_id VARCHAR,
            validated_at TIMESTAMP, PRIMARY KEY (index_code, date)
        )
        """
    )
    rows = [
        ("UNIV_TOP1000", date(2024, 1, 2), 100.0, 100.0, 100.0, 100.0, 0, None, "derived", None, None),
        ("UNIV_TOP1000", date(2024, 6, 28), 110.0, 110.0, 110.0, 110.0, 0, None, "derived", None, None),
        ("UNIV_TOP1000", date(2024, 12, 30), 120.0, 120.0, 120.0, 120.0, 0, None, "derived", None, None),
    ]
    for row in rows:
        con.execute(
            "INSERT INTO _index_catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            row,
        )
    con.close()


def _seed_catalog(db_path: Path, symbol: str = "ACME") -> None:
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS _catalog (
            symbol_id VARCHAR, security_id VARCHAR, exchange VARCHAR,
            timestamp TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, parquet_file VARCHAR,
            ingestion_version BIGINT, ingestion_ts TIMESTAMP
        )
        """
    )
    for d, close in [(date(2024, 1, 2), 50.0), (date(2024, 12, 30), 75.0)]:
        con.execute(
            "INSERT INTO _catalog VALUES (?, NULL, 'NSE', ?, NULL, NULL, NULL, ?, 1000, NULL, 1, ?)",
            [symbol, d, close, d],
        )
    con.close()


def test_benchmark_buyhold_resolves_from_index_catalog(tmp_path):
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    _seed_index_catalog(paths.ohlcv_db_path)
    r = benchmark_buyhold_return(
        tmp_path,
        benchmark=Benchmark(symbol="UNIV_TOP1000", source="index_catalog"),
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
    )
    assert r is not None
    assert r.start_price == 100.0
    assert r.end_price == 120.0
    assert abs(r.total_return_pct - 20.0) < 1e-9


def test_benchmark_buyhold_falls_back_to_catalog(tmp_path):
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    _seed_catalog(paths.ohlcv_db_path, symbol="ACME")
    r = benchmark_buyhold_return(
        tmp_path,
        benchmark=Benchmark(symbol="ACME", source="catalog"),
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
    )
    assert r is not None
    assert r.start_price == 50.0
    assert r.end_price == 75.0
    assert abs(r.total_return_pct - 50.0) < 1e-9


def test_benchmark_buyhold_returns_none_when_absent(tmp_path):
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    _seed_index_catalog(paths.ohlcv_db_path)
    r = benchmark_buyhold_return(
        tmp_path,
        benchmark=Benchmark(symbol="MISSING", source="index_catalog"),
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
    )
    assert r is None


def test_legacy_symbol_kwarg_still_works(tmp_path):
    """benchmark_buyhold_return(symbol=X) routed to _catalog for back-compat."""
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    _seed_catalog(paths.ohlcv_db_path, symbol="OLD")
    r = benchmark_buyhold_return(
        tmp_path,
        symbol="OLD",
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
    )
    assert r is not None
