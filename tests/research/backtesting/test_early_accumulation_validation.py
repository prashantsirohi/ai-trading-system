"""Early accumulation validation command tests."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.backtesting import early_accumulation_validation as validation
from ai_trading_system.research.backtesting.early_accumulation_validation import (
    EarlyAccumulationValidationConfig,
    run_validation,
)


CREATE_CATALOG = """
CREATE TABLE _catalog (
    symbol_id VARCHAR,
    security_id VARCHAR,
    exchange VARCHAR,
    timestamp TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    parquet_file VARCHAR,
    ingestion_version BIGINT,
    ingestion_ts TIMESTAMP
)
"""

CREATE_DELIVERY = """
CREATE TABLE _delivery (
    symbol_id VARCHAR,
    exchange VARCHAR,
    timestamp DATE,
    delivery_pct DOUBLE,
    volume BIGINT,
    delivery_qty BIGINT
)
"""


def _insert_series(conn: duckdb.DuckDBPyConnection, symbol: str, *, start: date, days: int, start_close: float, step: float) -> None:
    rows = []
    deliveries = []
    for idx in range(days):
        d = start + timedelta(days=idx)
        close = start_close + step * idx
        rows.append((symbol, None, "NSE", d.isoformat(), close, close * 1.02, close * 0.98, close, 1000 + idx, None, 1, d.isoformat()))
        deliveries.append((symbol, "NSE", d.isoformat(), 55.0 + (idx % 30), 1000 + idx, 500 + idx))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.executemany("INSERT INTO _delivery VALUES (?, ?, ?, ?, ?, ?)", deliveries)


def _fixture_db(tmp_path: Path) -> Path:
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    conn = duckdb.connect(str(paths.ohlcv_db_path))
    conn.execute(CREATE_CATALOG)
    conn.execute(CREATE_DELIVERY)
    start = date(2025, 1, 1)
    _insert_series(conn, "AAA", start=start, days=190, start_close=40.0, step=0.25)
    _insert_series(conn, "BBB", start=start, days=190, start_close=60.0, step=-0.03)
    _insert_series(conn, "NIFTY50", start=start, days=190, start_close=1000.0, step=1.0)
    conn.close()
    return paths.ohlcv_db_path


def test_run_validation_writes_artifacts_and_metrics(tmp_path: Path) -> None:
    _fixture_db(tmp_path)
    out_dir = tmp_path / "validation_out"

    summary = run_validation(
        EarlyAccumulationValidationConfig(
            project_root=tmp_path,
            output_dir=out_dir,
            start_date="2025-05-01",
            end_date="2025-06-30",
            max_snapshots=2,
            min_history_bars=80,
        )
    )

    assert summary["rows"] > 0
    assert "precision_at_25" in summary
    assert (out_dir / "early_accumulation_validation_summary.md").exists()
    assert (out_dir / "early_accumulation_validation_summary.json").exists()
    assert (out_dir / "early_accumulation_decile_returns.csv").exists()
    assert (out_dir / "early_accumulation_examples.csv").exists()
    examples = pd.read_csv(out_dir / "early_accumulation_examples.csv")
    assert {"fwd_return_20d", "fwd_return_60d", "fwd_return_120d", "regime_bucket"}.issubset(examples.columns)


def test_cli_smoke(monkeypatch, capsys) -> None:
    def _fake_run(config):
        assert config.cadence == "monthly"
        return {"artifact_dir": "/tmp/early-validation"}

    monkeypatch.setattr(validation, "run_validation", _fake_run)
    validation.main(["--cadence", "monthly"])
    captured = capsys.readouterr()
    assert "early-validation" in captured.out
