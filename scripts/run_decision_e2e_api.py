"""Launch a temporary, deterministically seeded decision API for Playwright."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sqlite3
import tempfile

import duckdb
import uvicorn

from ai_trading_system.domains.ranking.decision_history import DecisionHistoryRepository
from ai_trading_system.pipeline.registry import RegistryStore


def _fixture_module():
    path = Path(__file__).parents[1] / "tests/integration/test_decision_read_model_e2e.py"
    spec = importlib.util.spec_from_file_location("decision_e2e_fixture", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load decision E2E fixture from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    fixture = _fixture_module()
    temporary = tempfile.TemporaryDirectory(prefix="decision-api-e2e-")
    data_root = Path(temporary.name)
    os.environ["DATA_ROOT"] = str(data_root)
    os.environ["AI_TRADING_PROJECT_ROOT"] = str(Path(__file__).parents[1])
    os.environ["EXECUTION_API_KEY"] = os.getenv("PLAYWRIGHT_API_KEY", "local-dev-key")

    db_path = data_root / "control_plane.duckdb"
    registry = RegistryStore(Path(__file__).parents[1], db_path=db_path)
    repository = DecisionHistoryRepository(registry)
    context = fixture._context(registry, run_id="browser-e2e", run_date=fixture.RUN_DATE)
    repository.persist_rank_outputs(context, fixture._rank_outputs())
    repository.persist_lifecycle(context, fixture._lifecycle_state(), fixture._transitions())
    fixture._approve_persisted_versions(db_path)

    # Global console chrome requests health/workspace data on every page.
    # Provide the minimal trusted storage shape so those background requests
    # remain successful and the browser test can require zero failed requests.
    with duckdb.connect(str(data_root / "ohlcv.duckdb")) as conn:
        conn.execute("CREATE TABLE _catalog(symbol_id VARCHAR, exchange VARCHAR, timestamp TIMESTAMP, close DOUBLE, volume DOUBLE)")
        conn.execute("CREATE TABLE _delivery(symbol_id VARCHAR, exchange VARCHAR, timestamp TIMESTAMP)")
        conn.execute("INSERT INTO _catalog VALUES ('READY','NSE','2026-07-10',100,100000), ('BLOCK','NSE','2026-07-10',50,80000)")
        conn.execute("INSERT INTO _delivery VALUES ('READY','NSE','2026-07-10'), ('BLOCK','NSE','2026-07-10')")
    with sqlite3.connect(data_root / "masterdata.db") as conn:
        conn.execute("CREATE TABLE symbols(symbol_id TEXT, exchange TEXT)")
        conn.executemany("INSERT INTO symbols VALUES (?, 'NSE')", [("READY",), ("BLOCK",)])

    # Keep the temporary directory alive for the lifetime of uvicorn.
    uvicorn.run(
        "ai_trading_system.ui.execution_api.app:app",
        host="127.0.0.1",
        port=int(os.getenv("PLAYWRIGHT_API_PORT", "8090")),
        log_level="warning",
    )


if __name__ == "__main__":
    main()
