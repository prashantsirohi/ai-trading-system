from __future__ import annotations

from pathlib import Path

import duckdb

from core.contracts import StageContext
from ai_trading_system.domains.ingest.service import IngestOrchestrationService
from run.stages.ingest import classify_freshness_status


def test_classify_freshness_status_contract() -> None:
    assert classify_freshness_status("2026-04-07", "2026-04-07") == "fresh"
    assert classify_freshness_status("2026-04-07", "2026-04-06") == "delayed"
    assert classify_freshness_status("2026-04-07", None) == "stale"


def test_ingest_summary_includes_freshness_status(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog (symbol_id, exchange, timestamp)
            VALUES ('ABC', 'NSE', TIMESTAMP '2026-04-06 15:30:00')
            """
        )
    finally:
        conn.close()

    context = StageContext(
        project_root=tmp_path,
        db_path=db_path,
        run_id="run-1",
        run_date="2026-04-07",
        stage_name="ingest",
        attempt_number=1,
        params={},
    )
    service = IngestOrchestrationService(operation=lambda _ctx: {})
    payload = service.run_default(context)
    assert payload["freshness_status"] == "delayed"
