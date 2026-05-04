from __future__ import annotations

from pathlib import Path

import duckdb

from ai_trading_system.pipeline.contracts import StageContext
from ai_trading_system.domains.ingest.service import IngestOrchestrationService
from ai_trading_system.pipeline.stages.ingest import classify_freshness_status


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


def test_ingest_service_passes_context_run_date_to_daily_update_runner(
    tmp_path: Path,
    monkeypatch,
) -> None:
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
    finally:
        conn.close()

    captured: dict[str, object] = {}

    def fake_run(**kwargs):
        captured.update(kwargs)
        return {"target_end_date": kwargs["target_end_date"], "updated_symbols": []}

    monkeypatch.setattr("ai_trading_system.domains.ingest.daily_update_runner.run", fake_run)

    context = StageContext(
        project_root=tmp_path,
        db_path=db_path,
        run_id="run-2",
        run_date="2026-04-08",
        stage_name="ingest",
        attempt_number=1,
        params={"include_delivery": False, "validate_bhavcopy_after_ingest": False},
    )

    service = IngestOrchestrationService()
    payload = service.run_default(context)

    assert captured["target_end_date"] == "2026-04-08"
    assert payload["target_end_date"] == "2026-04-08"
    assert payload["freshness_status"] == "stale"
    assert captured["nse_allow_yfinance_fallback"] is True
    assert payload["nse_allow_yfinance_fallback_effective"] is True
    assert payload["nse_allow_yfinance_fallback_reason"] == "catalog_stale"


def test_ingest_service_respects_explicitly_disabled_yfinance_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
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
    finally:
        conn.close()

    captured: dict[str, object] = {}

    def fake_run(**kwargs):
        captured.update(kwargs)
        return {"target_end_date": kwargs["target_end_date"], "updated_symbols": []}

    monkeypatch.setattr("ai_trading_system.domains.ingest.daily_update_runner.run", fake_run)

    context = StageContext(
        project_root=tmp_path,
        db_path=db_path,
        run_id="run-3",
        run_date="2026-04-08",
        stage_name="ingest",
        attempt_number=1,
        params={
            "include_delivery": False,
            "validate_bhavcopy_after_ingest": False,
            "nse_allow_yfinance_fallback": False,
        },
    )

    service = IngestOrchestrationService()
    payload = service.run_default(context)

    assert captured["nse_allow_yfinance_fallback"] is False
    assert payload["nse_allow_yfinance_fallback_effective"] is False
    assert payload["nse_allow_yfinance_fallback_reason"] == "explicit"


def test_downstream_skip_eligible_requires_fresh_catalog_and_no_unresolved_dates() -> None:
    service = IngestOrchestrationService(operation=lambda _ctx: {})

    assert service.is_downstream_skip_eligible(
        {
            "rows_written": 0,
            "updated_symbols": [],
            "freshness_status": "fresh",
            "unresolved_date_count_all": 0,
        }
    ) is True

    assert service.is_downstream_skip_eligible(
        {
            "rows_written": 0,
            "updated_symbols": [],
            "freshness_status": "delayed",
            "unresolved_date_count_all": 0,
        }
    ) is False

    assert service.is_downstream_skip_eligible(
        {
            "rows_written": 0,
            "updated_symbols": [],
            "freshness_status": "fresh",
            "unresolved_date_count_all": 1,
        }
    ) is False
