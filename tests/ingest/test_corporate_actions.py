from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.domains.ingest import corporate_actions as ca_module
from ai_trading_system.domains.features.repository import ensure_feature_catalog_source
from ai_trading_system.domains.ingest.corporate_actions import (
    build_parser,
    ensure_corporate_action_schema,
    fetch_nse_corporate_actions,
    parse_corporate_action,
    recompute_adjusted_prices,
    run_corporate_action_normalization,
    upsert_corporate_actions,
)
from ai_trading_system.domains.ingest.repository import initialize_ingest_duckdb
from ai_trading_system.domains.ingest.service import IngestOrchestrationService
from ai_trading_system.domains.ingest.symbol_master import SymbolMaster
from ai_trading_system.pipeline.contracts import StageContext, StageResult
from ai_trading_system.pipeline.dq.engine import DataQualityEngine


def _seed_masterdb(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE symbols (
                symbol_id TEXT,
                canonical_symbol TEXT,
                isin TEXT,
                status TEXT,
                exchange TEXT,
                security_id TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO symbols
            (symbol_id, canonical_symbol, isin, status, exchange, security_id)
            VALUES ('AAA', 'AAA', 'INE000A01011', 'active', 'NSE', '1')
            """
        )
        conn.commit()
    finally:
        conn.close()


def _seed_catalog(path: Path) -> None:
    initialize_ingest_duckdb(path)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            INSERT INTO _catalog
            (symbol_id, security_id, exchange, timestamp, open, high, low, close, volume,
             instrument_type, is_benchmark, isin)
            VALUES
            ('AAA', '1', 'NSE', '2026-01-01', 100, 110, 90, 100, 1000, 'equity', false, 'INE000A01011'),
            ('AAA', '1', 'NSE', '2026-01-02', 200, 220, 180, 200, 1000, 'equity', false, 'INE000A01011'),
            ('AAA', '1', 'NSE', '2026-01-03', 400, 440, 360, 400, 1000, 'equity', false, 'INE000A01011'),
            ('NIFTY50', '', 'NSE', '2026-01-01', 1000, 1010, 990, 1005, 0, 'index', true, NULL)
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_parse_bonus_and_split_factors_prefer_isin_mapping() -> None:
    symbol_master = SymbolMaster(
        pd.DataFrame(
            [
                {
                    "symbol": "OLD",
                    "canonical_symbol": "AAA",
                    "isin": "INE000A01011",
                    "status": "active",
                }
            ]
        )
    )
    bonus = parse_corporate_action(
        {
            "symbol": "OLD",
            "isin": "INE000A01011",
            "exDate": "02-Jan-2026",
            "subject": "Bonus 1:1",
        },
        symbol_master=symbol_master,
    )
    split = parse_corporate_action(
        {
            "symbol": "OLD",
            "isin": "INE000A01011",
            "exDate": "03-Jan-2026",
            "subject": "Sub-division from Rs 10 to Re 2",
        },
        symbol_master=symbol_master,
    )

    assert bonus is not None
    assert bonus.symbol == "AAA"
    assert bonus.isin == "INE000A01011"
    assert bonus.price_factor == 0.5
    assert bonus.share_factor == 2.0
    assert split is not None
    assert split.price_factor == 0.2
    assert split.share_factor == 5.0


def test_recompute_adjusted_prices_compounds_from_raw_and_preserves_raw(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _seed_catalog(db_path)
    ensure_corporate_action_schema(db_path)
    actions = [
        parse_corporate_action(
            {
                "symbol": "AAA",
                "isin": "INE000A01011",
                "exDate": "02-Jan-2026",
                "subject": "Bonus 1:1",
            }
        ),
        parse_corporate_action(
            {
                "symbol": "AAA",
                "isin": "INE000A01011",
                "exDate": "03-Jan-2026",
                "subject": "Sub-division from Rs 10 to Re 2",
            }
        ),
    ]
    upsert_corporate_actions(db_path, [action for action in actions if action is not None])

    first = recompute_adjusted_prices(db_path)
    second = recompute_adjusted_prices(db_path)

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT symbol_id, CAST(timestamp AS DATE), close, adjusted_close, adjustment_factor, adjustment_source
            FROM _catalog
            ORDER BY symbol_id, timestamp
            """
        ).fetchall()
    finally:
        conn.close()

    assert first["raw_ohlc_unchanged"] == 1
    assert second["raw_ohlc_unchanged"] == 1
    assert rows == [
        ("AAA", pd.Timestamp("2026-01-01").date(), 100.0, 10.0, 0.1, "nse_corporate_actions"),
        ("AAA", pd.Timestamp("2026-01-02").date(), 200.0, 40.0, 0.2, "nse_corporate_actions"),
        ("AAA", pd.Timestamp("2026-01-03").date(), 400.0, 400.0, 1.0, None),
        ("NIFTY50", pd.Timestamp("2026-01-01").date(), 1005.0, 1005.0, 1.0, None),
    ]


def test_initialize_ingest_duckdb_migrates_catalog_history_adjustment_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    initialize_ingest_duckdb(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("ALTER TABLE _catalog_history DROP COLUMN adjusted_at")
        conn.execute("ALTER TABLE _catalog_history DROP COLUMN adjustment_version")
        conn.commit()
    finally:
        conn.close()

    initialize_ingest_duckdb(db_path)

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        columns = {
            row[0]
            for row in conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '_catalog_history'
                """
            ).fetchall()
        }
    finally:
        conn.close()
    assert {"adjusted_at", "adjustment_version"}.issubset(columns)


def test_recompute_adjusted_prices_falls_back_to_symbol_when_catalog_isin_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _seed_catalog(db_path)
    ensure_corporate_action_schema(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("UPDATE _catalog SET isin = NULL WHERE symbol_id = 'AAA'")
        conn.commit()
    finally:
        conn.close()
    action = parse_corporate_action(
        {
            "symbol": "AAA",
            "isin": "INE000A01011",
            "exDate": "02-Jan-2026",
            "subject": "Bonus 1:1",
        }
    )
    upsert_corporate_actions(db_path, [action] if action is not None else [])

    result = recompute_adjusted_prices(db_path)

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        adjusted = conn.execute(
            """
            SELECT close, adjusted_close, adjustment_factor
            FROM _catalog
            WHERE symbol_id = 'AAA'
              AND CAST(timestamp AS DATE) = DATE '2026-01-01'
            """
        ).fetchone()
    finally:
        conn.close()

    assert result["rows_adjusted"] == 1
    assert adjusted == (100.0, 50.0, 0.5)


def test_run_full_when_no_previous_success_and_dedupes_payload_hash(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    masterdb = tmp_path / "masterdata.db"
    _seed_catalog(db_path)
    _seed_masterdb(masterdb)

    raw = {
        "symbol": "AAA",
        "isin": "INE000A01011",
        "exDate": "02-Jan-2026",
        "subject": "Bonus 1:1",
    }

    def fetcher(**_: object) -> list[dict]:
        return [raw, dict(raw)]

    result = run_corporate_action_normalization(
        ohlcv_db_path=db_path,
        masterdb_path=masterdb,
        run_id="test-run",
        today=pd.Timestamp("2026-01-10").date(),
        fetcher=fetcher,
    )

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        action_count = conn.execute("SELECT COUNT(*) FROM _corporate_actions").fetchone()[0]
        log = conn.execute(
            "SELECT execution_mode, status, actions_fetched, actions_inserted FROM _module_execution_log"
        ).fetchone()
    finally:
        conn.close()

    assert result["status"] == "success"
    assert result["execution_mode"] == "full"
    assert result["actions_fetched"] == 2
    assert result["actions_inserted"] == 1
    assert action_count == 1
    assert log == ("full", "success", 2, 1)


def test_run_reports_clear_progress_steps(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    masterdb = tmp_path / "masterdata.db"
    _seed_catalog(db_path)
    _seed_masterdb(masterdb)
    events: list[dict] = []

    def fetcher(**_: object) -> list[dict]:
        return [
            {
                "symbol": "AAA",
                "isin": "INE000A01011",
                "exDate": "02-Jan-2026",
                "subject": "Bonus 1:1",
            }
        ]

    result = run_corporate_action_normalization(
        ohlcv_db_path=db_path,
        masterdb_path=masterdb,
        run_id="progress-test",
        today=pd.Timestamp("2026-01-10").date(),
        fetcher=fetcher,
        progress_callback=events.append,
    )

    completed_steps = [event.get("step") for event in events if event.get("event") == "step_done"]
    assert result["status"] == "success"
    assert completed_steps == [
        "Preparing schema",
        "Fetching NSE actions 2000-2026",
        "Parsing split/bonus actions",
        "Saving corporate actions",
        "Loading catalog rows",
        "Applying action factors",
        "Writing adjusted prices",
        "Verifying raw OHLC unchanged",
        "Recording execution log",
    ]


def test_run_skips_recent_mode_when_success_already_recorded_today(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    masterdb = tmp_path / "masterdata.db"
    _seed_catalog(db_path)
    _seed_masterdb(masterdb)
    ensure_corporate_action_schema(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO _module_execution_log
            (execution_id, module_name, execution_mode, status, started_at, ended_at, last_success_at)
            VALUES
            ('full-success', 'corporate_action_normalizer', 'full', 'success',
             TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:01', TIMESTAMP '2026-01-01 00:00:01'),
            ('recent-success', 'corporate_action_normalizer', 'recent', 'success',
             TIMESTAMP '2026-01-10 09:00:00', TIMESTAMP '2026-01-10 09:00:01', TIMESTAMP '2026-01-10 09:00:01')
            """
        )
        conn.commit()
    finally:
        conn.close()

    def fetcher(**_: object) -> list[dict]:
        raise AssertionError("recent corporate-action fetch should be skipped after same-day success")

    result = run_corporate_action_normalization(
        ohlcv_db_path=db_path,
        masterdb_path=masterdb,
        run_id="rerun",
        today=pd.Timestamp("2026-01-10").date(),
        fetcher=fetcher,
    )

    assert result["status"] == "success"
    assert result["execution_mode"] == "recent"
    assert result["skipped"] is True
    assert result["skip_reason"] == "recent_success_today"


def test_ingest_service_reports_corporate_action_progress(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    events: list[dict] = []
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="progress-run",
        run_date="2026-01-10",
        stage_name="ingest",
        attempt_number=1,
        params={"data_domain": "operational"},
        task_reporter=events.append,
    )

    def fake_run_corporate_action_normalization(**kwargs: object) -> dict:
        progress = kwargs["progress_callback"]
        progress({"event": "step_start", "step": "Preparing schema", "total": 9})
        progress({"event": "step_done", "step": "Preparing schema"})
        progress({"event": "years_start", "total": 2, "description": "Fetching NSE actions 2025-2026"})
        progress({"event": "year_done", "year": 2025, "fetched": 3})
        progress({"event": "actions_start", "total": 1, "description": "Applying action factors"})
        progress({"event": "action_done", "symbol": "AAA", "rows": 4})
        progress({"event": "step_start", "step": "Writing adjusted prices"})
        progress({"event": "step_done", "step": "Writing adjusted prices", "rows": 4})
        return {"status": "success"}

    monkeypatch.setattr(ca_module, "run_corporate_action_normalization", fake_run_corporate_action_normalization)

    result = IngestOrchestrationService().run_corporate_action_normalization(context)

    details = [str(event.get("detail") or "") for event in events]
    assert result["status"] == "success"
    assert all(event["task_name"] == "corporate_actions" for event in events)
    assert any("Preparing schema" in detail for detail in details)
    assert any("fetched 2025 rows=3 (1/2)" in detail for detail in details)
    assert any("adjusted AAA rows=4 (1/1)" in detail for detail in details)
    assert any(event["metadata"].get("completed_steps") for event in events)


def test_fetch_nse_corporate_actions_reports_year_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        def __init__(self, payload: list[dict] | None = None):
            self.payload = payload or []

        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict]:
            return self.payload

    class FakeSession:
        def __init__(self):
            self.headers = {}

        def get(self, url: str, params: dict | None = None, timeout: float = 20.0) -> FakeResponse:
            if params is None:
                return FakeResponse()
            return FakeResponse([{"symbol": "AAA", "exDate": params["from_date"], "subject": "Bonus 1:1"}])

    monkeypatch.setattr(ca_module.requests, "Session", FakeSession)
    events: list[dict] = []

    rows = fetch_nse_corporate_actions(
        start_date=pd.Timestamp("2025-01-01").date(),
        end_date=pd.Timestamp("2026-01-10").date(),
        progress=events.append,
    )

    assert len(rows) == 2
    assert [event.get("year") for event in events if event.get("event") == "year_done"] == [2025, 2026]
    assert any(event.get("event") == "years_done" and event.get("total_fetched") == 2 for event in events)


def test_recompute_adjusted_prices_reports_action_progress(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _seed_catalog(db_path)
    ensure_corporate_action_schema(db_path)
    action = parse_corporate_action(
        {
            "symbol": "AAA",
            "isin": "INE000A01011",
            "exDate": "02-Jan-2026",
            "subject": "Bonus 1:1",
        }
    )
    upsert_corporate_actions(db_path, [action] if action is not None else [])
    events: list[dict] = []

    recompute_adjusted_prices(db_path, progress=events.append)

    assert any(event.get("event") == "actions_start" and event.get("total") == 1 for event in events)
    assert any(event.get("event") == "action_done" and event.get("symbol") == "AAA" for event in events)
    assert "Writing adjusted prices" in [event.get("step") for event in events if event.get("event") == "step_done"]


def test_cli_progress_flags_parse() -> None:
    parser = build_parser()

    assert parser.parse_args([]).progress is None
    assert parser.parse_args(["--progress"]).progress is True
    assert parser.parse_args(["--no-progress"]).progress is False
    assert parser.parse_args(["--force", "--progress"]).force is True


def test_feature_catalog_source_uses_adjusted_close_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _seed_catalog(db_path)
    ensure_corporate_action_schema(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            UPDATE _catalog
            SET adjusted_close = 12.5
            WHERE symbol_id = 'AAA'
              AND CAST(timestamp AS DATE) = DATE '2026-01-01'
            """
        )
        ensure_feature_catalog_source(conn)
        value = conn.execute(
            """
            SELECT close
            FROM _catalog_feature_source
            WHERE symbol_id = 'AAA'
              AND CAST(timestamp AS DATE) = DATE '2026-01-01'
            """
        ).fetchone()[0]
    finally:
        conn.close()

    assert value == 12.5


def test_dq_raw_ohlc_preservation_uses_normalizer_metadata(tmp_path: Path) -> None:
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "missing.duckdb",
        run_id="run-1",
        run_date="2026-01-10",
        stage_name="ingest",
        attempt_number=1,
    )
    engine = DataQualityEngine(registry=None)  # type: ignore[arg-type]

    passed = engine._rule_ingest_raw_ohlc_unchanged_after_normalization(
        context,
        StageResult(metadata={"corporate_actions": {"status": "success", "raw_ohlc_unchanged": 1}}),
        "high",
    )
    failed = engine._rule_ingest_raw_ohlc_unchanged_after_normalization(
        context,
        StageResult(metadata={"corporate_actions": {"status": "success", "raw_ohlc_unchanged": 0}}),
        "high",
    )

    assert passed.status == "passed"
    assert failed.status == "failed"


def test_dq_large_raw_gap_near_action_requires_adjustment_marker(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _seed_catalog(db_path)
    ensure_corporate_action_schema(db_path)
    action = parse_corporate_action(
        {
            "symbol": "AAA",
            "isin": "INE000A01011",
            "exDate": "02-Jan-2026",
            "subject": "Bonus 1:1",
        }
    )
    split = parse_corporate_action(
        {
            "symbol": "AAA",
            "isin": "INE000A01011",
            "exDate": "03-Jan-2026",
            "subject": "Sub-division from Rs 10 to Re 2",
        }
    )
    upsert_corporate_actions(db_path, [item for item in (action, split) if item is not None])
    context = StageContext(
        project_root=tmp_path,
        db_path=db_path,
        run_id="run-1",
        run_date="2026-01-10",
        stage_name="ingest",
        attempt_number=1,
    )
    engine = DataQualityEngine(registry=None)  # type: ignore[arg-type]

    failed = engine._rule_ingest_corporate_action_explains_large_raw_gap(
        context,
        StageResult(metadata={}),
        "high",
    )
    recompute_adjusted_prices(db_path)
    passed = engine._rule_ingest_corporate_action_explains_large_raw_gap(
        context,
        StageResult(metadata={}),
        "high",
    )

    assert failed.status == "failed"
    assert passed.status == "passed"
