from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from analytics.registry import RegistryStore
from collectors.delivery_collector import DeliveryCollector
from collectors.nse_delivery_scraper import NseHistoricalDeliveryScraper
from run.preflight import PreflightChecker
from run.publisher import PublisherDeliveryManager
from run.orchestrator import PipelineOrchestrator
from run.stages import FeaturesStage, IngestStage, PublishStage, RankStage
from run.stages.base import PublishStageError, StageArtifact, StageContext
from utils.data_domains import ensure_domain_layout, get_domain_paths, research_static_end_date


def _init_catalog(db_path: Path, rows: list[tuple]) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _catalog (
                symbol_id VARCHAR,
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
            conn.execute("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row)
    finally:
        conn.close()


def test_stage_boundaries_and_registry_rows(tmp_path: Path) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    registry = RegistryStore(project_root)

    def ingest_op(context):
        _init_catalog(
            context.db_path,
            [("ABC", "NSE", f"{context.run_date} 15:30:00", 10.0, 11.0, 9.5, 10.8, 1000)],
        )
        return {"catalog_rows": 1, "symbol_count": 1, "latest_timestamp": f"{context.run_date} 15:30:00"}

    def feature_op(context):
        return {"snapshot_id": 42, "feature_rows": 10, "feature_registry_entries": 1}

    def rank_op(context):
        return {
            "ranked_signals": pd.DataFrame(
                [{"symbol_id": "ABC", "exchange": "NSE", "composite_score": 87.0}]
            ),
            "breakout_scan": pd.DataFrame(
                [{"symbol_id": "ABC", "sector": "Tech", "breakout_tag": "range_breakout_volume_supertrend"}]
            ),
            "stock_scan": pd.DataFrame([{"Symbol": "ABC", "category": "BUY"}]),
            "sector_dashboard": pd.DataFrame([{"Sector": "Tech", "RS": 0.9, "Momentum": 0.2}]),
            "__dashboard_payload__": {
                "summary": {"run_id": context.run_id, "ranked_count": 1, "breakout_count": 1, "top_symbol": "ABC", "top_sector": "Tech"},
                "ranked_signals": [{"symbol_id": "ABC", "composite_score": 87.0}],
                "breakout_scan": [{"symbol_id": "ABC", "breakout_tag": "range_breakout_volume_supertrend"}],
                "stock_scan": [{"Symbol": "ABC", "category": "BUY"}],
                "sector_dashboard": [{"Sector": "Tech", "RS": 0.9, "Momentum": 0.2}],
                "warnings": [],
            },
        }

    def publish_op(context):
        return {"targets": [{"target": "local_summary", "status": "completed"}]}

    orchestrator = PipelineOrchestrator(
        project_root=project_root,
        registry=registry,
        stages={
            "ingest": IngestStage(operation=ingest_op),
            "features": FeaturesStage(operation=feature_op),
            "rank": RankStage(operation=rank_op),
            "publish": PublishStage(operation=publish_op),
        },
    )
    result = orchestrator.run_pipeline(run_date="2026-03-28", params={"preflight": False})

    assert result["status"] == "completed"
    assert [stage["stage_name"] for stage in result["stages"]] == ["ingest", "features", "rank", "publish"]
    assert registry.count_rows("pipeline_run") == 1
    assert registry.count_rows("pipeline_stage_run") == 4
    assert registry.count_rows("pipeline_artifact") >= 4
    assert registry.count_rows("dq_result") >= 8
    conn = duckdb.connect(str(registry.db_path))
    try:
        artifact_row = conn.execute(
            "SELECT uri, content_hash FROM pipeline_artifact WHERE stage_name = 'rank' AND artifact_type = 'ranked_signals'"
        ).fetchone()
        breakout_row = conn.execute(
            "SELECT uri FROM pipeline_artifact WHERE stage_name = 'rank' AND artifact_type = 'breakout_scan'"
        ).fetchone()
        dashboard_payload_row = conn.execute(
            "SELECT uri FROM pipeline_artifact WHERE stage_name = 'rank' AND artifact_type = 'dashboard_payload'"
        ).fetchone()
    finally:
        conn.close()
    assert artifact_row[0].endswith("ranked_signals.csv")
    assert artifact_row[1]
    assert breakout_row[0].endswith("breakout_scan.csv")
    assert dashboard_payload_row[0].endswith("dashboard_payload.json")


def test_dq_critical_failure_blocks_downstream(tmp_path: Path) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    registry = RegistryStore(project_root)

    def bad_ingest(context):
        _init_catalog(
            context.db_path,
            [("BROKEN", "NSE", f"{context.run_date} 15:30:00", 10.0, 9.0, 11.0, 10.5, 1000)],
        )
        return {"catalog_rows": 1, "symbol_count": 1}

    orchestrator = PipelineOrchestrator(
        project_root=project_root,
        registry=registry,
        stages={
            "ingest": IngestStage(operation=bad_ingest),
            "features": FeaturesStage(operation=lambda context: {"snapshot_id": 1, "feature_rows": 1}),
            "rank": RankStage(operation=lambda context: {"ranked_signals": pd.DataFrame()}),
            "publish": PublishStage(operation=lambda context: {"targets": []}),
        },
    )

    try:
        orchestrator.run_pipeline(run_date="2026-03-28", params={"preflight": False})
        assert False, "Expected critical DQ failure"
    except Exception as exc:
        assert "ingest_ohlc_consistency" in str(exc)
    conn = duckdb.connect(str(registry.db_path))
    try:
        run_id = conn.execute("SELECT run_id FROM pipeline_run").fetchone()[0]
    finally:
        conn.close()
    stage_runs = registry.get_stage_runs(run_id)
    assert [row["stage_name"] for row in stage_runs] == ["ingest"]
    assert stage_runs[0]["status"] == "failed"
    alerts = registry.get_alerts(run_id)
    assert any(alert["alert_type"] == "critical_dq_failure" for alert in alerts)


def test_publish_failure_can_retry_independently(tmp_path: Path) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    registry = RegistryStore(project_root)

    def ingest_op(context):
        _init_catalog(
            context.db_path,
            [("ABC", "NSE", f"{context.run_date} 15:30:00", 10.0, 11.0, 9.5, 10.8, 1000)],
        )
        return {"catalog_rows": 1, "symbol_count": 1, "latest_timestamp": f"{context.run_date} 15:30:00"}

    def feature_op(context):
        return {"snapshot_id": 7, "feature_rows": 3}

    def rank_op(context):
        return {"ranked_signals": pd.DataFrame([{"symbol_id": "ABC", "composite_score": 99.0}])}

    publish_attempts = {"telegram": 0, "sheets": 0}

    def flaky_telegram(context, rank_artifact, datasets):
        publish_attempts["telegram"] += 1
        if publish_attempts["telegram"] <= 2:
            raise RuntimeError("timeout")
        return {"message_id": f"telegram-{context.run_id}"}

    def stable_sheet(context, rank_artifact, datasets):
        publish_attempts["sheets"] += 1
        return {"report_id": f"sheet-{context.run_id}"}

    orchestrator = PipelineOrchestrator(
        project_root=project_root,
        registry=registry,
        stages={
            "ingest": IngestStage(operation=ingest_op),
            "features": FeaturesStage(operation=feature_op),
            "rank": RankStage(operation=rank_op),
            "publish": PublishStage(
                channel_handlers={
                    "telegram_summary": flaky_telegram,
                    "google_sheets_stock_scan": stable_sheet,
                },
                delivery_manager=PublisherDeliveryManager(
                    max_attempts=2,
                    base_delay_seconds=0,
                    sleep_fn=lambda seconds: None,
                ),
            ),
        },
    )

    first = orchestrator.run_pipeline(run_date="2026-03-28", params={"preflight": False})
    assert first["status"] == "completed_with_publish_errors"
    run_id = first["run_id"]

    second = orchestrator.run_pipeline(
        run_id=run_id,
        stage_names=["publish"],
        run_date="2026-03-28",
        params={"preflight": False},
    )
    assert second["status"] == "completed"
    stage_runs = registry.get_stage_runs(run_id)
    publish_runs = [row for row in stage_runs if row["stage_name"] == "publish"]
    assert len(publish_runs) == 2
    assert len([row for row in stage_runs if row["stage_name"] == "features"]) == 1
    assert publish_attempts["sheets"] == 1
    assert publish_attempts["telegram"] == 3
    delivery_logs = registry.get_delivery_logs(run_id)
    assert any(log["channel"] == "google_sheets_stock_scan" and log["status"] == "delivered" for log in delivery_logs)
    assert any(log["channel"] == "google_sheets_stock_scan" and log["status"] == "duplicate" for log in delivery_logs)
    assert any(log["channel"] == "telegram_summary" and log["status"] == "retrying" for log in delivery_logs)
    assert any(log["channel"] == "telegram_summary" and log["status"] == "delivered" for log in delivery_logs)
    alerts = registry.get_alerts(run_id)
    assert any(alert["alert_type"] == "publish_degraded" for alert in alerts)
    run_record = registry.get_run(run_id)
    retry_events = [event for event in run_record["metadata"].get("events", []) if event["event_type"] == "retry_requested"]
    assert retry_events
    assert retry_events[-1]["requested_stages"] == ["publish"]


def test_publish_stage_rejects_unexpected_empty_required_artifact(tmp_path: Path) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    registry = RegistryStore(project_root)
    context = StageContext(
        project_root=project_root,
        db_path=project_root / "data" / "ohlcv.duckdb",
        run_id="run-1",
        run_date="2026-03-28",
        stage_name="publish",
        attempt_number=1,
        registry=registry,
        artifacts={},
    )
    artifact_path = context.output_dir() / "ranked_signals.csv"
    artifact_path.write_text("", encoding="utf-8")
    artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri=str(artifact_path),
        row_count=5,
        content_hash="hash",
    )

    with pytest.raises(PublishStageError):
        PublishStage()._read_artifact(artifact)


def test_preflight_flags_crlf_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path
    (project_root / ".env").write_bytes(b"FOO=bar\r\nBAR=baz\r\n")
    checker = PreflightChecker(project_root)

    result = checker.run(stage_names=["ingest"], params={"smoke": False})

    env_check = next(check for check in result["checks"] if check["name"] == "env_line_endings")
    assert env_check["status"] == "failed"
    assert env_check["severity"] == "high"


def test_ingest_stage_runs_delivery_collection_when_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    _init_catalog(
        tmp_path / "data" / "ohlcv.duckdb",
        [("ABC", "NSE", "2026-03-28 15:30:00", 10.0, 11.0, 9.0, 10.5, 1_000)],
    )
    captured: dict[str, object] = {}

    class FakeDeliveryCollector:
        def __init__(self, **kwargs):
            captured["init"] = kwargs

        def get_last_delivery_date(self):
            return "2026-03-25"

        def fetch_range(self, from_date, to_date, n_workers=4, symbols=None, save_raw=False):
            captured["fetch_args"] = {
                "from_date": from_date,
                "to_date": to_date,
                "n_workers": n_workers,
                "symbols": symbols,
                "save_raw": save_raw,
            }
            return 12

        def compute_delivery_features(self, exchange="NSE"):
            captured["feature_exchange"] = exchange
            return 48

    monkeypatch.setattr("collectors.delivery_collector.DeliveryCollector", FakeDeliveryCollector)

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="run-delivery",
        run_date="2026-03-28",
        stage_name="ingest",
        attempt_number=1,
        params={"include_delivery": True, "delivery_workers": 2},
    )
    stage = IngestStage(operation=lambda _context: {"updated_symbols": ["ABC", "ABC", "XYZ"]})

    result = stage.run(context)

    assert result.metadata["delivery_status"] == "completed"
    assert result.metadata["delivery_from_date"] == "2026-03-26"
    assert result.metadata["delivery_to_date"] == "2026-03-28"
    assert result.metadata["delivery_rows_ingested"] == 12
    assert result.metadata["delivery_feature_rows"] == 48
    assert captured["fetch_args"]["symbols"] == ["ABC", "XYZ"]
    assert captured["fetch_args"]["n_workers"] == 2


def test_ingest_stage_skips_delivery_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    _init_catalog(
        tmp_path / "data" / "ohlcv.duckdb",
        [("ABC", "NSE", "2026-03-28 15:30:00", 10.0, 11.0, 9.0, 10.5, 1_000)],
    )

    class FailingDeliveryCollector:
        def __init__(self, **kwargs):
            raise AssertionError("Delivery collector should not be created when disabled")

    monkeypatch.setattr("collectors.delivery_collector.DeliveryCollector", FailingDeliveryCollector)

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="run-delivery-disabled",
        run_date="2026-03-28",
        stage_name="ingest",
        attempt_number=1,
        params={"include_delivery": False},
    )
    stage = IngestStage(operation=lambda _context: {"updated_symbols": ["ABC"]})

    result = stage.run(context)

    assert result.metadata["delivery_status"] == "skipped"
    assert result.metadata["delivery_reason"] == "disabled"


def test_nse_delivery_scraper_normalizes_equity_rows(tmp_path: Path) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    sqlite3_path = project_root / "data" / "masterdata.db"
    import sqlite3

    conn = sqlite3.connect(sqlite3_path)
    try:
        conn.execute(
            'CREATE TABLE stock_details (Security_id INT, Name TEXT, Symbol TEXT, "Industry Group" TEXT, Industry TEXT, MCAP REAL, Sector TEXT, exchange TEXT)'
        )
        conn.execute("INSERT INTO stock_details VALUES (1, 'ABC', 'ABC', 'G', 'I', 1.0, 'S', 'NSE')")
        conn.commit()
    finally:
        conn.close()

    scraper = NseHistoricalDeliveryScraper(
        masterdb_path=str(sqlite3_path),
        raw_dir=str(project_root / "data" / "raw"),
        data_domain="operational",
    )
    raw = pd.DataFrame(
        [
            {
                "Symbol": "ABC",
                "Series": "EQ",
                "Date": "02-Jan-2025",
                "Total Traded Quantity": "1000",
                "Deliverable Qty": "600",
                "% Dly Qt to Traded Qty": "60.0",
            },
            {
                "Symbol": "ABC",
                "Series": "BE",
                "Date": "02-Jan-2025",
                "Total Traded Quantity": "10",
                "Deliverable Qty": "1",
                "% Dly Qt to Traded Qty": "10.0",
            },
        ]
    )

    normalized = scraper.normalize_frame(raw)

    assert list(normalized.columns) == [
        "symbol_id",
        "exchange",
        "timestamp",
        "delivery_pct",
        "volume",
        "delivery_qty",
    ]
    assert len(normalized) == 1
    assert normalized.iloc[0]["symbol_id"] == "ABC"
    assert normalized.iloc[0]["exchange"] == "NSE"
    assert float(normalized.iloc[0]["delivery_pct"]) == 60.0


def test_delivery_collector_securitywise_backend_writes_duckdb(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    db_path = project_root / "data" / "ohlcv.duckdb"
    masterdb_path = project_root / "data" / "masterdata.db"
    import sqlite3

    conn = sqlite3.connect(masterdb_path)
    try:
        conn.execute(
            'CREATE TABLE stock_details (Security_id INT, Name TEXT, Symbol TEXT, "Industry Group" TEXT, Industry TEXT, MCAP REAL, Sector TEXT, exchange TEXT)'
        )
        conn.execute("INSERT INTO stock_details VALUES (1, 'ABC', 'ABC', 'G', 'I', 1.0, 'S', 'NSE')")
        conn.commit()
    finally:
        conn.close()

    collector = DeliveryCollector(
        ohlcv_db_path=str(db_path),
        feature_store_dir=str(project_root / "data" / "feature_store"),
        masterdb_path=str(masterdb_path),
        data_domain="operational",
        source="nse_securitywise",
    )

    monkeypatch.setattr(
        collector.security_scraper,
        "get_nse_symbols",
        lambda limit=None: ["ABC"],
    )
    monkeypatch.setattr(
        collector.security_scraper,
        "fetch_symbol_history",
        lambda symbol, from_date, to_date, save_raw=False: pd.DataFrame(
            [
                {
                    "symbol_id": symbol,
                    "exchange": "NSE",
                    "timestamp": pd.Timestamp("2025-01-02"),
                    "delivery_pct": 55.0,
                    "volume": 1000,
                    "delivery_qty": 550,
                }
            ]
        ),
    )

    inserted = collector.fetch_range("2025-01-01", "2025-01-31", n_workers=1)

    assert inserted == 1
    conn = duckdb.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT symbol_id, exchange, delivery_pct, volume, delivery_qty FROM _delivery"
        ).fetchone()
    finally:
        conn.close()
    assert row == ("ABC", "NSE", 55.0, 1000, 550)


def test_rank_stage_records_degraded_outputs_in_metadata(tmp_path: Path) -> None:
    project_root = tmp_path
    context = StageContext(
        project_root=project_root,
        db_path=project_root / "data" / "ohlcv.duckdb",
        run_id="run-2",
        run_date="2026-03-28",
        stage_name="rank",
        attempt_number=1,
    )
    stage = RankStage(
        operation=lambda _context: {
            "ranked_signals": pd.DataFrame([{"symbol_id": "ABC", "composite_score": 10.0}]),
            "__stage_metadata__": {
                "degraded_outputs": ["stock_scan unavailable: boom"],
                "degraded_output_count": 1,
            },
        }
    )

    result = stage.run(context)

    assert result.metadata["degraded_output_count"] == 1
    assert result.metadata["degraded_outputs"] == ["stock_scan unavailable: boom"]


def test_data_domain_paths_separate_operational_and_research(tmp_path: Path) -> None:
    operational = get_domain_paths(project_root=tmp_path, data_domain="operational")
    research = get_domain_paths(project_root=tmp_path, data_domain="research")

    assert operational.ohlcv_db_path != research.ohlcv_db_path
    assert research.ohlcv_db_path.name == "research_ohlcv.duckdb"
    assert operational.feature_store_dir != research.feature_store_dir


def test_registry_store_uses_dedicated_control_plane_db(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)

    assert registry.db_path == tmp_path / "data" / "control_plane.duckdb"


def test_stage_context_writes_to_domain_specific_pipeline_runs_dir(tmp_path: Path) -> None:
    ensure_domain_layout(project_root=tmp_path, data_domain="research")
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "research" / "research_ohlcv.duckdb",
        run_id="research-run-1",
        run_date="2026-03-28",
        stage_name="rank",
        attempt_number=1,
        params={"data_domain": "research"},
    )

    output_dir = context.output_dir()

    assert str(output_dir).startswith(str(tmp_path / "data" / "research" / "pipeline_runs"))


def test_research_static_end_date_defaults_to_prior_year() -> None:
    assert research_static_end_date(date(2026, 3, 28)) == "2025-12-31"


def test_model_registry_eval_deploy_and_rollback(tmp_path: Path) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    registry = RegistryStore(project_root)

    model_a = registry.register_model(
        model_name="ranker",
        model_version="1.0.0",
        artifact_uri="models/ranker_v1.pkl",
        feature_schema_hash="hash-a",
        train_snapshot_ref="snapshot-100",
        approval_status="pending",
    )
    registry.record_model_eval(
        model_a,
        {"precision_at_10": 0.61, "sharpe": 1.2},
        dataset_ref="validation-2026-03-28",
    )
    registry.approve_model(model_a)
    first_deployment = registry.deploy_model(model_a, environment="prod", approved_by="ops")

    model_b = registry.register_model(
        model_name="ranker",
        model_version="1.1.0",
        artifact_uri="models/ranker_v1_1.pkl",
        feature_schema_hash="hash-b",
        train_snapshot_ref="snapshot-101",
        approval_status="approved",
    )
    registry.record_model_eval(
        model_b,
        {"precision_at_10": 0.65, "sharpe": 1.35},
        dataset_ref="validation-2026-03-29",
    )
    second_deployment = registry.deploy_model(model_b, environment="prod", approved_by="ops")
    rollback_deployment = registry.rollback_model_deployment("prod", approved_by="ops", notes="regression rollback")

    active = registry.get_active_deployment("prod")
    history = registry.get_deployment_history("prod")
    model_record = registry.get_model_record(model_b)
    evals = registry.get_model_evals(model_b)

    assert first_deployment
    assert second_deployment
    assert rollback_deployment
    assert model_record["approval_status"] == "approved"
    assert len(evals) == 2
    assert active["model_id"] == model_a
    assert len(history) == 3


def test_preflight_checker_detects_missing_live_credentials(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)

    for key in [
        "DHAN_API_KEY",
        "DHAN_CLIENT_ID",
        "DHAN_ACCESS_TOKEN",
        "DHAN_REFRESH_TOKEN",
        "DHAN_TOTP",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "GOOGLE_SPREADSHEET_ID",
        "GOOGLE_SHEETS_CREDENTIALS",
        "GOOGLE_TOKEN_PATH",
    ]:
        monkeypatch.delenv(key, raising=False)

    result = PreflightChecker(project_root).run(["ingest", "publish"], {"local_publish": False})
    assert result["status"] == "failed"
    failing_checks = {check["name"] for check in result["blocking_failures"]}
    assert "dhan_api_key" in failing_checks
    assert "telegram_bot_token" in failing_checks
    assert "google_spreadsheet_id" in failing_checks
