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
import run.orchestrator as orchestrator_module
from run.orchestrator import PipelineOrchestrator
from run.stages import FeaturesStage, IngestStage, PublishStage, RankStage
from run.stages.base import DataQualityCriticalError, PublishStageError, StageArtifact, StageContext
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
    assert [stage["stage_name"] for stage in result["stages"]] == ["ingest", "features", "rank", "execute", "publish"]
    assert registry.count_rows("pipeline_run") == 1
    assert registry.count_rows("pipeline_stage_run") == 5
    assert registry.count_rows("pipeline_artifact") >= 5
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


def test_recent_universe_price_jump_anomaly_blocks_downstream(tmp_path: Path) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    registry = RegistryStore(project_root)

    def bad_ingest(context):
        rows = []
        for idx in range(6):
            symbol = f"S{idx:03d}"
            rows.append((symbol, "NSE", "2026-03-27 15:30:00", 100.0, 101.0, 99.0, 100.0, 1000))
            rows.append((symbol, "NSE", "2026-03-28 15:30:00", 250.0, 251.0, 249.0, 250.0, 1000))
        _init_catalog(context.db_path, rows)
        return {"catalog_rows": len(rows), "symbol_count": 6, "latest_timestamp": "2026-03-28 15:30:00"}

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

    with pytest.raises(Exception) as exc_info:
        orchestrator.run_pipeline(
            run_date="2026-03-28",
            params={
                "preflight": False,
                "dq_jump_min_symbols": 5,
                "dq_jump_pct_gt30_threshold": 20.0,
                "dq_jump_pct_gt50_threshold": 10.0,
                "dq_jump_median_abs_pct_threshold": 15.0,
            },
        )

    assert "ingest_recent_universe_price_jump_anomaly" in str(exc_info.value)
    conn = duckdb.connect(str(registry.db_path))
    try:
        run_id = conn.execute("SELECT run_id FROM pipeline_run").fetchone()[0]
    finally:
        conn.close()
    stage_runs = registry.get_stage_runs(run_id)
    assert [row["stage_name"] for row in stage_runs] == ["ingest"]
    assert stage_runs[0]["status"] == "failed"


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


def test_publish_stage_builds_compact_telegram_tearsheet(tmp_path: Path) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    registry = RegistryStore(project_root)
    context = StageContext(
        project_root=project_root,
        db_path=project_root / "data" / "ohlcv.duckdb",
        run_id="run-1",
        run_date="2026-04-06",
        stage_name="publish",
        attempt_number=1,
        registry=registry,
        artifacts={},
    )

    ranked_df = pd.DataFrame(
        [
            {
                "symbol_id": f"SYM{i:02d}",
                "sector_name": "Banks",
                "composite_score": 90 - i,
                "close": 1000 + i,
                "rel_strength_score": 80 - i / 10,
            }
            for i in range(12)
        ]
    )
    breakout_df = pd.DataFrame(
        [
            {
                "symbol_id": f"BRK{i:02d}",
                "sector": "Tech",
                "setup_family": "range_breakout",
                "breakout_tag": "volume_confirmed",
                "setup_quality": 100 - i,
            }
            for i in range(12)
        ]
    )
    sector_df = pd.DataFrame(
        [
            {
                "Sector": f"Sector{i:02d}",
                "RS_rank": i + 1,
                "RS": 0.60 - i * 0.01,
                "Momentum": 0.10 - i * 0.01,
                "Quadrant": "Leading",
            }
            for i in range(12)
        ]
    )

    message = PublishStage()._build_telegram_tearsheet(
        context,
        {
            "ranked_signals": ranked_df,
            "breakout_scan": breakout_df,
            "sector_dashboard": sector_df,
            "dashboard_payload": {"summary": {"run_date": "2026-04-06", "top_symbol": "SYM00", "top_sector": "Sector00"}},
        },
    )

    assert "<b>Top 10 Sectors</b>" in message
    assert "<b>Top 10 Breakouts</b>" in message
    assert "<b>Top 10 Ranked Stocks</b>" in message
    assert "1. Sector00 | RS 0.60 | Mom +0.10 | Leading" in message
    assert "10. Sector09 | RS 0.51 | Mom +0.01 | Leading" in message
    assert "1. BRK00 | Tech | range_breakout | Tier n/a | Score - | watchlist | volume_confirmed" in message
    assert "10. BRK09 | Tech | range_breakout | Tier n/a | Score - | watchlist | volume_confirmed" in message
    assert "1. SYM00 | Banks | Score 90.0 | Close 1000.00 | RS 80.0" in message
    assert "10. SYM09 | Banks | Score 81.0 | Close 1009.00 | RS 79.1" in message
    assert "SYM10" not in message
    assert "BRK10" not in message
    assert "Sector10" not in message


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


def test_ingest_stage_bhavcopy_validation_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    _init_catalog(
        tmp_path / "data" / "ohlcv.duckdb",
        [
            ("ABC", "NSE", "2026-03-28 15:30:00", 10.0, 11.0, 9.0, 10.5, 1_000),
            ("XYZ", "NSE", "2026-03-28 15:30:00", 20.0, 21.0, 19.0, 20.5, 2_000),
        ],
    )
    captured: dict[str, object] = {}

    class FakeNSECollector:
        def __init__(self, data_dir: str):
            captured["data_dir"] = data_dir

        def get_bhavcopy(self, trade_date: str) -> pd.DataFrame:
            captured["trade_date"] = trade_date
            return pd.DataFrame(
                [
                    {"SYMBOL": "ABC", "SERIES": "EQ", "CLOSE_PRICE": 10.5},
                    {"SYMBOL": "XYZ", "SERIES": "EQ", "CLOSE_PRICE": 20.5},
                ]
            )

    monkeypatch.setattr("collectors.nse_collector.NSECollector", FakeNSECollector)

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="run-bhavcopy-pass",
        run_date="2026-03-28",
        stage_name="ingest",
        attempt_number=1,
        params={
            "include_delivery": False,
            "validate_bhavcopy_after_ingest": True,
            "bhavcopy_min_coverage": 1.0,
            "bhavcopy_max_mismatch_ratio": 0.0,
            "bhavcopy_close_tolerance_pct": 0.001,
        },
    )
    stage = IngestStage(operation=lambda _context: {"updated_symbols": ["ABC", "XYZ"]})

    result = stage.run(context)

    assert captured["trade_date"] == "2026-03-28"
    assert result.metadata["bhavcopy_validation_status"] == "passed"
    assert result.metadata["bhavcopy_validation_compared_rows"] == 2
    assert result.metadata["bhavcopy_validation_mismatch_rows"] == 0


def test_ingest_stage_bhavcopy_validation_blocks_on_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    _init_catalog(
        tmp_path / "data" / "ohlcv.duckdb",
        [
            ("ABC", "NSE", "2026-03-28 15:30:00", 10.0, 11.0, 9.0, 10.5, 1_000),
            ("XYZ", "NSE", "2026-03-28 15:30:00", 20.0, 21.0, 19.0, 20.5, 2_000),
        ],
    )

    class FakeNSECollector:
        def __init__(self, data_dir: str):
            self.data_dir = data_dir

        def get_bhavcopy(self, trade_date: str) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {"SYMBOL": "ABC", "SERIES": "EQ", "CLOSE_PRICE": 10.5},
                    {"SYMBOL": "XYZ", "SERIES": "EQ", "CLOSE_PRICE": 10.0},
                ]
            )

    monkeypatch.setattr("collectors.nse_collector.NSECollector", FakeNSECollector)

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="run-bhavcopy-fail",
        run_date="2026-03-28",
        stage_name="ingest",
        attempt_number=1,
        params={
            "include_delivery": False,
            "validate_bhavcopy_after_ingest": True,
            "bhavcopy_min_coverage": 1.0,
            "bhavcopy_max_mismatch_ratio": 0.1,
            "bhavcopy_close_tolerance_pct": 0.01,
        },
    )
    stage = IngestStage(operation=lambda _context: {"updated_symbols": ["ABC", "XYZ"]})

    with pytest.raises(DataQualityCriticalError, match="Bhavcopy validation gate blocked ingest stage"):
        stage.run(context)


def test_ingest_stage_uses_explicit_bhavcopy_validation_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    _init_catalog(
        tmp_path / "data" / "ohlcv.duckdb",
        [
            ("ABC", "NSE", "2026-03-27 15:30:00", 10.0, 11.0, 9.0, 10.5, 1_000),
        ],
    )
    captured: dict[str, str] = {}

    class FakeNSECollector:
        def __init__(self, data_dir: str):
            self.data_dir = data_dir

        def get_bhavcopy(self, trade_date: str) -> pd.DataFrame:
            captured["trade_date"] = trade_date
            return pd.DataFrame([{"SYMBOL": "ABC", "SERIES": "EQ", "CLOSE_PRICE": 10.5}])

    monkeypatch.setattr("collectors.nse_collector.NSECollector", FakeNSECollector)

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="run-bhavcopy-date",
        run_date="2026-03-28",
        stage_name="ingest",
        attempt_number=1,
        params={
            "include_delivery": False,
            "validate_bhavcopy_after_ingest": True,
            "bhavcopy_validation_date": "2026-03-27",
            "bhavcopy_min_coverage": 1.0,
            "bhavcopy_max_mismatch_ratio": 0.0,
            "bhavcopy_close_tolerance_pct": 0.001,
        },
    )
    stage = IngestStage(operation=lambda _context: {"updated_symbols": ["ABC"]})

    result = stage.run(context)

    assert captured["trade_date"] == "2026-03-27"
    assert result.metadata["bhavcopy_validation_date"] == "2026-03-27"
    assert result.metadata["bhavcopy_validation_status"] == "passed"


def test_ingest_stage_bhavcopy_validation_falls_back_to_yfinance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    _init_catalog(
        tmp_path / "data" / "ohlcv.duckdb",
        [
            ("ABC", "NSE", "2026-03-28 15:30:00", 10.0, 11.0, 9.0, 10.5, 1_000),
        ],
    )

    class EmptyBhavcopyCollector:
        def __init__(self, data_dir: str):
            self.data_dir = data_dir

        def get_bhavcopy(self, trade_date: str) -> pd.DataFrame:
            return pd.DataFrame()

    def fake_download(*args, **kwargs):
        return pd.DataFrame(
            {"Close": [10.5]},
            index=pd.to_datetime(["2026-03-28"]),
        )

    monkeypatch.setattr("collectors.nse_collector.NSECollector", EmptyBhavcopyCollector)
    monkeypatch.setattr("yfinance.download", fake_download)

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="run-bhavcopy-yf-fallback",
        run_date="2026-03-28",
        stage_name="ingest",
        attempt_number=1,
        params={
            "include_delivery": False,
            "validate_bhavcopy_after_ingest": True,
            "bhavcopy_validation_source": "auto",
            "bhavcopy_min_coverage": 1.0,
            "bhavcopy_max_mismatch_ratio": 0.0,
            "bhavcopy_close_tolerance_pct": 0.001,
        },
    )
    stage = IngestStage(operation=lambda _context: {"updated_symbols": ["ABC"]})

    result = stage.run(context)

    assert result.metadata["bhavcopy_validation_status"] == "passed"
    assert str(result.metadata["bhavcopy_validation_source"]).startswith("yfinance:")
    assert result.metadata["bhavcopy_validation_compared_rows"] == 1


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


def test_rank_stage_writes_pattern_scan_artifact_and_dashboard_payload(tmp_path: Path) -> None:
    project_root = tmp_path
    context = StageContext(
        project_root=project_root,
        db_path=project_root / "data" / "ohlcv.duckdb",
        run_id="run-pattern",
        run_date="2026-03-28",
        stage_name="rank",
        attempt_number=1,
    )
    stage = RankStage(
        operation=lambda _context: {
            "ranked_signals": pd.DataFrame([{"symbol_id": "ABC", "composite_score": 10.0}]),
            "pattern_scan": pd.DataFrame(
                [
                    {
                        "signal_id": "ABC-cup_handle-confirmed-2026-03-28",
                        "symbol_id": "ABC",
                        "pattern_family": "cup_handle",
                        "pattern_state": "confirmed",
                        "pattern_score": 88.0,
                    }
                ]
            ),
            "__dashboard_payload__": {
                "summary": {
                    "run_id": "run-pattern",
                    "ranked_count": 1,
                    "pattern_count": 1,
                    "pattern_confirmed_count": 1,
                    "pattern_watchlist_count": 0,
                    "pattern_family_counts": {"cup_handle": 1},
                },
                "ranked_signals": [{"symbol_id": "ABC", "composite_score": 10.0}],
                "pattern_scan": [{"symbol_id": "ABC", "pattern_family": "cup_handle"}],
                "warnings": [],
            },
        }
    )

    result = stage.run(context)

    assert (context.output_dir() / "pattern_scan.csv").exists()
    assert any(artifact.artifact_type == "pattern_scan" for artifact in result.artifacts)
    dashboard_payload = (context.output_dir() / "dashboard_payload.json").read_text(encoding="utf-8")
    assert '"pattern_count": 1' in dashboard_payload
    assert '"pattern_family": "cup_handle"' in dashboard_payload


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


def test_preflight_checker_reports_publish_dns_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    (project_root / "token.json").write_text("{}", encoding="utf-8")

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "dummy")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv("GOOGLE_SPREADSHEET_ID", "sheet-id")
    monkeypatch.delenv("GOOGLE_SHEETS_CREDENTIALS", raising=False)
    monkeypatch.delenv("GOOGLE_TOKEN_PATH", raising=False)

    def _dns_fail(_host, _port):
        raise OSError("dns blocked")

    monkeypatch.setattr("run.preflight.socket.getaddrinfo", _dns_fail)

    result = PreflightChecker(project_root).run(
        ["publish"],
        {"local_publish": False, "preflight_publish_network_checks": True},
    )
    assert result["status"] == "failed"
    failing_checks = {check["name"] for check in result["blocking_failures"]}
    assert "telegram_dns_api" in failing_checks
    assert "google_dns_oauth2" in failing_checks
    assert "google_dns_sheets" in failing_checks


def test_preflight_checker_can_skip_publish_dns_checks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    (project_root / "token.json").write_text("{}", encoding="utf-8")

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "dummy")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv("GOOGLE_SPREADSHEET_ID", "sheet-id")
    monkeypatch.delenv("GOOGLE_SHEETS_CREDENTIALS", raising=False)
    monkeypatch.delenv("GOOGLE_TOKEN_PATH", raising=False)

    def _dns_fail(_host, _port):
        raise OSError("dns blocked")

    monkeypatch.setattr("run.preflight.socket.getaddrinfo", _dns_fail)

    result = PreflightChecker(project_root).run(
        ["publish"],
        {"local_publish": False, "preflight_publish_network_checks": False},
    )
    assert result["status"] == "passed"


def test_orchestrator_parser_defaults_skip_preflight_and_uses_today() -> None:
    args = orchestrator_module.build_parser().parse_args([])

    assert args.run_date == date.today().isoformat()
    assert args.data_domain == "operational"
    assert args.skip_preflight is True
    assert args.auto_repair_quarantine is True
    assert args.terminal_mode == "compact"
    assert args.pattern_scan_enabled is True
    assert args.pattern_max_symbols == 150
    assert args.pattern_workers == 4
    assert args.pattern_lookback_days == 260
    assert args.pattern_smoothing_method == "rolling"
    assert args.stale_missing_symbol_grace_days == 3


def test_main_auto_repairs_quarantine_and_retries(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    run_calls: list[dict] = []
    repair_calls: list[dict] = []

    class FakeOrchestrator:
        def __init__(self, project_root: Path) -> None:
            self.project_root = Path(project_root)

        def _build_run_id(self, run_date: str) -> str:
            return f"pipeline-{run_date}-autotest"

        def run_pipeline(self, **kwargs):
            run_calls.append(kwargs)
            if len(run_calls) == 1:
                raise DataQualityCriticalError(
                    "ingest_unresolved_dates_present: Unresolved trade dates remain quarantined: "
                    "2026-04-08, 2026-04-09, 2026-04-10. unresolved_symbol_dates=9 eligible_symbols=996 "
                    "ratio=0.90% (max_dates=1, max_symbol_dates=10, max_ratio=1.00%)."
                )
            return {"run_id": kwargs["run_id"], "status": "completed", "stages": []}

    def fake_repair(*, project_root: Path, run_id: str, error_message: str, data_domain: str):
        repair_calls.append(
            {
                "project_root": Path(project_root),
                "run_id": run_id,
                "error_message": error_message,
                "data_domain": data_domain,
            }
        )
        return {"status": "completed", "report_dir": str(tmp_path)}

    monkeypatch.setattr(orchestrator_module, "PipelineOrchestrator", FakeOrchestrator)
    monkeypatch.setattr(orchestrator_module, "_run_auto_quarantine_repair", fake_repair)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run.orchestrator",
            "--stages",
            "ingest,features,rank",
        ],
    )

    orchestrator_module.main()

    assert len(run_calls) == 2
    assert run_calls[0]["run_id"] == run_calls[1]["run_id"]
    assert run_calls[0]["params"]["preflight"] is False
    assert repair_calls[0]["data_domain"] == "operational"
    assert "2026-04-08" in repair_calls[0]["error_message"]


def test_main_exits_cleanly_after_final_dq_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeOrchestrator:
        def __init__(self, project_root: Path) -> None:
            self.project_root = Path(project_root)

        def _build_run_id(self, run_date: str) -> str:
            return f"pipeline-{run_date}-blocked"

        def run_pipeline(self, **kwargs):
            raise DataQualityCriticalError("ingest_unresolved_dates_present: still blocked")

    monkeypatch.setattr(orchestrator_module, "PipelineOrchestrator", FakeOrchestrator)
    monkeypatch.setattr(orchestrator_module, "_run_auto_quarantine_repair", lambda **kwargs: None)
    monkeypatch.setattr("sys.argv", ["run.orchestrator"])

    with pytest.raises(SystemExit) as exc_info:
        orchestrator_module.main()

    assert exc_info.value.code == 1


def test_rank_stage_resumes_completed_tasks_on_retry(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    stage = RankStage()
    call_counts = {
        "rank_all": 0,
        "breakout_scan": 0,
        "pattern_scan": 0,
        "stock_scan": 0,
        "sector_dashboard": 0,
        "dashboard_payload": 0,
    }

    class FakeRanker:
        def __init__(self, **_kwargs) -> None:
            pass

        def rank_all(self, **_kwargs) -> pd.DataFrame:
            call_counts["rank_all"] += 1
            return pd.DataFrame([{"symbol_id": "AAA", "exchange": "NSE", "composite_score": 90.0}])

    monkeypatch.setattr("analytics.data_trust.load_data_trust_summary", lambda *_args, **_kwargs: {"status": "trusted"})
    monkeypatch.setattr("analytics.ranker.StockRanker", FakeRanker)
    monkeypatch.setattr(
        "channel.breakout_scan.scan_breakouts",
        lambda **_kwargs: call_counts.__setitem__("breakout_scan", call_counts["breakout_scan"] + 1)
        or pd.DataFrame([{"symbol_id": "AAA", "breakout_state": "qualified"}]),
    )
    monkeypatch.setattr(
        "analytics.patterns.data.load_pattern_frame",
        lambda *_args, **_kwargs: pd.DataFrame(
            {
                "symbol_id": ["AAA"] * 3,
                "timestamp": pd.date_range("2024-01-01", periods=3, freq="B"),
                "open": [10.0, 11.0, 12.0],
                "high": [10.5, 11.5, 12.5],
                "low": [9.5, 10.5, 11.5],
                "close": [10.0, 11.0, 12.0],
                "volume": [1000, 1000, 1000],
            }
        ),
    )
    monkeypatch.setattr(
        "analytics.patterns.build_pattern_signals",
        lambda **_kwargs: call_counts.__setitem__("pattern_scan", call_counts["pattern_scan"] + 1)
        or pd.DataFrame([{"symbol_id": "AAA", "pattern_family": "cup_handle", "pattern_state": "confirmed"}]),
    )
    monkeypatch.setattr("channel.stock_scan.load_sector_rs", lambda: pd.DataFrame({"RS": [1.0]}))
    monkeypatch.setattr("channel.stock_scan.load_stock_vs_sector", lambda: pd.DataFrame({"relative_strength": [1.0]}))
    monkeypatch.setattr("channel.stock_scan.load_sector_mapping", lambda: {"AAA": "Tech"})
    monkeypatch.setattr(
        "channel.stock_scan.scan_stocks",
        lambda *_args, **_kwargs: call_counts.__setitem__("stock_scan", call_counts["stock_scan"] + 1)
        or pd.DataFrame([{"Symbol": "AAA", "category": "BUY"}]),
    )
    monkeypatch.setattr("channel.sector_dashboard.load_sector_rs", lambda: pd.DataFrame({"RS": [1.0]}))
    monkeypatch.setattr("channel.sector_dashboard.load_stock_vs_sector", lambda: pd.DataFrame({"relative_strength": [1.0]}))
    monkeypatch.setattr("channel.sector_dashboard.load_sector_mapping", lambda: {"AAA": "Tech"})
    monkeypatch.setattr("channel.sector_dashboard.compute_sector_momentum", lambda *_args, **_kwargs: pd.DataFrame({"Momentum": [0.2]}))
    monkeypatch.setattr(
        "channel.sector_dashboard.build_dashboard",
        lambda *_args, **_kwargs: call_counts.__setitem__("sector_dashboard", call_counts["sector_dashboard"] + 1)
        or pd.DataFrame([{"Sector": "Tech", "RS": 1.0, "Momentum": 0.2}]),
    )

    original_payload_builder = stage._build_dashboard_payload

    def flaky_payload_builder(*args, **kwargs):
        call_counts["dashboard_payload"] += 1
        if call_counts["dashboard_payload"] == 1:
            raise RuntimeError("payload build failed")
        return original_payload_builder(*args, **kwargs)

    monkeypatch.setattr(stage, "_build_dashboard_payload", flaky_payload_builder)

    context_attempt_1 = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-11-resume",
        run_date="2026-04-11",
        stage_name="rank",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational", "preflight": False},
    )
    with pytest.raises(RuntimeError, match="payload build failed"):
        stage.run(context_attempt_1)

    context_attempt_2 = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-11-resume",
        run_date="2026-04-11",
        stage_name="rank",
        attempt_number=2,
        registry=registry,
        params={"data_domain": "operational", "preflight": False},
    )
    result = stage.run(context_attempt_2)

    assert call_counts["rank_all"] == 1
    assert call_counts["breakout_scan"] == 1
    assert call_counts["pattern_scan"] == 1
    assert call_counts["stock_scan"] == 1
    assert call_counts["sector_dashboard"] == 1
    assert call_counts["dashboard_payload"] == 2
    task_status = result.metadata["task_status"]
    assert task_status["rank_core"]["status"] == "skipped"
    assert int(task_status["rank_core"]["resumed_from_attempt"]) == 1
    assert task_status["breakout_scan"]["status"] == "skipped"
