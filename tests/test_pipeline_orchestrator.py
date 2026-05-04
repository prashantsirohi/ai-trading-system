from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.domains.ranking.patterns.cache import PatternCacheStore
from ai_trading_system.domains.ingest.delivery import DeliveryCollector
from ai_trading_system.domains.ingest.nse_delivery_scraper import NseHistoricalDeliveryScraper
from ai_trading_system.pipeline.preflight import PreflightChecker
from ai_trading_system.domains.publish.delivery_manager import PublisherDeliveryManager
import ai_trading_system.pipeline.orchestrator as orchestrator_module
from ai_trading_system.pipeline.orchestrator import PipelineOrchestrator
from ai_trading_system.pipeline.stages import FeaturesStage, IngestStage, PublishStage, RankStage
from ai_trading_system.domains.ranking.service import build_integrated_stock_scan_view
from ai_trading_system.pipeline.contracts import DataQualityCriticalError, PublishStageError, StageArtifact, StageContext
from ai_trading_system.platform.db.paths import ensure_domain_layout, get_domain_paths, research_static_end_date


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
    assert [stage["stage_name"] for stage in result["stages"]] == ["ingest", "features", "rank", "events", "execute", "insight", "publish"]
    assert registry.count_rows("pipeline_run") == 1
    assert registry.count_rows("pipeline_stage_run") == 7
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


def test_build_integrated_stock_scan_view_preserves_discoveries_and_best_context() -> None:
    ranked = pd.DataFrame(
        [
            {
                "symbol_id": "RANKED1",
                "exchange": "NSE",
                "composite_score": 95.0,
                "rel_strength_score": 90.0,
                "stage2_score": 88.0,
                "close": 110.0,
            }
        ]
    )
    pattern_df = pd.DataFrame(
        [
            {
                "symbol_id": "DISCOVERED",
                "exchange": "NSE",
                "pattern_family": "cup_handle",
                "pattern_state": "confirmed",
                "pattern_lifecycle_state": "confirmed",
                "pattern_operational_tier": "tier_1",
                "pattern_priority_score": 95.0,
                "pattern_score": 91.0,
                "rel_strength_score": 83.0,
                "stage2_score": 79.0,
            },
            {
                "symbol_id": "DISCOVERED",
                "exchange": "NSE",
                "pattern_family": "flag",
                "pattern_state": "watchlist",
                "pattern_lifecycle_state": "watchlist",
                "pattern_operational_tier": "tier_2",
                "pattern_priority_score": 70.0,
                "pattern_score": 75.0,
                "rel_strength_score": 70.0,
                "stage2_score": 65.0,
            },
            {
                "symbol_id": "SUPPRESS",
                "exchange": "NSE",
                "pattern_family": "head_shoulders",
                "pattern_state": "confirmed",
                "pattern_lifecycle_state": "confirmed",
                "pattern_operational_tier": "suppression_only",
                "pattern_priority_score": 99.0,
            },
            {
                "symbol_id": "EXPIRED",
                "exchange": "NSE",
                "pattern_family": "vcp",
                "pattern_state": "confirmed",
                "pattern_lifecycle_state": "expired",
                "pattern_operational_tier": "tier_1",
                "pattern_priority_score": 98.0,
            },
        ]
    )
    breakout_df = pd.DataFrame(
        [
            {
                "symbol_id": "DISCOVERED",
                "exchange": "NSE",
                "breakout_state": "watchlist",
                "breakout_score": 6.0,
                "rel_strength_score": 81.0,
                "stage2_score": 78.0,
            },
            {
                "symbol_id": "BREAKOUT_ONLY",
                "exchange": "NSE",
                "breakout_state": "qualified",
                "breakout_score": 9.0,
                "rel_strength_score": 85.0,
                "stage2_score": 82.0,
            },
            {
                "symbol_id": "FILTERED",
                "exchange": "NSE",
                "breakout_state": "filtered_by_regime",
                "breakout_score": 10.0,
            },
        ]
    )
    legacy_stock_scan = pd.DataFrame(
        [
            {"Symbol": "RANKED1", "category": "BUY"},
            {"Symbol": "BREAKOUT_ONLY", "category": "WATCH"},
        ]
    )

    merged = build_integrated_stock_scan_view(
        ranked_df=ranked,
        pattern_df=pattern_df,
        breakout_df=breakout_df,
        legacy_stock_scan_df=legacy_stock_scan,
    )

    assert merged["symbol_id"].tolist() == ["RANKED1", "DISCOVERED", "BREAKOUT_ONLY"]
    lookup = {row["symbol_id"]: row for _, row in merged.iterrows()}

    assert int(lookup["RANKED1"]["rank"]) == 1
    assert bool(lookup["RANKED1"]["discovered_by_pattern_scan"]) is False
    assert lookup["RANKED1"]["category"] == "BUY"

    assert pd.isna(lookup["DISCOVERED"]["rank"])
    assert pd.isna(lookup["DISCOVERED"]["composite_score"])
    assert bool(lookup["DISCOVERED"]["pattern_positive"]) is True
    assert bool(lookup["DISCOVERED"]["breakout_positive"]) is True
    assert bool(lookup["DISCOVERED"]["discovered_by_pattern_scan"]) is True
    assert lookup["DISCOVERED"]["pattern_family"] == "cup_handle"
    assert float(lookup["DISCOVERED"]["pattern_priority_score"]) == 95.0

    assert pd.isna(lookup["BREAKOUT_ONLY"]["rank"])
    assert bool(lookup["BREAKOUT_ONLY"]["pattern_positive"]) is False
    assert bool(lookup["BREAKOUT_ONLY"]["breakout_positive"]) is True
    assert bool(lookup["BREAKOUT_ONLY"]["discovered_by_pattern_scan"]) is False
    assert lookup["BREAKOUT_ONLY"]["category"] == "WATCH"

    assert "SUPPRESS" not in set(merged["symbol_id"])
    assert "EXPIRED" not in set(merged["symbol_id"])
    assert "FILTERED" not in set(merged["symbol_id"])


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
                "return_5": 11 - i * 0.2,
                "return_20": 22 - i * 0.4,
                "delivery_pct": 62 - i,
                "volume_zscore_20": 2.5 if i < 5 else 1.1,
                "stage2_label": "strong_stage2" if i < 4 else "stage2",
            }
            for i in range(12)
        ]
    )
    prior_ranked_df = ranked_df.copy()
    prior_ranked_df.loc[:, "composite_score"] = prior_ranked_df["composite_score"] - 5
    prior_ranked_df.loc[0, "composite_score"] = 70.0
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
            "ranked_signals_full": ranked_df,
            "prior_ranked_signals": prior_ranked_df,
            "prior_breakouts_per_run": [
                (
                    "pipeline-2026-03-30-rank",
                    pd.DataFrame(
                        [
                            {
                                "symbol_id": "SYM00",
                                "breakout_detected": True,
                                "prior_range_high": 1010.0,
                                "candidate_tier": "A",
                            }
                        ]
                    ),
                )
            ],
            "breakout_scan": breakout_df,
            "sector_dashboard": sector_df,
            "dashboard_payload": {"summary": {"run_date": "2026-04-06", "top_symbol": "SYM00", "top_sector": "Sector00"}},
        },
    )

    assert "<b>Market Moves Snapshot</b>" in message
    assert "P+V+D: SYM00 11.0% Del 62 VolZ 2.5" in message
    assert "Volume shock: SYM00 VolZ 2.5 Del 62 5d 11.0%" in message
    assert "Rank climber: SYM00 RankΔ +11 ScoreΔ +20.0" in message
    assert "Failed risk: SYM00 -1.0% below A" in message
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

    monkeypatch.setattr("ai_trading_system.domains.ingest.delivery.DeliveryCollector", FakeDeliveryCollector)

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

    monkeypatch.setattr("ai_trading_system.domains.ingest.delivery.DeliveryCollector", FailingDeliveryCollector)

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

    monkeypatch.setattr("ai_trading_system.domains.ingest.providers.nse.NSECollector", FakeNSECollector)

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

    monkeypatch.setattr("ai_trading_system.domains.ingest.providers.nse.NSECollector", FakeNSECollector)

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

    monkeypatch.setattr("ai_trading_system.domains.ingest.providers.nse.NSECollector", FakeNSECollector)

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

    monkeypatch.setattr("ai_trading_system.domains.ingest.providers.nse.NSECollector", EmptyBhavcopyCollector)
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
                        "pattern_lifecycle_state": "confirmed",
                        "pattern_score": 88.0,
                        "pattern_operational_tier": "tier_1",
                        "pattern_priority_score": 93.0,
                        "pattern_priority_rank": 1,
                        "volume_zscore_20": 2.6,
                        "volume_zscore_50": 1.8,
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
                "pattern_scan": [
                    {
                        "symbol_id": "ABC",
                        "pattern_family": "cup_handle",
                        "pattern_operational_tier": "tier_1",
                        "pattern_priority_score": 93.0,
                        "pattern_priority_rank": 1,
                        "volume_zscore_20": 2.6,
                        "volume_zscore_50": 1.8,
                    }
                ],
                "warnings": [],
            },
        }
    )

    result = stage.run(context)

    assert (context.output_dir() / "pattern_scan.csv").exists()
    assert any(artifact.artifact_type == "pattern_scan" for artifact in result.artifacts)
    pattern_scan = pd.read_csv(context.output_dir() / "pattern_scan.csv")
    assert pattern_scan.iloc[0]["pattern_operational_tier"] == "tier_1"
    assert float(pattern_scan.iloc[0]["pattern_priority_score"]) == 93.0
    assert int(pattern_scan.iloc[0]["pattern_priority_rank"]) == 1
    assert float(pattern_scan.iloc[0]["volume_zscore_20"]) == 2.6
    dashboard_payload = (context.output_dir() / "dashboard_payload.json").read_text(encoding="utf-8")
    assert '"pattern_count": 1' in dashboard_payload
    assert '"pattern_family": "cup_handle"' in dashboard_payload
    assert '"pattern_operational_tier": "tier_1"' in dashboard_payload
    assert '"volume_zscore_20": 2.6' in dashboard_payload


def test_rank_stage_incremental_pattern_scan_reuses_cached_inactive_symbols(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path
    paths = ensure_domain_layout(project_root=project_root, data_domain="operational")
    registry = RegistryStore(project_root)

    conn = duckdb.connect(str(paths.ohlcv_db_path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
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
        rows = []
        for i in range(25):
            ts = f"2026-03-{i+1:02d} 15:30:00"
            rows.append(("AAA", "NSE", ts, 100.0, 101.0, 99.0, 100.0, 1000))
            close_bbb = 100.0 if i < 24 else 104.0
            volume_bbb = 1000 if i < 24 else 2500
            rows.append(("BBB", "NSE", ts, close_bbb, close_bbb + 1, close_bbb - 1, close_bbb, volume_bbb))
        conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()

    cache_store = PatternCacheStore(project_root / "data" / "control_plane.duckdb")
    cache_store.write_signals(
        pd.DataFrame(
            [
                {
                    "signal_id": "AAA-flag-confirmed-2026-03-27",
                    "symbol_id": "AAA",
                    "exchange": "NSE",
                    "pattern_family": "flag",
                    "pattern_state": "confirmed",
                    "signal_date": "2026-03-27",
                    "stage2_score": 88.0,
                    "stage2_label": "strong_stage2",
                    "breakout_level": 101.0,
                    "watchlist_trigger_level": 100.5,
                    "invalidation_price": 95.0,
                    "pattern_score": 91.0,
                    "setup_quality": 80.0,
                    "width_bars": 12,
                }
            ]
        ),
        scan_run_id="full:2026-03-27:2",
        replace_date="2026-03-27",
    )

    ranked_signals = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "composite_score": 95.0,
                "rel_strength_score": 92.0,
                "sector_rs_value": 0.84,
                "stage2_score": 88.0,
                "stage2_label": "strong_stage2",
            },
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "composite_score": 90.0,
                "rel_strength_score": 86.0,
                "sector_rs_value": 0.78,
                "stage2_score": 82.0,
                "stage2_label": "stage2",
            },
        ]
    )
    pattern_frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "timestamp": pd.Timestamp("2026-03-27"), "close": 100.0},
            {"symbol_id": "AAA", "exchange": "NSE", "timestamp": pd.Timestamp("2026-03-28"), "close": 100.0},
            {"symbol_id": "BBB", "exchange": "NSE", "timestamp": pd.Timestamp("2026-03-27"), "close": 100.0},
            {"symbol_id": "BBB", "exchange": "NSE", "timestamp": pd.Timestamp("2026-03-28"), "close": 104.0},
        ]
    )

    import ai_trading_system.analytics.data_trust as data_trust_module
    import ai_trading_system.analytics.patterns.data as pattern_data_module
    import ai_trading_system.analytics.patterns.evaluation as pattern_eval_module
    import ai_trading_system.analytics.ranker as ranker_module
    from ai_trading_system.domains.ranking import breakout as breakout_module
    from ai_trading_system.domains.ranking import sector_dashboard as sector_dashboard_module
    from ai_trading_system.domains.ranking import stock_scan as stock_scan_module
    from ai_trading_system.domains.ranking.patterns import universe as pattern_universe_module

    class _FakeRanker:
        def __init__(self, *args, **kwargs):
            pass

        def rank_all(self, **kwargs):
            return ranked_signals.copy()

    monkeypatch.setattr(data_trust_module, "load_data_trust_summary", lambda *args, **kwargs: {"status": "healthy"})
    monkeypatch.setattr(ranker_module, "StockRanker", _FakeRanker)
    monkeypatch.setattr(
        pattern_universe_module,
        "build_pattern_seed_universe",
        lambda **kwargs: (
            ["AAA", "BBB"],
            {
                "seed_source_counts": {"cached": 1, "stage2_structural": 0, "unusual_movers": 1, "liquidity_remaining": 0},
                "broad_universe_count": 2,
                "feature_ready_count": 0,
                "liquidity_pass_count": 0,
                "seed_symbol_count": 2,
                "latest_cached_signal_date": "2026-03-27",
                "pattern_seed_max_symbols": 400,
                "seed_symbols_digest": "seed-digest",
            },
        ),
    )
    monkeypatch.setattr(
        breakout_module,
        "scan_breakouts",
        lambda **kwargs: pd.DataFrame([{"symbol_id": "BBB", "breakout_tag": "fresh_breakout"}]),
    )
    monkeypatch.setattr(pattern_data_module, "load_pattern_frame", lambda *args, **kwargs: pattern_frame.copy())

    def _fake_scan(frame, *, config, progress_callback=None):
        assert set(frame["symbol_id"].astype(str)) == {"AAA", "BBB"}
        return (
            pd.DataFrame(
                [
                    {
                        "signal_id": "BBB-vcp-confirmed-2026-03-28",
                        "symbol_id": "BBB",
                        "exchange": "NSE",
                        "pattern_family": "vcp",
                        "pattern_state": "confirmed",
                        "signal_date": "2026-03-28",
                        "stage2_score": 82.0,
                        "stage2_label": "stage2",
                        "breakout_level": 104.0,
                        "watchlist_trigger_level": 103.5,
                        "invalidation_price": 98.0,
                        "setup_quality": 75.0,
                        "width_bars": 20,
                    }
                ]
            ),
            {},
            {},
        )

    monkeypatch.setattr(pattern_eval_module, "_scan_pattern_signals", _fake_scan)
    monkeypatch.setattr(stock_scan_module, "load_sector_rs", lambda: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8]}))
    monkeypatch.setattr(stock_scan_module, "load_stock_vs_sector", lambda: pd.DataFrame({"Symbol": ["BBB"], "category": ["BUY"]}))
    monkeypatch.setattr(stock_scan_module, "load_sector_mapping", lambda: pd.DataFrame({"Symbol": ["BBB"], "Sector": ["Tech"]}))
    monkeypatch.setattr(stock_scan_module, "scan_stocks", lambda *args, **kwargs: pd.DataFrame({"Symbol": ["BBB"], "category": ["BUY"]}))
    monkeypatch.setattr(sector_dashboard_module, "load_sector_rs", lambda: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8]}))
    monkeypatch.setattr(sector_dashboard_module, "compute_sector_momentum", lambda *args, **kwargs: pd.DataFrame({"Sector": ["Tech"], "Momentum": [0.2]}))
    monkeypatch.setattr(sector_dashboard_module, "build_dashboard", lambda *args, **kwargs: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8], "Momentum": [0.2]}))

    context = StageContext(
        project_root=project_root,
        db_path=paths.ohlcv_db_path,
        run_id="run-pattern-incremental",
        run_date="2026-03-28",
        stage_name="rank",
        attempt_number=1,
        registry=registry,
        params={
            "data_domain": "operational",
            "pattern_scan_enabled": True,
            "pattern_scan_mode": "incremental",
            "pattern_stage2_only": True,
            "pattern_workers": 1,
            "pattern_max_symbols": 2,
        },
    )

    result = RankStage().run(context)

    pattern_artifact = pd.read_csv(context.output_dir() / "pattern_scan.csv")
    cached_today = cache_store.read_cached_signals(signal_date="2026-03-28")
    cache_conn = duckdb.connect(str(project_root / "data" / "control_plane.duckdb"), read_only=True)
    try:
        cache_rows = cache_conn.execute(
            """
            SELECT scan_run_id, symbol_id, signal_date, as_of_date, pattern_lifecycle_state
            FROM pattern_cache
            ORDER BY as_of_date, symbol_id
            """
        ).fetchall()
    finally:
        cache_conn.close()

    assert set(pattern_artifact["symbol_id"].astype(str)) == {"AAA", "BBB"}
    assert "pattern_lifecycle_state" in pattern_artifact.columns
    assert set(cached_today["symbol_id"].astype(str)) == {"AAA", "BBB"}
    assert ("full:2026-03-27:2", "AAA", pd.Timestamp("2026-03-27").date(), pd.Timestamp("2026-03-27").date(), "confirmed") in cache_rows
    latest_rows = [row for row in cache_rows if row[0] == "incremental:2026-03-28:2"]
    assert latest_rows == [
        ("incremental:2026-03-28:2", "AAA", pd.Timestamp("2026-03-27").date(), pd.Timestamp("2026-03-28").date(), "confirmed"),
        ("incremental:2026-03-28:2", "BBB", pd.Timestamp("2026-03-28").date(), pd.Timestamp("2026-03-28").date(), "confirmed"),
    ]
    assert any(artifact.artifact_type == "pattern_scan" for artifact in result.artifacts)


def test_rank_stage_pattern_scan_uses_broad_seed_universe_not_ranked_shortlist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path
    paths = ensure_domain_layout(project_root=project_root, data_domain="operational")
    registry = RegistryStore(project_root)

    ranked_signals = pd.DataFrame(
        [
            {
                "symbol_id": "RANKED1",
                "exchange": "NSE",
                "composite_score": 95.0,
                "rel_strength_score": 90.0,
                "sector_rs_value": 0.82,
                "stage2_score": 88.0,
                "stage2_label": "strong_stage2",
            }
        ]
    )
    requested_symbols: list[str] = []

    import ai_trading_system.analytics.data_trust as data_trust_module
    import ai_trading_system.analytics.patterns.data as pattern_data_module
    import ai_trading_system.analytics.patterns.evaluation as pattern_eval_module
    import ai_trading_system.analytics.ranker as ranker_module
    from ai_trading_system.domains.ranking import breakout as breakout_module
    from ai_trading_system.domains.ranking import sector_dashboard as sector_dashboard_module
    from ai_trading_system.domains.ranking import stock_scan as stock_scan_module
    from ai_trading_system.domains.ranking.patterns import universe as pattern_universe_module

    class _FakeRanker:
        def __init__(self, *args, **kwargs):
            pass

        def rank_all(self, **kwargs):
            return ranked_signals.copy()

    monkeypatch.setattr(data_trust_module, "load_data_trust_summary", lambda *args, **kwargs: {"status": "healthy"})
    monkeypatch.setattr(ranker_module, "StockRanker", _FakeRanker)
    monkeypatch.setattr(
        pattern_universe_module,
        "build_pattern_seed_universe",
        lambda **kwargs: (
            ["UNRANKED", "RANKED1"],
            {
                "seed_source_counts": {"cached": 0, "stage2_structural": 1, "unusual_movers": 1, "liquidity_remaining": 0},
                "broad_universe_count": 2,
                "feature_ready_count": 2,
                "liquidity_pass_count": 2,
                "seed_symbol_count": 2,
                "latest_cached_signal_date": None,
                "pattern_seed_max_symbols": 400,
                "seed_symbols_digest": "seed-digest",
            },
        ),
    )
    monkeypatch.setattr(
        breakout_module,
        "scan_breakouts",
        lambda **kwargs: pd.DataFrame([{"symbol_id": "RANKED1", "breakout_tag": "fresh_breakout"}]),
    )

    def _load_pattern_frame(*args, **kwargs):
        requested_symbols.extend(kwargs.get("symbols") or [])
        assert list(kwargs.get("symbols") or []) == ["UNRANKED", "RANKED1"]
        return pd.DataFrame(
            [
                {"symbol_id": "UNRANKED", "exchange": "NSE", "timestamp": pd.Timestamp("2026-04-22"), "close": 100.0},
                {"symbol_id": "UNRANKED", "exchange": "NSE", "timestamp": pd.Timestamp("2026-04-23"), "close": 104.0},
                {"symbol_id": "RANKED1", "exchange": "NSE", "timestamp": pd.Timestamp("2026-04-22"), "close": 90.0},
                {"symbol_id": "RANKED1", "exchange": "NSE", "timestamp": pd.Timestamp("2026-04-23"), "close": 92.0},
            ]
        )

    monkeypatch.setattr(pattern_data_module, "load_pattern_frame", _load_pattern_frame)

    def _fake_scan(frame, *, config, progress_callback=None):
        assert set(frame["symbol_id"].astype(str)) == {"UNRANKED", "RANKED1"}
        return (
            pd.DataFrame(
                [
                    {
                        "signal_id": "UNRANKED-vcp-confirmed-2026-04-23",
                        "symbol_id": "UNRANKED",
                        "exchange": "NSE",
                        "pattern_family": "vcp",
                        "pattern_state": "confirmed",
                        "signal_date": "2026-04-23",
                        "breakout_level": 104.0,
                        "watchlist_trigger_level": 103.0,
                        "invalidation_price": 99.0,
                        "pattern_score": 82.0,
                        "setup_quality": 76.0,
                        "width_bars": 18,
                    }
                ]
            ),
            {},
            {},
        )

    monkeypatch.setattr(pattern_eval_module, "_scan_pattern_signals", _fake_scan)
    monkeypatch.setattr(stock_scan_module, "load_sector_rs", lambda: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8]}))
    monkeypatch.setattr(stock_scan_module, "load_stock_vs_sector", lambda: pd.DataFrame({"Symbol": ["RANKED1"], "category": ["BUY"]}))
    monkeypatch.setattr(stock_scan_module, "load_sector_mapping", lambda: pd.DataFrame({"Symbol": ["RANKED1"], "Sector": ["Tech"]}))
    monkeypatch.setattr(stock_scan_module, "scan_stocks", lambda *args, **kwargs: pd.DataFrame({"Symbol": ["RANKED1"], "category": ["BUY"]}))
    monkeypatch.setattr(sector_dashboard_module, "load_sector_rs", lambda: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8]}))
    monkeypatch.setattr(sector_dashboard_module, "compute_sector_momentum", lambda *args, **kwargs: pd.DataFrame({"Sector": ["Tech"], "Momentum": [0.2]}))
    monkeypatch.setattr(sector_dashboard_module, "build_dashboard", lambda *args, **kwargs: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8], "Momentum": [0.2]}))

    context = StageContext(
        project_root=project_root,
        db_path=paths.ohlcv_db_path,
        run_id="run-pattern-broad-seed",
        run_date="2026-04-23",
        stage_name="rank",
        attempt_number=1,
        registry=registry,
        params={
            "data_domain": "operational",
            "pattern_scan_enabled": True,
            "pattern_scan_mode": "full",
            "pattern_stage2_only": False,
            "pattern_workers": 1,
            "pattern_max_symbols": 10,
            "pattern_seed_max_symbols": 10,
        },
    )

    result = RankStage().run(context)

    pattern_artifact = pd.read_csv(context.output_dir() / "pattern_scan.csv")
    assert requested_symbols == ["UNRANKED", "RANKED1"]
    assert pattern_artifact["symbol_id"].tolist() == ["UNRANKED"]
    assert result.metadata["pattern_seed_metadata"]["fallback_used"] is False


def test_rank_stage_pattern_seed_falls_back_to_ranked_symbols_on_seed_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path
    paths = ensure_domain_layout(project_root=project_root, data_domain="operational")
    registry = RegistryStore(project_root)

    ranked_signals = pd.DataFrame(
        [
            {
                "symbol_id": "RANKED1",
                "exchange": "NSE",
                "composite_score": 95.0,
                "rel_strength_score": 90.0,
                "sector_rs_value": 0.82,
                "stage2_score": 88.0,
                "stage2_label": "strong_stage2",
            },
            {
                "symbol_id": "RANKED2",
                "exchange": "NSE",
                "composite_score": 92.0,
                "rel_strength_score": 84.0,
                "sector_rs_value": 0.75,
                "stage2_score": 80.0,
                "stage2_label": "stage2",
            },
        ]
    )
    requested_symbols: list[str] = []

    import ai_trading_system.analytics.data_trust as data_trust_module
    import ai_trading_system.analytics.patterns.data as pattern_data_module
    import ai_trading_system.analytics.patterns.evaluation as pattern_eval_module
    import ai_trading_system.analytics.ranker as ranker_module
    from ai_trading_system.domains.ranking import breakout as breakout_module
    from ai_trading_system.domains.ranking import sector_dashboard as sector_dashboard_module
    from ai_trading_system.domains.ranking import stock_scan as stock_scan_module
    from ai_trading_system.domains.ranking.patterns import universe as pattern_universe_module

    class _FakeRanker:
        def __init__(self, *args, **kwargs):
            pass

        def rank_all(self, **kwargs):
            return ranked_signals.copy()

    monkeypatch.setattr(data_trust_module, "load_data_trust_summary", lambda *args, **kwargs: {"status": "healthy"})
    monkeypatch.setattr(ranker_module, "StockRanker", _FakeRanker)
    monkeypatch.setattr(
        pattern_universe_module,
        "build_pattern_seed_universe",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("seed boom")),
    )
    monkeypatch.setattr(
        breakout_module,
        "scan_breakouts",
        lambda **kwargs: pd.DataFrame([{"symbol_id": "RANKED2", "breakout_tag": "fresh_breakout"}]),
    )

    def _load_pattern_frame(*args, **kwargs):
        requested_symbols.extend(kwargs.get("symbols") or [])
        assert list(kwargs.get("symbols") or []) == ["RANKED1", "RANKED2"]
        return pd.DataFrame(
            [
                {"symbol_id": "RANKED1", "exchange": "NSE", "timestamp": pd.Timestamp("2026-04-22"), "close": 100.0},
                {"symbol_id": "RANKED1", "exchange": "NSE", "timestamp": pd.Timestamp("2026-04-23"), "close": 101.0},
                {"symbol_id": "RANKED2", "exchange": "NSE", "timestamp": pd.Timestamp("2026-04-22"), "close": 90.0},
                {"symbol_id": "RANKED2", "exchange": "NSE", "timestamp": pd.Timestamp("2026-04-23"), "close": 95.0},
            ]
        )

    monkeypatch.setattr(pattern_data_module, "load_pattern_frame", _load_pattern_frame)

    def _fake_scan(frame, *, config, progress_callback=None):
        assert set(frame["symbol_id"].astype(str)) == {"RANKED1", "RANKED2"}
        return (
            pd.DataFrame(
                [
                    {
                        "signal_id": "RANKED2-vcp-confirmed-2026-04-23",
                        "symbol_id": "RANKED2",
                        "exchange": "NSE",
                        "pattern_family": "vcp",
                        "pattern_state": "confirmed",
                        "signal_date": "2026-04-23",
                        "breakout_level": 95.0,
                        "watchlist_trigger_level": 94.0,
                        "invalidation_price": 89.0,
                        "pattern_score": 80.0,
                        "setup_quality": 73.0,
                        "width_bars": 16,
                    }
                ]
            ),
            {},
            {},
        )

    monkeypatch.setattr(pattern_eval_module, "_scan_pattern_signals", _fake_scan)
    monkeypatch.setattr(stock_scan_module, "load_sector_rs", lambda: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8]}))
    monkeypatch.setattr(stock_scan_module, "load_stock_vs_sector", lambda: pd.DataFrame({"Symbol": ["RANKED2"], "category": ["BUY"]}))
    monkeypatch.setattr(stock_scan_module, "load_sector_mapping", lambda: pd.DataFrame({"Symbol": ["RANKED2"], "Sector": ["Tech"]}))
    monkeypatch.setattr(stock_scan_module, "scan_stocks", lambda *args, **kwargs: pd.DataFrame({"Symbol": ["RANKED2"], "category": ["BUY"]}))
    monkeypatch.setattr(sector_dashboard_module, "load_sector_rs", lambda: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8]}))
    monkeypatch.setattr(sector_dashboard_module, "compute_sector_momentum", lambda *args, **kwargs: pd.DataFrame({"Sector": ["Tech"], "Momentum": [0.2]}))
    monkeypatch.setattr(sector_dashboard_module, "build_dashboard", lambda *args, **kwargs: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8], "Momentum": [0.2]}))

    context = StageContext(
        project_root=project_root,
        db_path=paths.ohlcv_db_path,
        run_id="run-pattern-seed-fallback",
        run_date="2026-04-23",
        stage_name="rank",
        attempt_number=1,
        registry=registry,
        params={
            "data_domain": "operational",
            "pattern_scan_enabled": True,
            "pattern_scan_mode": "full",
            "pattern_stage2_only": False,
            "pattern_workers": 1,
            "pattern_max_symbols": 10,
        },
    )

    result = RankStage().run(context)

    pattern_artifact = pd.read_csv(context.output_dir() / "pattern_scan.csv")
    assert requested_symbols == ["RANKED1", "RANKED2"]
    assert pattern_artifact["symbol_id"].tolist() == ["RANKED2"]
    assert result.metadata["pattern_seed_metadata"]["fallback_used"] is True


def test_rank_stage_pattern_scan_failure_records_actionable_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path
    paths = ensure_domain_layout(project_root=project_root, data_domain="operational")
    registry = RegistryStore(project_root)

    ranked_signals = pd.DataFrame(
        [
            {
                "symbol_id": "RANKED1",
                "exchange": "NSE",
                "composite_score": 95.0,
                "rel_strength_score": 90.0,
                "sector_rs_value": 0.82,
                "stage2_score": 88.0,
                "stage2_label": "strong_stage2",
            }
        ]
    )

    import ai_trading_system.analytics.data_trust as data_trust_module
    import ai_trading_system.analytics.patterns as patterns_module
    import ai_trading_system.analytics.patterns.data as pattern_data_module
    import ai_trading_system.analytics.ranker as ranker_module
    from ai_trading_system.domains.ranking import breakout as breakout_module
    from ai_trading_system.domains.ranking import sector_dashboard as sector_dashboard_module
    from ai_trading_system.domains.ranking import stock_scan as stock_scan_module
    from ai_trading_system.domains.ranking.patterns import universe as pattern_universe_module

    class _FakeRanker:
        def __init__(self, *args, **kwargs):
            pass

        def rank_all(self, **kwargs):
            return ranked_signals.copy()

    monkeypatch.setattr(data_trust_module, "load_data_trust_summary", lambda *args, **kwargs: {"status": "healthy"})
    monkeypatch.setattr(ranker_module, "StockRanker", _FakeRanker)
    monkeypatch.setattr(
        pattern_universe_module,
        "build_pattern_seed_universe",
        lambda **kwargs: (
            ["RANKED1"],
            {
                "seed_source_counts": {"cached": 0, "stage2_structural": 1, "unusual_movers": 0, "liquidity_remaining": 0},
                "broad_universe_count": 1,
                "feature_ready_count": 1,
                "liquidity_pass_count": 1,
                "seed_symbol_count": 1,
                "latest_cached_signal_date": None,
                "pattern_seed_max_symbols": 400,
                "seed_symbols_digest": "seed-digest",
            },
        ),
    )
    monkeypatch.setattr(
        breakout_module,
        "scan_breakouts",
        lambda **kwargs: pd.DataFrame([{"symbol_id": "RANKED1", "breakout_tag": "fresh_breakout"}]),
    )
    monkeypatch.setattr(
        pattern_data_module,
        "load_pattern_frame",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {"symbol_id": "RANKED1", "exchange": "NSE", "timestamp": pd.Timestamp("2026-04-22"), "close": 100.0},
                {"symbol_id": "RANKED1", "exchange": "NSE", "timestamp": pd.Timestamp("2026-04-23"), "close": 101.0},
            ]
        ),
    )
    monkeypatch.setattr(
        patterns_module,
        "build_pattern_signals",
        lambda **kwargs: (_ for _ in ()).throw(PermissionError("pattern cache write denied")),
    )
    monkeypatch.setattr(stock_scan_module, "load_sector_rs", lambda: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8]}))
    monkeypatch.setattr(stock_scan_module, "load_stock_vs_sector", lambda: pd.DataFrame({"Symbol": ["RANKED1"], "category": ["BUY"]}))
    monkeypatch.setattr(stock_scan_module, "load_sector_mapping", lambda: pd.DataFrame({"Symbol": ["RANKED1"], "Sector": ["Tech"]}))
    monkeypatch.setattr(stock_scan_module, "scan_stocks", lambda *args, **kwargs: pd.DataFrame({"Symbol": ["RANKED1"], "category": ["BUY"]}))
    monkeypatch.setattr(sector_dashboard_module, "load_sector_rs", lambda: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8]}))
    monkeypatch.setattr(sector_dashboard_module, "compute_sector_momentum", lambda *args, **kwargs: pd.DataFrame({"Sector": ["Tech"], "Momentum": [0.2]}))
    monkeypatch.setattr(sector_dashboard_module, "build_dashboard", lambda *args, **kwargs: pd.DataFrame({"Sector": ["Tech"], "RS": [0.8], "Momentum": [0.2]}))

    context = StageContext(
        project_root=project_root,
        db_path=paths.ohlcv_db_path,
        run_id="run-pattern-traceback",
        run_date="2026-04-23",
        stage_name="rank",
        attempt_number=1,
        registry=registry,
        params={
            "data_domain": "operational",
            "pattern_scan_enabled": True,
            "pattern_scan_mode": "full",
            "pattern_stage2_only": False,
            "pattern_workers": 1,
            "pattern_max_symbols": 10,
            "pattern_seed_max_symbols": 10,
        },
    )

    RankStage().run(context)

    task_status = json.loads((context.output_dir() / "task_status.json").read_text())
    pattern_status = task_status["pattern_scan"]
    assert pattern_status["status"] == "failed"
    assert pattern_status["error_class"] == "PermissionError"
    assert pattern_status["error_message"] == "pattern cache write denied"
    assert "PermissionError: pattern cache write denied" in pattern_status["error_traceback"]


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

    monkeypatch.setattr("ai_trading_system.pipeline.preflight.socket.getaddrinfo", _dns_fail)

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

    monkeypatch.setattr("ai_trading_system.pipeline.preflight.socket.getaddrinfo", _dns_fail)

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
    assert args.pattern_scan_mode == "incremental"
    assert args.pattern_max_symbols == 150
    assert args.pattern_seed_max_symbols == 400
    assert args.pattern_min_liquidity_score == 0.2
    assert args.pattern_unusual_mover_min_vol20_avg == 100000.0
    assert args.pattern_workers == 4
    assert args.pattern_lookback_days == 260
    assert args.pattern_smoothing_method == "rolling"
    assert args.pattern_watchlist_expiry_bars == 10
    assert args.pattern_confirmed_expiry_bars == 20
    assert args.pattern_invalidated_retention_bars == 5
    assert args.pattern_incremental_ranked_buffer == 50
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
            "ai_trading_system.pipeline.orchestrator",
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


def test_main_auto_repairs_pre_features_quarantine_and_retries(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
                    "ingest_latest_trade_date_quarantine_clear: Active quarantine rows remain after ingest "
                    "(trade_dates=2026-04-30, 2026-05-04, rows=11, symbols=11, latest_trade_date=2026-05-04, "
                    "latest_critical_symbols=11, latest_noncritical_symbols=0, critical_universe=1000, "
                    "critical_ratio=1.10% max_symbols=10, effective_max_symbols=10, max_ratio=1.00%)."
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
            "ai_trading_system.pipeline.orchestrator",
            "--stages",
            "ingest,features,rank",
            "--run-date",
            "2026-05-04",
        ],
    )

    orchestrator_module.main()

    assert len(run_calls) == 2
    assert run_calls[0]["run_id"] == run_calls[1]["run_id"]
    assert repair_calls[0]["data_domain"] == "operational"
    assert "2026-04-30" in repair_calls[0]["error_message"]
    assert "2026-05-04" in repair_calls[0]["error_message"]


def test_main_publish_only_without_run_id_resolves_latest_publishable_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    run_calls: list[dict] = []

    class FakeOrchestrator:
        def __init__(self, project_root: Path) -> None:
            self.project_root = Path(project_root)

        def _build_run_id(self, run_date: str) -> str:
            return f"pipeline-{run_date}-autogen"

        def run_pipeline(self, **kwargs):
            run_calls.append(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "stages": []}

    monkeypatch.setattr(orchestrator_module, "PipelineOrchestrator", FakeOrchestrator)
    monkeypatch.setattr(
        orchestrator_module,
        "_resolve_latest_publishable_run_id",
        lambda *_args, **_kwargs: "pipeline-2026-04-21-retryme",
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "ai_trading_system.pipeline.orchestrator",
            "--stages",
            "publish",
            "--local-publish",
        ],
    )

    orchestrator_module.main()

    assert len(run_calls) == 1
    assert run_calls[0]["run_id"] == "pipeline-2026-04-21-retryme"
    assert run_calls[0]["stage_names"] == ["publish"]


def test_main_publish_only_without_run_id_exits_when_no_publishable_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class FakeOrchestrator:
        def __init__(self, project_root: Path) -> None:
            self.project_root = Path(project_root)

        def _build_run_id(self, run_date: str) -> str:
            return f"pipeline-{run_date}-autogen"

    monkeypatch.setattr(orchestrator_module, "PipelineOrchestrator", FakeOrchestrator)
    monkeypatch.setattr(
        orchestrator_module,
        "_resolve_latest_publishable_run_id",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "ai_trading_system.pipeline.orchestrator",
            "--stages",
            "publish",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        orchestrator_module.main()
    assert exc_info.value.code == 1


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
    monkeypatch.setattr("sys.argv", ["ai_trading_system.pipeline.orchestrator"])

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

    monkeypatch.setattr("ai_trading_system.analytics.data_trust.load_data_trust_summary", lambda *_args, **_kwargs: {"status": "trusted"})
    monkeypatch.setattr("ai_trading_system.analytics.ranker.StockRanker", FakeRanker)
    monkeypatch.setattr(
        "ai_trading_system.domains.ranking.breakout.scan_breakouts",
        lambda **_kwargs: call_counts.__setitem__("breakout_scan", call_counts["breakout_scan"] + 1)
        or pd.DataFrame([{"symbol_id": "AAA", "breakout_state": "qualified"}]),
    )
    monkeypatch.setattr(
        "ai_trading_system.analytics.patterns.data.load_pattern_frame",
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
        "ai_trading_system.analytics.patterns.build_pattern_signals",
        lambda **_kwargs: call_counts.__setitem__("pattern_scan", call_counts["pattern_scan"] + 1)
        or pd.DataFrame([{"symbol_id": "AAA", "pattern_family": "cup_handle", "pattern_state": "confirmed"}]),
    )
    monkeypatch.setattr("ai_trading_system.domains.ranking.stock_scan.load_sector_rs", lambda: pd.DataFrame({"RS": [1.0]}))
    monkeypatch.setattr("ai_trading_system.domains.ranking.stock_scan.load_stock_vs_sector", lambda: pd.DataFrame({"relative_strength": [1.0]}))
    monkeypatch.setattr("ai_trading_system.domains.ranking.stock_scan.load_sector_mapping", lambda: {"AAA": "Tech"})
    monkeypatch.setattr(
        "ai_trading_system.domains.ranking.stock_scan.scan_stocks",
        lambda *_args, **_kwargs: call_counts.__setitem__("stock_scan", call_counts["stock_scan"] + 1)
        or pd.DataFrame([{"Symbol": "AAA", "category": "BUY"}]),
    )
    monkeypatch.setattr("ai_trading_system.domains.ranking.sector_dashboard.load_sector_rs", lambda: pd.DataFrame({"RS": [1.0]}))
    monkeypatch.setattr("ai_trading_system.domains.ranking.sector_dashboard.load_stock_vs_sector", lambda: pd.DataFrame({"relative_strength": [1.0]}))
    monkeypatch.setattr("ai_trading_system.domains.ranking.sector_dashboard.load_sector_mapping", lambda: {"AAA": "Tech"})
    monkeypatch.setattr("ai_trading_system.domains.ranking.sector_dashboard.compute_sector_momentum", lambda *_args, **_kwargs: pd.DataFrame({"Momentum": [0.2]}))
    monkeypatch.setattr(
        "ai_trading_system.domains.ranking.sector_dashboard.build_dashboard",
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
