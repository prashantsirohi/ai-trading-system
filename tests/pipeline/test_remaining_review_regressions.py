from pathlib import Path
import json

import duckdb
import pandas as pd
import pytest

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult
from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.pipeline.orchestrator import PipelineOrchestrator
from ai_trading_system.research.perf_tracker.backfill import _latest_attempt_per_date, run_backfill
from ai_trading_system.research.perf_tracker.forward_returns import compute_forward_returns
from ai_trading_system.research.perf_tracker.quality import annotate_return_quality


def _artifact(registry, root, run_id, stage, attempt, name, status="completed"):
    sid = registry.start_stage(run_id, stage, attempt)
    path = root / "data" / "pipeline_runs" / run_id / stage / f"attempt_{attempt}" / f"{name}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("symbol_id,exchange,composite_score,watchlist_bucket\nRELIANCE,NSE,90,QUALITY\n")
    artifact = StageArtifact.from_file(name, path, attempt_number=attempt)
    registry.record_artifact(run_id, stage, attempt, artifact)
    registry.finish_stage(sid, status)
    return artifact


def test_reuse_and_performance_selection_use_promoted_date_bounded_evidence(tmp_path):
    registry = RegistryStore(tmp_path)
    old, future = "pipeline-2026-05-08-old", "pipeline-2026-05-11-future"
    for run_id, day in ((old, "2026-05-08"), (future, "2026-05-11")):
        registry.create_run(run_id, "daily", day)
        _artifact(registry, tmp_path, run_id, "rank", 1, "ranked_signals")
        registry.update_run(run_id, status="completed", finished=True)
    # Same producer attempt includes the full universe; a failed retry must not win.
    path = tmp_path / "universe.csv"
    path.write_text("symbol_id\nRELIANCE\n")
    artifact = StageArtifact.from_file("ranked_universe", path, attempt_number=1)
    registry.record_artifact(old, "rank", 1, artifact)
    with registry._writer() as conn:
        conn.execute("UPDATE pipeline_artifact SET lifecycle_status = 'promoted' WHERE artifact_type = ?", ["ranked_universe"])
    _artifact(registry, tmp_path, old, "rank", 2, "ranked_signals", "failed")
    bucket = _artifact(registry, tmp_path, old, "publish", 3, "watchlist_buckets")
    orchestrator = PipelineOrchestrator(tmp_path, registry=registry)
    result = {}
    orchestrator._attach_latest_rank_artifacts(result, run_id="new", as_of="2026-05-08")
    assert set(result["rank"]) == {"ranked_signals", "ranked_universe"}
    assert result["rank"]["ranked_signals"].attempt_number == 1
    selected = _latest_attempt_per_date(tmp_path / "data" / "pipeline_runs", project_root=tmp_path)
    assert selected["2026-05-08"]["lineage"]["rank"]["attempt"] == 1
    assert selected["2026-05-08"]["buckets"] == Path(bucket.uri)
    Path(result["rank"]["ranked_signals"].uri).write_text("tampered")
    with pytest.raises(ValueError, match="promoted"):
        _latest_attempt_per_date(tmp_path / "data" / "pipeline_runs", project_root=tmp_path)


@pytest.mark.parametrize("force", [False, True])
def test_no_change_ingest_still_produces_current_rank_for_all_consumers(tmp_path, force):
    calls = []
    class Stage:
        def __init__(self, name): self.name = name
        def run(self, context):
            calls.append(self.name)
            if self.name == "ingest":
                return StageResult(metadata={"downstream_skip_eligible": True, "downstream_input_fingerprint": "unchanged"})
            if self.name == "rank":
                path = context.output_dir() / "rank.csv"
                path.write_text("symbol_id\nRELIANCE\n")
                return StageResult(artifacts=[StageArtifact.from_file(name, path, attempt_number=context.attempt_number)
                    for name in ("ranked_signals", "ranked_universe")])
            context.require_artifact("rank", "ranked_universe" if self.name == "fundamental_discovery" else "ranked_signals")
            return StageResult()
    names = ["ingest", "rank", "fundamentals", "fundamental_discovery", "candidates", "publish"]
    runner = PipelineOrchestrator(tmp_path, allow_control_plane_migrations=True, stages={n: Stage(n) for n in names})
    # This is a handoff regression; mathematical DQ is covered by the real stage suites.
    runner.dq_engine.evaluate = lambda context, result: []
    result = runner.run_pipeline(stage_names=names, run_date="2026-05-08",
        params={"force_rerun": force, "preflight": False, "local_publish": True, "enable_fundamentals": True,
                "fundamental_discovery_mode": "shadow"})
    assert result["status"] == "completed"
    assert set(calls) == set(names)


def _price_db(tmp_path, missing=False, adjusted=True):
    path = tmp_path / "prices.duckdb"
    rows = []
    for i, day in enumerate(pd.bdate_range("2026-05-01", periods=7)):
        rows.append(("CALENDAR", "NSE", day, 100., 100.))
        if not (missing and i == 5):
            rows.append(("RELIANCE", "NSE", day, 120. if i == 0 else 100., 100.))
    frame = pd.DataFrame(rows, columns=["symbol_id", "exchange", "timestamp", "close", "adjusted_close"])
    with duckdb.connect(str(path)) as conn:
        conn.register("frame", frame)
        conn.execute("CREATE TABLE _catalog AS SELECT * FROM frame")
        if not adjusted: conn.execute("ALTER TABLE _catalog DROP COLUMN adjusted_close")
    return path


@pytest.mark.parametrize("missing,adjusted", [(False, True), (True, True), (False, False)])
def test_adjusted_returns_do_not_shift_missing_sessions_or_fallback_to_raw(tmp_path, missing, adjusted):
    path = _price_db(tmp_path, missing, adjusted)
    row = pd.DataFrame([{"run_date": "2026-05-01", "symbol_id": "RELIANCE", "exchange": "NSE"}])
    out = annotate_return_quality(compute_forward_returns(row, ohlcv_db_path=path, horizons=(5, 60)))
    if not missing and adjusted:
        assert out.iloc[0].fwd_5d_return == 0
        assert str(out.iloc[0].fwd_5d_matured_at.date()) == "2026-05-08"
        assert out.iloc[0].data_quality_status == "trusted"
    else:
        assert pd.isna(out.iloc[0].fwd_5d_return)
        assert out.iloc[0].data_quality_status == "quarantined"
        assert "missing_adjusted" in out.iloc[0].data_quality_reason
    assert pd.isna(out.iloc[0].fwd_60d_return)


def test_dq_packet_preserves_failed_and_relaxed_results(tmp_path):
    from ai_trading_system.pipeline.stages.insight import _dq_summary
    registry = RegistryStore(tmp_path)
    registry.record_dq_result("run", "ingest", "failed", "critical", "failed", 2, "bad prices")
    registry.record_dq_result("run", "rank", "relaxed", "warning", "passed", 1, "relaxed", relaxed_from="critical")
    context = StageContext(tmp_path, tmp_path / "unused", "run", "2026-05-08", "insight", 1, registry=registry)
    result = _dq_summary(context)
    assert result["status"] == "available"
    rows = {r["rule_id"]: r for r in result["results"]}
    assert rows["failed"]["failed_count"] == 2
    assert rows["relaxed"]["relaxed_from"] == "critical"


def test_double_narrative_failure_is_never_attached_for_publishing(tmp_path, monkeypatch):
    from ai_trading_system.pipeline.stages import narrative
    from ai_trading_system.pipeline.stages.publish import PublishStage
    packet = tmp_path / "packet.json"
    packet.write_text("{}")
    artifacts = {"insight": {n: StageArtifact.from_file(n, packet) for n in ("combined_insight_packet", "analyst_brief")}}
    context = StageContext(tmp_path, tmp_path / "unused", "run", "2026-05-08", "narrative", 1, artifacts=artifacts)
    monkeypatch.setattr(narrative, "build_market_synthesis", lambda *a, **k: ({}, {}))
    monkeypatch.setattr(narrative, "render_market_report_markdown", lambda *a, **k: "REJECTED CONTENT must buy")
    monkeypatch.setattr(narrative, "validate_report", lambda *a, **k: {"status": "failed", "issues": ["rejected"]})
    result = narrative.NarrativeStage().run(context)
    assert result.metadata["validation_status"] == "failed"
    for artifact in result.artifacts:
        if artifact.artifact_type in {"daily_insight_markdown", "telegram_summary", "llm_synthesis"}:
            assert "REJECTED CONTENT" not in Path(artifact.uri).read_text()
    context.artifacts["narrative"] = {a.artifact_type: a for a in result.artifacts}
    data = {}
    PublishStage()._attach_insight_datasets(context, data)
    assert "insight_telegram_summary" not in data
    assert "latest_insight" not in data


@pytest.mark.parametrize("component", ["run_backfill", "build_research_quality_reports", "build_tracker_health", "build_ranking_feedback_summary", "write_json"])
def test_all_performance_stage_failures_are_nonblocking(tmp_path, monkeypatch, component):
    from ai_trading_system.pipeline.stages import perf_tracker as stage
    monkeypatch.setattr(stage, "run_backfill", lambda **k: {})
    monkeypatch.setattr(stage, "build_research_quality_reports", lambda **k: {"summary": {}, "frames": {}})
    monkeypatch.setattr(stage, "build_tracker_health", lambda **k: {"status": "ok"})
    monkeypatch.setattr(stage, "build_ranking_feedback_summary", lambda **k: {})
    context = StageContext(tmp_path, tmp_path / "unused", "run", "2026-05-08", "perf_tracker", 1)
    def fail(*a, **k): raise OSError(component)
    monkeypatch.setattr(context if component == "write_json" else stage, component, fail)
    result = stage.PerfTrackerStage().run(context)
    assert result.metadata["status"] == "failed"
    assert component in result.metadata["error"]


def test_performance_backfill_persists_lineage_policy_and_archives_replaced_rows(tmp_path):
    from ai_trading_system.research.perf_tracker.schema import open_research_db
    registry = RegistryStore(tmp_path)
    run_id = "pipeline-2026-05-01-reviewed"
    registry.create_run(run_id, "daily", "2026-05-01")
    rank = _artifact(registry, tmp_path, run_id, "rank", 1, "ranked_signals")
    relocated = tmp_path / "imported-ranked.csv"
    relocated.write_bytes(Path(rank.uri).read_bytes())
    with registry._writer() as conn:
        conn.execute("UPDATE pipeline_artifact SET uri = ? WHERE run_id = ? AND stage_name = ?",
                     [str(relocated), run_id, "rank"])
    _artifact(registry, tmp_path, run_id, "publish", 2, "watchlist_buckets")
    registry.update_run(run_id, status="completed", finished=True)
    _price_db(tmp_path).rename(tmp_path / "data" / "ohlcv.duckdb")
    assert run_backfill(project_root=tmp_path)["rows_upserted"] == 1
    with open_research_db(project_root=tmp_path) as conn:
        row = conn.execute("SELECT fwd_5d_return, watchlist_bucket, source_lineage_json, return_policy_version, source_run_id FROM rank_cohort_performance_trusted").fetchone()
        assert row[0] == 0
        assert row[1] == "QUALITY"
        assert json.loads(row[2])["publish"]["attempt"] == 2
        assert row[3] == "adjusted_exchange_sessions_v1"
        assert row[4] == run_id
    run_backfill(project_root=tmp_path)
    with open_research_db(project_root=tmp_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rank_cohort_performance").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM rank_cohort_performance_history").fetchone()[0] == 1
        conn.execute("UPDATE rank_cohort_performance SET return_policy_version = NULL")
        assert conn.execute("SELECT COUNT(*) FROM rank_cohort_performance_trusted").fetchone()[0] == 0
