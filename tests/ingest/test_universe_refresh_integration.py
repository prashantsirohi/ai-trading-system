from datetime import date, timedelta
import json

import pytest

from ai_trading_system.domains.ingest import service as ingest, universe_refresh
from ai_trading_system.pipeline.contracts import DataQualityCriticalError, StageContext


def context(tmp_path, params=None, run_date=None):
    events = []
    ctx = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="test-refresh",
        run_date=run_date or date.today().isoformat(),
        stage_name="ingest",
        attempt_number=1,
        params={"universe_refresh_enabled": True, **(params or {})},
        task_reporter=events.append,
    )
    return ctx, events


def test_refresh_runs_before_daily_ingest_and_propagates_changed_symbols(
    tmp_path, monkeypatch
):
    from ai_trading_system.domains.ingest import daily_update_runner

    ctx, events = context(tmp_path)
    calls = []

    def refresh(**kwargs):
        calls.append("refresh")
        assert kwargs["apply"]
        kwargs["progress_callback"](
            {
                "detail": "[1/2] NEWCO (BSE) · Rebuilding technical indicators",
                "completed": 4,
                "total": 7,
            }
        )
        return {"status": "completed_with_gaps", "updated_symbols": ["NEWCO"]}

    monkeypatch.setattr(universe_refresh, "run_refresh", refresh)
    worker = ingest.IngestOrchestrationService()
    monkeypatch.setattr(
        worker,
        "resolve_yfinance_fallback_policy",
        lambda ctx: (calls.append("fallback") or (False, "test")),
    )
    monkeypatch.setattr(
        daily_update_runner,
        "run",
        lambda **kw: (
            calls.append("ingest") or {"updated_symbols": [], "rows_written": 0}
        ),
    )
    monkeypatch.setattr(
        worker,
        "run_corporate_action_normalization",
        lambda *a, **kw: {"status": "completed"},
    )
    monkeypatch.setattr(
        ingest, "fetch_catalog_summary", lambda path: (1, 1, date.today())
    )
    monkeypatch.setattr(worker, "run_bhavcopy_validation", lambda *a: {})
    monkeypatch.setattr(worker, "run_delivery_collection", lambda *a: {})
    monkeypatch.setattr(worker, "run_stale_quarantine_sweep", lambda *a: {})
    result = worker.run(ctx)
    assert calls == ["refresh", "fallback", "ingest"]
    assert result.metadata["updated_symbols"] == ["NEWCO"]
    assert result.metadata["downstream_changed_symbols"] == ["NEWCO"]
    assert not result.metadata["downstream_skip_eligible"]
    assert len(result.artifacts) == 2
    assert events[0]["task_name"] == "universe_refresh"
    assert "BSE" in events[0]["detail"]
    assert events[0]["metadata"]["completed_steps"] == 4
    assert events[0]["metadata"]["total_steps"] == 35


@pytest.mark.parametrize("status", ["failed", "preview"])
def test_failed_refresh_blocks_ingest_with_report(tmp_path, monkeypatch, status):
    ctx, events = context(tmp_path)
    monkeypatch.setattr(
        universe_refresh,
        "run_refresh",
        lambda **kw: {"status": status, "blocked": ["bad identity"]},
    )
    worker = ingest.IngestOrchestrationService()
    monkeypatch.setattr(
        worker,
        "resolve_yfinance_fallback_policy",
        lambda ctx: pytest.fail("must not start ordinary ingest"),
    )
    with pytest.raises(DataQualityCriticalError, match="Universe refresh failed"):
        worker.run_default(ctx)
    report = json.loads(
        (ctx.output_dir() / "universe_refresh_summary.json").read_text()
    )
    assert report["status"] == status
    assert events[-1]["status"] == "failed"


@pytest.mark.parametrize(
    "params,historical",
    [
        ({"data_domain": "research"}, False),
        ({"canary_mode": True}, False),
        ({"symbol_limit": 5}, False),
        ({"dry_run": True}, False),
        ({}, True),
    ],
)
def test_safe_scopes_skip_acquisition(tmp_path, monkeypatch, params, historical):
    ctx, events = context(
        tmp_path,
        params,
        (date.today() - timedelta(days=1)).isoformat() if historical else None,
    )
    monkeypatch.setattr(
        universe_refresh,
        "run_refresh",
        lambda **kw: pytest.fail("unexpected acquisition"),
    )
    result = ingest.IngestOrchestrationService().run_universe_refresh(ctx)
    assert result["status"] == "skipped"
    assert events[0]["status"] == "skip"
    assert not (tmp_path / "data").exists()


def test_env_cadence_and_not_due_are_reported(tmp_path, monkeypatch):
    monkeypatch.setenv("UNIVERSE_REFRESH_CADENCE", "28-days")
    ctx, events = context(tmp_path)

    def refresh(**kwargs):
        assert kwargs["cadence"] == "28-days"
        kwargs["progress_callback"](
            {
                "detail": "Universe refresh not due; continuing daily ingest",
                "completed": 0,
                "total": 5,
                "status": "ok",
            }
        )
        return {"status": "not_due"}

    monkeypatch.setattr(universe_refresh, "run_refresh", refresh)
    assert (
        ingest.IngestOrchestrationService().run_universe_refresh(ctx)["status"]
        == "not_due"
    )
    assert "not due" in events[-1]["detail"]


def test_default_ingest_reads_maintenance_receipt_without_acquisition(tmp_path, monkeypatch):
    from ai_trading_system.platform.db.paths import get_domain_paths
    from ai_trading_system.domains.ingest.onboarding_gate import pending_identities, exclude_pending
    import pandas as pd

    ctx, events = context(tmp_path, run_date="2026-10-01")
    ctx.params.pop("universe_refresh_enabled")
    monkeypatch.delenv("UNIVERSE_REFRESH_ENABLED", raising=False)
    paths = get_domain_paths(project_root=tmp_path, data_domain="operational")
    path = paths.stage_store_dir / "universe_refresh" / "state.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"last_success": "2026-09-11", "pending": {"NEWCO": {"exchange": "NSE"}}}))
    before = path.read_bytes()
    monkeypatch.setattr(universe_refresh, "run_refresh", lambda **kw: pytest.fail("daily ingest must not acquire identities"))
    monkeypatch.setattr(universe_refresh, "load_sources", lambda: pytest.fail("must not contact BSE"))
    result = ingest.IngestOrchestrationService().run_universe_refresh(ctx)
    assert result["mode"] == "separate_maintenance"
    assert result["status"] == "maintenance_required"
    assert result["decision_session"] == "2026-10-01"
    assert result["quarantined_symbols"] == ["NEWCO"]
    assert path.read_bytes() == before
    frame = pd.DataFrame({"symbol_id": ["NEWCO", "EXISTING"], "exchange": ["NSE", "NSE"]})
    assert exclude_pending(frame, identities=pending_identities(state_path=path)).symbol_id.tolist() == ["EXISTING"]
    assert "required" in events[-1]["detail"]

    # The default full service continues ordinary ingest, despite overdue source
    # acquisition, and registers the receipt as evidence without applying it.
    from ai_trading_system.domains.ingest import daily_update_runner
    captured = []
    monkeypatch.setattr(daily_update_runner, "run", lambda **kw: (captured.append(kw) or {"target_end_date": ctx.run_date, "updated_symbols": ["EXISTING"]}))
    monkeypatch.setattr(ingest, "fetch_catalog_summary", lambda path: (1000, 100, ctx.run_date))
    worker = ingest.IngestOrchestrationService()
    monkeypatch.setattr(worker, "resolve_yfinance_fallback_policy", lambda ctx: (False, "test"))
    monkeypatch.setattr(worker, "run_corporate_action_normalization", lambda *a, **kw: {"status": "completed"})
    monkeypatch.setattr(worker, "run_bhavcopy_validation", lambda *a: {})
    monkeypatch.setattr(worker, "run_delivery_collection", lambda *a: {})
    monkeypatch.setattr(worker, "_include_delivery_changed_symbols", lambda *a: None)
    monkeypatch.setattr(worker, "run_stale_quarantine_sweep", lambda *a: {})
    stage_result = worker.run(ctx)
    assert captured[0]["target_end_date"] == "2026-10-01"
    assert stage_result.metadata["universe_refresh"]["status"] == "maintenance_required"
    assert stage_result.metadata["updated_symbols"] == ["EXISTING"]
    assert {artifact.artifact_type for artifact in stage_result.artifacts} == {"ingest_summary", "universe_refresh_summary"}
    assert path.read_bytes() == before


def test_invalid_separate_checkpoint_blocks_before_daily_ingest(tmp_path, monkeypatch):
    from ai_trading_system.platform.db.paths import get_domain_paths

    ctx, _ = context(tmp_path, {"universe_refresh_enabled": False})
    path = get_domain_paths(project_root=tmp_path).stage_store_dir / "universe_refresh" / "state.json"
    path.parent.mkdir(parents=True)
    path.write_text('{"pending": []}')
    worker = ingest.IngestOrchestrationService()
    monkeypatch.setattr(worker, "resolve_yfinance_fallback_policy", lambda ctx: pytest.fail("must block"))
    with pytest.raises(DataQualityCriticalError, match="maintenance checkpoint"):
        worker.run_default(ctx)


def test_refresh_events_move_existing_terminal_bar(tmp_path, monkeypatch):
    from ai_trading_system.pipeline.orchestrator import TerminalProgressRenderer

    ctx, _ = context(tmp_path)
    renderer = TerminalProgressRenderer(mode="compact")
    renderer.emit_run_header(
        run_id=ctx.run_id,
        run_date=ctx.run_date,
        data_domain="operational",
        stages=["ingest"],
    )
    renderer.emit_stage(stage_name="ingest", status="running")
    ctx.task_reporter = renderer.emit_task

    def refresh(**kwargs):
        kwargs["progress_callback"](
            {
                "detail": "[1/2] NEWCO (BSE) · Rebuilding technical indicators",
                "completed": 50,
                "total": 100,
            }
        )
        return {"status": "completed"}

    monkeypatch.setattr(universe_refresh, "run_refresh", refresh)
    try:
        ingest.IngestOrchestrationService().run_universe_refresh(ctx)
        assert renderer._current_stage_fraction == pytest.approx(0.1)
        assert "NEWCO (BSE)" in renderer._bar.postfix
        assert renderer._bar.n > 0
    finally:
        renderer._bar.close()
