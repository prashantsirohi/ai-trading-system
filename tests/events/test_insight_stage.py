from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.domains.events.event_llm_router import _build_llm_packet, _enforce_report_contract
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.insight import InsightStage, validate_report


def _context(tmp_path: Path) -> StageContext:
    rank_dir = tmp_path / "data" / "operational" / "pipeline_runs" / "r1" / "rank" / "attempt_1"
    events_dir = tmp_path / "data" / "operational" / "pipeline_runs" / "r1" / "events" / "attempt_1"
    rank_dir.mkdir(parents=True)
    events_dir.mkdir(parents=True)
    ranked = rank_dir / "ranked_signals.csv"
    pd.DataFrame([{"symbol_id": "RELIANCE", "composite_score": 91.0, "sector_name": "Energy"}]).to_csv(ranked, index=False)
    dashboard = rank_dir / "dashboard_payload.json"
    dashboard.write_text(json.dumps({"summary": {"data_trust_status": "trusted"}, "data_trust": {"status": "trusted"}}), encoding="utf-8")
    snapshot = events_dir / "market_events_snapshot.json"
    snapshot.write_text(
        json.dumps(
            {
                "market_intel_status": "ok",
                "events": [
                    {
                        "raw_event_id": 10,
                        "resolved_event_id": 20,
                        "symbol": "RELIANCE",
                        "category": "capex_expansion",
                        "tier": "A",
                        "alert_level": "critical",
                        "importance_score": 9.0,
                        "trust_score": 95.0,
                        "event_hash": "event-hash-1",
                        "title": "Capex announced",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    enrichment = events_dir / "events_enrichment.json"
    enrichment.write_text(json.dumps({"signals": []}), encoding="utf-8")
    summary = events_dir / "events_summary.json"
    summary.write_text(json.dumps({"market_intel_status": "ok"}), encoding="utf-8")
    return StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="r1",
        run_date="2026-05-01",
        stage_name="insight",
        attempt_number=1,
        artifacts={
            "rank": {
                "ranked_signals": StageArtifact.from_file("ranked_signals", ranked),
                "dashboard_payload": StageArtifact.from_file("dashboard_payload", dashboard),
            },
            "events": {
                "market_events_snapshot": StageArtifact.from_file("market_events_snapshot", snapshot),
                "events_enrichment": StageArtifact.from_file("events_enrichment", enrichment),
                "events_summary": StageArtifact.from_file("events_summary", summary),
            },
        },
        params={},
    )


def test_insight_stage_writes_event_confluence_for_top_ranked_event(tmp_path, monkeypatch):
    monkeypatch.delenv("OPENROUTER_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    result = InsightStage().run(_context(tmp_path))
    assert result.metadata["confluence_count"] == 1
    confluence = next(a for a in result.artifacts if a.artifact_type == "event_confluence")
    df = pd.read_csv(confluence.uri)
    assert df.iloc[0]["symbol"] == "RELIANCE"
    assert df.iloc[0]["rank_position"] == 1
    usage = json.loads(Path(next(a for a in result.artifacts if a.artifact_type == "model_usage").uri).read_text())
    assert usage["status"] == "skipped_no_api_key"


def test_report_validator_rejects_uncited_event_and_guarantee_language():
    packet = {
        "rank": {"top_ranked": [{"symbol_id": "RELIANCE"}]},
        "market_intel": {"top_events": [{"symbol": "RELIANCE", "event_hash": "h1"}]},
        "data_trust": {"status": "trusted"},
    }
    report = "RELIANCE has a capex event and is a guaranteed buy."
    validation = validate_report(report, packet)
    rules = {issue["rule"] for issue in validation["issues"]}
    assert "event_claim_must_be_cited" in rules
    assert "no_buy_sell_guarantee_language" in rules


def test_insight_stage_includes_trigger_only_bulk_deal_confluence(tmp_path, monkeypatch):
    ctx = _context(tmp_path)
    snapshot_path = Path(ctx.artifacts["events"]["market_events_snapshot"].uri)
    snapshot_path.write_text(json.dumps({"market_intel_status": "stale", "events": []}), encoding="utf-8")
    enrichment_path = Path(ctx.artifacts["events"]["events_enrichment"].uri)
    enrichment_path.write_text(
        json.dumps(
            {
                "signals": [
                    {
                        "event_count": 0,
                        "events": [],
                        "severity": "medium",
                        "trigger": {
                            "symbol": "RELIANCE",
                            "trigger_type": "bulk_deal",
                            "as_of_date": "2026-05-01",
                            "trigger_strength": 1.2,
                            "trigger_metadata": {
                                "client_name": "TEST FUND",
                                "side": "BUY",
                                "deal_value_cr": 120.0,
                                "quantity": 100000,
                                "trade_date": "2026-05-01",
                                "deal_hash": "bulk-hash-1",
                            },
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("OPENROUTER_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    result = InsightStage().run(ctx)
    assert result.metadata["confluence_count"] == 1
    confluence = pd.read_csv(next(a for a in result.artifacts if a.artifact_type == "event_confluence").uri)
    assert confluence.iloc[0]["category"] == "bulk_deal"
    assert confluence.iloc[0]["event_hash"] == "bulk-hash-1"
    packet = json.loads(Path(next(a for a in result.artifacts if a.artifact_type == "event_packet").uri).read_text())
    assert packet["market_intel_status"] == "stale"
    assert packet["event_counts"]["important"] == 1


def test_report_validator_flags_fenced_and_token_capped_output():
    packet = {
        "rank": {"top_ranked": [{"symbol_id": "RELIANCE"}]},
        "market_intel": {"market_intel_status": "stale", "top_events": []},
        "data_trust": {"status": "trusted"},
    }
    validation = validate_report(
        "```markdown\n# Report\nNo events",
        packet,
        model_usage={"possible_truncation": True},
    )
    rules = {issue["rule"] for issue in validation["issues"]}
    assert "no_markdown_fence_wrappers" in rules
    assert "llm_output_may_be_truncated" in rules
    assert "market_intel_status_must_show_warning" in rules


def test_llm_packet_is_event_first_and_report_contract_is_enforced():
    packet = {
        "run_id": "r1",
        "rank": {"top_ranked": [{"symbol_id": f"SYM{i}", "composite_score": i} for i in range(100)]},
        "market_intel": {
            "market_intel_status": "stale",
            "event_counts": {"important": 1},
            "top_events": [{"symbol": "RELIANCE", "event_hash": "h1", "category": "bulk_deal"}],
            "important_events": [{"symbol": "RELIANCE", "event_hash": "h1", "category": "bulk_deal"}],
        },
    }
    llm_packet = _build_llm_packet(packet)
    assert list(llm_packet).index("market_intel") < list(llm_packet).index("rank")
    assert llm_packet["market_intel"]["top_events"][0]["event_hash"] == "h1"
    assert len(llm_packet["rank"]["top_ranked"]) == 12

    fixed = _enforce_report_contract("No forward price targets given.\n", packet=llm_packet)
    assert "Market intel status: stale" in fixed
    assert "price target" not in fixed.lower()


def test_validator_allows_client_name_tokens_inside_cited_event_lines():
    packet = {
        "rank": {"top_ranked": [{"symbol_id": "SYNGENE"}]},
        "market_intel": {
            "top_events": [{"symbol": "SYNGENE", "event_hash": "trigger:h1"}],
        },
        "data_trust": {"status": "trusted"},
    }
    report = "| SYNGENE | SELL | NK SECURITIES PSL HRTI bulk deal [trigger:h1] |"
    validation = validate_report(report, packet)
    assert validation["status"] == "passed"


def test_insight_stage_falls_back_when_llm_output_fails_validation(tmp_path, monkeypatch):
    def bad_report(*args, **kwargs):
        return "RELIANCE has an uncited capex event and is a guaranteed buy.", {
            "status": "completed",
            "route": "daily_market_report",
            "model": "test-model",
            "possible_truncation": True,
        }

    monkeypatch.setattr("ai_trading_system.pipeline.stages.insight.build_market_report", bad_report)
    result = InsightStage().run(_context(tmp_path))
    assert result.metadata["validation_status"] == "passed"
    assert result.metadata["model_status"] == "validation_fallback"
    usage = json.loads(Path(next(a for a in result.artifacts if a.artifact_type == "model_usage").uri).read_text())
    assert usage["llm_status"] == "completed"
    assert usage["llm_possible_truncation"] is True
    assert usage["possible_truncation"] is False
