from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.domains.ranking.payloads import (
    attach_market_direction_to_payload,
    attach_market_regime_phase_to_payload,
    build_dashboard_payload,
)
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.pipeline.contracts import StageContext
from ai_trading_system.domains.publish.publish_payloads import (
    build_publish_datasets,
    build_publish_metadata,
)
from ai_trading_system.domains.publish.telegram_summary_builder import build_telegram_summary
from ai_trading_system.domains.fundamentals.insight_readmodels import (
    GREAT_RESULT_PRIORITY,
    _curated_stock_tags,
)


def test_build_publish_datasets_loads_optional_artifacts_with_defaults(tmp_path: Path) -> None:
    ranked_artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri=str(tmp_path / "ranked_signals.csv"),
        content_hash="ranked-hash",
    )
    breakout_artifact = StageArtifact(
        artifact_type="breakout_scan",
        uri=str(tmp_path / "breakout_scan.csv"),
        content_hash="breakout-hash",
    )

    artifacts = {
        "breakout_scan": breakout_artifact,
    }

    def artifact_for(name: str) -> StageArtifact | None:
        return artifacts.get(name)

    def read_artifact(artifact: StageArtifact) -> pd.DataFrame:
        if artifact.artifact_type == "ranked_signals":
            return pd.DataFrame([{"symbol_id": "AAA", "composite_score": 90.0}])
        if artifact.artifact_type == "breakout_scan":
            return pd.DataFrame([{"symbol_id": "AAA", "setup_family": "range_breakout"}])
        return pd.DataFrame()

    datasets = build_publish_datasets(
        context_artifact_for=artifact_for,
        read_artifact=read_artifact,
        read_json_artifact=lambda _artifact: {"summary": {"run_date": "2026-04-16"}},
        ranked_signals_artifact=ranked_artifact,
    )

    assert not datasets["ranked_signals"].empty
    assert not datasets["breakout_scan"].empty
    assert datasets["stock_scan"].empty
    assert datasets["sector_dashboard"].empty
    assert datasets["sector_rotation"].empty
    assert datasets["industry_rotation"].empty
    assert datasets["stock_rotation"].empty
    assert datasets["accumulation_distribution"].empty
    assert datasets["sector_custom_indices"].empty
    assert datasets["sector_rotation_payload"] == {}
    assert datasets["dashboard_payload"] == {}
    assert datasets["publish_trust_status"] == "unknown"
    assert datasets["publish_mode_telegram"] == "concise"
    assert datasets["publish_mode_sheets"] == "full"
    assert datasets["publish_mode_dashboard"] == "structured_json"
    assert datasets["publish_rows_telegram"][0]["signal_classification"] == "actionable"
    assert datasets["publish_rows_telegram"][0]["publish_confidence"] is None
    assert datasets["stage2_summary"]["uptrend_count"] == 0
    assert datasets["stage2_breakdown_symbols"] == ["AAA"]


def test_build_publish_datasets_loads_sector_rotation_artifacts(tmp_path: Path) -> None:
    ranked_path = tmp_path / "ranked_signals.csv"
    ranked_path.write_text("symbol_id,composite_score\nAAA,90\n", encoding="utf-8")
    ranked_artifact = StageArtifact.from_file("ranked_signals", ranked_path, row_count=1)
    artifacts: dict[str, StageArtifact] = {"ranked_signals": ranked_artifact}
    for artifact_type, body in {
        "sector_rotation": "industry,quadrant,rs_ratio\nBanks,Leading,104\n",
        "industry_rotation": "rotation_group_name,quadrant,rs_ratio\nPSU Bank,Leading,106\n",
        "stock_rotation": "symbol,quadrant,rotation_adjusted_score\nAAA,Leading,82\n",
        "accumulation_distribution": "symbol,delivery_signal,accumulation_score\nAAA,Accumulation,78\n",
        "sector_custom_indices": "date,industry,sector_index,weighting_method,constituent_count\n2026-04-30,Banks,110,market_cap,12\n",
    }.items():
        path = tmp_path / f"{artifact_type}.csv"
        path.write_text(body, encoding="utf-8")
        artifacts[artifact_type] = StageArtifact.from_file(artifact_type, path, row_count=1)
    payload_path = tmp_path / "sector_rotation_payload.json"
    payload_path.write_text('{"benchmark_name":"UNIV_TOP1000"}', encoding="utf-8")
    artifacts["sector_rotation_payload"] = StageArtifact.from_file("sector_rotation_payload", payload_path, row_count=1)

    datasets = build_publish_datasets(
        context_artifact_for=lambda name: artifacts.get(name),
        read_artifact=lambda artifact: pd.read_csv(Path(artifact.uri)),
        read_json_artifact=lambda artifact: json.loads(Path(artifact.uri).read_text(encoding="utf-8")),
        ranked_signals_artifact=ranked_artifact,
    )

    assert datasets["sector_rotation"]["industry"].tolist() == ["Banks"]
    assert datasets["industry_rotation"]["rotation_group_name"].tolist() == ["PSU Bank"]
    assert datasets["stock_rotation"]["symbol"].tolist() == ["AAA"]
    assert datasets["accumulation_distribution"]["delivery_signal"].tolist() == ["Accumulation"]
    assert datasets["sector_custom_indices"]["weighting_method"].tolist() == ["market_cap"]
    assert datasets["sector_rotation_payload"]["benchmark_name"] == "UNIV_TOP1000"


def test_build_publish_datasets_loads_fundamental_artifacts_and_dashboard_payload(tmp_path: Path) -> None:
    ranked_path = tmp_path / "ranked_signals.csv"
    ranked_path.write_text("symbol_id,composite_score\nAAA,90\n", encoding="utf-8")
    ranked_artifact = StageArtifact.from_file("ranked_signals", ranked_path, row_count=1)
    artifacts: dict[str, StageArtifact] = {"ranked_signals": ranked_artifact}
    for artifact_type in (
        "great_results",
        "turnaround_candidates",
        "compounder_candidates",
        "sector_earnings_leadership",
        "sector_valuation_daily",
        "universe_valuation_daily",
        "valuation_cycle_features",
    ):
        path = tmp_path / f"{artifact_type}.csv"
        path.write_text("symbol,insight_score\nAAA,88\n", encoding="utf-8")
        artifacts[artifact_type] = StageArtifact.from_file(artifact_type, path, row_count=1)
    payload_path = tmp_path / "fundamental_dashboard_payload.json"
    payload_path.write_text(
        '{"summary":{"great_results_count":1},"universe":{"pe_ttm":24.1},"top_great_results":[{"symbol":"AAA"}],"top_turnarounds":[],"top_compounders":[],"sector_earnings_leadership":[],"valuation_chart":[]}',
        encoding="utf-8",
    )
    artifacts["fundamental_dashboard_payload"] = StageArtifact.from_file(
        "fundamental_dashboard_payload", payload_path, row_count=1
    )

    datasets = build_publish_datasets(
        context_artifact_for=lambda name: artifacts.get(name),
        read_artifact=lambda artifact: pd.read_csv(Path(artifact.uri)),
        read_json_artifact=lambda artifact: json.loads(Path(artifact.uri).read_text(encoding="utf-8")),
        ranked_signals_artifact=ranked_artifact,
    )

    assert not datasets["great_results"].empty
    assert not datasets["turnaround_candidates"].empty
    assert not datasets["compounder_candidates"].empty
    assert datasets["fundamental_dashboard_payload"]["universe"]["pe_ttm"] == 24.1
    assert datasets["dashboard_payload"]["fundamentals"]["summary"]["great_results_count"] == 1
    assert datasets["dashboard_payload"]["fundamentals"]["great_results"] == [{"symbol": "AAA"}]


def test_curated_stock_tags_use_latest_date_priority_and_one_row_per_symbol() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "report_date": "2025-12-31",
                "insight_type": "blowout_result",
                "insight_score": 99,
                "evidence_json": '{"note":"old blowout"}',
            },
            {
                "symbol": "AAA",
                "report_date": "2026-03-31",
                "insight_type": "great_result",
                "insight_score": 95,
                "evidence_json": '{"note":"latest great"}',
            },
            {
                "symbol": "AAA",
                "report_date": "2026-03-31",
                "insight_type": "blowout_result",
                "insight_score": 88,
                "evidence_json": '{"note":"latest blowout"}',
            },
            {
                "symbol": "BBB",
                "report_date": "2026-03-31",
                "insight_type": "profit_acceleration_result",
                "insight_score": 90,
                "evidence_json": '{"note":"profit acceleration"}',
            },
        ]
    )

    curated = _curated_stock_tags(frame, GREAT_RESULT_PRIORITY, limit=100)

    assert curated["report_date"].astype(str).unique().tolist() == ["2026-03-31"]
    assert curated["symbol"].tolist() == ["AAA", "BBB"]
    assert curated.iloc[0]["insight_type"] == "blowout_result"
    assert curated.iloc[0]["evidence"] == "latest blowout"


def test_build_publish_metadata_uses_top_ranked_symbol() -> None:
    ranked_artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri="/tmp/ranked_signals.csv",
        content_hash="hash-a",
    )
    ranked_df = pd.DataFrame(
        [{"symbol_id": "INFY", "rank_confidence": 0.85}, {"symbol_id": "RELIANCE", "rank_confidence": 0.70}]
    )
    targets = [{"channel": "telegram_summary", "status": "delivered"}]

    metadata = build_publish_metadata(
        rank_artifact=ranked_artifact,
        ranked_df=ranked_df,
        targets=targets,
    )

    assert metadata["rank_artifact_uri"] == "/tmp/ranked_signals.csv"
    assert metadata["rank_artifact_hash"] == "hash-a"
    assert metadata["top_symbol"] == "INFY"
    assert metadata["top_publish_confidence"] == 0.85
    assert metadata["targets"] == targets
    assert "completed_at" in metadata


def test_build_publish_datasets_adds_stage2_breakdown_and_telegram_summary_line() -> None:
    ranked_df = pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 95.0, "stage2_label": "strong_stage2", "is_stage2_uptrend": True},
            {"symbol_id": "BBB", "composite_score": 92.0, "stage2_label": "stage2", "is_stage2_uptrend": True},
            {"symbol_id": "CCC", "composite_score": 89.0, "stage2_label": "stage1_to_stage2", "is_stage2_uptrend": False},
        ]
    )
    ranked_artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri="/tmp/ranked.csv",
        content_hash="ranked-hash",
    )
    datasets = build_publish_datasets(
        context_artifact_for=lambda _name: None,
        read_artifact=lambda _artifact: ranked_df.copy(),
        read_json_artifact=lambda _artifact: {"summary": {"run_date": "2026-04-21"}},
        ranked_signals_artifact=ranked_artifact,
    )
    assert datasets["stage2_summary"]["uptrend_count"] == 2
    assert datasets["stage2_summary"]["counts_by_label"]["strong_stage2"] == 1
    assert datasets["stage2_breakdown_symbols"] == ["AAA", "BBB", "CCC"]

    message = build_telegram_summary(run_date="2026-04-21", datasets=datasets)
    assert "Stage2:" in message
    assert "strong_stage2:1" in message


def test_telegram_summary_includes_market_direction_line() -> None:
    datasets = {
        "dashboard_payload": {
            "summary": {
                "run_date": "2026-04-21",
                "data_trust_status": "trusted",
            },
            "market_direction": {
                "market_state": "bull",
                "breadth_velocity": "positive",
                "direction_bias": "Confirmed uptrend",
                "action": "hold",
                "allowed_exposure": 0.80,
            },
        },
        "ranked_signals": pd.DataFrame([{"symbol_id": "AAA", "composite_score": 90.0}]),
    }
    message = build_telegram_summary(run_date="2026-04-21", datasets=datasets)

    assert "Market Direction:" in message
    assert "Confirmed uptrend" in message
    assert "Exposure: <b>80%</b>" in message


def test_telegram_summary_includes_market_regime_phase() -> None:
    datasets = {
        "dashboard_payload": {
            "summary": {
                "run_date": "2026-04-21",
            },
            "market_direction": {
                "market_state": "bull",
                "breadth_velocity": "positive",
                "direction_bias": "Confirmed uptrend",
                "action": "hold",
                "allowed_exposure": 0.80,
            },
            "market_regime_phase": {
                "regime_phase": "base_forming_stage1",
                "phase_label": "Base forming (S1)",
                "phase_emoji": "🟡",
                "driven_by": {
                    "regime": "neutral",
                    "breadth_velocity_bucket": "positive",
                    "s2_pct": 0.20,
                },
            },
        },
        "ranked_signals": pd.DataFrame([{"symbol_id": "AAA", "composite_score": 90.0}]),
    }
    message = build_telegram_summary(run_date="2026-04-21", datasets=datasets)

    assert "Base forming (S1)" in message


def test_telegram_summary_includes_fundamental_pulse() -> None:
    datasets = {
        "dashboard_payload": {"summary": {"run_date": "2026-05-07"}},
        "ranked_signals": pd.DataFrame([{"symbol_id": "AAA", "composite_score": 90.0}]),
        "fundamental_dashboard_payload": {
            "universe": {"pe_ttm": 24.1, "pe_200dma": 22.8, "pe_percentile_5y": 82, "valuation_zone": "expensive"}
        },
        "sector_earnings_leadership": pd.DataFrame([{"sector_name": "Capital Goods", "sector_fundamental_score": 92}]),
        "great_results": pd.DataFrame([{"symbol": "ABC", "insight_score": 90}]),
        "turnaround_candidates": pd.DataFrame([{"symbol": "XYZ", "insight_score": 82}]),
        "compounder_candidates": pd.DataFrame([{"symbol": "TCS", "insight_score": 78}]),
    }

    message = build_telegram_summary(run_date="2026-05-07", datasets=datasets)

    assert "Fundamental Pulse" in message
    assert "Universe PE: <b>24.1</b>" in message
    assert "Capital Goods" in message
    assert "ABC" in message


def test_attach_market_direction_to_payload_flattens_summary_fields() -> None:
    payload = {"summary": {"run_date": "2026-04-21"}}
    direction = {
        "direction_bias": "Recovery attempt",
        "action": "scale_in",
        "allowed_exposure": 0.35,
        "new_buys_allowed": True,
        "required_min_score": 75,
        "required_breakout_tier": "strict",
        "required_setup_quality_gte": 0.70,
        "breadth_velocity": "very_positive",
        "regime_age_days": 18,
        "confidence_capped": 0.92,
    }

    out = attach_market_direction_to_payload(payload, direction)

    assert out["market_direction"] == direction
    assert out["summary"]["direction_bias"] == "Recovery attempt"
    assert out["summary"]["direction_action"] == "scale_in"
    assert out["summary"]["allowed_exposure"] == 0.35
    assert out["summary"]["breadth_velocity_bucket"] == "very_positive"
    assert out["summary"]["regime_confidence_capped"] == 0.92


def test_attach_market_regime_phase_to_payload_flattens_summary_fields() -> None:
    payload = {"summary": {"run_date": "2026-04-21"}}
    phase = {
        "regime_phase": "transition_stage1_to_stage2",
        "phase_label": "Transition S1 → S2",
        "phase_emoji": "🟢",
        "driven_by": {
            "market_stage": "S1",
            "regime": "neutral",
            "breadth_velocity_bucket": "positive",
            "s2_pct": 0.31,
            "transition_s2_threshold": 0.30,
        },
    }

    out = attach_market_regime_phase_to_payload(payload, phase)

    assert out["market_regime_phase"] == phase
    assert out["summary"]["regime_phase"] == "transition_stage1_to_stage2"
    assert out["summary"]["regime_phase_label"] == "Transition S1 → S2"
    assert out["summary"]["regime_phase_emoji"] == "🟢"
    assert out["summary"]["regime_phase_s2_pct"] == 0.31
    assert out["summary"]["regime_phase_market_stage"] == "S1"
    assert out["summary"]["regime_phase_velocity"] == "positive"


def test_build_dashboard_payload_explains_empty_discoveries_when_ranked_covers_stock_scan(tmp_path: Path) -> None:
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="pipeline-2026-04-24-fixture",
        run_date="2026-04-24",
        stage_name="rank",
        attempt_number=1,
        params={},
    )
    ranked_df = pd.DataFrame(
        [
            {"symbol_id": "AAA", "rank": 1, "composite_score": 91.0},
            {"symbol_id": "BBB", "rank": 2, "composite_score": 88.0},
        ]
    )
    stock_scan_df = pd.DataFrame(
        [
            {"symbol_id": "AAA", "rank": 1, "composite_score": 91.0, "pattern_positive": True, "breakout_positive": False, "discovered_by_pattern_scan": False},
            {"symbol_id": "BBB", "rank": 2, "composite_score": 88.0, "pattern_positive": False, "breakout_positive": True, "discovered_by_pattern_scan": False},
        ]
    )

    payload = build_dashboard_payload(
        context=context,
        ranked_df=ranked_df,
        breakout_df=pd.DataFrame(),
        pattern_df=pd.DataFrame(),
        stock_scan_df=stock_scan_df,
        sector_dashboard_df=pd.DataFrame(),
        warnings=[],
        trust_summary={"status": "trusted"},
        task_status={},
    )

    summary = payload["summary"]
    assert summary["ranked_universe_covers_stock_scan"] is True
    assert summary["ranked_universe_stock_scan_coverage_pct"] == 100.0
    assert summary["discovery_visibility_reason"] == "ranked_universe_covers_stock_scan"
    assert "ranked universe already covers the full stock-scan symbol set" in summary["discovery_visibility_note"]


def test_build_dashboard_payload_includes_top_stage2_leaders(tmp_path: Path) -> None:
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="pipeline-2026-04-24-fixture",
        run_date="2026-04-24",
        stage_name="rank",
        attempt_number=1,
        params={},
    )
    stock_scan_df = pd.DataFrame(
        [
            {"symbol_id": "A1", "rank": 3, "composite_score": 80.0, "stage2_label": "strong_stage2"},
            {"symbol_id": "A2", "rank": 1, "composite_score": 70.0, "stage2_label": "stage2"},
            {"symbol_id": "A3", "rank": 2, "composite_score": 90.0, "stage2_label": "strong_stage2"},
            {"symbol_id": "A4", "rank": None, "composite_score": 95.0, "stage2_label": "stage2"},
            {"symbol_id": "B1", "rank": 4, "composite_score": 99.0, "stage2_label": "non_stage2"},
        ]
    )
    ranked_df = stock_scan_df[["symbol_id", "rank", "composite_score", "stage2_label"]].copy()

    payload = build_dashboard_payload(
        context=context,
        ranked_df=ranked_df,
        breakout_df=pd.DataFrame(),
        pattern_df=pd.DataFrame(),
        stock_scan_df=stock_scan_df,
        sector_dashboard_df=pd.DataFrame(),
        warnings=[],
        trust_summary={"status": "trusted"},
        task_status={},
    )

    stage2_leaders = payload["stage2_leaders"]
    assert [row["symbol_id"] for row in stage2_leaders] == ["A2", "A3", "A1", "A4"]
    assert payload["summary"]["stage2_total_count"] == 4
    assert payload["summary"]["stage2_leader_count"] == 4
    assert payload["summary"]["stage2_label_counts"] == {"strong_stage2": 2, "stage2": 2}
