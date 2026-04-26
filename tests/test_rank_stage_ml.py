from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.analytics.alpha.scoring import OperationalMLOverlayService
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.pipeline.stages import RankStage
from ai_trading_system.pipeline.contracts import StageContext


def _rank_outputs(context: StageContext) -> dict[str, pd.DataFrame]:
    return {
        "ranked_signals": pd.DataFrame(
            [{"symbol_id": "AAA", "exchange": "NSE", "composite_score": 91.0}]
        ),
        "stock_scan": pd.DataFrame([{"Symbol": "AAA", "category": "BUY"}]),
        "sector_dashboard": pd.DataFrame([{"Sector": "Tech", "RS": 0.9, "Momentum": 0.2}]),
        "__dashboard_payload__": {
            "summary": {
                "run_id": context.run_id,
                "run_date": context.run_date,
                "ranked_count": 1,
            },
            "ranked_signals": [{"symbol_id": "AAA", "composite_score": 91.0}],
            "warnings": [],
        },
    }


def test_rank_stage_shadow_mode_writes_overlay_and_prediction_logs(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-04-shadow",
        run_date="2026-04-04",
        stage_name="rank",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational", "ml_mode": "shadow_ml"},
    )
    stage = RankStage(
        operation=_rank_outputs,
        ml_overlay_builder=lambda ctx, ranked_df: {
            "status": "shadow_ready",
            "prediction_date": ctx.run_date,
            "overlay_df": pd.DataFrame(
                [
                    {
                        "symbol_id": "AAA",
                        "exchange": "NSE",
                        "technical_score": 91.0,
                        "technical_rank": 1,
                        "ml_5d_prob": 0.81,
                        "ml_5d_rank": 1,
                        "ml_20d_prob": 0.77,
                        "ml_20d_rank": 1,
                        "blend_5d_score": 88.0,
                        "blend_5d_rank": 1,
                        "blend_20d_score": 87.0,
                        "blend_20d_rank": 1,
                    }
                ]
            ),
            "prediction_logs": {
                5: {
                    "rows": [
                        {
                            "symbol_id": "AAA",
                            "exchange": "NSE",
                            "model_id": "model-5",
                            "model_name": "alpha5",
                            "model_version": "v1",
                            "score": 0.81,
                            "probability": 0.81,
                            "prediction": 1,
                            "rank": 1,
                        }
                    ],
                    "prediction_date": ctx.run_date,
                    "deployment_mode": "shadow_ml",
                    "model_id": "model-5",
                },
                20: {
                    "rows": [
                        {
                            "symbol_id": "AAA",
                            "exchange": "NSE",
                            "model_id": "model-20",
                            "model_name": "alpha20",
                            "model_version": "v1",
                            "score": 0.77,
                            "probability": 0.77,
                            "prediction": 1,
                            "rank": 1,
                        }
                    ],
                    "prediction_date": ctx.run_date,
                    "deployment_mode": "shadow_ml",
                    "model_id": "model-20",
                },
            },
            "metadata": {"builder": "test"},
        },
    )

    result = stage.run(context)

    assert result.metadata["ml_status"] == "shadow_ready"
    assert result.metadata["ml_overlay_rows"] == 1
    assert result.metadata["ml_prediction_log_rows"] == 2
    assert any(artifact.artifact_type == "ml_overlay" for artifact in result.artifacts)
    assert registry.count_rows("prediction_log") == 2

    dashboard_payload = json.loads((context.output_dir() / "dashboard_payload.json").read_text(encoding="utf-8"))
    assert dashboard_payload["summary"]["ml_mode"] == "shadow_ml"
    assert dashboard_payload["summary"]["ml_status"] == "shadow_ready"


def test_rank_stage_ml_failure_degrades_without_failing_pipeline(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-04-fallback",
        run_date="2026-04-04",
        stage_name="rank",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational", "ml_mode": "shadow_ml"},
    )
    stage = RankStage(
        operation=_rank_outputs,
        ml_overlay_builder=lambda ctx, ranked_df: (_ for _ in ()).throw(RuntimeError("model missing")),
    )

    result = stage.run(context)

    assert result.metadata["ml_status"] == "degraded"
    assert "ml overlay unavailable: model missing" in result.metadata["degraded_outputs"]
    assert registry.count_rows("prediction_log") == 0


def test_operational_overlay_service_skips_when_no_deployments_exist(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    service = OperationalMLOverlayService(
        project_root=tmp_path,
        registry=registry,
        data_domain="operational",
    )

    result = service.build_shadow_overlay(prediction_date="2026-04-04")

    assert result["status"] == "skipped_no_active_deployment"
    assert result["overlay_df"].empty
