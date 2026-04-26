from __future__ import annotations

import json
from pathlib import Path

import duckdb

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.domains.execution import ExecutionService, ExecutionStore, PaperExecutionAdapter
from ai_trading_system.domains.execution.models import OrderIntent
from ai_trading_system.research.recipes import (
    build_validation_review,
    get_recipe,
    get_recipe_bundle,
    pick_bundle_winner,
    write_recipe_bundle_summary,
    write_recipe_summary,
)
import ai_trading_system.research.run_recipe as run_recipe_module
from ai_trading_system.ui.execution_api.services.control_center import _TASKS, _create_task, list_operator_tasks
from ai_trading_system.ui.execution_api.services.ml_workbench import (
    approve_workbench_model,
    delete_workbench_recipe,
    delete_workbench_recipe_bundle,
    deploy_workbench_model,
    load_latest_execute_run,
    load_execution_workbench_settings,
    load_recipe_bundle_results,
    load_recipe_results,
    load_workbench_execution_fills,
    load_workbench_execution_orders,
    load_workbench_execution_positions,
    load_workbench_recipe_bundles,
    load_workbench_recipes,
    load_workbench_trade_report,
    load_model_workbench_detail,
    load_workbench_datasets,
    load_workbench_deployments,
    load_workbench_models,
    rollback_workbench_deployment,
    save_execution_workbench_settings,
    save_workbench_recipe,
    save_workbench_recipe_bundle,
    workbench_recipe_config_path,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _seed_prediction_logs_and_outcomes(registry: RegistryStore, *, model_id: str, horizon: int) -> None:
    registry.replace_prediction_log(
        "2026-03-01",
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "model_id": model_id,
                "model_name": "alpha",
                "model_version": "v1",
                "probability": 0.9,
                "prediction": 1,
                "rank": 1,
            },
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "model_id": model_id,
                "model_name": "alpha",
                "model_version": "v1",
                "probability": 0.1,
                "prediction": 0,
                "rank": 2,
            },
        ],
        deployment_mode="shadow_ml",
        horizon=horizon,
        model_id=model_id,
    )
    pending = registry.get_unscored_prediction_logs(horizon, deployment_mode="shadow_ml", model_id=model_id)
    registry.replace_shadow_eval(
        [
            {
                "prediction_log_id": row["prediction_log_id"],
                "prediction_date": row["prediction_date"],
                "model_id": model_id,
                "deployment_mode": "shadow_ml",
                "horizon": horizon,
                "symbol_id": row["symbol_id"],
                "exchange": row["exchange"],
                "future_date": "2026-03-06",
                "realized_return": 0.04 if row["symbol_id"] == "AAA" else -0.01,
                "hit": row["symbol_id"] == "AAA",
            }
            for row in pending
        ]
    )


def test_ml_workbench_service_loaders_return_registry_views(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    dataset_id = registry.register_dataset(
        dataset_ref="research:training:test_h5",
        dataset_uri=str(tmp_path / "datasets" / "test_h5.parquet"),
        data_domain="research",
        engine_name="lightgbm",
        feature_schema_version="alpha_v1",
        label_version="forward_return_v1",
        target_column="target_5d",
        from_date="2018-01-01",
        to_date="2025-12-31",
        horizon=5,
        row_count=1200,
        symbol_count=100,
        metadata={"validation_start": "2024-01-01", "validation_fraction": 0.2},
    )
    assert dataset_id

    model_id = registry.register_model(
        model_name="alpha",
        model_version="v1",
        artifact_uri=str(tmp_path / "models" / "alpha_v1.txt"),
        feature_schema_hash="schema-hash",
        train_snapshot_ref="research:training:test_h5",
        approval_status="pending",
        metadata={
            "engine": "lightgbm",
            "horizon": 5,
            "evaluation": {"validation_auc": 0.64, "precision_at_10pct": 0.41},
            "walkforward_summary": {"avg_validation_auc": 0.61},
        },
    )
    registry.record_model_eval(
        model_id,
        {"validation_auc": 0.64, "walkforward_avg_validation_auc": 0.61},
        dataset_ref="research:training:test_h5",
    )
    registry.approve_model(model_id)
    registry.deploy_model(model_id, environment="operational_shadow_5d", approved_by="test")
    registry.record_drift_metrics(
        [
            {
                "prediction_date": "2026-03-01",
                "model_id": model_id,
                "deployment_mode": "shadow_ml",
                "horizon": 5,
                "metric_name": "score_psi",
                "metric_value": 0.08,
                "threshold_value": 0.2,
                "status": "pass",
            }
        ]
    )
    registry.record_promotion_gate_results(
        model_id,
        [
            {
                "gate_name": "validation_auc",
                "status": "pass",
                "metric_value": 0.64,
                "threshold_value": 0.58,
            }
        ],
    )
    _seed_prediction_logs_and_outcomes(registry, model_id=model_id, horizon=5)

    datasets = load_workbench_datasets(tmp_path)
    models = load_workbench_models(tmp_path)
    deployments = load_workbench_deployments(tmp_path)
    detail = load_model_workbench_detail(model_id, tmp_path)

    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_ref"] == "research:training:test_h5"
    assert float(datasets.iloc[0]["validation_fraction"]) == 0.2

    assert len(models) == 1
    assert models.iloc[0]["model_id"] == model_id
    assert float(models.iloc[0]["validation_auc"]) == 0.64

    assert len(deployments) == 1
    assert deployments.iloc[0]["environment"] == "operational_shadow_5d"
    assert deployments.iloc[0]["model_name"] == "alpha"

    assert detail["model"]["model_id"] == model_id
    assert len(detail["evaluations"]) == 2
    assert len(detail["drift_metrics"]) == 1
    assert len(detail["promotion_gates"]) == 1
    assert detail["monitor_summary"]["matured_rows"] == 2


def test_ml_workbench_approve_deploy_and_rollback_helpers_update_registry(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    model_id = registry.register_model(
        model_name="alpha",
        model_version="v2",
        artifact_uri=str(tmp_path / "models" / "alpha_v2.txt"),
        feature_schema_hash="schema-hash",
        train_snapshot_ref="research:training:test_h20",
        approval_status="pending",
        metadata={"engine": "lightgbm", "horizon": 20},
    )
    prior_model_id = registry.register_model(
        model_name="alpha",
        model_version="v1",
        artifact_uri=str(tmp_path / "models" / "alpha_v1.txt"),
        feature_schema_hash="schema-hash",
        train_snapshot_ref="research:training:test_h20_prev",
        approval_status="approved",
        metadata={"engine": "lightgbm", "horizon": 20},
    )
    registry.deploy_model(prior_model_id, environment="operational_shadow_20d", approved_by="seed")

    approval = approve_workbench_model(model_id, tmp_path)
    deployment = deploy_workbench_model(
        model_id,
        environment="operational_shadow_20d",
        approved_by="ui-test",
        notes="shadow rollout",
        project_root=tmp_path,
    )
    rollback = rollback_workbench_deployment(
        environment="operational_shadow_20d",
        approved_by="ui-test",
        notes="rollback to prior",
        project_root=tmp_path,
    )

    assert approval["before"]["approval_status"] == "pending"
    assert approval["after"]["approval_status"] == "approved"
    assert deployment["active_deployment"]["model_id"] == model_id
    assert deployment["active_deployment"]["environment"] == "operational_shadow_20d"
    assert rollback["active_deployment"]["model_id"] == prior_model_id


def test_workbench_recipe_catalog_and_result_loader(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    source_config = REPO_ROOT / "src" / "ai_trading_system" / "platform" / "config" / "research_recipes.toml"
    (tmp_path / "config" / "research_recipes.toml").write_text(source_config.read_text(encoding="utf-8"), encoding="utf-8")

    recipes = load_workbench_recipes(tmp_path)
    recipe = get_recipe("breakout_5d", tmp_path)
    validation_review = build_validation_review(
        recipe,
        {
            "evaluation": {"validation_auc": 0.61, "precision_at_10pct": 0.42},
            "walkforward": {"summary": {"avg_validation_auc": 0.6}},
        },
    )
    report_path = write_recipe_summary(
        tmp_path,
        "breakout_5d",
        {
            "recipe_name": "breakout_5d",
            "executed_at": "2026-04-05T10:00:00+00:00",
            "model_id": "model-123",
            "model_name": "breakout_sector_alpha",
            "model_version": "h5_v1",
            "engine": "lightgbm",
            "horizon": 5,
            "strategy_tag": "swing_breakout",
            "feature_set_variant": "default",
            "experiment_notes": "baseline test",
            "dataset_ref": "research:training:test_h5",
            "validation_review": validation_review,
            "evaluation": {"validation_auc": 0.61, "precision_at_10pct": 0.42},
            "walkforward_summary": {"avg_validation_auc": 0.6},
            "promotion_status": "insufficient_data",
            "shadow_summary": {"matured_rows": 0},
            "approval_status": "pending",
            "shadow_environment": "operational_shadow_5d",
        },
    )

    results = load_recipe_results(tmp_path)

    assert not recipes.empty
    assert "breakout_5d" in recipes["recipe_name"].tolist()
    assert validation_review["overall_status"] == "pass"
    assert report_path.exists()
    assert len(results) == 1
    assert results.iloc[0]["recipe_name"] == "breakout_5d"
    assert results.iloc[0]["validation_status"] == "pass"
    assert results.iloc[0]["strategy_tag"] == "swing_breakout"
    assert results.iloc[0]["feature_set_variant"] == "default"


def test_workbench_bundle_loader_and_winner_selection(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    source_config = REPO_ROOT / "src" / "ai_trading_system" / "platform" / "config" / "research_recipes.toml"
    (tmp_path / "config" / "research_recipes.toml").write_text(source_config.read_text(encoding="utf-8"), encoding="utf-8")

    bundle = get_recipe_bundle("daily_research", tmp_path)
    bundles = load_workbench_recipe_bundles(tmp_path)
    candidate_a = {
        "recipe_name": "breakout_5d",
        "model_id": "model-a",
        "model_name": "breakout_sector_alpha",
        "model_version": "h5_v1",
        "strategy_tag": "swing_breakout",
        "feature_set_variant": "default",
        "validation_review": {"overall_status": "pass"},
        "promotion_status": "insufficient_data",
        "evaluation": {"validation_auc": 0.61, "precision_at_10pct": 0.42},
        "walkforward_summary": {"avg_validation_auc": 0.60},
    }
    candidate_b = {
        "recipe_name": "breakout_20d",
        "model_id": "model-b",
        "model_name": "breakout_sector_alpha",
        "model_version": "h20_v1",
        "strategy_tag": "position_breakout",
        "feature_set_variant": "sector_plus",
        "validation_review": {"overall_status": "pass"},
        "promotion_status": "insufficient_data",
        "evaluation": {"validation_auc": 0.63, "precision_at_10pct": 0.41},
        "walkforward_summary": {"avg_validation_auc": 0.64},
    }
    winner = pick_bundle_winner(bundle, [candidate_a, candidate_b])
    report_path = write_recipe_bundle_summary(
        tmp_path,
        "daily_research",
        {
            "bundle_name": "daily_research",
            "executed_at": "2026-04-05T11:00:00+00:00",
            "selection_metric": "walkforward_avg_validation_auc",
            "winner": winner,
            "candidates": [candidate_a, candidate_b],
        },
    )

    bundle_results = load_recipe_bundle_results(tmp_path)

    assert not bundles.empty
    assert bundles.iloc[0]["bundle_name"] == "daily_research"
    assert winner["model_id"] == "model-b"
    assert report_path.exists()
    assert len(bundle_results) == 1
    assert bundle_results.iloc[0]["winner_model_id"] == "model-b"
    assert bundle_results.iloc[0]["winner_strategy_tag"] == "position_breakout"


def test_recipe_bundle_auto_actions_apply_only_to_winner(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    source_config = REPO_ROOT / "src" / "ai_trading_system" / "platform" / "config" / "research_recipes.toml"
    (tmp_path / "config" / "research_recipes.toml").write_text(source_config.read_text(encoding="utf-8"), encoding="utf-8")

    registry = RegistryStore(tmp_path)
    winner_model_id = registry.register_model(
        model_name="breakout_sector_alpha",
        model_version="h20_v1",
        artifact_uri=str(tmp_path / "models" / "winner.txt"),
        feature_schema_hash="schema-hash",
        train_snapshot_ref="research:training:winner",
        approval_status="pending",
        metadata={"horizon": 20},
    )
    loser_model_id = registry.register_model(
        model_name="breakout_sector_alpha",
        model_version="h5_v1",
        artifact_uri=str(tmp_path / "models" / "loser.txt"),
        feature_schema_hash="schema-hash",
        train_snapshot_ref="research:training:loser",
        approval_status="pending",
        metadata={"horizon": 5},
    )

    calls: list[tuple[str, bool | None, bool | None]] = []

    def _fake_execute_recipe(recipe_name: str, *, project_root=None, auto_approve=None, auto_deploy=None):
        calls.append((recipe_name, auto_approve, auto_deploy))
        if recipe_name == "breakout_20d":
            return {
                "recipe_name": recipe_name,
                "model_id": winner_model_id,
                "model_name": "breakout_sector_alpha",
                "model_version": "h20_v1",
                "validation_review": {"overall_status": "pass"},
                "promotion_status": "pass",
                "approval_status": "pending",
                "evaluation": {"validation_auc": 0.63, "precision_at_10pct": 0.41},
                "walkforward_summary": {"avg_validation_auc": 0.64},
                "shadow_environment": "operational_shadow_20d",
            }
        return {
            "recipe_name": recipe_name,
            "model_id": loser_model_id,
            "model_name": "breakout_sector_alpha",
            "model_version": "h5_v1",
            "validation_review": {"overall_status": "pass"},
            "promotion_status": "fail",
            "approval_status": "pending",
            "evaluation": {"validation_auc": 0.61, "precision_at_10pct": 0.42},
            "walkforward_summary": {"avg_validation_auc": 0.60},
            "shadow_environment": "operational_shadow_5d",
        }

    monkeypatch.setattr(run_recipe_module, "execute_recipe", _fake_execute_recipe)

    summary = run_recipe_module.execute_recipe_bundle(
        "daily_research",
        project_root=tmp_path,
        auto_approve=True,
        auto_deploy=True,
    )

    winner_record = registry.get_model_record(winner_model_id)
    loser_record = registry.get_model_record(loser_model_id)
    active_20d = registry.get_active_deployment("operational_shadow_20d")
    active_5d = registry.get_active_deployment("operational_shadow_5d")

    assert calls == [("breakout_5d", False, False), ("breakout_20d", False, False)]
    assert summary["winner"]["model_id"] == winner_model_id
    assert winner_record["approval_status"] == "approved"
    assert loser_record["approval_status"] == "pending"
    assert active_20d is not None and active_20d["model_id"] == winner_model_id
    assert active_5d is None


def test_workbench_recipe_and_bundle_save_delete_roundtrip(tmp_path: Path) -> None:
    config_path = workbench_recipe_config_path(tmp_path)

    save_workbench_recipe(
        project_root=tmp_path,
        recipe_name="custom_recipe_5d",
        description="Custom recipe",
        strategy_tag="swing_breakout",
        feature_set_variant="volume_plus",
        experiment_notes="Testing volume expansion features",
        engine="lightgbm",
        horizon=5,
        from_date="2019-01-01",
        to_date="2025-12-31",
        dataset_name="custom_dataset_5d",
        model_name="custom_alpha",
        model_version="v1",
        validation_fraction=0.2,
        progress_interval=25,
        min_train_years=5,
        shadow_environment="operational_shadow_5d",
        auto_approve=False,
        auto_deploy=False,
        min_validation_auc=0.56,
        min_walkforward_auc=0.57,
        min_precision_at_10pct=0.36,
    )
    recipes = load_workbench_recipes(tmp_path)
    assert config_path.exists()
    assert "custom_recipe_5d" in recipes["recipe_name"].tolist()
    saved_recipe = recipes[recipes["recipe_name"] == "custom_recipe_5d"].iloc[0]
    assert saved_recipe["strategy_tag"] == "swing_breakout"
    assert saved_recipe["feature_set_variant"] == "volume_plus"

    save_workbench_recipe_bundle(
        project_root=tmp_path,
        bundle_name="custom_bundle",
        description="My bundle",
        recipes=["custom_recipe_5d"],
        selection_metric="validation_auc",
    )
    bundles = load_workbench_recipe_bundles(tmp_path)
    assert "custom_bundle" in bundles["bundle_name"].tolist()

    delete_workbench_recipe_bundle(project_root=tmp_path, bundle_name="custom_bundle")
    bundles_after_delete = load_workbench_recipe_bundles(tmp_path)
    if bundles_after_delete.empty:
        assert True
    else:
        assert "custom_bundle" not in bundles_after_delete["bundle_name"].tolist()

    delete_workbench_recipe(project_root=tmp_path, recipe_name="custom_recipe_5d")
    recipes_after_delete = load_workbench_recipes(tmp_path)
    if recipes_after_delete.empty:
        assert True
    else:
        assert "custom_recipe_5d" not in recipes_after_delete["recipe_name"].tolist()


def test_execution_loaders_and_latest_execute_summary(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    service.submit_order(
        OrderIntent(symbol_id="AAA", exchange="NSE", quantity=10, side="BUY"),
        market_price=100.0,
    )
    service.submit_order(
        OrderIntent(symbol_id="BBB", exchange="NSE", quantity=5, side="BUY"),
        market_price=200.0,
    )

    execute_dir = tmp_path / "data" / "pipeline_runs" / "pipeline-2026-04-05-ui" / "execute" / "attempt_1"
    execute_dir.mkdir(parents=True, exist_ok=True)
    (execute_dir / "trade_actions.csv").write_text(
        "action,symbol_id,exchange,side,strategy_mode,reason\nBUY,AAA,NSE,BUY,technical,target_entry\n",
        encoding="utf-8",
    )
    (execute_dir / "executed_orders.csv").write_text(
        "order_id,broker,symbol_id,status\norder-1,paper,AAA,FILLED\n",
        encoding="utf-8",
    )
    (execute_dir / "executed_fills.csv").write_text(
        "fill_id,order_id,symbol_id,price\nfill-1,order-1,AAA,100.0\n",
        encoding="utf-8",
    )
    (execute_dir / "positions.csv").write_text(
        "symbol_id,exchange,quantity,avg_entry_price,last_fill_price\nAAA,NSE,10,100.0,100.0\n",
        encoding="utf-8",
    )
    (execute_dir / "execute_summary.json").write_text(
        json.dumps(
            {
                "summary": {
                    "strategy_mode": "technical",
                    "actions_count": 1,
                    "order_count": 1,
                    "fill_count": 1,
                    "open_position_count": 1,
                },
                "run_date": "2026-04-05",
                "parameters": {"strategy_mode": "technical", "execution_preview": True, "execution_top_n": 5},
                "positions_before": [],
                "positions_after": [{"symbol_id": "AAA", "exchange": "NSE", "quantity": 10}],
            }
        ),
        encoding="utf-8",
    )

    orders = load_workbench_execution_orders(tmp_path)
    fills = load_workbench_execution_fills(tmp_path)
    positions = load_workbench_execution_positions(tmp_path)
    latest = load_latest_execute_run(tmp_path)

    assert not orders.empty
    assert not fills.empty
    assert not positions.empty
    assert set(positions["symbol_id"].tolist()) == {"AAA", "BBB"}
    assert latest["summary"]["actions_count"] == 1
    assert latest["trade_actions"].iloc[0]["symbol_id"] == "AAA"
    assert latest["run_date"] == "2026-04-05"
    assert latest["parameters"]["execution_preview"] is True


def test_trade_report_returns_realized_and_unrealized_pnl(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(data_dir / "ohlcv.duckdb"))
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
        conn.execute(
            "INSERT INTO _catalog VALUES ('AAA','NSE','2026-04-05 15:30:00',100,111,99,110,1000)"
        )
        conn.execute(
            "INSERT INTO _catalog VALUES ('BBB','NSE','2026-04-05 15:30:00',200,221,199,220,1000)"
        )
    finally:
        conn.close()

    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    service.submit_order(OrderIntent(symbol_id="AAA", exchange="NSE", quantity=10, side="BUY"), market_price=100.0)
    service.submit_order(OrderIntent(symbol_id="AAA", exchange="NSE", quantity=5, side="SELL"), market_price=110.0)
    service.submit_order(OrderIntent(symbol_id="BBB", exchange="NSE", quantity=4, side="BUY"), market_price=200.0)

    report = load_workbench_trade_report(tmp_path)

    assert report["summary"]["closed_trade_count"] == 1
    assert report["summary"]["open_positions"] == 2
    assert report["summary"]["realized_pnl"] == 50.0
    assert report["summary"]["unrealized_pnl"] == 130.0
    open_positions = report["open_positions"].set_index("symbol_id")
    assert float(open_positions.loc["AAA", "unrealized_pnl"]) == 50.0
    assert float(open_positions.loc["BBB", "unrealized_pnl"]) == 80.0


def test_execution_workbench_settings_roundtrip(tmp_path: Path) -> None:
    defaults = load_execution_workbench_settings(tmp_path)
    assert defaults["execution_enabled"] is False
    assert defaults["default_preview_only"] is True

    saved = save_execution_workbench_settings(
        project_root=tmp_path,
        settings={
            "execution_enabled": True,
            "default_strategy_mode": "hybrid_confirm",
            "default_execution_top_n": 7,
        },
    )
    reloaded = load_execution_workbench_settings(tmp_path)

    assert saved["execution_enabled"] is True
    assert reloaded["execution_enabled"] is True
    assert reloaded["default_strategy_mode"] == "hybrid_confirm"
    assert reloaded["default_execution_top_n"] == 7


def test_control_center_create_task_sets_task_metadata_without_duplicate_task_id_error(tmp_path: Path) -> None:
    task_id = _create_task("pipeline", "Smoke UI task", {"foo": "bar"}, project_root=tmp_path)

    assert task_id.startswith("task-")


def test_list_operator_tasks_normalizes_missing_task_id_field() -> None:
    _TASKS["task-legacy"] = {
        "label": "Legacy task",
        "task_type": "pipeline",
        "status": "running",
        "started_at": "2026-04-05 18:00:00",
    }

    tasks = list_operator_tasks()
    legacy = next(task for task in tasks if task["task_id"] == "task-legacy")

    assert legacy["label"] == "Legacy task"
    assert legacy["task_type"] == "pipeline"
