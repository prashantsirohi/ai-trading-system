"""Shared helpers for the standalone ML workbench UI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import duckdb
import pandas as pd

from analytics.registry import RegistryStore
from execution import ExecutionStore, PortfolioManager
from research.recipes import (
    delete_recipe,
    delete_recipe_bundle,
    load_recipe_bundle_catalog,
    load_recipe_catalog,
    recipe_config_path,
    save_recipe,
    save_recipe_bundle,
)
from utils.data_domains import ensure_domain_layout


def _project_root(project_root: str | Path | None = None) -> Path:
    return Path(project_root) if project_root else Path(__file__).resolve().parents[2]


def _execution_settings_path(project_root: str | Path | None = None) -> Path:
    return _project_root(project_root) / "config" / "execution_workbench.json"


def load_execution_workbench_settings(project_root: str | Path | None = None) -> Dict[str, Any]:
    path = _execution_settings_path(project_root)
    default = {
        "execution_enabled": False,
        "default_strategy_mode": "technical",
        "default_ml_mode": "baseline_only",
        "default_execution_top_n": 5,
        "default_ml_horizon": 5,
        "default_ml_confirm_threshold": 0.55,
        "default_execution_capital": 1_000_000.0,
        "default_fixed_quantity_enabled": False,
        "default_execution_fixed_quantity": 10,
        "default_paper_slippage_bps": 5.0,
        "default_preview_only": True,
    }
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return {**default, **payload}


def save_execution_workbench_settings(
    *,
    project_root: str | Path | None = None,
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    path = _execution_settings_path(project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {**load_execution_workbench_settings(project_root), **settings}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def load_workbench_recipes(project_root: str | Path | None = None) -> pd.DataFrame:
    catalog = load_recipe_catalog(_project_root(project_root))
    rows = [
        {
            "recipe_name": recipe.name,
            "description": recipe.description,
            "strategy_tag": recipe.strategy_tag,
            "feature_set_variant": recipe.feature_set_variant,
            "experiment_notes": recipe.experiment_notes,
            "engine": recipe.engine,
            "horizon": recipe.horizon,
            "from_date": recipe.from_date,
            "to_date": recipe.to_date,
            "model_name": recipe.model_name,
            "model_version": recipe.model_version,
            "shadow_environment": recipe.shadow_environment,
            "auto_approve": recipe.auto_approve,
            "auto_deploy": recipe.auto_deploy,
            "min_validation_auc": recipe.validation.min_validation_auc,
            "min_walkforward_auc": recipe.validation.min_walkforward_auc,
            "min_precision_at_10pct": recipe.validation.min_precision_at_10pct,
        }
        for recipe in catalog.values()
    ]
    return pd.DataFrame(rows)


def workbench_recipe_config_path(project_root: str | Path | None = None) -> Path:
    return recipe_config_path(_project_root(project_root))


def save_workbench_recipe(
    *,
    project_root: str | Path | None = None,
    recipe_name: str,
    description: str,
    strategy_tag: str,
    feature_set_variant: str,
    experiment_notes: str,
    engine: str,
    horizon: int,
    from_date: str,
    to_date: str,
    dataset_name: str,
    model_name: str,
    model_version: str,
    validation_fraction: float,
    progress_interval: int,
    min_train_years: int,
    shadow_environment: str,
    auto_approve: bool,
    auto_deploy: bool,
    min_validation_auc: float,
    min_walkforward_auc: float,
    min_precision_at_10pct: float,
) -> Dict[str, Any]:
    config_path = save_recipe(
        project_root=_project_root(project_root),
        recipe_name=recipe_name,
        description=description,
        strategy_tag=strategy_tag,
        feature_set_variant=feature_set_variant,
        experiment_notes=experiment_notes,
        engine=engine,
        horizon=horizon,
        from_date=from_date,
        to_date=to_date,
        dataset_name=dataset_name,
        model_name=model_name,
        model_version=model_version,
        validation_fraction=validation_fraction,
        progress_interval=progress_interval,
        min_train_years=min_train_years,
        shadow_environment=shadow_environment,
        auto_approve=auto_approve,
        auto_deploy=auto_deploy,
        min_validation_auc=min_validation_auc,
        min_walkforward_auc=min_walkforward_auc,
        min_precision_at_10pct=min_precision_at_10pct,
    )
    return {"config_path": str(config_path), "recipe_name": recipe_name}


def delete_workbench_recipe(
    *,
    project_root: str | Path | None = None,
    recipe_name: str,
) -> Dict[str, Any]:
    config_path = delete_recipe(project_root=_project_root(project_root), recipe_name=recipe_name)
    return {"config_path": str(config_path), "recipe_name": recipe_name}


def load_workbench_recipe_bundles(project_root: str | Path | None = None) -> pd.DataFrame:
    bundles = load_recipe_bundle_catalog(_project_root(project_root))
    rows = [
        {
            "bundle_name": bundle.name,
            "description": bundle.description,
            "recipes": ", ".join(bundle.recipes),
            "selection_metric": bundle.selection_metric,
        }
        for bundle in bundles.values()
    ]
    return pd.DataFrame(rows)


def save_workbench_recipe_bundle(
    *,
    project_root: str | Path | None = None,
    bundle_name: str,
    description: str,
    recipes: list[str] | tuple[str, ...],
    selection_metric: str,
) -> Dict[str, Any]:
    config_path = save_recipe_bundle(
        project_root=_project_root(project_root),
        bundle_name=bundle_name,
        description=description,
        recipes=recipes,
        selection_metric=selection_metric,
    )
    return {"config_path": str(config_path), "bundle_name": bundle_name}


def delete_workbench_recipe_bundle(
    *,
    project_root: str | Path | None = None,
    bundle_name: str,
) -> Dict[str, Any]:
    config_path = delete_recipe_bundle(project_root=_project_root(project_root), bundle_name=bundle_name)
    return {"config_path": str(config_path), "bundle_name": bundle_name}


def load_recipe_results(
    project_root: str | Path | None = None,
    *,
    latest_only: bool = True,
    limit: int = 50,
) -> pd.DataFrame:
    root = _project_root(project_root)
    recipe_root = root / "reports" / "research" / "recipes"
    if not recipe_root.exists():
        return pd.DataFrame()

    if latest_only:
        candidate_paths = sorted(recipe_root.glob("*/latest.json"), reverse=True)
    else:
        candidate_paths = sorted(recipe_root.glob("*/*.json"), reverse=True)

    rows: list[dict[str, Any]] = []
    for path in candidate_paths[:limit]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        validation = payload.get("validation_review", {}) or {}
        evaluation = payload.get("evaluation", {}) or {}
        walkforward = payload.get("walkforward_summary", {}) or {}
        shadow_summary = payload.get("shadow_summary", {}) or {}
        deployment = payload.get("deployment", {}) or {}
        rows.append(
            {
                "recipe_name": payload.get("recipe_name"),
                "executed_at": payload.get("executed_at"),
                "model_id": payload.get("model_id"),
                "model_name": payload.get("model_name"),
                "model_version": payload.get("model_version"),
                "engine": payload.get("engine"),
                "horizon": payload.get("horizon"),
                "strategy_tag": payload.get("strategy_tag"),
                "feature_set_variant": payload.get("feature_set_variant"),
                "experiment_notes": payload.get("experiment_notes"),
                "dataset_ref": payload.get("dataset_ref"),
                "validation_status": validation.get("overall_status"),
                "promotion_status": payload.get("promotion_status"),
                "approval_status": payload.get("approval_status"),
                "validation_auc": evaluation.get("validation_auc"),
                "precision_at_10pct": evaluation.get("precision_at_10pct"),
                "walkforward_avg_validation_auc": walkforward.get("avg_validation_auc"),
                "shadow_top_decile_hit_rate": shadow_summary.get("top_decile_hit_rate"),
                "shadow_top_decile_avg_return": shadow_summary.get("top_decile_avg_return"),
                "shadow_matured_rows": shadow_summary.get("matured_rows"),
                "deployment_environment": deployment.get("environment") or payload.get("shadow_environment"),
                "deployment_id": deployment.get("deployment_id"),
                "report_path": payload.get("report_path", str(path)),
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    registry = RegistryStore(root)
    active_deployments = pd.DataFrame(registry.list_deployments(limit=200, status="active"))
    if not active_deployments.empty:
        active = active_deployments[["model_id", "environment"]].rename(columns={"environment": "active_environment"})
        frame = frame.merge(active, on="model_id", how="left")
    else:
        frame["active_environment"] = None
    return frame.sort_values("executed_at", ascending=False)


def load_recipe_bundle_results(
    project_root: str | Path | None = None,
    *,
    latest_only: bool = True,
    limit: int = 20,
) -> pd.DataFrame:
    root = _project_root(project_root)
    bundle_root = root / "reports" / "research" / "recipe_bundles"
    if not bundle_root.exists():
        return pd.DataFrame()

    if latest_only:
        candidate_paths = sorted(bundle_root.glob("*/latest.json"), reverse=True)
    else:
        candidate_paths = sorted(bundle_root.glob("*/*.json"), reverse=True)

    rows: list[dict[str, Any]] = []
    for path in candidate_paths[:limit]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        winner = payload.get("winner") or {}
        winner_validation = winner.get("validation_review", {}) or {}
        winner_evaluation = winner.get("evaluation", {}) or {}
        winner_walkforward = winner.get("walkforward_summary", {}) or {}
        rows.append(
            {
                "bundle_name": payload.get("bundle_name"),
                "executed_at": payload.get("executed_at"),
                "selection_metric": payload.get("selection_metric"),
                "winner_recipe_name": winner.get("recipe_name"),
                "winner_model_id": winner.get("model_id"),
                "winner_model_name": winner.get("model_name"),
                "winner_model_version": winner.get("model_version"),
                "winner_strategy_tag": winner.get("strategy_tag"),
                "winner_feature_set_variant": winner.get("feature_set_variant"),
                "winner_validation_status": winner_validation.get("overall_status"),
                "winner_promotion_status": winner.get("promotion_status"),
                "winner_validation_auc": winner_evaluation.get("validation_auc"),
                "winner_walkforward_avg_validation_auc": winner_walkforward.get("avg_validation_auc"),
                "winner_precision_at_10pct": winner_evaluation.get("precision_at_10pct"),
                "candidate_count": len(payload.get("candidates") or []),
                "report_path": payload.get("report_path", str(path)),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values("executed_at", ascending=False)


def load_workbench_datasets(
    project_root: str | Path | None = None,
    *,
    limit: int = 100,
) -> pd.DataFrame:
    registry = RegistryStore(_project_root(project_root))
    rows = registry.list_datasets(limit=limit, data_domain="research")
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    metadata = frame.pop("metadata").apply(lambda value: value or {})
    frame["validation_start"] = metadata.apply(lambda item: item.get("validation_start"))
    frame["validation_fraction"] = metadata.apply(lambda item: item.get("validation_fraction"))
    frame["strategy_tag"] = metadata.apply(lambda item: item.get("strategy_tag"))
    frame["feature_set_variant"] = metadata.apply(lambda item: item.get("feature_set_variant"))
    frame["recipe_name"] = metadata.apply(lambda item: item.get("recipe_name"))
    return frame


def approve_workbench_model(
    model_id: str,
    project_root: str | Path | None = None,
) -> Dict[str, Any]:
    registry = RegistryStore(_project_root(project_root))
    before = registry.get_model_record(model_id)
    registry.approve_model(model_id)
    after = registry.get_model_record(model_id)
    return {"before": before, "after": after}


def load_workbench_models(
    project_root: str | Path | None = None,
    *,
    limit: int = 100,
) -> pd.DataFrame:
    registry = RegistryStore(_project_root(project_root))
    rows = registry.list_models(limit=limit)
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    metadata = frame.pop("metadata").apply(lambda value: value or {})
    frame["engine"] = metadata.apply(lambda item: item.get("engine"))
    frame["horizon"] = metadata.apply(lambda item: item.get("horizon"))
    frame["strategy_tag"] = metadata.apply(lambda item: item.get("strategy_tag"))
    frame["feature_set_variant"] = metadata.apply(lambda item: item.get("feature_set_variant"))
    frame["recipe_name"] = metadata.apply(lambda item: item.get("recipe_name"))
    frame["validation_auc"] = metadata.apply(lambda item: (item.get("evaluation") or {}).get("validation_auc"))
    frame["precision_at_10pct"] = metadata.apply(lambda item: (item.get("evaluation") or {}).get("precision_at_10pct"))
    frame["walkforward_avg_validation_auc"] = metadata.apply(
        lambda item: (item.get("walkforward_summary") or {}).get("avg_validation_auc")
    )
    return frame


def load_workbench_deployments(
    project_root: str | Path | None = None,
    *,
    limit: int = 50,
) -> pd.DataFrame:
    registry = RegistryStore(_project_root(project_root))
    deployments = pd.DataFrame(registry.list_deployments(limit=limit))
    if deployments.empty:
        return deployments

    models = pd.DataFrame(registry.list_models(limit=500))
    if models.empty:
        return deployments

    model_cols = ["model_id", "model_name", "model_version", "approval_status"]
    merged = deployments.merge(models[model_cols], on="model_id", how="left")
    return merged


def deploy_workbench_model(
    model_id: str,
    *,
    environment: str,
    approved_by: str,
    notes: str | None = None,
    project_root: str | Path | None = None,
) -> Dict[str, Any]:
    registry = RegistryStore(_project_root(project_root))
    deployment_id = registry.deploy_model(
        model_id=model_id,
        environment=environment,
        approved_by=approved_by,
        notes=notes,
    )
    return {
        "deployment_id": deployment_id,
        "active_deployment": registry.get_active_deployment(environment),
    }


def rollback_workbench_deployment(
    *,
    environment: str,
    approved_by: str,
    notes: str | None = None,
    project_root: str | Path | None = None,
) -> Dict[str, Any]:
    registry = RegistryStore(_project_root(project_root))
    deployment_id = registry.rollback_model_deployment(
        environment=environment,
        approved_by=approved_by,
        notes=notes,
    )
    return {
        "deployment_id": deployment_id,
        "active_deployment": registry.get_active_deployment(environment),
    }


def load_model_workbench_detail(
    model_id: str,
    project_root: str | Path | None = None,
    *,
    lookback_days: int = 60,
) -> Dict[str, Any]:
    registry = RegistryStore(_project_root(project_root))
    model_record = registry.get_model_record(model_id)
    metadata = model_record.get("metadata", {}) or {}
    horizon = metadata.get("horizon")

    detail: Dict[str, Any] = {
        "model": model_record,
        "metadata": metadata,
        "evaluations": pd.DataFrame(registry.get_model_evals(model_id)),
        "drift_metrics": pd.DataFrame(registry.get_latest_drift_metrics(model_id=model_id)),
        "promotion_gates": pd.DataFrame(registry.get_promotion_gate_results(model_id)),
        "deployments": pd.DataFrame(
            [row for row in registry.list_deployments(limit=100) if row.get("model_id") == model_id]
        ),
        "monitor_summary": {},
    }
    if horizon is not None:
        detail["monitor_summary"] = registry.get_prediction_monitor_summary(
            model_id=model_id,
            horizon=int(horizon),
            deployment_mode="shadow_ml",
            lookback_days=lookback_days,
        )
    return detail


def load_workbench_execution_orders(
    project_root: str | Path | None = None,
    *,
    limit: int = 100,
) -> pd.DataFrame:
    store = ExecutionStore(_project_root(project_root))
    rows = store.list_orders()
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.head(limit)


def load_workbench_execution_fills(
    project_root: str | Path | None = None,
    *,
    limit: int = 100,
) -> pd.DataFrame:
    store = ExecutionStore(_project_root(project_root))
    rows = store.list_fills()
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.head(limit)


def load_workbench_execution_positions(
    project_root: str | Path | None = None,
) -> pd.DataFrame:
    store = ExecutionStore(_project_root(project_root))
    portfolio = PortfolioManager(store)
    rows = portfolio.open_positions_frame()
    return pd.DataFrame(rows)


def load_workbench_trade_report(
    project_root: str | Path | None = None,
    *,
    data_domain: str = "operational",
) -> Dict[str, Any]:
    root = _project_root(project_root)
    store = ExecutionStore(root)
    fills = pd.DataFrame(store.list_fills())
    if fills.empty:
        return {
            "summary": {
                "open_positions": 0,
                "closed_trade_count": 0,
                "win_rate": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
            },
            "open_positions": pd.DataFrame(),
            "closed_trades": pd.DataFrame(),
            "fills": pd.DataFrame(),
        }

    fills["filled_at"] = pd.to_datetime(fills["filled_at"], errors="coerce")
    fills = fills.sort_values(["filled_at", "fill_id"]).reset_index(drop=True)
    latest_prices = _load_latest_prices(root, data_domain=data_domain)

    open_rows: list[dict[str, Any]] = []
    closed_rows: list[dict[str, Any]] = []
    for (symbol_id, exchange), group in fills.groupby(["symbol_id", "exchange"], sort=True):
        quantity = 0
        avg_cost = 0.0
        for row in group.to_dict(orient="records"):
            fill_qty = int(row.get("quantity") or 0)
            fill_price = float(row.get("price") or 0.0)
            side = str(row.get("side", "BUY")).upper()
            filled_at = row.get("filled_at")
            if side == "BUY":
                new_qty = quantity + fill_qty
                if new_qty > 0:
                    avg_cost = ((quantity * avg_cost) + (fill_qty * fill_price)) / new_qty
                quantity = new_qty
            else:
                closed_qty = min(quantity, fill_qty)
                realized_pnl = (fill_price - avg_cost) * closed_qty
                closed_rows.append(
                    {
                        "symbol_id": symbol_id,
                        "exchange": exchange,
                        "closed_quantity": closed_qty,
                        "entry_avg_price": round(avg_cost, 4),
                        "exit_price": round(fill_price, 4),
                        "realized_pnl": round(realized_pnl, 2),
                        "filled_at": filled_at.isoformat() if pd.notna(filled_at) else None,
                        "status": "win" if realized_pnl > 0 else "loss" if realized_pnl < 0 else "flat",
                    }
                )
                quantity = max(0, quantity - fill_qty)
                if quantity == 0:
                    avg_cost = 0.0

        if quantity > 0:
            current_price = latest_prices.get((str(symbol_id), str(exchange)))
            market_value = (current_price or 0.0) * quantity if current_price is not None else None
            unrealized = ((current_price - avg_cost) * quantity) if current_price is not None else None
            open_rows.append(
                {
                    "symbol_id": symbol_id,
                    "exchange": exchange,
                    "quantity": int(quantity),
                    "avg_entry_price": round(avg_cost, 4),
                    "current_price": round(float(current_price), 4) if current_price is not None else None,
                    "market_value": round(float(market_value), 2) if market_value is not None else None,
                    "unrealized_pnl": round(float(unrealized), 2) if unrealized is not None else None,
                    "return_pct": round(((current_price / avg_cost) - 1) * 100, 2) if current_price is not None and avg_cost else None,
                }
            )

    open_df = pd.DataFrame(open_rows).sort_values("unrealized_pnl", ascending=False) if open_rows else pd.DataFrame()
    closed_df = pd.DataFrame(closed_rows).sort_values("filled_at", ascending=False) if closed_rows else pd.DataFrame()
    realized_pnl = float(closed_df["realized_pnl"].sum()) if not closed_df.empty else 0.0
    unrealized_pnl = float(open_df["unrealized_pnl"].fillna(0.0).sum()) if not open_df.empty else 0.0
    win_rate = (
        float((closed_df["realized_pnl"] > 0).sum()) / float(len(closed_df))
        if not closed_df.empty
        else 0.0
    )
    return {
        "summary": {
            "open_positions": int(len(open_df)),
            "closed_trade_count": int(len(closed_df)),
            "win_rate": round(win_rate, 4),
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "total_pnl": round(realized_pnl + unrealized_pnl, 2),
        },
        "open_positions": open_df,
        "closed_trades": closed_df,
        "fills": fills,
    }


def load_latest_execute_run(
    project_root: str | Path | None = None,
    *,
    data_domain: str = "operational",
) -> Dict[str, Any]:
    root = _project_root(project_root)
    paths = ensure_domain_layout(project_root=root, data_domain=data_domain)
    candidates = sorted(paths.pipeline_runs_dir.glob("*/execute/attempt_*/execute_summary.json"), reverse=True)
    if not candidates:
        return {"summary": {}, "trade_actions": pd.DataFrame(), "executed_orders": pd.DataFrame(), "executed_fills": pd.DataFrame(), "positions": pd.DataFrame()}

    summary_path = candidates[0]
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    attempt_dir = summary_path.parent
    frames: Dict[str, pd.DataFrame] = {}
    for artifact_name in ("trade_actions", "executed_orders", "executed_fills", "positions"):
        artifact_path = attempt_dir / f"{artifact_name}.csv"
        if artifact_path.exists():
            try:
                frames[artifact_name] = pd.read_csv(artifact_path)
            except Exception:
                frames[artifact_name] = pd.DataFrame()
        else:
            frames[artifact_name] = pd.DataFrame()
    return {
        "summary": payload.get("summary", {}),
        "run_date": payload.get("run_date"),
        "parameters": payload.get("parameters", {}),
        "positions_before": payload.get("positions_before", []),
        "positions_after": payload.get("positions_after", []),
        "report_path": str(summary_path),
        **frames,
    }


def _load_latest_prices(project_root: Path, *, data_domain: str = "operational") -> dict[tuple[str, str], float]:
    paths = ensure_domain_layout(project_root=project_root, data_domain=data_domain)
    if not paths.ohlcv_db_path.exists():
        return {}
    conn = duckdb.connect(str(paths.ohlcv_db_path))
    try:
        rows = conn.execute(
            """
            SELECT symbol_id, exchange, close
            FROM (
                SELECT
                    symbol_id,
                    exchange,
                    close,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol_id, exchange
                        ORDER BY timestamp DESC
                    ) AS rn
                FROM _catalog
            )
            WHERE rn = 1
            """
        ).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()
    return {
        (str(symbol_id), str(exchange)): float(close)
        for symbol_id, exchange, close in rows
        if close is not None
    }
