"""One-command research recipe runner for validation-focused ML experiments."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from analytics.alpha.dataset_builder import AlphaDatasetBuilder
from analytics.alpha.policy import PromotionThresholds, evaluate_promotion_candidate
from analytics.alpha.training import train_and_register_model
from analytics.registry import RegistryStore
from research.recipes import (
    build_validation_review,
    get_recipe,
    get_recipe_bundle,
    pick_bundle_winner,
    write_recipe_summary,
    write_recipe_bundle_summary,
)
from research.train_pipeline import build_engine
from utils.data_domains import ensure_domain_layout
from utils.logger import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a simplified research recipe")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--recipe", help="Recipe name from config/research_recipes.toml")
    group.add_argument("--bundle", help="Bundle name from config/research_recipes.toml")
    parser.add_argument("--auto-approve", action="store_true", help="Approve the model when promotion gates pass")
    parser.add_argument("--auto-deploy", action="store_true", help="Deploy to the recipe shadow environment when gates pass")
    return parser


def execute_recipe(
    recipe_name: str,
    *,
    project_root: str | Path | None = None,
    auto_approve: bool | None = None,
    auto_deploy: bool | None = None,
) -> Dict[str, Any]:
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[1]
    recipe = get_recipe(recipe_name, root)
    paths = ensure_domain_layout(project_root=root, data_domain="research")
    engine = build_engine(recipe.engine, paths=paths)
    builder = AlphaDatasetBuilder(project_root=root, data_domain="research")
    registry = RegistryStore(root)

    prepared = builder.prepare(
        engine=engine,
        dataset_name=recipe.dataset_name,
        from_date=recipe.from_date,
        to_date=recipe.to_date,
        horizon=recipe.horizon,
        validation_fraction=recipe.validation_fraction,
        register_dataset=True,
        extra_metadata={
            "strategy_tag": recipe.strategy_tag,
            "feature_set_variant": recipe.feature_set_variant,
            "experiment_notes": recipe.experiment_notes,
            "recipe_name": recipe.name,
        },
    )
    training_df, dataset_meta = AlphaDatasetBuilder.load_prepared_dataset(prepared.dataset_path)

    trained = train_and_register_model(
        engine=engine,
        registry=registry,
        training_df=training_df,
        dataset_meta=dataset_meta,
        horizon=recipe.horizon,
        model_name=recipe.model_name,
        model_version=recipe.model_version,
        progress_interval=recipe.progress_interval,
        min_train_years=recipe.min_train_years,
        extra_metadata={
            "strategy_tag": recipe.strategy_tag,
            "feature_set_variant": recipe.feature_set_variant,
            "experiment_notes": recipe.experiment_notes,
            "recipe_name": recipe.name,
        },
    )

    validation_review = build_validation_review(recipe, trained)
    promotion_review = evaluate_promotion_candidate(
        registry=registry,
        model_id=trained["model_id"],
        horizon=recipe.horizon,
        deployment_mode="shadow_ml",
        lookback_days=60,
        thresholds=PromotionThresholds(
            min_validation_auc=recipe.validation.min_validation_auc,
            min_walkforward_auc=recipe.validation.min_walkforward_auc,
        ),
    )
    registry.record_promotion_gate_results(trained["model_id"], promotion_review["gate_results"])

    should_approve = bool(recipe.auto_approve if auto_approve is None else auto_approve)
    should_deploy = bool(recipe.auto_deploy if auto_deploy is None else auto_deploy)
    approval_status = registry.get_model_record(trained["model_id"])["approval_status"]
    deployment_result: Dict[str, Any] | None = None

    if should_approve and promotion_review["overall_status"] == "pass":
        registry.approve_model(trained["model_id"])
        approval_status = "approved"

    if should_deploy and promotion_review["overall_status"] == "pass":
        if approval_status != "approved":
            registry.approve_model(trained["model_id"])
            approval_status = "approved"
        deployment_id = registry.deploy_model(
            trained["model_id"],
            environment=recipe.shadow_environment,
            approved_by="recipe-runner",
            notes=f"Auto-deployed from recipe {recipe.name}",
        )
        deployment_result = {
            "deployment_id": deployment_id,
            "environment": recipe.shadow_environment,
            "active_deployment": registry.get_active_deployment(recipe.shadow_environment),
        }

    summary = {
        "recipe_name": recipe.name,
        "description": recipe.description,
        "strategy_tag": recipe.strategy_tag,
        "feature_set_variant": recipe.feature_set_variant,
        "experiment_notes": recipe.experiment_notes,
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "dataset_ref": dataset_meta["dataset_ref"],
        "dataset_uri": dataset_meta.get("dataset_uri", prepared.dataset_path),
        "model_id": trained["model_id"],
        "model_name": recipe.model_name,
        "model_version": recipe.model_version,
        "engine": recipe.engine,
        "horizon": recipe.horizon,
        "validation_review": validation_review,
        "evaluation": trained.get("evaluation", {}),
        "walkforward_summary": (trained.get("walkforward", {}) or {}).get("summary", {}),
        "promotion_status": promotion_review["overall_status"],
        "promotion_gates": promotion_review["gate_results"],
        "shadow_summary": promotion_review.get("shadow_summary", {}),
        "approval_status": approval_status,
        "deployment": deployment_result,
        "shadow_environment": recipe.shadow_environment,
    }
    report_path = write_recipe_summary(root, recipe.name, summary)
    summary["report_path"] = str(report_path)
    return summary


def execute_recipe_bundle(
    bundle_name: str,
    *,
    project_root: str | Path | None = None,
    auto_approve: bool | None = None,
    auto_deploy: bool | None = None,
) -> Dict[str, Any]:
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[1]
    bundle = get_recipe_bundle(bundle_name, root)
    registry = RegistryStore(root)
    summaries = [
        execute_recipe(
            recipe_name,
            project_root=root,
            auto_approve=False,
            auto_deploy=False,
        )
        for recipe_name in bundle.recipes
    ]
    winner = dict(pick_bundle_winner(bundle, summaries))
    should_approve = bool(auto_approve)
    should_deploy = bool(auto_deploy)

    if should_approve and winner.get("promotion_status") == "pass":
        registry.approve_model(winner["model_id"])
        winner["approval_status"] = "approved"

    if should_deploy and winner.get("promotion_status") == "pass":
        if winner.get("approval_status") != "approved":
            registry.approve_model(winner["model_id"])
            winner["approval_status"] = "approved"
        deployment_id = registry.deploy_model(
            winner["model_id"],
            environment=str(winner["shadow_environment"]),
            approved_by="recipe-runner",
            notes=f"Auto-deployed bundle winner from {bundle.name}",
        )
        winner["deployment"] = {
            "deployment_id": deployment_id,
            "environment": str(winner["shadow_environment"]),
            "active_deployment": registry.get_active_deployment(str(winner["shadow_environment"])),
        }

    updated_summaries = [
        winner if summary.get("model_id") == winner.get("model_id") else summary
        for summary in summaries
    ]
    bundle_summary = {
        "bundle_name": bundle.name,
        "description": bundle.description,
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "selection_metric": bundle.selection_metric,
        "winner": winner,
        "candidates": updated_summaries,
    }
    report_path = write_recipe_bundle_summary(root, bundle.name, bundle_summary)
    bundle_summary["report_path"] = str(report_path)
    return bundle_summary


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    if args.bundle:
        bundle = get_recipe_bundle(args.bundle, project_root)
        with log_context(run_id=f"bundle-{bundle.name}", stage_name="recipe_bundle"):
            summary = execute_recipe_bundle(
                bundle.name,
                project_root=project_root,
                auto_approve=args.auto_approve,
                auto_deploy=args.auto_deploy,
            )
            winner = summary["winner"]
            logger.info(
                "Recipe bundle complete bundle=%s winner_recipe=%s model_id=%s validation_status=%s report=%s",
                bundle.name,
                winner["recipe_name"],
                winner["model_id"],
                (winner.get("validation_review") or {}).get("overall_status"),
                summary["report_path"],
            )
        return

    recipe = get_recipe(args.recipe, project_root)
    with log_context(run_id=f"recipe-{recipe.name}", stage_name="recipe"):
        summary = execute_recipe(
            recipe.name,
            project_root=project_root,
            auto_approve=args.auto_approve,
            auto_deploy=args.auto_deploy,
        )
        logger.info(
            "Recipe run complete recipe=%s model_id=%s validation_status=%s promotion_status=%s report=%s",
            recipe.name,
            summary["model_id"],
            summary["validation_review"]["overall_status"],
            summary["promotion_status"],
            summary["report_path"],
        )


if __name__ == "__main__":
    main()
