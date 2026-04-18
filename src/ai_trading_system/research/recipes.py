"""Recipe definitions and validation helpers for simplified research workflows."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import re
import tomllib
from typing import Any, Dict
import ai_trading_system.platform.config as platform_config


@dataclass(frozen=True)
class RecipeValidationConfig:
    min_validation_auc: float = 0.55
    min_walkforward_auc: float = 0.55
    min_precision_at_10pct: float = 0.35


@dataclass(frozen=True)
class ResearchRecipe:
    name: str
    description: str
    engine: str
    horizon: int
    from_date: str
    to_date: str
    dataset_name: str
    model_name: str
    model_version: str
    strategy_tag: str = "general"
    feature_set_variant: str = "default"
    experiment_notes: str = ""
    validation_fraction: float = 0.2
    progress_interval: int = 25
    min_train_years: int = 5
    shadow_environment: str = "operational_shadow_5d"
    auto_approve: bool = False
    auto_deploy: bool = False
    validation: RecipeValidationConfig = RecipeValidationConfig()


@dataclass(frozen=True)
class ResearchRecipeBundle:
    name: str
    description: str
    recipes: tuple[str, ...]
    selection_metric: str = "walkforward_avg_validation_auc"


def recipe_config_path(project_root: str | Path | None = None) -> Path:
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[3]
    legacy_path = root / "config" / "research_recipes.toml"
    if legacy_path.exists():
        return legacy_path
    platform_path = Path(platform_config.__file__).resolve().parent / "research_recipes.toml"
    return platform_path if project_root is None else legacy_path


def _load_recipe_payload(project_root: str | Path | None = None) -> Dict[str, Any]:
    config_path = recipe_config_path(project_root)
    if not config_path.exists():
        return {"recipes": {}, "bundles": {}}
    return tomllib.loads(config_path.read_text(encoding="utf-8"))


def _validate_config_name(name: str, kind: str) -> str:
    normalized = (name or "").strip()
    if not normalized:
        raise ValueError(f"{kind} name is required")
    if not re.fullmatch(r"[A-Za-z0-9_]+", normalized):
        raise ValueError(f"{kind} name must use only letters, numbers, and underscores")
    return normalized


def _toml_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml_literal(item) for item in value) + "]"
    return json.dumps(str(value))


def _serialize_recipe_payload(payload: Dict[str, Any]) -> str:
    lines: list[str] = []
    recipes = payload.get("recipes") or {}
    bundles = payload.get("bundles") or {}

    for recipe_name in sorted(recipes):
        recipe_payload = dict(recipes[recipe_name])
        validation_payload = dict(recipe_payload.pop("validation", {}) or {})
        lines.append(f"[recipes.{recipe_name}]")
        for key in (
            "description",
            "strategy_tag",
            "feature_set_variant",
            "experiment_notes",
            "engine",
            "horizon",
            "from_date",
            "to_date",
            "dataset_name",
            "model_name",
            "model_version",
            "validation_fraction",
            "progress_interval",
            "min_train_years",
            "shadow_environment",
            "auto_approve",
            "auto_deploy",
        ):
            if key in recipe_payload:
                lines.append(f"{key} = {_toml_literal(recipe_payload[key])}")
        lines.append("")
        lines.append(f"[recipes.{recipe_name}.validation]")
        for key in ("min_validation_auc", "min_walkforward_auc", "min_precision_at_10pct"):
            if key in validation_payload:
                lines.append(f"{key} = {_toml_literal(validation_payload[key])}")
        lines.append("")

    for bundle_name in sorted(bundles):
        bundle_payload = dict(bundles[bundle_name])
        lines.append(f"[bundles.{bundle_name}]")
        for key in ("description", "recipes", "selection_metric"):
            if key in bundle_payload:
                lines.append(f"{key} = {_toml_literal(bundle_payload[key])}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def save_recipe(
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
) -> Path:
    normalized = _validate_config_name(recipe_name, "Recipe")
    payload = _load_recipe_payload(project_root)
    recipes = dict(payload.get("recipes") or {})
    recipes[normalized] = {
        "description": description.strip(),
        "strategy_tag": strategy_tag.strip(),
        "feature_set_variant": feature_set_variant.strip(),
        "experiment_notes": experiment_notes.strip(),
        "engine": engine,
        "horizon": int(horizon),
        "from_date": from_date.strip(),
        "to_date": to_date.strip(),
        "dataset_name": dataset_name.strip(),
        "model_name": model_name.strip(),
        "model_version": model_version.strip(),
        "validation_fraction": float(validation_fraction),
        "progress_interval": int(progress_interval),
        "min_train_years": int(min_train_years),
        "shadow_environment": shadow_environment,
        "auto_approve": bool(auto_approve),
        "auto_deploy": bool(auto_deploy),
        "validation": {
            "min_validation_auc": float(min_validation_auc),
            "min_walkforward_auc": float(min_walkforward_auc),
            "min_precision_at_10pct": float(min_precision_at_10pct),
        },
    }
    payload["recipes"] = recipes
    payload["bundles"] = dict(payload.get("bundles") or {})
    config_path = recipe_config_path(project_root)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(_serialize_recipe_payload(payload), encoding="utf-8")
    return config_path


def delete_recipe(*, project_root: str | Path | None = None, recipe_name: str) -> Path:
    normalized = _validate_config_name(recipe_name, "Recipe")
    payload = _load_recipe_payload(project_root)
    recipes = dict(payload.get("recipes") or {})
    bundles = dict(payload.get("bundles") or {})
    recipes.pop(normalized, None)
    for bundle_name, bundle_payload in list(bundles.items()):
        scoped = [item for item in bundle_payload.get("recipes", []) if str(item) != normalized]
        bundles[bundle_name] = {**bundle_payload, "recipes": scoped}
    payload["recipes"] = recipes
    payload["bundles"] = bundles
    config_path = recipe_config_path(project_root)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(_serialize_recipe_payload(payload), encoding="utf-8")
    return config_path


def save_recipe_bundle(
    *,
    project_root: str | Path | None = None,
    bundle_name: str,
    description: str,
    recipes: list[str] | tuple[str, ...],
    selection_metric: str,
) -> Path:
    normalized = _validate_config_name(bundle_name, "Bundle")
    payload = _load_recipe_payload(project_root)
    recipe_catalog = dict(payload.get("recipes") or {})
    selected = [_validate_config_name(item, "Recipe") for item in recipes if str(item).strip()]
    if not selected:
        raise ValueError("Bundle must include at least one recipe")
    missing = [item for item in selected if item not in recipe_catalog]
    if missing:
        raise ValueError(f"Unknown recipes in bundle: {', '.join(sorted(missing))}")

    bundles = dict(payload.get("bundles") or {})
    bundles[normalized] = {
        "description": description.strip(),
        "recipes": selected,
        "selection_metric": selection_metric.strip(),
    }
    payload["recipes"] = recipe_catalog
    payload["bundles"] = bundles
    config_path = recipe_config_path(project_root)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(_serialize_recipe_payload(payload), encoding="utf-8")
    return config_path


def delete_recipe_bundle(*, project_root: str | Path | None = None, bundle_name: str) -> Path:
    normalized = _validate_config_name(bundle_name, "Bundle")
    payload = _load_recipe_payload(project_root)
    bundles = dict(payload.get("bundles") or {})
    bundles.pop(normalized, None)
    payload["recipes"] = dict(payload.get("recipes") or {})
    payload["bundles"] = bundles
    config_path = recipe_config_path(project_root)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(_serialize_recipe_payload(payload), encoding="utf-8")
    return config_path


def load_recipe_catalog(project_root: str | Path | None = None) -> Dict[str, ResearchRecipe]:
    config_path = recipe_config_path(project_root)
    payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
    catalog: Dict[str, ResearchRecipe] = {}
    for recipe_name, recipe_payload in (payload.get("recipes") or {}).items():
        validation_payload = recipe_payload.get("validation") or {}
        catalog[recipe_name] = ResearchRecipe(
            name=recipe_name,
            description=recipe_payload["description"],
            strategy_tag=str(recipe_payload.get("strategy_tag", "general")),
            feature_set_variant=str(recipe_payload.get("feature_set_variant", "default")),
            experiment_notes=str(recipe_payload.get("experiment_notes", "")),
            engine=recipe_payload["engine"],
            horizon=int(recipe_payload["horizon"]),
            from_date=recipe_payload["from_date"],
            to_date=recipe_payload["to_date"],
            dataset_name=recipe_payload["dataset_name"],
            model_name=recipe_payload["model_name"],
            model_version=recipe_payload["model_version"],
            validation_fraction=float(recipe_payload.get("validation_fraction", 0.2)),
            progress_interval=int(recipe_payload.get("progress_interval", 25)),
            min_train_years=int(recipe_payload.get("min_train_years", 5)),
            shadow_environment=recipe_payload.get("shadow_environment", "operational_shadow_5d"),
            auto_approve=bool(recipe_payload.get("auto_approve", False)),
            auto_deploy=bool(recipe_payload.get("auto_deploy", False)),
            validation=RecipeValidationConfig(
                min_validation_auc=float(validation_payload.get("min_validation_auc", 0.55)),
                min_walkforward_auc=float(validation_payload.get("min_walkforward_auc", 0.55)),
                min_precision_at_10pct=float(validation_payload.get("min_precision_at_10pct", 0.35)),
            ),
        )
    return catalog


def load_recipe_bundle_catalog(project_root: str | Path | None = None) -> Dict[str, ResearchRecipeBundle]:
    config_path = recipe_config_path(project_root)
    payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
    bundles: Dict[str, ResearchRecipeBundle] = {}
    for bundle_name, bundle_payload in (payload.get("bundles") or {}).items():
        bundles[bundle_name] = ResearchRecipeBundle(
            name=bundle_name,
            description=bundle_payload["description"],
            recipes=tuple(str(item) for item in bundle_payload.get("recipes", [])),
            selection_metric=str(bundle_payload.get("selection_metric", "walkforward_avg_validation_auc")),
        )
    return bundles


def get_recipe(name: str, project_root: str | Path | None = None) -> ResearchRecipe:
    catalog = load_recipe_catalog(project_root)
    if name not in catalog:
        available = ", ".join(sorted(catalog))
        raise KeyError(f"Unknown recipe '{name}'. Available recipes: {available}")
    return catalog[name]


def get_recipe_bundle(name: str, project_root: str | Path | None = None) -> ResearchRecipeBundle:
    bundles = load_recipe_bundle_catalog(project_root)
    if name not in bundles:
        available = ", ".join(sorted(bundles))
        raise KeyError(f"Unknown bundle '{name}'. Available bundles: {available}")
    return bundles[name]


def build_validation_review(recipe: ResearchRecipe, trained: Dict[str, Any]) -> Dict[str, Any]:
    evaluation = trained.get("evaluation", {}) or {}
    walkforward = (trained.get("walkforward", {}) or {}).get("summary", {}) or {}
    gates = [
        {
            "metric_name": "validation_auc",
            "metric_value": evaluation.get("validation_auc"),
            "threshold_value": recipe.validation.min_validation_auc,
        },
        {
            "metric_name": "walkforward_avg_validation_auc",
            "metric_value": walkforward.get("avg_validation_auc"),
            "threshold_value": recipe.validation.min_walkforward_auc,
        },
        {
            "metric_name": "precision_at_10pct",
            "metric_value": evaluation.get("precision_at_10pct"),
            "threshold_value": recipe.validation.min_precision_at_10pct,
        },
    ]

    results = []
    for gate in gates:
        metric_value = gate["metric_value"]
        threshold_value = gate["threshold_value"]
        if metric_value is None:
            status = "insufficient_data"
        else:
            status = "pass" if float(metric_value) >= float(threshold_value) else "fail"
        results.append(
            {
                "metric_name": gate["metric_name"],
                "metric_value": float(metric_value) if metric_value is not None else None,
                "threshold_value": float(threshold_value),
                "status": status,
            }
        )

    overall_status = "pass" if results and all(row["status"] == "pass" for row in results) else "fail"
    if any(row["status"] == "insufficient_data" for row in results):
        overall_status = "insufficient_data"
    return {"overall_status": overall_status, "gates": results}


def _status_score(status: Any) -> int:
    normalized = str(status or "").lower()
    if normalized == "pass":
        return 2
    if normalized == "insufficient_data":
        return 1
    return 0


def _summary_metric(summary: Dict[str, Any], metric_name: str) -> float:
    if metric_name == "walkforward_avg_validation_auc":
        value = ((summary.get("walkforward_summary") or {}).get("avg_validation_auc"))
    elif metric_name == "validation_auc":
        value = ((summary.get("evaluation") or {}).get("validation_auc"))
    elif metric_name == "precision_at_10pct":
        value = ((summary.get("evaluation") or {}).get("precision_at_10pct"))
    else:
        value = summary.get(metric_name)
    try:
        return float(value)
    except Exception:
        return float("-inf")


def pick_bundle_winner(
    bundle: ResearchRecipeBundle,
    summaries: list[Dict[str, Any]],
) -> Dict[str, Any]:
    if not summaries:
        raise ValueError(f"Bundle {bundle.name} produced no summaries")

    def _sort_key(summary: Dict[str, Any]) -> tuple[float, ...]:
        return (
            float(_status_score((summary.get("validation_review") or {}).get("overall_status"))),
            float(_status_score(summary.get("promotion_status"))),
            _summary_metric(summary, bundle.selection_metric),
            _summary_metric(summary, "validation_auc"),
            _summary_metric(summary, "precision_at_10pct"),
        )

    return max(summaries, key=_sort_key)


def recipe_report_dir(project_root: str | Path, recipe_name: str) -> Path:
    root = Path(project_root)
    return root / "reports" / "research" / "recipes" / recipe_name


def recipe_bundle_report_dir(project_root: str | Path, bundle_name: str) -> Path:
    root = Path(project_root)
    return root / "reports" / "research" / "recipe_bundles" / bundle_name


def write_recipe_summary(
    project_root: str | Path,
    recipe_name: str,
    summary: Dict[str, Any],
) -> Path:
    report_dir = recipe_report_dir(project_root, recipe_name)
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = str(summary.get("executed_at", "unknown")).replace(":", "-")
    model_id = str(summary.get("model_id", "unknown"))
    report_path = report_dir / f"{timestamp}__{model_id}.json"
    latest_path = report_dir / "latest.json"
    payload = {**summary, "report_path": str(report_path)}
    encoded = json.dumps(payload, indent=2)
    report_path.write_text(encoded, encoding="utf-8")
    latest_path.write_text(encoded, encoding="utf-8")
    return report_path


def write_recipe_bundle_summary(
    project_root: str | Path,
    bundle_name: str,
    summary: Dict[str, Any],
) -> Path:
    report_dir = recipe_bundle_report_dir(project_root, bundle_name)
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = str(summary.get("executed_at", "unknown")).replace(":", "-")
    winner_model_id = str((summary.get("winner") or {}).get("model_id", "unknown"))
    report_path = report_dir / f"{timestamp}__{winner_model_id}.json"
    latest_path = report_dir / "latest.json"
    payload = {**summary, "report_path": str(report_path)}
    encoded = json.dumps(payload, indent=2)
    report_path.write_text(encoded, encoding="utf-8")
    latest_path.write_text(encoded, encoding="utf-8")
    return report_path
