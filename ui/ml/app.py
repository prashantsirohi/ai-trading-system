"""Standalone Streamlit workbench for ML training and monitoring workflows."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

# Ensure imports work even when Streamlit is launched from outside repo root.
PROJECT_ROOT_PATH = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_PATH))

from core.bootstrap import ensure_project_root_on_path

ensure_project_root_on_path(__file__)

from core.env import load_project_env
from ui.services import (
    approve_workbench_model,
    delete_workbench_recipe,
    delete_workbench_recipe_bundle,
    deploy_workbench_model,
    get_task_logs,
    load_execution_workbench_settings,
    launch_pipeline_task,
    launch_prepare_dataset_task,
    launch_recipe_bundle_task,
    launch_recipe_run_task,
    launch_shadow_monitor_task,
    launch_train_model_task,
    list_operator_tasks,
    load_latest_execute_run,
    load_recipe_bundle_results,
    load_recipe_results,
    load_shadow_overlay_frame,
    load_shadow_summary_frame,
    load_workbench_execution_fills,
    load_workbench_execution_orders,
    load_workbench_execution_positions,
    load_workbench_trade_report,
    load_workbench_recipe_bundles,
    load_workbench_recipes,
    load_workbench_datasets,
    load_workbench_deployments,
    load_workbench_models,
    load_model_workbench_detail,
    pivot_shadow_summary_frame,
    rollback_workbench_deployment,
    save_execution_workbench_settings,
    save_workbench_recipe,
    save_workbench_recipe_bundle,
    workbench_recipe_config_path,
)
from core.paths import research_static_end_date

PROJECT_ROOT = str(PROJECT_ROOT_PATH)
load_project_env(PROJECT_ROOT)


def _refresh_button() -> None:
    if st.sidebar.button("Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


def _format_float(value: object) -> str:
    if value is None or value == "":
        return "—"
    try:
        return f"{float(value):.4f}"
    except Exception:
        return str(value)


def _recommended_environment(horizon: object) -> str:
    try:
        return "operational_shadow_20d" if int(horizon) == 20 else "operational_shadow_5d"
    except Exception:
        return "operational_shadow_5d"


def _display_frame(frame: pd.DataFrame, *, height: int = 320) -> None:
    if frame is None or frame.empty:
        st.info("No data available yet.")
        return
    display = frame.copy()
    for column in display.columns:
        if "date" in column or column.endswith("_at"):
            try:
                display[column] = pd.to_datetime(display[column]).astype(str)
            except Exception:
                display[column] = display[column].astype(str)
    st.dataframe(display, use_container_width=True, height=height)


def _render_active_deployment_banner() -> None:
    deployments = _deployments_frame()
    if deployments.empty:
        st.info("No ML deployments recorded yet.")
        return

    active = deployments[deployments["status"].astype(str).str.lower() == "active"].copy()
    if active.empty:
        st.warning("No active ML shadow deployments are live right now.")
        return

    st.markdown("**Active Shadow Deployments**")
    banner_cols = st.columns(max(1, len(active)))
    for idx, row in enumerate(active.itertuples(index=False)):
        with banner_cols[idx]:
            label = f"{getattr(row, 'environment', 'unknown')}"
            model_name = getattr(row, "model_name", "unknown")
            model_version = getattr(row, "model_version", "unknown")
            st.metric(label, f"{model_name}:{model_version}", getattr(row, "model_id", "—"))


@st.cache_data(show_spinner=False, ttl=60)
def _datasets_frame() -> pd.DataFrame:
    return load_workbench_datasets(PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=60)
def _models_frame() -> pd.DataFrame:
    return load_workbench_models(PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=60)
def _deployments_frame() -> pd.DataFrame:
    return load_workbench_deployments(PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=60)
def _shadow_overlay_frame() -> pd.DataFrame:
    return load_shadow_overlay_frame(PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=60)
def _shadow_summary_frame(grain: str, horizon: int) -> pd.DataFrame:
    return pivot_shadow_summary_frame(load_shadow_summary_frame(grain, horizon, periods=8, project_root=PROJECT_ROOT))


@st.cache_data(show_spinner=False, ttl=60)
def _model_detail(model_id: str) -> dict:
    return load_model_workbench_detail(model_id, PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=60)
def _recipes_frame() -> pd.DataFrame:
    return load_workbench_recipes(PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=60)
def _recipe_results_frame() -> pd.DataFrame:
    return load_recipe_results(PROJECT_ROOT, latest_only=True)


@st.cache_data(show_spinner=False, ttl=60)
def _recipe_bundles_frame() -> pd.DataFrame:
    return load_workbench_recipe_bundles(PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=60)
def _recipe_bundle_results_frame() -> pd.DataFrame:
    return load_recipe_bundle_results(PROJECT_ROOT, latest_only=True)


@st.cache_data(show_spinner=False, ttl=30)
def _execution_orders_frame() -> pd.DataFrame:
    return load_workbench_execution_orders(PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=30)
def _execution_fills_frame() -> pd.DataFrame:
    return load_workbench_execution_fills(PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=30)
def _execution_positions_frame() -> pd.DataFrame:
    return load_workbench_execution_positions(PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=30)
def _latest_execute_run() -> dict:
    return load_latest_execute_run(PROJECT_ROOT, data_domain="operational")


@st.cache_data(show_spinner=False, ttl=30)
def _execution_settings() -> dict:
    return load_execution_workbench_settings(PROJECT_ROOT)


@st.cache_data(show_spinner=False, ttl=30)
def _trade_report() -> dict:
    return load_workbench_trade_report(PROJECT_ROOT, data_domain="operational")


def _render_builder_tab() -> None:
    st.subheader("Recipe Builder")
    st.caption(f"Config file: {workbench_recipe_config_path(PROJECT_ROOT)}")

    recipes = _recipes_frame()
    bundles = _recipe_bundles_frame()
    recipe_lookup = (
        {row.recipe_name: row for row in recipes.itertuples(index=False)}
        if not recipes.empty
        else {}
    )
    bundle_lookup = (
        {row.bundle_name: row for row in bundles.itertuples(index=False)}
        if not bundles.empty
        else {}
    )

    recipe_col, bundle_col = st.columns(2)

    with recipe_col:
        st.markdown("**Recipes**")
        recipe_options = ["New Recipe"] + sorted(recipe_lookup.keys())
        selected_recipe = st.selectbox("Edit Recipe", options=recipe_options, key="builder_recipe_select")
        selected_recipe_row = recipe_lookup.get(selected_recipe)

        with st.form("builder_recipe_form"):
            recipe_name = st.text_input(
                "Recipe Name",
                value="" if selected_recipe_row is None else str(selected_recipe_row.recipe_name),
            )
            description = st.text_input(
                "Description",
                value="" if selected_recipe_row is None else str(selected_recipe_row.description),
            )
            strategy_tag = st.text_input(
                "Strategy Tag",
                value="general" if selected_recipe_row is None else str(selected_recipe_row.strategy_tag),
            )
            feature_set_variant = st.text_input(
                "Feature Set Variant",
                value="default" if selected_recipe_row is None else str(selected_recipe_row.feature_set_variant),
            )
            experiment_notes = st.text_area(
                "Experiment Notes",
                value="" if selected_recipe_row is None else str(selected_recipe_row.experiment_notes),
                height=100,
            )
            engine = st.selectbox(
                "Engine",
                options=["lightgbm", "xgboost"],
                index=0 if selected_recipe_row is None or str(selected_recipe_row.engine) == "lightgbm" else 1,
                key="builder_recipe_engine",
            )
            horizon = st.selectbox(
                "Horizon",
                options=[5, 10, 20],
                index=0 if selected_recipe_row is None else [5, 10, 20].index(int(selected_recipe_row.horizon)) if int(selected_recipe_row.horizon) in [5, 10, 20] else 0,
                key="builder_recipe_horizon",
            )
            from_date = st.text_input(
                "From Date",
                value="2018-01-01" if selected_recipe_row is None else str(selected_recipe_row.from_date),
            )
            to_date = st.text_input(
                "To Date",
                value=research_static_end_date() if selected_recipe_row is None else str(selected_recipe_row.to_date),
            )
            dataset_name = st.text_input(
                "Dataset Name",
                value="" if selected_recipe_row is None else str(selected_recipe_row.dataset_name),
            )
            model_name = st.text_input(
                "Model Name",
                value="" if selected_recipe_row is None else str(selected_recipe_row.model_name),
            )
            model_version = st.text_input(
                "Model Version",
                value="v1" if selected_recipe_row is None else str(selected_recipe_row.model_version),
            )
            validation_fraction = st.slider(
                "Validation Fraction",
                min_value=0.05,
                max_value=0.40,
                value=0.20 if selected_recipe_row is None else float(selected_recipe_row.validation_fraction),
                step=0.05,
                key="builder_recipe_validation_fraction",
            )
            progress_interval = st.number_input(
                "Progress Interval",
                min_value=1,
                max_value=500,
                value=25 if selected_recipe_row is None else int(selected_recipe_row.progress_interval),
                step=1,
            )
            min_train_years = st.number_input(
                "Min Train Years",
                min_value=1,
                max_value=15,
                value=5 if selected_recipe_row is None else int(selected_recipe_row.min_train_years),
                step=1,
            )
            shadow_environment = st.selectbox(
                "Shadow Environment",
                options=["operational_shadow_5d", "operational_shadow_20d"],
                index=0 if selected_recipe_row is None or str(selected_recipe_row.shadow_environment) == "operational_shadow_5d" else 1,
                key="builder_recipe_environment",
            )
            auto_approve = st.checkbox(
                "Default Auto-Approve",
                value=False if selected_recipe_row is None else bool(selected_recipe_row.auto_approve),
            )
            auto_deploy = st.checkbox(
                "Default Auto-Deploy",
                value=False if selected_recipe_row is None else bool(selected_recipe_row.auto_deploy),
            )
            min_validation_auc = st.number_input(
                "Min Validation AUC",
                min_value=0.0,
                max_value=1.0,
                value=0.55 if selected_recipe_row is None else float(selected_recipe_row.min_validation_auc),
                step=0.01,
            )
            min_walkforward_auc = st.number_input(
                "Min Walkforward AUC",
                min_value=0.0,
                max_value=1.0,
                value=0.55 if selected_recipe_row is None else float(selected_recipe_row.min_walkforward_auc),
                step=0.01,
            )
            min_precision_at_10pct = st.number_input(
                "Min Precision@10%",
                min_value=0.0,
                max_value=1.0,
                value=0.35 if selected_recipe_row is None else float(selected_recipe_row.min_precision_at_10pct),
                step=0.01,
            )
            submitted = st.form_submit_button("Save Recipe", use_container_width=True)
            if submitted:
                try:
                    result = save_workbench_recipe(
                        project_root=PROJECT_ROOT,
                        recipe_name=recipe_name,
                        description=description,
                        strategy_tag=strategy_tag,
                        feature_set_variant=feature_set_variant,
                        experiment_notes=experiment_notes,
                        engine=engine,
                        horizon=int(horizon),
                        from_date=from_date,
                        to_date=to_date,
                        dataset_name=dataset_name,
                        model_name=model_name,
                        model_version=model_version,
                        validation_fraction=float(validation_fraction),
                        progress_interval=int(progress_interval),
                        min_train_years=int(min_train_years),
                        shadow_environment=shadow_environment,
                        auto_approve=bool(auto_approve),
                        auto_deploy=bool(auto_deploy),
                        min_validation_auc=float(min_validation_auc),
                        min_walkforward_auc=float(min_walkforward_auc),
                        min_precision_at_10pct=float(min_precision_at_10pct),
                    )
                    st.success(f"Saved recipe `{result['recipe_name']}`")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Save failed: {exc}")

        if selected_recipe_row is not None:
            if st.button("Delete Recipe", key=f"delete_recipe_{selected_recipe}", use_container_width=True):
                try:
                    result = delete_workbench_recipe(project_root=PROJECT_ROOT, recipe_name=str(selected_recipe))
                    st.success(f"Deleted recipe `{result['recipe_name']}`")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Delete failed: {exc}")

    with bundle_col:
        st.markdown("**Bundles**")
        bundle_options = ["New Bundle"] + sorted(bundle_lookup.keys())
        selected_bundle = st.selectbox("Edit Bundle", options=bundle_options, key="builder_bundle_select")
        selected_bundle_row = bundle_lookup.get(selected_bundle)
        existing_recipe_names = sorted(recipe_lookup.keys())
        selected_bundle_recipes = []
        if selected_bundle_row is not None:
            selected_bundle_recipes = [item.strip() for item in str(selected_bundle_row.recipes).split(",") if item.strip()]

        with st.form("builder_bundle_form"):
            bundle_name = st.text_input(
                "Bundle Name",
                value="" if selected_bundle_row is None else str(selected_bundle_row.bundle_name),
            )
            description = st.text_input(
                "Bundle Description",
                value="" if selected_bundle_row is None else str(selected_bundle_row.description),
            )
            chosen_recipes = st.multiselect(
                "Recipes In Bundle",
                options=existing_recipe_names,
                default=selected_bundle_recipes,
            )
            selection_metric = st.selectbox(
                "Winner Selection Metric",
                options=["walkforward_avg_validation_auc", "validation_auc", "precision_at_10pct"],
                index=0 if selected_bundle_row is None else ["walkforward_avg_validation_auc", "validation_auc", "precision_at_10pct"].index(str(selected_bundle_row.selection_metric)) if str(selected_bundle_row.selection_metric) in ["walkforward_avg_validation_auc", "validation_auc", "precision_at_10pct"] else 0,
            )
            submitted = st.form_submit_button("Save Bundle", use_container_width=True)
            if submitted:
                try:
                    result = save_workbench_recipe_bundle(
                        project_root=PROJECT_ROOT,
                        bundle_name=bundle_name,
                        description=description,
                        recipes=chosen_recipes,
                        selection_metric=selection_metric,
                    )
                    st.success(f"Saved bundle `{result['bundle_name']}`")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Save failed: {exc}")

        if selected_bundle_row is not None:
            if st.button("Delete Bundle", key=f"delete_bundle_{selected_bundle}", use_container_width=True):
                try:
                    result = delete_workbench_recipe_bundle(project_root=PROJECT_ROOT, bundle_name=str(selected_bundle))
                    st.success(f"Deleted bundle `{result['bundle_name']}`")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Delete failed: {exc}")


def _render_training_tab() -> None:
    st.subheader("Launch ML Workflows")
    bundles = _recipe_bundles_frame()
    if not bundles.empty:
        st.markdown("**Run Best Daily Research**")
        default_bundle = bundles.iloc[0]
        with st.form("run_daily_bundle_form"):
            st.caption(default_bundle["description"])
            auto_approve = st.checkbox("Auto-approve winner if promotion gates pass", value=False, key="bundle_auto_approve")
            auto_deploy = st.checkbox("Auto-deploy winner if promotion gates pass", value=False, key="bundle_auto_deploy")
            submitted = st.form_submit_button("Run Best Daily Research", use_container_width=True)
            if submitted:
                task_id = launch_recipe_bundle_task(
                    project_root=PROJECT_ROOT,
                    label=f"Run bundle {default_bundle['bundle_name']}",
                    bundle=str(default_bundle["bundle_name"]),
                    auto_approve=bool(auto_approve),
                    auto_deploy=bool(auto_deploy),
                )
                st.success(f"Daily research bundle launched as task `{task_id}`.")
        st.divider()

    recipes = _recipes_frame()
    if not recipes.empty:
        st.markdown("**Run A Preset Recipe**")
        recipe_options = {
            f"{row.recipe_name} (h{row.horizon}, {row.engine})": row.recipe_name
            for row in recipes.itertuples(index=False)
        }
        with st.form("run_recipe_form"):
            selected_recipe_label = st.selectbox("Recipe", options=list(recipe_options.keys()))
            auto_approve = st.checkbox("Auto-approve if promotion gates pass", value=False)
            auto_deploy = st.checkbox("Auto-deploy to shadow if promotion gates pass", value=False)
            submitted = st.form_submit_button("Run Recipe", use_container_width=True)
            if submitted:
                recipe_name = recipe_options[selected_recipe_label]
                task_id = launch_recipe_run_task(
                    project_root=PROJECT_ROOT,
                    label=f"Run recipe {recipe_name}",
                    recipe=recipe_name,
                    auto_approve=bool(auto_approve),
                    auto_deploy=bool(auto_deploy),
                )
                st.success(f"Recipe run launched as task `{task_id}`.")
        st.divider()

    st.markdown("**Advanced Manual Controls**")
    col_dataset, col_train = st.columns(2)

    default_to_date = research_static_end_date()
    default_from_date = "2015-01-01"

    with col_dataset:
        with st.form("prepare_dataset_form"):
            st.markdown("**Prepare Dataset**")
            engine = st.selectbox("Engine", options=["lightgbm", "xgboost"], index=0, key="dataset_engine")
            dataset_name = st.text_input("Dataset Name", value="lightgbm_workbench")
            from_date = st.text_input("From Date", value=default_from_date)
            to_date = st.text_input("To Date", value=default_to_date)
            horizon = st.selectbox("Horizon", options=[5, 20], index=0, key="dataset_horizon")
            validation_fraction = st.slider("Validation Fraction", min_value=0.05, max_value=0.40, value=0.20, step=0.05)
            submitted = st.form_submit_button("Prepare Dataset", use_container_width=True)
            if submitted:
                task_id = launch_prepare_dataset_task(
                    project_root=PROJECT_ROOT,
                    label=f"Prepare dataset {dataset_name} h{horizon}",
                    engine=engine,
                    dataset_name=dataset_name,
                    from_date=from_date,
                    to_date=to_date,
                    horizon=int(horizon),
                    validation_fraction=float(validation_fraction),
                )
                st.success(f"Dataset preparation launched as task `{task_id}`.")

    with col_train:
        with st.form("train_model_form"):
            st.markdown("**Train Model**")
            engine = st.selectbox("Training Engine", options=["lightgbm", "xgboost"], index=0, key="train_engine")
            model_name = st.text_input("Model Name", value="alpha_workbench")
            model_version = st.text_input("Model Version", value="v1")
            dataset_uri = st.text_input("Prepared Dataset URI (optional)", value="")
            from_date = st.text_input("Training From Date", value=default_from_date)
            to_date = st.text_input("Training To Date", value=default_to_date)
            horizon = st.selectbox("Training Horizon", options=[5, 20], index=0, key="train_horizon")
            progress_interval = st.number_input("Progress Interval", min_value=1, max_value=500, value=25, step=1)
            min_train_years = st.number_input("Min Train Years", min_value=1, max_value=15, value=5, step=1)
            submitted = st.form_submit_button("Train Model", use_container_width=True)
            if submitted:
                task_id = launch_train_model_task(
                    project_root=PROJECT_ROOT,
                    label=f"Train model {model_name}:{model_version} h{horizon}",
                    engine=engine,
                    model_name=model_name,
                    model_version=model_version,
                    horizon=int(horizon),
                    from_date=from_date,
                    to_date=to_date,
                    progress_interval=int(progress_interval),
                    min_train_years=int(min_train_years),
                    dataset_uri=dataset_uri.strip() or None,
                )
                st.success(f"Model training launched as task `{task_id}`.")

    st.divider()
    with st.form("shadow_monitor_form"):
        st.markdown("**Update Shadow Monitoring**")
        prediction_date = st.text_input("Prediction Date", value=datetime.now().date().isoformat())
        backfill_days = st.number_input("Backfill Days", min_value=0, max_value=365, value=0, step=1)
        submitted = st.form_submit_button("Run Shadow Monitor", use_container_width=True)
        if submitted:
            task_id = launch_shadow_monitor_task(
                label=f"Shadow monitor {prediction_date}",
                prediction_date=prediction_date,
                backfill_days=int(backfill_days),
            )
            st.success(f"Shadow monitoring launched as task `{task_id}`.")


def _render_datasets_tab() -> None:
    st.subheader("Registered Datasets")
    datasets = _datasets_frame()
    _display_frame(datasets)
    if datasets.empty:
        return

    dataset_labels = {
        f"{row.dataset_ref} ({row.engine_name}, h{row.horizon})": row.dataset_ref
        for row in datasets.itertuples(index=False)
    }
    selected_ref = st.selectbox("Inspect Dataset", options=list(dataset_labels.keys()))
    selected_row = datasets[datasets["dataset_ref"] == dataset_labels[selected_ref]].iloc[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Rows", f"{int(selected_row['row_count'] or 0):,}")
    col2.metric("Symbols", f"{int(selected_row['symbol_count'] or 0):,}")
    col3.metric("Horizon", str(selected_row["horizon"]))
    col4.metric("Validation Fraction", _format_float(selected_row.get("validation_fraction")))

    st.caption(selected_row["dataset_uri"])


def _render_models_tab() -> None:
    st.subheader("Model Registry")
    models = _models_frame()
    _display_frame(models)
    if models.empty:
        return

    model_labels = {
        f"{row.model_name}:{row.model_version} [{row.model_id}]": row.model_id
        for row in models.itertuples(index=False)
    }
    selected_model_id = st.selectbox("Inspect Model", options=list(model_labels.keys()))
    detail = _model_detail(model_labels[selected_model_id])
    model_record = detail["model"]
    metadata = detail["metadata"]
    monitor_summary = detail.get("monitor_summary", {})

    metric_cols = st.columns(6)
    metric_cols[0].metric("Approval", model_record.get("approval_status", "—"))
    metric_cols[1].metric("Engine", metadata.get("engine", "—"))
    metric_cols[2].metric("Horizon", str(metadata.get("horizon", "—")))
    metric_cols[3].metric("Validation AUC", _format_float((metadata.get("evaluation") or {}).get("validation_auc")))
    metric_cols[4].metric(
        "P@10",
        _format_float((metadata.get("evaluation") or {}).get("precision_at_10pct")),
    )
    metric_cols[5].metric(
        "WF AUC",
        _format_float((metadata.get("walkforward_summary") or {}).get("avg_validation_auc")),
    )

    st.caption(model_record.get("artifact_uri", ""))
    st.write(f"Training dataset: `{model_record.get('train_snapshot_ref', '—')}`")
    st.write(
        f"Strategy tag: `{metadata.get('strategy_tag', '—')}` | "
        f"Feature set: `{metadata.get('feature_set_variant', '—')}`"
    )
    if metadata.get("experiment_notes"):
        st.caption(str(metadata.get("experiment_notes")))

    active_deployments = detail["deployments"]
    active_environment_rows = pd.DataFrame()
    if active_deployments is not None and not active_deployments.empty and "status" in active_deployments.columns:
        active_environment_rows = active_deployments[
            active_deployments["status"].astype(str).str.lower() == "active"
        ].copy()
    if not active_environment_rows.empty:
        active_envs = ", ".join(sorted(active_environment_rows["environment"].astype(str).tolist()))
        st.info(f"Active in: {active_envs}")

    action_col, deploy_col, rollback_col = st.columns(3)
    with action_col:
        st.markdown("**Model Controls**")
        if model_record.get("approval_status") != "approved":
            if st.button("Approve Model", key=f"approve_{selected_model_id}", use_container_width=True):
                result = approve_workbench_model(model_record["model_id"], PROJECT_ROOT)
                st.success(f"Model approved: `{result['after']['model_id']}`")
                st.cache_data.clear()
                st.rerun()
        else:
            st.success("Model is already approved.")

    with deploy_col:
        st.markdown("**Deploy To Environment**")
        default_environment = _recommended_environment(metadata.get("horizon"))
        environment_options = list(dict.fromkeys([default_environment, "operational_shadow_5d", "operational_shadow_20d"]))
        with st.form(f"deploy_model_{selected_model_id}"):
            environment = st.selectbox(
                "Environment",
                options=environment_options,
                index=0,
                key=f"deploy_env_{selected_model_id}",
            )
            approved_by = st.text_input("Approved By", value="ml-workbench", key=f"approved_by_{selected_model_id}")
            notes = st.text_input("Notes", value="", key=f"deploy_notes_{selected_model_id}")
            submitted = st.form_submit_button("Deploy Model", use_container_width=True)
            if submitted:
                try:
                    result = deploy_workbench_model(
                        model_record["model_id"],
                        environment=environment,
                        approved_by=approved_by.strip() or "ml-workbench",
                        notes=notes.strip() or None,
                        project_root=PROJECT_ROOT,
                    )
                    st.success(
                        f"Deployment complete: `{result['deployment_id']}` active in `{environment}`."
                    )
                    st.cache_data.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Deploy failed: {exc}")

    with rollback_col:
        st.markdown("**Rollback Environment**")
        rollback_default = _recommended_environment(metadata.get("horizon"))
        active_env_options = (
            sorted(active_environment_rows["environment"].astype(str).tolist())
            if not active_environment_rows.empty
            else [rollback_default]
        )
        with st.form(f"rollback_model_{selected_model_id}"):
            environment = st.selectbox(
                "Rollback Environment",
                options=active_env_options,
                index=0,
                key=f"rollback_env_{selected_model_id}",
            )
            approved_by = st.text_input("Rollback Approved By", value="ml-workbench", key=f"rollback_by_{selected_model_id}")
            notes = st.text_input("Rollback Notes", value="", key=f"rollback_notes_{selected_model_id}")
            submitted = st.form_submit_button("Rollback Deployment", use_container_width=True)
            if submitted:
                try:
                    result = rollback_workbench_deployment(
                        environment=environment,
                        approved_by=approved_by.strip() or "ml-workbench",
                        notes=notes.strip() or None,
                        project_root=PROJECT_ROOT,
                    )
                    st.success(
                        f"Rollback complete: `{result['deployment_id']}` active in `{environment}`."
                    )
                    st.cache_data.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Rollback failed: {exc}")

    summary_cols = st.columns(4)
    summary_cols[0].metric("Predictions", str(monitor_summary.get("prediction_rows", 0)))
    summary_cols[1].metric("Matured", str(monitor_summary.get("matured_rows", 0)))
    summary_cols[2].metric("Top Decile Hit Rate", _format_float(monitor_summary.get("top_decile_hit_rate")))
    summary_cols[3].metric("Top Decile Avg Return", _format_float(monitor_summary.get("top_decile_avg_return")))

    st.markdown("**Evaluations**")
    _display_frame(detail["evaluations"], height=220)
    st.markdown("**Deployments**")
    _display_frame(detail["deployments"], height=220)
    st.markdown("**Latest Drift Metrics**")
    _display_frame(detail["drift_metrics"], height=220)
    st.markdown("**Promotion Gates**")
    _display_frame(detail["promotion_gates"], height=220)


def _render_results_tab() -> None:
    st.subheader("Validation Results")
    bundle_results = _recipe_bundle_results_frame()
    if not bundle_results.empty:
        latest_bundle = bundle_results.iloc[0]
        st.markdown("**Best Daily Research Winner**")
        winner_cols = st.columns(6)
        winner_cols[0].metric("Bundle", str(latest_bundle["bundle_name"]))
        winner_cols[1].metric("Winner", str(latest_bundle["winner_recipe_name"]))
        winner_cols[2].metric("Validation", str(latest_bundle["winner_validation_status"]))
        winner_cols[3].metric("Promotion", str(latest_bundle["winner_promotion_status"]))
        winner_cols[4].metric("WF AUC", _format_float(latest_bundle.get("winner_walkforward_avg_validation_auc")))
        winner_cols[5].metric("P@10", _format_float(latest_bundle.get("winner_precision_at_10pct")))
        st.write(
            f"Strategy tag: `{latest_bundle.get('winner_strategy_tag') or '—'}` | "
            f"Feature set: `{latest_bundle.get('winner_feature_set_variant') or '—'}`"
        )
        st.caption(str(latest_bundle.get("report_path", "")))
        st.divider()

    results = _recipe_results_frame()
    if results.empty:
        st.info("No recipe results yet. Run a preset recipe from the Training tab.")
        return

    leaderboard_cols = st.columns(5)
    leaderboard_cols[0].metric("Recipes Tracked", str(results["recipe_name"].nunique()))
    leaderboard_cols[1].metric(
        "Validation Pass",
        str(int((results["validation_status"].astype(str) == "pass").sum())),
    )
    leaderboard_cols[2].metric(
        "Promotion Pass",
        str(int((results["promotion_status"].astype(str) == "pass").sum())),
    )
    leaderboard_cols[3].metric(
        "Shadow Live",
        str(int(results["active_environment"].notna().sum())),
    )
    leaderboard_cols[4].metric(
        "Latest Run",
        str(pd.to_datetime(results.iloc[0]["executed_at"]).date()) if results.iloc[0]["executed_at"] else "—",
    )

    st.markdown("**Latest Recipe Leaderboard**")
    display_cols = [
        "recipe_name",
        "strategy_tag",
        "feature_set_variant",
        "executed_at",
        "validation_status",
        "promotion_status",
        "approval_status",
        "active_environment",
        "validation_auc",
        "walkforward_avg_validation_auc",
        "precision_at_10pct",
        "shadow_top_decile_hit_rate",
        "shadow_matured_rows",
        "model_id",
    ]
    leaderboard = results[[column for column in display_cols if column in results.columns]].copy()
    _display_frame(leaderboard, height=280)

    result_options = {
        f"{row.recipe_name} [{row.model_id}]": row.model_id
        for row in results.itertuples(index=False)
    }
    selected_result = st.selectbox("Inspect Result", options=list(result_options.keys()))
    selected_row = results[results["model_id"] == result_options[selected_result]].iloc[0]

    detail_cols = st.columns(6)
    detail_cols[0].metric("Validation", str(selected_row["validation_status"]))
    detail_cols[1].metric("Promotion", str(selected_row["promotion_status"]))
    detail_cols[2].metric("Validation AUC", _format_float(selected_row.get("validation_auc")))
    detail_cols[3].metric("WF AUC", _format_float(selected_row.get("walkforward_avg_validation_auc")))
    detail_cols[4].metric("P@10", _format_float(selected_row.get("precision_at_10pct")))
    detail_cols[5].metric("Active Env", str(selected_row.get("active_environment") or "—"))

    st.write(
        f"Strategy tag: `{selected_row.get('strategy_tag') or '—'}` | "
        f"Feature set: `{selected_row.get('feature_set_variant') or '—'}`"
    )
    if selected_row.get("experiment_notes"):
        st.caption(str(selected_row.get("experiment_notes")))

    st.caption(selected_row.get("report_path", ""))


def _render_monitoring_tab() -> None:
    st.subheader("Operational ML Monitoring")
    deployments = _deployments_frame()
    _render_active_deployment_banner()
    st.markdown("**Deployments**")
    _display_frame(deployments, height=220)

    overlay = _shadow_overlay_frame()
    st.markdown("**Latest Shadow Overlay**")
    _display_frame(overlay, height=280)

    weekly_col, monthly_col = st.columns(2)
    with weekly_col:
        st.markdown("**5D Weekly Summary**")
        _display_frame(_shadow_summary_frame("week", 5), height=240)
        st.markdown("**20D Weekly Summary**")
        _display_frame(_shadow_summary_frame("week", 20), height=240)
    with monthly_col:
        st.markdown("**5D Monthly Summary**")
        _display_frame(_shadow_summary_frame("month", 5), height=240)
        st.markdown("**20D Monthly Summary**")
        _display_frame(_shadow_summary_frame("month", 20), height=240)


def _render_execution_tab() -> None:
    st.subheader("Paper Auto-Trading")
    st.caption("Run paper auto buy/sell from ranked signals and review the resulting order, fill, and position state.")
    settings = _execution_settings()

    state_cols = st.columns([1.2, 1, 1])
    with state_cols[0]:
        enabled = st.toggle(
            "Auto-Trading Enabled",
            value=bool(settings.get("execution_enabled", False)),
            help="When disabled, execute runs can still preview actions but will not place paper orders.",
        )
        if enabled != bool(settings.get("execution_enabled", False)):
            save_execution_workbench_settings(project_root=PROJECT_ROOT, settings={"execution_enabled": bool(enabled)})
            st.cache_data.clear()
            st.rerun()
    with state_cols[1]:
        default_preview = bool(settings.get("default_preview_only", True))
        st.metric("Default Mode", "Preview" if default_preview else "Execute")
    with state_cols[2]:
        st.metric("Safety State", "Enabled" if enabled else "Paused")

    with st.expander("Save Default Execution Preferences", expanded=False):
        with st.form("execution_defaults_form"):
            default_strategy_mode = st.selectbox(
                "Default Strategy Mode",
                options=["technical", "ml", "hybrid_confirm", "hybrid_overlay"],
                index=["technical", "ml", "hybrid_confirm", "hybrid_overlay"].index(
                    str(settings.get("default_strategy_mode", "technical"))
                ),
                key="default_strategy_mode",
            )
            default_ml_mode = st.selectbox(
                "Default ML Mode",
                options=["baseline_only", "shadow_ml"],
                index=["baseline_only", "shadow_ml"].index(str(settings.get("default_ml_mode", "baseline_only"))),
                key="default_ml_mode",
            )
            default_preview_only = st.checkbox(
                "Default To Preview Only",
                value=bool(settings.get("default_preview_only", True)),
            )
            default_top_n = st.number_input(
                "Default Target Positions",
                min_value=1,
                max_value=50,
                value=int(settings.get("default_execution_top_n", 5)),
                step=1,
            )
            default_ml_horizon = st.selectbox(
                "Default ML Horizon",
                options=[5, 20],
                index=0 if int(settings.get("default_ml_horizon", 5)) == 5 else 1,
                key="default_ml_horizon",
            )
            default_confirm = st.slider(
                "Default ML Confirm Threshold",
                min_value=0.0,
                max_value=1.0,
                value=float(settings.get("default_ml_confirm_threshold", 0.55)),
                step=0.01,
                key="default_confirm",
            )
            default_capital = st.number_input(
                "Default Capital",
                min_value=10000.0,
                value=float(settings.get("default_execution_capital", 1_000_000.0)),
                step=10000.0,
            )
            default_fixed_qty_enabled = st.checkbox(
                "Default Fixed Quantity Enabled",
                value=bool(settings.get("default_fixed_quantity_enabled", False)),
            )
            default_fixed_qty = st.number_input(
                "Default Fixed Quantity",
                min_value=1,
                max_value=10000,
                value=int(settings.get("default_execution_fixed_quantity", 10)),
                step=1,
                disabled=not default_fixed_qty_enabled,
            )
            default_slippage = st.number_input(
                "Default Paper Slippage (bps)",
                min_value=0.0,
                max_value=100.0,
                value=float(settings.get("default_paper_slippage_bps", 5.0)),
                step=0.5,
            )
            submitted_defaults = st.form_submit_button("Save Defaults", use_container_width=True)
            if submitted_defaults:
                save_execution_workbench_settings(
                    project_root=PROJECT_ROOT,
                    settings={
                        "default_strategy_mode": default_strategy_mode,
                        "default_ml_mode": default_ml_mode,
                        "default_preview_only": bool(default_preview_only),
                        "default_execution_top_n": int(default_top_n),
                        "default_ml_horizon": int(default_ml_horizon),
                        "default_ml_confirm_threshold": float(default_confirm),
                        "default_execution_capital": float(default_capital),
                        "default_fixed_quantity_enabled": bool(default_fixed_qty_enabled),
                        "default_execution_fixed_quantity": int(default_fixed_qty),
                        "default_paper_slippage_bps": float(default_slippage),
                    },
                )
                st.success("Execution defaults saved.")
                st.cache_data.clear()
                st.rerun()

    with st.form("paper_execute_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            run_date = st.text_input("Run Date", value=datetime.now().date().isoformat())
            strategy_mode = st.selectbox(
                "Strategy Mode",
                options=["technical", "ml", "hybrid_confirm", "hybrid_overlay"],
                index=["technical", "ml", "hybrid_confirm", "hybrid_overlay"].index(
                    str(settings.get("default_strategy_mode", "technical"))
                ),
            )
            execution_top_n = st.number_input(
                "Target Positions",
                min_value=1,
                max_value=50,
                value=int(settings.get("default_execution_top_n", 5)),
                step=1,
            )
        with col2:
            ml_mode = st.selectbox(
                "ML Overlay Mode",
                options=["baseline_only", "shadow_ml"],
                index=["baseline_only", "shadow_ml"].index(str(settings.get("default_ml_mode", "baseline_only"))),
            )
            ml_horizon = st.selectbox(
                "ML Horizon",
                options=[5, 20],
                index=0 if int(settings.get("default_ml_horizon", 5)) == 5 else 1,
            )
            ml_confirm_threshold = st.slider(
                "ML Confirm Threshold",
                min_value=0.0,
                max_value=1.0,
                value=float(settings.get("default_ml_confirm_threshold", 0.55)),
                step=0.01,
            )
            preview_only = st.checkbox(
                "Preview Only",
                value=bool(settings.get("default_preview_only", True)),
                help="Computes actions without placing paper orders or fills.",
            )
        with col3:
            execution_capital = st.number_input(
                "Capital",
                min_value=10000.0,
                value=float(settings.get("default_execution_capital", 1_000_000.0)),
                step=10000.0,
            )
            fixed_quantity_enabled = st.checkbox(
                "Use Fixed Quantity",
                value=bool(settings.get("default_fixed_quantity_enabled", False)),
            )
            execution_fixed_quantity = st.number_input(
                "Fixed Quantity",
                min_value=1,
                max_value=10000,
                value=int(settings.get("default_execution_fixed_quantity", 10)),
                step=1,
                disabled=not fixed_quantity_enabled,
            )
            paper_slippage_bps = st.number_input(
                "Paper Slippage (bps)",
                min_value=0.0,
                max_value=100.0,
                value=float(settings.get("default_paper_slippage_bps", 5.0)),
                step=0.5,
            )
        submitted = st.form_submit_button(
            "Preview Actions" if preview_only or not enabled else "Run Paper Auto-Trading",
            use_container_width=True,
        )
        if submitted:
            params = {
                "preflight": False,
                "data_domain": "operational",
                "ml_mode": ml_mode,
                "strategy_mode": strategy_mode,
                "execution_enabled": bool(enabled),
                "execution_preview": bool(preview_only),
                "execution_top_n": int(execution_top_n),
                "execution_ml_horizon": int(ml_horizon),
                "execution_ml_confirm_threshold": float(ml_confirm_threshold),
                "execution_capital": float(execution_capital),
                "paper_slippage_bps": float(paper_slippage_bps),
            }
            if fixed_quantity_enabled:
                params["execution_fixed_quantity"] = int(execution_fixed_quantity)
            task_id = launch_pipeline_task(
                project_root=PROJECT_ROOT,
                label=f"{'Preview' if preview_only or not enabled else 'Paper auto-trading'} {strategy_mode} {run_date}",
                stage_names=["rank", "execute"],
                run_date=run_date,
                params=params,
            )
            st.success(f"{'Preview' if preview_only or not enabled else 'Paper execution'} launched as task `{task_id}`.")

    st.divider()
    st.markdown("**Daily Run Shortcut**")
    with st.form("daily_execution_form"):
        daily_strategy_mode = st.selectbox(
            "Daily Strategy Mode",
            options=["technical", "ml", "hybrid_confirm", "hybrid_overlay"],
            index=["technical", "ml", "hybrid_confirm", "hybrid_overlay"].index(
                str(settings.get("default_strategy_mode", "technical"))
            ),
            key="daily_strategy_mode",
        )
        daily_preview = st.checkbox(
            "Run Daily In Preview Mode",
            value=bool(settings.get("default_preview_only", True)),
            key="daily_preview",
        )
        submitted_daily = st.form_submit_button("Run Daily Paper Workflow", use_container_width=True)
        if submitted_daily:
            task_id = launch_pipeline_task(
                project_root=PROJECT_ROOT,
                label=f"Daily paper workflow {daily_strategy_mode}",
                stage_names=["rank", "execute"],
                run_date=datetime.now().date().isoformat(),
                params={
                    "preflight": False,
                    "data_domain": "operational",
                    "ml_mode": str(settings.get("default_ml_mode", "baseline_only")),
                    "strategy_mode": daily_strategy_mode,
                    "execution_enabled": bool(enabled),
                    "execution_preview": bool(daily_preview),
                    "execution_top_n": int(settings.get("default_execution_top_n", 5)),
                    "execution_ml_horizon": int(settings.get("default_ml_horizon", 5)),
                    "execution_ml_confirm_threshold": float(settings.get("default_ml_confirm_threshold", 0.55)),
                    "execution_capital": float(settings.get("default_execution_capital", 1_000_000.0)),
                    "paper_slippage_bps": float(settings.get("default_paper_slippage_bps", 5.0)),
                    **(
                        {"execution_fixed_quantity": int(settings.get("default_execution_fixed_quantity", 10))}
                        if bool(settings.get("default_fixed_quantity_enabled", False))
                        else {}
                    ),
                },
            )
            st.success(f"Daily workflow launched as task `{task_id}`.")

    latest = _latest_execute_run()
    summary = latest.get("summary", {}) or {}
    if summary:
        st.markdown("**Latest Execute Run**")
        metric_cols = st.columns(5)
        metric_cols[0].metric("Strategy", str(summary.get("strategy_mode", "—")))
        metric_cols[1].metric("Actions", str(summary.get("actions_count", 0)))
        metric_cols[2].metric("Orders", str(summary.get("order_count", 0)))
        metric_cols[3].metric("Fills", str(summary.get("fill_count", 0)))
        metric_cols[4].metric("Open Positions", str(summary.get("open_position_count", 0)))
        status_cols = st.columns(3)
        status_cols[0].metric("Run Status", str(summary.get("execution_status", "—")))
        status_cols[1].metric("Preview", "Yes" if bool(summary.get("preview_only")) else "No")
        status_cols[2].metric("Enabled", "Yes" if bool(summary.get("execution_enabled", True)) else "No")
        if latest.get("report_path"):
            st.caption(str(latest["report_path"]))

        latest_params = dict(latest.get("parameters") or {})
        latest_run_date = str(latest.get("run_date") or datetime.now().date().isoformat())
        if bool(summary.get("preview_only")):
            button_label = "Execute Latest Preview" if enabled else "Enable Auto-Trading To Execute Preview"
            if st.button(
                button_label,
                use_container_width=True,
                disabled=not bool(enabled),
                key="execute_latest_preview_button",
            ):
                rerun_params = {**latest_params}
                rerun_params["preflight"] = False
                rerun_params["execution_preview"] = False
                rerun_params["execution_enabled"] = True
                task_id = launch_pipeline_task(
                    project_root=PROJECT_ROOT,
                    label=f"Execute latest preview {latest_run_date}",
                    stage_names=["rank", "execute"],
                    run_date=latest_run_date,
                    params=rerun_params,
                )
                st.success(f"Preview promoted to execution as task `{task_id}`.")

        display_cols = st.columns(2)
        with display_cols[0]:
            st.markdown("**Latest Trade Actions**")
            _display_frame(latest.get("trade_actions", pd.DataFrame()), height=220)
        with display_cols[1]:
            st.markdown("**Latest Cycle Orders**")
            _display_frame(latest.get("executed_orders", pd.DataFrame()), height=220)
        display_cols = st.columns(2)
        with display_cols[0]:
            st.markdown("**Latest Cycle Fills**")
            _display_frame(latest.get("executed_fills", pd.DataFrame()), height=220)
        with display_cols[1]:
            st.markdown("**Latest Positions Snapshot**")
            _display_frame(latest.get("positions", pd.DataFrame()), height=220)

    st.divider()
    st.markdown("**Current Execution Ledger**")
    ledger_cols = st.columns(3)
    with ledger_cols[0]:
        st.markdown("**Open Positions**")
        _display_frame(_execution_positions_frame(), height=260)
    with ledger_cols[1]:
        st.markdown("**Recent Orders**")
        _display_frame(_execution_orders_frame(), height=260)
    with ledger_cols[2]:
        st.markdown("**Recent Fills**")
        _display_frame(_execution_fills_frame(), height=260)


def _render_trades_tab() -> None:
    st.subheader("Trades And P&L")
    st.caption("Track open positions, closed trades, and current gain/loss from the paper execution ledger.")

    report = _trade_report()
    summary = report.get("summary", {}) or {}
    metric_cols = st.columns(6)
    metric_cols[0].metric("Open Positions", str(summary.get("open_positions", 0)))
    metric_cols[1].metric("Closed Trades", str(summary.get("closed_trade_count", 0)))
    metric_cols[2].metric("Win Rate", _format_float((summary.get("win_rate", 0.0) or 0.0) * 100) + "%")
    metric_cols[3].metric("Realized P&L", f"{summary.get('realized_pnl', 0.0):.2f}")
    metric_cols[4].metric("Unrealized P&L", f"{summary.get('unrealized_pnl', 0.0):.2f}")
    metric_cols[5].metric("Total P&L", f"{summary.get('total_pnl', 0.0):.2f}")

    open_positions = report.get("open_positions", pd.DataFrame())
    closed_trades = report.get("closed_trades", pd.DataFrame())
    fills = report.get("fills", pd.DataFrame())

    cols = st.columns(2)
    with cols[0]:
        st.markdown("**Open Positions**")
        _display_frame(open_positions, height=300)
    with cols[1]:
        st.markdown("**Closed Trades**")
        _display_frame(closed_trades, height=300)

    st.markdown("**Trade Ledger**")
    _display_frame(fills, height=280)


def _render_tasks_tab() -> None:
    st.subheader("ML Task Log")
    tasks = [
        task
        for task in list_operator_tasks(PROJECT_ROOT)
        if task.get("task_type") in {"ml_prepare_dataset", "ml_train_model", "ml_recipe_run", "ml_recipe_bundle", "shadow_monitor", "ml_workbench", "pipeline"}
    ]
    if not tasks:
        st.info("No ML tasks recorded in this process yet.")
        return

    task_frame = pd.DataFrame(tasks)
    display_columns = ["task_id", "task_type", "label", "status", "started_at", "finished_at", "error"]
    for column in display_columns:
        if column not in task_frame.columns:
            task_frame[column] = None
    task_frame = task_frame[display_columns]
    _display_frame(task_frame, height=260)

    task_options = {
        f"{task.get('label', 'task')} [{task.get('task_id', 'unknown')}]": task.get("task_id", "")
        for task in tasks
        if task.get("task_id")
    }
    if not task_options:
        st.info("Tasks are present but none expose a usable task id yet.")
        return
    selected_task = st.selectbox("Inspect Task Logs", options=list(task_options.keys()))
    logs = get_task_logs(task_options[selected_task], PROJECT_ROOT)
    if logs:
        st.code("\n".join(logs[-200:]), language="text")
    else:
        st.info("No logs captured yet.")


def main() -> None:
    st.set_page_config(page_title="ML Workbench", layout="wide")
    st.title("ML Workbench")
    st.caption("Dedicated UI for dataset preparation, model training, and ML result inspection.")

    st.sidebar.title("Controls")
    _refresh_button()
    st.sidebar.markdown(
        "\n".join(
            [
                "Run directly with:",
                "`PYTHONPATH=. ./.venv/bin/streamlit run ui/ml/app.py`",
            ]
        )
    )
    _render_active_deployment_banner()

    tab_builder, tab_train, tab_results, tab_execution, tab_trades, tab_datasets, tab_models, tab_monitoring, tab_tasks = st.tabs(
        ["Builder", "Training", "Results", "Execution", "Trades", "Datasets", "Models", "Monitoring", "Tasks"]
    )
    with tab_builder:
        _render_builder_tab()
    with tab_train:
        _render_training_tab()
    with tab_results:
        _render_results_tab()
    with tab_execution:
        _render_execution_tab()
    with tab_trades:
        _render_trades_tab()
    with tab_datasets:
        _render_datasets_tab()
    with tab_models:
        _render_models_tab()
    with tab_monitoring:
        _render_monitoring_tab()
    with tab_tasks:
        _render_tasks_tab()


if __name__ == "__main__":
    main()
