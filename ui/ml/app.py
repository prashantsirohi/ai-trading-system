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
    deploy_workbench_model,
    get_task_logs,
    launch_prepare_dataset_task,
    launch_shadow_monitor_task,
    launch_train_model_task,
    list_operator_tasks,
    load_shadow_overlay_frame,
    load_shadow_summary_frame,
    load_workbench_datasets,
    load_workbench_deployments,
    load_workbench_models,
    load_model_workbench_detail,
    pivot_shadow_summary_frame,
    rollback_workbench_deployment,
)
from utils.data_domains import research_static_end_date

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


def _render_training_tab() -> None:
    st.subheader("Launch ML Workflows")
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


def _render_tasks_tab() -> None:
    st.subheader("ML Task Log")
    tasks = [
        task
        for task in list_operator_tasks()
        if task.get("task_type") in {"ml_prepare_dataset", "ml_train_model", "shadow_monitor", "ml_workbench"}
    ]
    if not tasks:
        st.info("No ML tasks recorded in this process yet.")
        return

    task_frame = pd.DataFrame(tasks)[["task_id", "task_type", "label", "status", "started_at", "finished_at", "error"]]
    _display_frame(task_frame, height=260)

    task_options = {f"{task['label']} [{task['task_id']}]": task["task_id"] for task in tasks}
    selected_task = st.selectbox("Inspect Task Logs", options=list(task_options.keys()))
    logs = get_task_logs(task_options[selected_task])
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

    tab_train, tab_datasets, tab_models, tab_monitoring, tab_tasks = st.tabs(
        ["Training", "Datasets", "Models", "Monitoring", "Tasks"]
    )
    with tab_train:
        _render_training_tab()
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
