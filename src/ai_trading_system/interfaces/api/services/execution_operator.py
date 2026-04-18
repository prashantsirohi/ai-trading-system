"""Application-service layer for the execution API and React console."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from analytics.registry import RegistryStore
from ai_trading_system.interfaces.api.services.control_center import (
    find_latest_publishable_run,
    get_operator_task,
    get_recent_runs,
    get_run_details,
    launch_pipeline_task,
    launch_shadow_monitor_task,
    launch_streamlit_dashboard_task,
    list_operator_tasks,
    list_project_processes,
    terminate_operator_task,
    terminate_project_process,
)
from ai_trading_system.interfaces.api.services.execution_data import load_shadow_overlay_frame, load_shadow_summary_frame, pivot_shadow_summary_frame
from ai_trading_system.interfaces.api.services.readmodels import (
    get_execution_summary_read_model,
    get_market_snapshot_read_model,
    get_pipeline_workspace_snapshot_read_model,
    get_ranking_snapshot_read_model,
)

STAGE_LABELS = {
    "ingest": "Updating market data",
    "features": "Computing indicators",
    "rank": "Refreshing rankings",
    "publish": "Publishing outputs",
}

ACTION_INFERENCE = {
    ("ingest", "features", "rank", "publish"): "full_pipeline",
    ("ingest", "features", "rank"): "market_refresh",
    ("publish",): "publish_retry",
}

NON_PIPELINE_PHASES = {
    "shadow_monitor": "refreshing_overlay",
    "streamlit_dashboard": "launching_process",
    "ml_workbench": "launching_process",
}

PUBLISH_CHANNEL_LABELS = {
    "google_sheets_portfolio": "Portfolio Sheet",
    "telegram_summary": "Telegram Summary",
    "google_sheets_dashboard": "Dashboard Sheet",
    "quantstats_dashboard_tearsheet": "QuantStats Tearsheet",
    "local_summary": "Local Summary",
}


def _records(frame: pd.DataFrame, *, limit: Optional[int] = None) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    display = frame.copy()
    if limit is not None:
        display = display.head(int(limit))
    display = display.where(pd.notnull(display), None)
    for column in display.columns:
        if pd.api.types.is_datetime64_any_dtype(display[column]):
            display[column] = pd.to_datetime(display[column], errors="coerce").astype(str)
    return display.to_dict(orient="records")


def _task_registry(project_root: str | Path) -> RegistryStore:
    return RegistryStore(Path(project_root))


def _infer_operator_action_type(task: dict[str, Any]) -> str | None:
    metadata = task.get("metadata") or {}
    explicit = metadata.get("operator_action_type")
    if explicit:
        return str(explicit)
    task_type = str(task.get("task_type") or "")
    if task_type == "pipeline":
        stages = tuple(str(stage) for stage in metadata.get("stages") or [])
        return ACTION_INFERENCE.get(stages, "pipeline_task")
    if task_type == "shadow_monitor":
        return "shadow_refresh"
    if task_type == "streamlit_dashboard":
        return "open_research"
    return task_type or None


def _pipeline_stage_statuses(
    registry: RegistryStore,
    run_id: str,
    stage_sequence: list[str],
    *,
    started_after: str | None = None,
) -> list[dict[str, Any]]:
    raw_stage_runs = registry.get_stage_runs(run_id, started_after=started_after)
    latest_by_stage: dict[str, dict[str, Any]] = {}
    for row in raw_stage_runs:
        latest_by_stage[str(row["stage_name"])] = row
    statuses: list[dict[str, Any]] = []
    for stage_name in stage_sequence:
        row = latest_by_stage.get(stage_name)
        statuses.append(
            {
                "stage_name": stage_name,
                "label": STAGE_LABELS.get(stage_name, stage_name.replace("_", " ").title()),
                "status": row.get("status") if row else "pending",
                "attempt_number": row.get("attempt_number") if row else None,
                "error_message": row.get("error_message") if row else None,
            }
        )
    return statuses


def _current_phase_label(task: dict[str, Any]) -> str:
    status = str(task.get("status") or "").lower()
    if status == "completed":
        return "completed"
    if status == "completed_with_publish_errors":
        return "completed with publish errors"
    if status == "failed":
        return "failed"
    if status == "terminated":
        return "terminated"
    task_type = str(task.get("task_type") or "")
    return NON_PIPELINE_PHASES.get(task_type, "starting")


def _expected_publish_channels(task: dict[str, Any]) -> list[str]:
    metadata = dict(task.get("metadata") or {})
    params = dict(metadata.get("params") or {})
    stages = [str(stage) for stage in metadata.get("stages") or []]
    if "publish" not in stages and str(task.get("task_type") or "") != "pipeline":
        return []
    if bool(params.get("local_publish", False)):
        return ["local_summary"]
    channels = ["google_sheets_portfolio", "telegram_summary"]
    channels.append("google_sheets_dashboard")
    if bool(params.get("publish_quantstats", True)):
        channels.append("quantstats_dashboard_tearsheet")
    return channels


def _build_publish_progress(registry: RegistryStore, run_id: str, task: dict[str, Any]) -> dict[str, Any]:
    delivery_logs = registry.get_delivery_logs(run_id)
    expected_channels = _expected_publish_channels(task)
    latest_by_channel: dict[str, dict[str, Any]] = {}
    for row in delivery_logs:
        latest_by_channel[str(row.get("channel"))] = row

    channels: list[dict[str, Any]] = []
    ordered_channels = expected_channels + [channel for channel in latest_by_channel.keys() if channel not in expected_channels]
    for channel in ordered_channels:
        row = latest_by_channel.get(channel, {})
        inferred_status = None
        inferred_detail = None
        if not row and channel == "local_summary" and str(task.get("status") or "").lower() == "completed":
            inferred_status = "delivered"
            inferred_detail = "Local publish summary completed."
        status = str(row.get("status") or inferred_status or ("pending" if channel in expected_channels else "unknown")).lower()
        channels.append(
            {
                "channel": channel,
                "label": PUBLISH_CHANNEL_LABELS.get(channel, channel.replace("_", " ").title()),
                "status": status,
                "attempt_number": row.get("attempt_number") or (1 if inferred_status == "delivered" else None),
                "detail": row.get("error_message") or row.get("external_report_id") or row.get("external_message_id") or inferred_detail or (
                    "Waiting for publish delivery" if status == "pending" else None
                ),
            }
        )
    delivered = len([row for row in channels if row.get("status") in {"delivered", "duplicate"}])
    failed = len([row for row in channels if row.get("status") == "failed"])
    retrying = len([row for row in channels if row.get("status") == "retrying"])
    pending = len([row for row in channels if row.get("status") in {"pending", "unknown"}])
    return {
        "channels": channels,
        "summary": {
            "delivered": delivered,
            "failed": failed,
            "retrying": retrying,
            "pending": pending,
            "total": len(channels),
        },
    }


def _enrich_task(project_root: str | Path, task: dict[str, Any]) -> dict[str, Any]:
    registry = _task_registry(project_root)
    enriched = dict(task)
    metadata = dict(enriched.get("metadata") or {})
    operator_action_type = _infer_operator_action_type(enriched)
    run_id = metadata.get("run_id")
    stage_sequence = [str(stage) for stage in metadata.get("stages") or []]
    current_stage = metadata.get("current_stage")
    stage_statuses: list[dict[str, Any]] = []
    run: dict[str, Any] | None = None

    if run_id:
        try:
            run = registry.get_run(str(run_id))
        except KeyError:
            run = None
        else:
            if stage_sequence:
                stage_statuses = _pipeline_stage_statuses(
                    registry,
                    str(run_id),
                    stage_sequence,
                    started_after=str(enriched.get("started_at")) if enriched.get("started_at") else None,
                )
            if stage_statuses and any(str(row.get("status") or "").lower() != "pending" for row in stage_statuses):
                current_stage = run.get("current_stage") or current_stage

    if stage_sequence and not stage_statuses:
        for stage_name in stage_sequence:
            stage_statuses.append(
                {
                    "stage_name": stage_name,
                    "label": STAGE_LABELS.get(stage_name, stage_name.replace("_", " ").title()),
                    "status": "pending",
                    "attempt_number": None,
                    "error_message": None,
                }
            )

    current_stage_label = None
    if current_stage:
        current_stage_label = STAGE_LABELS.get(str(current_stage), str(current_stage).replace("_", " ").title())
    elif stage_statuses:
        running = next((row for row in stage_statuses if row.get("status") == "running"), None)
        completed = [row for row in stage_statuses if row.get("status") == "completed"]
        if running:
            current_stage = running["stage_name"]
            current_stage_label = running["label"]
        elif completed:
            current_stage = completed[-1]["stage_name"]
            current_stage_label = completed[-1]["label"]
    elif run_id and run is not None and str(run.get("status") or "").lower() == "completed":
        current_stage_label = "completed"

    enriched["operator_action_type"] = operator_action_type
    enriched["run_id"] = run_id
    enriched["current_stage"] = current_stage
    enriched["current_stage_label"] = current_stage_label or _current_phase_label(enriched)
    enriched["stage_sequence"] = stage_sequence
    enriched["stage_statuses"] = stage_statuses
    enriched["phase_label"] = _current_phase_label(enriched)
    enriched["origin_action"] = operator_action_type
    metadata_pid = metadata.get("pid")
    enriched["can_terminate"] = bool(metadata_pid) or str(enriched.get("status") or "").lower() == "running"
    if metadata_pid:
        enriched["terminate_reason"] = "Subprocess-backed task can be terminated with SIGTERM."
    elif str(enriched.get("status") or "").lower() == "running":
        enriched["terminate_reason"] = "In-process task termination is limited; stale tasks can be reconciled but live threads cannot be force-stopped safely."
    else:
        enriched["terminate_reason"] = "Task is already terminal."
    enriched["publish_progress"] = {"channels": [], "summary": {"delivered": 0, "failed": 0, "retrying": 0, "pending": 0, "total": 0}}
    if run_id and "publish" in stage_sequence:
        enriched["publish_progress"] = _build_publish_progress(registry, str(run_id), enriched)
    return enriched


def get_task_detail(project_root: str | Path, task_id: str) -> dict[str, Any]:
    task = get_operator_task(task_id, project_root)
    return _enrich_task(project_root, task)


def list_task_details(project_root: str | Path, *, limit: int = 50) -> list[dict[str, Any]]:
    tasks = list_operator_tasks(project_root)[:limit]
    return [_enrich_task(project_root, task) for task in tasks]


def get_execution_summary(project_root: str | Path) -> dict[str, Any]:
    root = Path(project_root)
    tasks = list_task_details(root, limit=100)
    return get_execution_summary_read_model(root, tasks=tasks)


def get_ranking_snapshot(project_root: str | Path, *, limit: int = 25) -> dict[str, Any]:
    return get_ranking_snapshot_read_model(project_root, limit=limit)


def get_market_snapshot(project_root: str | Path, *, limit: int = 25) -> dict[str, Any]:
    return get_market_snapshot_read_model(project_root, limit=limit)


def get_pipeline_workspace_snapshot(project_root: str | Path, *, limit: int = 20) -> dict[str, Any]:
    return get_pipeline_workspace_snapshot_read_model(project_root, limit=limit)


def get_shadow_snapshot(project_root: str | Path) -> dict[str, Any]:
    overlay = load_shadow_overlay_frame(project_root)
    weekly_5 = pivot_shadow_summary_frame(load_shadow_summary_frame("weekly", 5, project_root=project_root))
    weekly_20 = pivot_shadow_summary_frame(load_shadow_summary_frame("weekly", 20, project_root=project_root))
    monthly_5 = pivot_shadow_summary_frame(load_shadow_summary_frame("monthly", 5, project_root=project_root))
    monthly_20 = pivot_shadow_summary_frame(load_shadow_summary_frame("monthly", 20, project_root=project_root))
    return {
        "overlay": _records(overlay, limit=20),
        "weekly_5d": _records(weekly_5, limit=12),
        "weekly_20d": _records(weekly_20, limit=12),
        "monthly_5d": _records(monthly_5, limit=12),
        "monthly_20d": _records(monthly_20, limit=12),
    }


def get_task_snapshot(project_root: str | Path, task_id: str, *, after: int = 0, log_limit: int = 300) -> dict[str, Any]:
    task = get_task_detail(project_root, task_id)
    logs = _task_registry(project_root).get_operator_task_logs(task_id, after=after, limit=log_limit)
    return {
        "task": task,
        "logs": logs,
    }


def run_pipeline_action(
    project_root: str | Path,
    *,
    label: str,
    stages: list[str],
    params: Optional[dict[str, Any]] = None,
    run_id: Optional[str] = None,
    run_date: Optional[str] = None,
) -> dict[str, Any]:
    task_id = launch_pipeline_task(
        project_root=project_root,
        label=label,
        stage_names=stages,
        params=params or {},
        run_id=run_id,
        run_date=run_date,
    )
    return get_task_detail(task_id=task_id, project_root=project_root)


def retry_publish_action(project_root: str | Path, *, local_publish: bool = False) -> dict[str, Any]:
    publishable_run = find_latest_publishable_run(project_root, limit=50)
    if not publishable_run:
        raise ValueError("No publishable run found for publish retry. A completed rank artifact is required.")
    latest_run_id = str(publishable_run["run_id"])
    return run_pipeline_action(
        project_root,
        label=f"Publish retry {latest_run_id}",
        stages=["publish"],
        params={"data_domain": "operational", "preflight": False, "local_publish": bool(local_publish)},
        run_id=latest_run_id,
    )


def run_shadow_action(project_root: str | Path, *, label: str, backfill_days: int = 0, prediction_date: str | None = None) -> dict[str, Any]:
    _ = Path(project_root)
    task_id = launch_shadow_monitor_task(label=label, backfill_days=backfill_days, prediction_date=prediction_date)
    return get_task_detail(task_id=task_id, project_root=project_root)


def launch_research_action(project_root: str | Path, *, port: int = 8501) -> dict[str, Any]:
    task_id = launch_streamlit_dashboard_task(project_root=project_root, port=port)
    return get_task_detail(task_id=task_id, project_root=project_root)


def get_process_snapshot(project_root: str | Path) -> dict[str, Any]:
    return {"processes": list_project_processes(project_root)}


def terminate_process_action(project_root: str | Path, pid: int) -> dict[str, Any]:
    return terminate_project_process(project_root, int(pid))


def terminate_task_action(project_root: str | Path, task_id: str) -> dict[str, Any]:
    return terminate_operator_task(task_id, project_root)
