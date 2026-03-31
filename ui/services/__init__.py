"""Shared service layer for dashboard UIs."""

from .execution_data import (
    get_execution_context,
    get_execution_db_stats,
    get_execution_health,
    load_latest_rank_frames,
    load_execution_payload,
    load_shadow_overlay_frame,
    load_shadow_summary_frame,
    pivot_shadow_summary_frame,
)
from .control_center import (
    get_recent_runs,
    get_run_details,
    get_task_logs,
    launch_streamlit_dashboard_task,
    list_project_processes,
    terminate_project_process,
    launch_pipeline_task,
    launch_shadow_monitor_task,
    list_operator_tasks,
)

__all__ = [
    "get_execution_context",
    "get_execution_db_stats",
    "get_execution_health",
    "load_latest_rank_frames",
    "load_execution_payload",
    "load_shadow_overlay_frame",
    "load_shadow_summary_frame",
    "pivot_shadow_summary_frame",
    "get_recent_runs",
    "get_run_details",
    "get_task_logs",
    "launch_streamlit_dashboard_task",
    "list_project_processes",
    "terminate_project_process",
    "launch_pipeline_task",
    "launch_shadow_monitor_task",
    "list_operator_tasks",
]
