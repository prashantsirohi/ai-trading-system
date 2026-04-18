"""NiceGUI execution control center for live operational workflows."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd

from core.bootstrap import ensure_project_root_on_path

ensure_project_root_on_path(__file__)

from ai_trading_system.interfaces.api.services import (  # noqa: E402
    find_latest_publishable_run,
    get_execution_db_stats,
    get_execution_health,
    load_latest_rank_frames,
    get_recent_runs,
    get_run_details,
    get_task_logs,
    launch_streamlit_dashboard_task,
    launch_pipeline_task,
    launch_shadow_monitor_task,
    list_operator_tasks,
    list_project_processes,
    load_execution_payload,
    load_shadow_overlay_frame,
    load_shadow_summary_frame,
    pivot_shadow_summary_frame,
    terminate_project_process,
)

try:  # pragma: no cover - optional dependency boundary
    from nicegui import ui
except ImportError:  # pragma: no cover - optional dependency boundary
    ui = None


PROJECT_ROOT = Path(__file__).resolve().parents[5]


def _status_chip(status: str | None) -> str:
    normalized = (status or "").lower()
    if normalized in {"completed", "ok"}:
        return "emerald"
    if normalized in {"completed_with_publish_errors", "warn", "warning"}:
        return "amber"
    if normalized in {"failed", "error"}:
        return "rose"
    if normalized == "running":
        return "sky"
    return "slate"


def _display_frame(frame: pd.DataFrame, limit: int = 20) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    display = frame.head(limit).copy()
    for column in ("prediction_date", "period_start", "started_at", "ended_at", "run_date"):
        if column in display.columns:
            try:
                display[column] = pd.to_datetime(display[column]).dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                display[column] = display[column].astype(str)
    float_cols = display.select_dtypes(include=["float64", "float32"]).columns
    if len(float_cols) > 0:
        display[float_cols] = display[float_cols].round(2)
    return display.fillna("")


def _empty_state(icon: str, title: str, body: str) -> None:
    with ui.column().classes("w-full items-center justify-center gap-2 py-10 text-center"):
        ui.icon(icon).classes("text-4xl text-slate-300")
        ui.label(title).classes("text-base font-semibold text-slate-600")
        ui.label(body).classes("max-w-[420px] text-sm text-slate-400")


def _curate_frame(title: str, frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    display = frame.copy()
    preferred: dict[str, list[str]] = {
        "Current Control State": ["setting", "value"],
        "Operational Health Checks": ["name", "status", "detail"],
        "Market Health Checks": ["name", "status", "detail"],
        "Top Ranked Signals": [
            "symbol_id",
            "close",
            "composite_score",
            "rel_strength_score",
            "trend_score_score",
            "sector_strength_score",
            "vol_intensity_score",
            "prox_high_score",
            "delivery_pct_score",
        ],
        "Breakout Monitor": [
            "symbol_id",
            "sector",
            "setup_family",
            "execution_label",
            "market_regime",
            "setup_quality",
            "close",
            "breakout_pct",
            "base_width_pct_30",
            "contraction_ratio",
            "volume_ratio",
            "adx_14",
            "near_52w_high_pct",
        ],
        "Sector Leadership": [
            "Sector",
            "Quadrant",
            "RS_rank",
            "RS_rank_pct",
            "Momentum_rank",
            "Momentum_rank_pct",
            "RS",
            "Momentum",
            "RS_20",
            "RS_50",
            "RS_100",
        ],
        "Recent Pipeline Runs": ["run_id", "status", "current_stage", "started_at", "ended_at", "error_class"],
        "Run Summary": ["run_id", "status", "current_stage", "started_at", "ended_at", "error_class", "error_message"],
        "Stage Attempts": ["stage_name", "status", "attempt_number", "started_at", "ended_at", "error_class", "error_message"],
        "Alerts": ["created_at", "alert_type", "severity", "stage_name", "message"],
        "Publish / Delivery Logs": ["channel", "status", "recipient", "created_at", "message"],
        "Latest ML Overlay": [
            "symbol_id",
            "technical_score",
            "technical_rank",
            "ml_5d_prob",
            "ml_5d_rank",
            "ml_20d_prob",
            "ml_20d_rank",
            "blend_20d_score",
            "blend_20d_rank",
        ],
        "5D Weekly Challenger Summary": ["period_start", "picks_technical", "hit_rate_technical", "avg_return_technical", "picks_ml", "hit_rate_ml", "avg_return_ml", "picks_blend", "hit_rate_blend", "avg_return_blend"],
        "20D Weekly Challenger Summary": ["period_start", "picks_technical", "hit_rate_technical", "avg_return_technical", "picks_ml", "hit_rate_ml", "avg_return_ml", "picks_blend", "hit_rate_blend", "avg_return_blend"],
        "5D Monthly Challenger Summary": ["period_start", "picks_technical", "hit_rate_technical", "avg_return_technical", "picks_ml", "hit_rate_ml", "avg_return_ml", "picks_blend", "hit_rate_blend", "avg_return_blend"],
        "20D Monthly Challenger Summary": ["period_start", "picks_technical", "hit_rate_technical", "avg_return_technical", "picks_ml", "hit_rate_ml", "avg_return_ml", "picks_blend", "hit_rate_blend", "avg_return_blend"],
        "Task Queue": ["task_id", "status", "label", "task_type", "started_at", "finished_at", "error"],
        "Live Task Log": ["log_line"],
    }
    order = preferred.get(title)
    if title.startswith("Run Summary"):
        order = preferred.get("Run Summary")
    if order:
        selected = [column for column in order if column in display.columns]
        remainder = [column for column in display.columns if column not in selected]
        display = display[selected + remainder[:4]]
    for col in ("status", "severity", "category", "Quadrant"):
        if col in display.columns:
            display[col] = display[col].astype(str).str.upper()
    return display


def _table(
    container,
    title: str,
    frame: pd.DataFrame,
    *,
    limit: int = 20,
    subtitle: str = "",
    empty: str = "No data available.",
) -> None:
    container.clear()
    with container:
        row_count = 0 if frame is None else len(frame.index)
        with ui.row().classes("w-full items-center justify-between gap-3 mb-3"):
            with ui.column().classes("gap-1"):
                ui.label(title).classes("text-lg font-semibold text-slate-800")
                if subtitle:
                    ui.label(subtitle).classes("text-sm text-slate-500")
            ui.label(f"{row_count} rows").classes("text-xs uppercase tracking-[0.18em] text-slate-400")
        display = _display_frame(_curate_frame(title, frame), limit=limit)
        if display.empty:
            _empty_state("table_rows", title, empty)
            return
        columns = [
            {
                "name": str(column),
                "label": str(column),
                "field": str(column),
                "align": "left",
                "sortable": True,
            }
            for column in display.columns
        ]
        rows = display.astype(object).where(pd.notnull(display), "").to_dict(orient="records")
        ui.table(columns=columns, rows=rows, row_key=display.columns[0]).props(
            "dense flat bordered separator=cell wrap-cells square rows-per-page-options=[10,20,50]"
        ).classes("w-full text-sm rounded-2xl overflow-hidden")


def _metric(parent, title: str, value: Any, subtitle: str = "", *, tone: str = "slate") -> None:
    palette = {
        "slate": "from-slate-900 to-slate-700 text-white",
        "emerald": "from-emerald-600 to-emerald-500 text-white",
        "amber": "from-amber-500 to-orange-400 text-white",
        "rose": "from-rose-600 to-pink-500 text-white",
        "sky": "from-sky-600 to-cyan-500 text-white",
    }
    with parent:
        with ui.card().classes(
            f"min-w-[220px] rounded-[24px] border-0 bg-gradient-to-br {palette.get(tone, palette['slate'])} shadow-lg p-5"
        ):
            ui.label(title).classes("text-xs uppercase tracking-[0.2em] opacity-75")
            ui.label(str(value)).classes("text-3xl font-bold mt-2")
            if subtitle:
                ui.label(subtitle).classes("text-sm mt-1 opacity-85")


def _bar_chart_option(
    frame: pd.DataFrame,
    *,
    label_col: str,
    value_col: str,
    title: str,
    limit: int = 10,
) -> Optional[dict[str, Any]]:
    if frame is None or frame.empty or label_col not in frame.columns or value_col not in frame.columns:
        return None
    chart = frame[[label_col, value_col]].head(limit).copy()
    chart[label_col] = chart[label_col].astype(str)
    chart[value_col] = pd.to_numeric(chart[value_col], errors="coerce")
    chart = chart.dropna(subset=[value_col])
    if chart.empty:
        return None
    return {
        "backgroundColor": "transparent",
        "title": {"text": title, "left": "left", "textStyle": {"color": "#0f172a", "fontWeight": 600}},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "grid": {"left": 40, "right": 16, "top": 48, "bottom": 24, "containLabel": True},
        "xAxis": {
            "type": "value",
            "axisLabel": {"color": "#64748b"},
            "splitLine": {"lineStyle": {"color": "rgba(148,163,184,0.18)"}},
        },
        "yAxis": {
            "type": "category",
            "data": chart[label_col].tolist(),
            "axisLabel": {"color": "#334155"},
        },
        "series": [
            {
                "type": "bar",
                "data": chart[value_col].round(2).tolist(),
                "itemStyle": {
                    "borderRadius": [0, 8, 8, 0],
                    "color": {
                        "type": "linear",
                        "x": 0,
                        "y": 0,
                        "x2": 1,
                        "y2": 0,
                        "colorStops": [
                            {"offset": 0, "color": "#2563eb"},
                            {"offset": 1, "color": "#14b8a6"},
                        ],
                    },
                },
            }
        ],
    }


def _chart(
    container,
    title: str,
    option: Optional[dict[str, Any]],
    *,
    subtitle: str = "",
    empty: str = "No chart data available.",
) -> None:
    container.clear()
    with container:
        with ui.row().classes("w-full items-center justify-between gap-3 mb-3"):
            with ui.column().classes("gap-1"):
                ui.label(title).classes("text-lg font-semibold text-slate-800")
                if subtitle:
                    ui.label(subtitle).classes("text-sm text-slate-500")
        if not option:
            _empty_state("bar_chart", title, empty)
            return
        ui.echart(option).classes("w-full h-[360px]")


def _summary_tiles(container, title: str, items: list[dict[str, Any]], *, empty: str) -> None:
    container.clear()
    with container:
        with ui.row().classes("w-full items-center justify-between gap-3 mb-4"):
            ui.label(title).classes("text-lg font-semibold text-slate-800")
        if not items:
            _empty_state("info", title, empty)
            return
        with ui.row().classes("w-full gap-4 flex-wrap"):
            for item in items:
                tone = _status_chip(str(item.get("tone") or item.get("value")))
                badge_palette = {
                    "emerald": "bg-emerald-100 text-emerald-700",
                    "amber": "bg-amber-100 text-amber-700",
                    "rose": "bg-rose-100 text-rose-700",
                    "sky": "bg-sky-100 text-sky-700",
                    "slate": "bg-slate-100 text-slate-700",
                }
                with ui.card().classes("min-w-[240px] rounded-[22px] border border-slate-200 shadow-none p-4 bg-white"):
                    ui.label(str(item.get("label", ""))).classes("text-xs uppercase tracking-[0.18em] text-slate-400")
                    ui.label(str(item.get("value", "—"))).classes("text-2xl font-semibold text-slate-900 mt-2")
                    if item.get("badge"):
                        ui.label(str(item["badge"])).classes(
                            f"inline-flex mt-3 rounded-full px-3 py-1 text-xs font-semibold {badge_palette.get(tone, badge_palette['slate'])}"
                        )
                    if item.get("detail"):
                        ui.label(str(item["detail"])).classes("text-sm text-slate-500 mt-2")


def build_execution_ui() -> None:
    if ui is None:  # pragma: no cover
        raise ImportError("nicegui is not installed. Install it with `python -m pip install nicegui`.")

    ui.add_head_html(
        """
        <style>
          body {
            background:
              radial-gradient(circle at top left, rgba(20,184,166,0.18), transparent 34%),
              radial-gradient(circle at top right, rgba(59,130,246,0.18), transparent 28%),
              linear-gradient(180deg, #eff6ff 0%, #f8fafc 42%, #eef2ff 100%);
          }
          .section-card {
            border: 1px solid rgba(148,163,184,0.16);
            box-shadow: 0 18px 55px rgba(15,23,42,0.08);
          }
          .glass-panel {
            background: rgba(255,255,255,0.92);
            backdrop-filter: blur(16px);
          }
          .hero-panel {
            background:
              linear-gradient(135deg, rgba(15,23,42,0.96) 0%, rgba(30,41,59,0.94) 36%, rgba(37,99,235,0.90) 72%, rgba(20,184,166,0.88) 100%);
          }
          .action-tile:hover {
            transform: translateY(-2px);
            box-shadow: 0 18px 40px rgba(15,23,42,0.12);
          }
          .action-tile {
            transition: all 160ms ease;
          }
          .nicegui-card {
            border-radius: 26px;
          }
        </style>
        """
    )

    state: dict[str, Any] = {
        "skip_preflight": False,
        "local_publish": False,
        "full_rebuild": False,
        "feature_tail_bars": 252,
        "symbol_limit": 25,
        "streamlit_port": 8501,
        "selected_run_id": None,
        "selected_task_id": None,
        "selected_process_pid": None,
    }
    run_select = None
    task_select = None
    process_select = None

    def pipeline_params(include_publish: bool = False) -> dict[str, Any]:
        return {
            "data_domain": "operational",
            "preflight": not state["skip_preflight"],
            "local_publish": bool(state["local_publish"]) if include_publish else False,
            "full_rebuild": bool(state["full_rebuild"]),
            "feature_tail_bars": int(state["feature_tail_bars"]),
            "symbol_limit": int(state["symbol_limit"]) if state["symbol_limit"] else None,
        }

    pending: dict[str, Any] = {"fn": None, "label": "", "body": ""}

    with ui.dialog() as confirm_dialog, ui.card().classes("rounded-[24px] p-6 w-[520px]"):
        confirm_title = ui.label("").classes("text-xl font-semibold text-slate-800")
        confirm_body = ui.label("").classes("text-sm text-slate-600")
        with ui.row().classes("justify-end gap-3 mt-4"):
            ui.button("Cancel", on_click=confirm_dialog.close).props("flat")

            def _confirm() -> None:
                confirm_dialog.close()
                if pending["fn"]:
                    pending["fn"]()

            ui.button("Run", on_click=_confirm).props("unelevated color=primary")

    def ask_confirm(label: str, body: str, fn: Callable[[], None]) -> None:
        pending["fn"] = fn
        pending["label"] = label
        pending["body"] = body
        confirm_title.text = label
        confirm_body.text = body
        confirm_dialog.open()

    def toast_started(label: str, task_id: str) -> None:
        ui.notify(f"{label} started: {task_id}", type="positive", position="top")
        state["selected_task_id"] = task_id
        refresh_all()

    def run_full_pipeline() -> None:
        task_id = launch_pipeline_task(
            project_root=PROJECT_ROOT,
            label="Full operational pipeline",
            stage_names=["ingest", "features", "rank", "publish"],
            params=pipeline_params(include_publish=True),
        )
        toast_started("Full pipeline", task_id)

    def run_market_refresh() -> None:
        task_id = launch_pipeline_task(
            project_root=PROJECT_ROOT,
            label="Market refresh",
            stage_names=["ingest", "features", "rank"],
            params=pipeline_params(),
        )
        toast_started("Market refresh", task_id)

    def run_feature_refresh() -> None:
        task_id = launch_pipeline_task(
            project_root=PROJECT_ROOT,
            label="Features + rank",
            stage_names=["features", "rank"],
            params=pipeline_params(),
        )
        toast_started("Features + rank", task_id)

    def run_rank_refresh() -> None:
        task_id = launch_pipeline_task(
            project_root=PROJECT_ROOT,
            label="Rank refresh",
            stage_names=["rank"],
            params=pipeline_params(),
        )
        toast_started("Rank refresh", task_id)

    def run_publish_retry() -> None:
        publishable_run = find_latest_publishable_run(PROJECT_ROOT, limit=50)
        if not publishable_run:
            ui.notify("No publishable run found for publish retry.", type="warning", position="top")
            return
        latest_run_id = str(publishable_run["run_id"])
        task_id = launch_pipeline_task(
            project_root=PROJECT_ROOT,
            label=f"Publish retry {latest_run_id}",
            stage_names=["publish"],
            params={"data_domain": "operational", "preflight": False, "local_publish": state["local_publish"]},
            run_id=latest_run_id,
        )
        toast_started("Publish retry", task_id)

    def run_shadow(backfill_days: int = 0) -> None:
        label = "Shadow refresh" if backfill_days == 0 else f"Shadow bootstrap {backfill_days}D"
        task_id = launch_shadow_monitor_task(label=label, backfill_days=backfill_days)
        toast_started(label, task_id)

    def run_streamlit_launcher() -> None:
        task_id = launch_streamlit_dashboard_task(
            project_root=PROJECT_ROOT,
            port=int(state["streamlit_port"]),
        )
        toast_started("Research dashboard", task_id)

    def terminate_selected_process() -> None:
        pid = state.get("selected_process_pid")
        if not pid:
            ui.notify("No process selected.", type="warning", position="top")
            return
        result = terminate_project_process(PROJECT_ROOT, int(pid))
        if result.get("ok"):
            ui.notify(result["message"], type="positive", position="top")
        else:
            ui.notify(result["message"], type="warning", position="top")
        refresh_all()

    with ui.left_drawer(value=False).classes("glass-panel w-[360px] border-r border-slate-200 p-5") as drawer:
        ui.label("Run Controls").classes("text-2xl font-semibold text-slate-800")
        ui.label("Set operational parameters once, then launch from the action cards.").classes(
            "text-sm text-slate-500 mb-4"
        )
        ui.label("Pipeline Flags").classes("text-xs uppercase tracking-[0.2em] text-slate-400 mt-2")
        ui.switch("Skip preflight", value=state["skip_preflight"], on_change=lambda e: state.__setitem__("skip_preflight", bool(e.value))).classes("mt-1")
        ui.switch("Local publish only", value=state["local_publish"], on_change=lambda e: state.__setitem__("local_publish", bool(e.value)))
        ui.switch("Full feature rebuild", value=state["full_rebuild"], on_change=lambda e: state.__setitem__("full_rebuild", bool(e.value)))
        ui.separator().classes("my-4")
        ui.label("Execution Parameters").classes("text-xs uppercase tracking-[0.2em] text-slate-400")
        ui.number("Feature tail bars", value=state["feature_tail_bars"], min=50, max=400, step=10).props("outlined dense").bind_value(state, "feature_tail_bars").classes("w-full")
        ui.number("Canary symbol limit", value=state["symbol_limit"], min=5, max=200, step=5).props("outlined dense").bind_value(state, "symbol_limit").classes("w-full")
        ui.number("Research Streamlit port", value=state["streamlit_port"], min=8501, max=8999, step=1).props("outlined dense").bind_value(state, "streamlit_port").classes("w-full")
        with ui.card().classes("mt-6 rounded-[22px] bg-slate-900 text-white border-0 p-4"):
            ui.label("Operator Note").classes("text-xs uppercase tracking-[0.2em] opacity-60")
            ui.label("Use the drawer for run defaults. Launch flows from the action board, then inspect the selected run and current task below.").classes("text-sm mt-2 opacity-90")

    ui.query("body").classes("font-[Instrument_Sans],text-slate-800")
    with ui.column().classes("w-full max-w-[1680px] mx-auto px-6 py-6 gap-6"):
        with ui.card().classes("hero-panel rounded-[32px] border-0 shadow-2xl p-8 text-white"):
            with ui.row().classes("w-full justify-between items-start gap-6"):
                with ui.column().classes("gap-2 max-w-[920px]"):
                    ui.label("Execution Control Center").classes("text-4xl font-bold tracking-tight")
                    ui.label(
                        "Run the live pipeline, monitor freshness, inspect ranked signals, review alerts, and track ML challengers from one execution surface."
                    ).classes("text-base opacity-90")
                ui.button("Open Controls", on_click=drawer.toggle).props("outline color=white").classes("rounded-xl font-semibold")

            with ui.row().classes("gap-3 mt-6 flex-wrap"):
                ui.button(
                    "Run Full Pipeline",
                    on_click=lambda: ask_confirm(
                        "Run Full Pipeline",
                        "This will execute ingest, features, rank, and publish on the operational domain.",
                        run_full_pipeline,
                    ),
                ).props("unelevated").classes("bg-white text-slate-900 rounded-xl px-5 py-3 font-semibold")
                ui.button(
                    "Market Refresh",
                    on_click=lambda: ask_confirm(
                        "Run Market Refresh",
                        "This will execute ingest, features, and rank on the operational domain.",
                        run_market_refresh,
                    ),
                ).props("outline color=white").classes("rounded-xl px-5 py-3 font-semibold")
                ui.button(
                    "Shadow Refresh",
                    on_click=lambda: ask_confirm(
                        "Run Shadow Refresh",
                        "This will score the current universe with the trained 5d and 20d LightGBM challengers.",
                        lambda: run_shadow(0),
                    ),
                ).props("outline color=white").classes("rounded-xl px-5 py-3 font-semibold")

        metrics_row = ui.row().classes("w-full gap-4 flex-wrap")

        with ui.column().classes("w-full gap-6"):
            with ui.tabs().classes("w-full") as tabs:
                control_tab = ui.tab("Control")
                ranking_tab = ui.tab("Ranking")
                market_tab = ui.tab("Market")
                operations_tab = ui.tab("Operations")
                shadow_tab = ui.tab("Shadow")
                tasks_tab = ui.tab("Tasks")
                processes_tab = ui.tab("Processes")

            with ui.tab_panels(tabs, value=control_tab).classes("w-full"):
                with ui.tab_panel(control_tab):
                    with ui.row().classes("w-full gap-6 items-start"):
                        action_board = ui.card().classes("glass-panel section-card w-[430px] rounded-[28px] shadow-sm border-0 p-5")
                        control_summary_container = ui.card().classes("glass-panel section-card flex-1 rounded-[26px] shadow-sm border-0 p-5")

                    with action_board:
                        ui.label("Action Board").classes("text-xl font-semibold text-slate-800")
                        ui.label("Run the common flows directly from here.").classes("text-sm text-slate-500 mb-3")

                        def action_card(title: str, subtitle: str, action: Callable[[], None], confirm_text: str, icon: str) -> None:
                            with ui.card().classes("action-tile rounded-[22px] border border-slate-200 shadow-none p-4 bg-white mb-3"):
                                with ui.row().classes("items-start gap-3"):
                                    ui.icon(icon).classes("text-2xl text-sky-600 mt-1")
                                    with ui.column().classes("gap-1 flex-1"):
                                        ui.label(title).classes("text-lg font-semibold text-slate-800")
                                        ui.label(subtitle).classes("text-sm text-slate-500 mb-3")
                                ui.button(
                                    "Run",
                                    on_click=lambda: ask_confirm(title, confirm_text, action),
                                ).props("unelevated color=primary").classes("rounded-xl")

                        action_card("Ingest + Features + Rank", "Update market data, recompute signals, and refresh the board.", run_market_refresh, "Run ingest, features, and rank now?", "radar")
                        action_card("Features + Rank", "Skip ingest and rebuild operational indicators plus ranking.", run_feature_refresh, "Run features and rank on the current operational store?", "insights")
                        action_card("Rank Only", "Refresh the ranked artifacts from already-computed data.", run_rank_refresh, "Run rank only?", "leaderboard")
                        action_card("Publish Latest Run", "Retry publish for the most recent operational run.", run_publish_retry, "Retry publish for the latest pipeline run?", "send")
                        action_card("Shadow Bootstrap 30D", "Seed recent challenger-vs-champion history for weekly/monthly views.", lambda: run_shadow(30), "Backfill 30 days of shadow predictions and outcomes?", "psychology")
                        action_card("Launch Research Streamlit", "Start the research/backtesting dashboard on the configured Streamlit port.", run_streamlit_launcher, f"Launch Streamlit research dashboard on port {state['streamlit_port']}?", "monitoring")

                    control_health_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5 mt-6")

                with ui.tab_panel(ranking_tab):
                    ranking_summary_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5 mb-6")
                    with ui.row().classes("w-full gap-6 items-start"):
                        ranked_chart_container = ui.card().classes("glass-panel section-card w-[420px] rounded-[26px] shadow-sm border-0 p-5")
                        ranked_container = ui.card().classes("glass-panel section-card flex-1 rounded-[26px] shadow-sm border-0 p-5")

                with ui.tab_panel(market_tab):
                    market_summary_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5 mb-6")
                    with ui.row().classes("w-full gap-6 items-start"):
                        breakout_container = ui.card().classes("glass-panel section-card w-1/2 rounded-[26px] shadow-sm border-0 p-5")
                        sector_chart_container = ui.card().classes("glass-panel section-card w-1/2 rounded-[26px] shadow-sm border-0 p-5")
                    sectors_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5")
                    health_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5")

                with ui.tab_panel(operations_tab):
                    operations_summary_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5 mb-6")
                    with ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5 mb-6"):
                        ui.label("Run Inspector").classes("text-xl font-semibold text-slate-800")
                        ui.label("Select any recent run to inspect stage attempts, alerts, and publish activity.").classes(
                            "text-sm text-slate-500 mb-4"
                        )
                        with ui.row().classes("items-center gap-4 flex-wrap"):
                            run_select = ui.select(
                                options={},
                                value=state["selected_run_id"],
                                label="Selected run",
                                on_change=lambda e: state.__setitem__("selected_run_id", e.value),
                            ).props("outlined dense options-dense").classes("min-w-[460px]")
                            ui.button("Refresh Panel", on_click=lambda: refresh_all()).props("flat color=primary")
                    with ui.row().classes("w-full gap-6 items-start"):
                        recent_runs_container = ui.card().classes("glass-panel section-card w-1/2 rounded-[26px] shadow-sm border-0 p-5")
                        run_summary_container = ui.card().classes("glass-panel section-card w-1/2 rounded-[26px] shadow-sm border-0 p-5")
                    with ui.row().classes("w-full gap-6 items-start"):
                        stage_runs_container = ui.card().classes("glass-panel section-card w-1/2 rounded-[26px] shadow-sm border-0 p-5")
                        alerts_container = ui.card().classes("glass-panel section-card w-1/2 rounded-[26px] shadow-sm border-0 p-5")
                    delivery_logs_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5")

                with ui.tab_panel(shadow_tab):
                    with ui.row().classes("w-full gap-6 items-start"):
                        overlay_container = ui.card().classes("glass-panel section-card w-1/2 rounded-[26px] shadow-sm border-0 p-5")
                        weekly5_container = ui.card().classes("glass-panel section-card w-1/2 rounded-[26px] shadow-sm border-0 p-5")
                    with ui.row().classes("w-full gap-6 items-start"):
                        weekly20_container = ui.card().classes("glass-panel section-card w-1/2 rounded-[26px] shadow-sm border-0 p-5")
                        monthly5_container = ui.card().classes("glass-panel section-card w-1/2 rounded-[26px] shadow-sm border-0 p-5")
                    monthly20_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5")

                with ui.tab_panel(tasks_tab):
                    tasks_summary_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5 mb-6")
                    with ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5 mb-6"):
                        ui.label("Task Monitor").classes("text-xl font-semibold text-slate-800")
                        ui.label("Follow the currently running UI-triggered task or inspect completed runs with full logs.").classes(
                            "text-sm text-slate-500 mb-4"
                        )
                        with ui.row().classes("items-center gap-4 flex-wrap"):
                            task_select = ui.select(
                                options={},
                                value=state["selected_task_id"],
                                label="Selected task",
                                on_change=lambda e: state.__setitem__("selected_task_id", e.value),
                            ).props("outlined dense options-dense").classes("min-w-[460px]")
                            ui.button("Jump To Active", on_click=lambda: state.__setitem__("selected_task_id", None)).props("flat color=primary")
                    task_queue_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5")
                    task_log_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5 mt-6")

                with ui.tab_panel(processes_tab):
                    processes_summary_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5 mb-6")
                    with ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5 mb-6"):
                        ui.label("Process Control").classes("text-xl font-semibold text-slate-800")
                        ui.label("Inspect project processes and terminate stale dashboard or pipeline sessions safely.").classes(
                            "text-sm text-slate-500 mb-4"
                        )
                        with ui.row().classes("items-center gap-4 flex-wrap"):
                            process_select = ui.select(
                                options={},
                                value=state["selected_process_pid"],
                                label="Selected process",
                                on_change=lambda e: state.__setitem__("selected_process_pid", e.value),
                            ).props("outlined dense options-dense").classes("min-w-[520px]")
                            ui.button(
                                "Terminate Process",
                                on_click=lambda: ask_confirm(
                                    "Terminate Project Process",
                                    f"Send SIGTERM to the selected project process ({state.get('selected_process_pid')})?",
                                    terminate_selected_process,
                                ),
                            ).props("flat color=negative")
                            ui.button("Refresh Processes", on_click=lambda: refresh_all()).props("flat color=primary")
                    process_table_container = ui.card().classes("glass-panel section-card w-full rounded-[26px] shadow-sm border-0 p-5")

        task_log_area = ui.textarea(label="Current Task Log").props("readonly autogrow outlined").classes("w-full")

    def _populate_task_log() -> None:
        tasks = list_operator_tasks(PROJECT_ROOT)
        running_ids = [task["task_id"] for task in tasks if task.get("status") == "running"]
        chosen = state["selected_task_id"] or (running_ids[0] if running_ids else (tasks[0]["task_id"] if tasks else None))
        state["selected_task_id"] = chosen
        logs = get_task_logs(chosen, PROJECT_ROOT) if chosen else []
        task_log_area.value = "\n".join(logs[-80:]) if logs else "No task logs yet."

    def refresh_all() -> None:
        payload = load_execution_payload(PROJECT_ROOT)
        rank_frames = load_latest_rank_frames(PROJECT_ROOT)
        db_stats = get_execution_db_stats(PROJECT_ROOT)
        health = get_execution_health(PROJECT_ROOT, payload)
        shadow_overlay = load_shadow_overlay_frame(PROJECT_ROOT)
        weekly_5 = pivot_shadow_summary_frame(load_shadow_summary_frame("week", 5, periods=8, project_root=PROJECT_ROOT))
        weekly_20 = pivot_shadow_summary_frame(load_shadow_summary_frame("week", 20, periods=8, project_root=PROJECT_ROOT))
        monthly_5 = pivot_shadow_summary_frame(load_shadow_summary_frame("month", 5, periods=6, project_root=PROJECT_ROOT))
        monthly_20 = pivot_shadow_summary_frame(load_shadow_summary_frame("month", 20, periods=6, project_root=PROJECT_ROOT))
        recent_runs = get_recent_runs(PROJECT_ROOT, limit=12)
        tasks = pd.DataFrame(list_operator_tasks(PROJECT_ROOT))
        processes = pd.DataFrame(list_project_processes(PROJECT_ROOT))

        summary = payload.get("summary", {})
        ranked = rank_frames.get("ranked_signals", pd.DataFrame())
        breakouts = rank_frames.get("breakout_scan", pd.DataFrame())
        sectors = rank_frames.get("sector_dashboard", pd.DataFrame())
        health_checks = pd.DataFrame(health.get("checks", []))

        if recent_runs and state["selected_run_id"] not in {row["run_id"] for row in recent_runs}:
            state["selected_run_id"] = recent_runs[0]["run_id"]

        if run_select is not None:
            run_select.options = {
                row["run_id"]: f"{row['run_id']}  |  {row.get('status', 'unknown')}  |  {row.get('started_at') or 'n/a'}"
                for row in recent_runs
            }
            run_select.value = state["selected_run_id"]
            run_select.update()

        details = get_run_details(PROJECT_ROOT, state["selected_run_id"]) if state["selected_run_id"] else None

        task_rows = list_operator_tasks(PROJECT_ROOT)
        if task_rows and state["selected_task_id"] not in {row["task_id"] for row in task_rows}:
            running = [row["task_id"] for row in task_rows if row.get("status") == "running"]
            state["selected_task_id"] = running[0] if running else task_rows[0]["task_id"]
        if task_select is not None:
            task_select.options = {
                row["task_id"]: f"{row['task_id']}  |  {row.get('status', 'unknown')}  |  {row.get('label', '')}"
                for row in task_rows
            }
            task_select.value = state["selected_task_id"]
            task_select.update()

        process_rows = list_project_processes(PROJECT_ROOT)
        if process_rows and state["selected_process_pid"] not in {row["pid"] for row in process_rows}:
            state["selected_process_pid"] = process_rows[0]["pid"]
        if process_select is not None:
            process_select.options = {
                row["pid"]: f"{row['pid']}  |  {row.get('kind', 'other')}  |  port={row.get('port') or '—'}  |  {row.get('etime', '')}"
                for row in process_rows
            }
            process_select.value = state["selected_process_pid"]
            process_select.update()

        metrics_row.clear()
        with metrics_row:
            _metric(metrics_row, "Latest OHLCV", db_stats.get("latest_date") or "—", f"{db_stats.get('symbols', 0)} symbols", tone="sky")
            _metric(metrics_row, "Top Symbol", summary.get("top_symbol") or "—", f"{summary.get('ranked_count', 0)} ranked", tone="slate")
            _metric(metrics_row, "Breakouts", summary.get("breakout_count", 0), f"Top sector: {summary.get('top_sector') or '—'}", tone="emerald")
            _metric(metrics_row, "Health", health.get("status", "unknown").upper(), tone=_status_chip(health.get("status")))
            _metric(metrics_row, "Shadow Overlay", len(shadow_overlay), "ML challenger snapshot", tone="amber")
            if details and details.get("run"):
                run_status = (details["run"] or {}).get("status", "unknown")
                _metric(metrics_row, "Selected Run", state["selected_run_id"] or "—", run_status.upper(), tone=_status_chip(run_status))

        control_summary = pd.DataFrame(
            [
                {"setting": "data_domain", "value": "operational"},
                {"setting": "skip_preflight", "value": state["skip_preflight"]},
                {"setting": "local_publish", "value": state["local_publish"]},
                {"setting": "full_rebuild", "value": state["full_rebuild"]},
                {"setting": "feature_tail_bars", "value": state["feature_tail_bars"]},
                {"setting": "symbol_limit", "value": state["symbol_limit"]},
                {"setting": "selected_run_id", "value": state["selected_run_id"] or "latest"},
                {"setting": "selected_task_id", "value": state["selected_task_id"] or "auto"},
            ]
        )
        _table(
            control_summary_container,
            "Current Control State",
            control_summary,
            limit=20,
            subtitle="Active defaults that will be applied when you launch pipeline actions from this console.",
        )
        _table(
            control_health_container,
            "Operational Health Checks",
            health_checks,
            limit=12,
            subtitle="Critical runtime and data-quality checks for the live operational store.",
        )

        ranked_chart = _bar_chart_option(
            ranked,
            label_col="symbol_id",
            value_col="composite_score",
            title="Top Composite Scores",
            limit=12,
        )
        sector_chart = _bar_chart_option(
            sectors,
            label_col="Sector",
            value_col="RS_rank_pct" if "RS_rank_pct" in sectors.columns else ("RS" if "RS" in sectors.columns else sectors.columns[1]),
            title="Sector Leadership",
            limit=12,
        )

        ranking_tiles = [
            {
                "label": "Top Ranked Symbol",
                "value": ranked.iloc[0]["symbol_id"] if not ranked.empty and "symbol_id" in ranked.columns else "—",
                "badge": "LIVE",
                "tone": "completed",
                "detail": f"{len(ranked)} symbols in the latest live ranking artifact.",
            },
            {
                "label": "Top Composite Score",
                "value": ranked.iloc[0]["composite_score"] if not ranked.empty and "composite_score" in ranked.columns else "—",
                "badge": "RANKING",
                "tone": "running",
                "detail": "Higher values indicate stronger combined technical alignment.",
            },
            {
                "label": "Breakout Candidates",
                "value": len(breakouts),
                "badge": "SETUPS",
                "tone": "completed",
                "detail": "Current breakouts confirmed by price structure, volume, and trend filters.",
            },
        ]
        market_tiles = [
            {
                "label": "Top Sector",
                "value": summary.get("top_sector") or "—",
                "badge": "LEADERSHIP",
                "tone": "completed",
                "detail": f"{len(sectors)} sectors available in the latest sector dashboard artifact.",
            },
            {
                "label": "Breakout Count",
                "value": summary.get("breakout_count", 0),
                "badge": "MOMENTUM",
                "tone": "completed",
                "detail": "Sector and breakout tabs should move together when market participation broadens.",
            },
            {
                "label": "Operational Health",
                "value": health.get("status", "unknown").upper(),
                "badge": "RISK",
                "tone": health.get("status", "unknown"),
                "detail": "Use this to quickly spot freshness or schema drift issues before acting on signals.",
            },
        ]
        selected_run = details["run"] if details and details.get("run") else {}
        operations_tiles = [
            {
                "label": "Selected Run",
                "value": state["selected_run_id"] or "—",
                "badge": (selected_run or {}).get("status", "idle").upper(),
                "tone": (selected_run or {}).get("status", "slate"),
                "detail": f"Current stage: {(selected_run or {}).get('current_stage') or '—'}",
            },
            {
                "label": "Run Alerts",
                "value": len(details["alerts"]) if details else 0,
                "badge": "ALERTS",
                "tone": "warning" if details and details["alerts"] else "completed",
                "detail": "Review alerts before trusting publish or execution outcomes.",
            },
            {
                "label": "Delivery Logs",
                "value": len(details["delivery_logs"]) if details else 0,
                "badge": "PUBLISH",
                "tone": "running",
                "detail": "Recent publish and notification activity for the selected run.",
            },
        ]
        task_rows_list = list_operator_tasks(PROJECT_ROOT)
        selected_task = next((row for row in task_rows_list if row["task_id"] == state["selected_task_id"]), None)
        tasks_tiles = [
            {
                "label": "Selected Task",
                "value": state["selected_task_id"] or "—",
                "badge": (selected_task or {}).get("status", "idle").upper(),
                "tone": (selected_task or {}).get("status", "slate"),
                "detail": (selected_task or {}).get("label") or "No task selected.",
            },
            {
                "label": "Active Tasks",
                "value": len([row for row in task_rows_list if row.get("status") == "running"]),
                "badge": "QUEUE",
                "tone": "running" if any(row.get("status") == "running" for row in task_rows_list) else "completed",
                "detail": "Background jobs launched from the UI are tracked here.",
            },
            {
                "label": "Task History",
                "value": len(task_rows_list),
                "badge": "MONITOR",
                "tone": "slate",
                "detail": "Use the selector to inspect logs from recent UI-triggered operations.",
            },
        ]
        selected_process = next((row for row in process_rows if row["pid"] == state["selected_process_pid"]), None)
        process_tiles = [
            {
                "label": "Selected PID",
                "value": state["selected_process_pid"] or "—",
                "badge": (selected_process or {}).get("kind", "IDLE").upper(),
                "tone": "running" if selected_process else "slate",
                "detail": (selected_process or {}).get("command") or "No process selected.",
            },
            {
                "label": "Project Processes",
                "value": len(process_rows),
                "badge": "RUNNING",
                "tone": "running" if process_rows else "slate",
                "detail": "All active Streamlit, NiceGUI, pipeline, and shadow-monitor processes tied to this project.",
            },
            {
                "label": "Research URL",
                "value": f"http://localhost:{int(state['streamlit_port'])}",
                "badge": "STREAMLIT",
                "tone": "sky",
                "detail": "Launch or reconnect to the research dashboard from this port.",
            },
        ]

        _summary_tiles(
            ranking_summary_container,
            "Ranking Snapshot",
            ranking_tiles,
            empty="No ranking snapshot available yet.",
        )
        _summary_tiles(
            market_summary_container,
            "Market Snapshot",
            market_tiles,
            empty="No market snapshot available yet.",
        )
        _summary_tiles(
            operations_summary_container,
            "Run Snapshot",
            operations_tiles,
            empty="No selected run details available yet.",
        )
        _summary_tiles(
            tasks_summary_container,
            "Task Snapshot",
            tasks_tiles,
            empty="No task activity yet.",
        )
        _summary_tiles(
            processes_summary_container,
            "Process Snapshot",
            process_tiles,
            empty="No project processes found.",
        )

        _chart(
            ranked_chart_container,
            "Ranking Chart",
            ranked_chart,
            subtitle="Top composite scores from the latest operational rank run.",
        )
        _table(
            ranked_container,
            "Top Ranked Signals",
            ranked,
            limit=18,
            subtitle="Current leader board with the most relevant technical factor columns first.",
        )
        _chart(
            sector_chart_container,
            "Sector Chart",
            sector_chart,
            subtitle="Relative sector leadership from the latest sector dashboard artifact.",
        )
        _table(
            breakout_container,
            "Breakout Monitor",
            breakouts,
            limit=18,
            subtitle="Setups currently passing the breakout scanner with confirmation metrics.",
        )
        _table(
            sectors_container,
            "Sector Leadership",
            sectors,
            limit=12,
            subtitle="Sector leaderboard with rank, momentum, and quadrant context.",
        )
        _table(
            health_container,
            "Market Health Checks",
            health_checks,
            limit=12,
            subtitle="Operational checks that explain whether the live market view is trustworthy.",
        )

        recent_runs_df = pd.DataFrame(recent_runs)
        _table(
            recent_runs_container,
            "Recent Pipeline Runs",
            recent_runs_df,
            limit=12,
            subtitle="Most recent orchestrator runs with status and failure context.",
        )
        if details:
            _table(
                run_summary_container,
                f"Run Summary: {state['selected_run_id']}",
                pd.DataFrame([details["run"]]),
                limit=1,
                subtitle="Primary metadata and final state for the selected pipeline run.",
            )
            _table(
                stage_runs_container,
                "Stage Attempts",
                pd.DataFrame(details["stages"]),
                limit=12,
                subtitle="Stage-level execution history for the selected run.",
            )
            _table(
                alerts_container,
                "Alerts",
                pd.DataFrame(details["alerts"]),
                limit=12,
                subtitle="Operational and pipeline alerts emitted during this run.",
                empty="No alerts for the selected run.",
            )
            _table(
                delivery_logs_container,
                "Publish / Delivery Logs",
                pd.DataFrame(details["delivery_logs"]),
                limit=12,
                subtitle="Downstream publish and notification activity for the selected run.",
                empty="No delivery activity for the selected run.",
            )
        else:
            _table(run_summary_container, "Run Summary", pd.DataFrame(), subtitle="Primary metadata and final state for the selected pipeline run.", empty="No run selected.")
            _table(stage_runs_container, "Stage Attempts", pd.DataFrame(), subtitle="Stage-level execution history for the selected run.")
            _table(alerts_container, "Alerts", pd.DataFrame(), subtitle="Operational and pipeline alerts emitted during this run.")
            _table(delivery_logs_container, "Publish / Delivery Logs", pd.DataFrame(), subtitle="Downstream publish and notification activity for the selected run.")

        _table(
            overlay_container,
            "Latest ML Overlay",
            shadow_overlay,
            limit=20,
            subtitle="Current champion-challenger overlay from technical and LightGBM models.",
        )
        _table(
            weekly5_container,
            "5D Weekly Challenger Summary",
            weekly_5,
            limit=8,
            subtitle="Weekly comparison of technical, ML, and blended 5-day challenger performance.",
        )
        _table(
            weekly20_container,
            "20D Weekly Challenger Summary",
            weekly_20,
            limit=8,
            subtitle="Weekly comparison of technical, ML, and blended 20-day challenger performance.",
        )
        _table(
            monthly5_container,
            "5D Monthly Challenger Summary",
            monthly_5,
            limit=6,
            subtitle="Monthly rollup for the 5-day challenger framework.",
        )
        _table(
            monthly20_container,
            "20D Monthly Challenger Summary",
            monthly_20,
            limit=6,
            subtitle="Monthly rollup for the 20-day challenger framework.",
        )
        _table(
            task_queue_container,
            "Task Queue",
            tasks,
            limit=20,
            subtitle="UI-triggered background jobs with status and completion state.",
            empty="No UI-triggered tasks yet.",
        )
        _table(
            task_log_container,
            "Live Task Log",
            pd.DataFrame({"log_line": get_task_logs(state["selected_task_id"], PROJECT_ROOT) or []}),
            limit=80,
            subtitle="Streaming log view for the currently selected task.",
            empty="No task logs yet.",
        )
        _table(
            process_table_container,
            "Project Processes",
            processes,
            limit=50,
            subtitle="OS-level processes that belong to this project and can be terminated safely from the UI.",
            empty="No project processes found.",
        )
        _populate_task_log()

    refresh_all()
    ui.timer(5.0, refresh_all)


def main(**run_kwargs: Any) -> None:
    if ui is None:  # pragma: no cover - optional dependency boundary
        raise ImportError(
            "nicegui is not installed. Install it with `python -m pip install nicegui`."
        )
    build_execution_ui()
    options = {"title": "AI Trading Execution Console", "reload": False}
    options.update(run_kwargs)
    ui.run(**options)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the NiceGUI execution control center")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--show", action="store_true", help="Open the browser automatically")
    return parser


if __name__ in {"__main__", "__mp_main__"}:  # pragma: no cover - launcher boundary
    args = build_parser().parse_args()
    main(host=args.host, port=args.port, show=args.show)
