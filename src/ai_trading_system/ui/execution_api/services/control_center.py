"""Execution control services for the React V2 operator console."""

from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import threading
import traceback
import uuid
import importlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.platform.logging.logger import logger


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[5]
_TASKS: dict[str, dict[str, Any]] = {}
_TASK_LOCK = threading.Lock()


def _load_shadow_monitor_module():
    """Load the canonical shadow monitor module."""
    return importlib.import_module("ai_trading_system.research.shadow_monitor")


def _load_pipeline_orchestrator_class():
    """Load PipelineOrchestrator lazily to avoid UI import-time dependency chains."""
    module_name = "ai_trading_system.pipeline.orchestrator"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        if str(DEFAULT_PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(DEFAULT_PROJECT_ROOT))
        module = importlib.import_module(module_name)
    return getattr(module, "PipelineOrchestrator")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _registry(project_root: str | Path | None = None) -> RegistryStore:
    return RegistryStore(Path(project_root) if project_root else DEFAULT_PROJECT_ROOT)


def _task_snapshot(task_id: str, project_root: str | Path | None = None) -> Dict[str, Any]:
    with _TASK_LOCK:
        cached = dict(_TASKS.get(task_id, {}))
    try:
        stored = _registry(project_root).get_operator_task(task_id)
    except KeyError:
        if cached and "task_id" not in cached:
            cached["task_id"] = task_id
        return cached
    merged = {**stored, **cached}
    merged["task_id"] = merged.get("task_id") or task_id
    merged["metadata"] = {
        **(stored.get("metadata") or {}),
        **(cached.get("metadata") or {}),
    }
    if "logs" not in merged:
        merged["logs"] = []
    return merged


def _process_exists(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _latest_task_log_rows(task_id: str, project_root: str | Path | None = None, limit: int = 10) -> List[Dict[str, Any]]:
    return _registry(project_root).get_operator_task_logs(task_id, after=0, limit=max(1, int(limit)))[-int(limit) :]


def _derive_task_terminal_state(task_id: str, task: Dict[str, Any], project_root: str | Path | None = None) -> Dict[str, Any]:
    status = str(task.get("status") or "").lower()
    if status not in {"running", "queued"}:
        return {}

    metadata = dict(task.get("metadata") or {})
    if task.get("error"):
        return {"status": "failed", "finished_at": task.get("finished_at") or _now(), "error": str(task.get("error"))}

    run_id = metadata.get("run_id")
    if run_id:
        requested_stages = [str(stage) for stage in metadata.get("stages") or []]
        fresh_stage_runs = _registry(project_root).get_stage_runs(str(run_id), started_after=task.get("started_at"))
        try:
            run = _registry(project_root).get_run(str(run_id))
        except KeyError:
            run = None
        if run is not None:
            run_status = str(run.get("status") or "").lower()
            fresh_requested_stage_seen = (
                not requested_stages
                or any(str(row.get("stage_name")) in requested_stages for row in fresh_stage_runs)
            )
            if fresh_requested_stage_seen and run_status in {"failed", "completed", "completed_with_publish_errors"}:
                derived_status = "failed" if run_status == "failed" else run_status
                return {
                    "status": derived_status,
                    "finished_at": task.get("finished_at") or _now(),
                    "error": run.get("error_message") if derived_status == "failed" else task.get("error"),
                }

    for row in reversed(_latest_task_log_rows(task_id, project_root, limit=12)):
        message = str(row.get("message") or "")
        if "failed:" in message.lower() or "failed with exit_code" in message.lower():
            error_text = message.split("] ", 1)[-1]
            if ":" in error_text:
                error_text = error_text.split(":", 1)[-1].strip()
            return {"status": "failed", "finished_at": task.get("finished_at") or _now(), "error": error_text}
        if "completed" in message.lower() or "exited with code=0" in message.lower():
            return {"status": "completed", "finished_at": task.get("finished_at") or _now(), "error": None}

    pid = metadata.get("pid")
    if pid:
        try:
            resolved_pid = int(pid)
        except (TypeError, ValueError):
            resolved_pid = None
        if resolved_pid and not _process_exists(resolved_pid):
            return {
                "status": "failed",
                "finished_at": task.get("finished_at") or _now(),
                "error": "Background process is no longer running.",
            }
    return {}


def reconcile_operator_task(task_id: str, project_root: str | Path | None = None, *, persist: bool = True) -> Dict[str, Any]:
    task = _task_snapshot(task_id, project_root)
    if not task:
        raise KeyError(f"Unknown task_id: {task_id}")
    derived = _derive_task_terminal_state(task_id, task, project_root)
    if derived:
        task.update({key: value for key, value in derived.items() if value is not None})
        if persist:
            _set_task(task_id, project_root=project_root, **derived)
    return task


def list_operator_tasks(project_root: str | Path | None = None) -> List[Dict[str, Any]]:
    rows = _registry(project_root).list_operator_tasks()
    with _TASK_LOCK:
        legacy_rows = []
        for task_id, payload in _TASKS.items():
            if any(row["task_id"] == task_id for row in rows):
                continue
            row = {
                "task_id": task_id,
                "task_type": "",
                "label": "",
                "status": "unknown",
                "started_at": "",
                "finished_at": None,
                "result": None,
                "error": None,
                "logs": [],
                "metadata": {},
            }
            row.update(payload or {})
            row["task_id"] = row.get("task_id") or task_id
            legacy_rows.append(row)
    combined = list(sorted([*rows, *legacy_rows], key=lambda item: item.get("started_at", ""), reverse=True))
    return [reconcile_operator_task(str(row.get("task_id")), project_root, persist=False) for row in combined if row.get("task_id")]


def get_operator_task(task_id: str, project_root: str | Path | None = None) -> Dict[str, Any]:
    return reconcile_operator_task(task_id, project_root, persist=False)


def _set_task(task_key: str, project_root: str | Path | None = None, **updates: Any) -> None:
    with _TASK_LOCK:
        task = _TASKS.setdefault(task_key, {})
        task.update(updates)
        payload = dict(task)
    _registry(project_root).update_operator_task(
        task_key,
        status=payload.get("status"),
        finished_at=payload.get("finished_at"),
        result=payload.get("result"),
        error=payload.get("error"),
        metadata=payload.get("metadata"),
    )


def _append_task_log(task_id: str, message: str, project_root: str | Path | None = None) -> None:
    rendered = f"[{_now()}] {message}"
    with _TASK_LOCK:
        task = _TASKS.setdefault(task_id, {})
        logs = task.setdefault("logs", [])
        logs.append(rendered)
        if len(logs) > 300:
            del logs[:-300]
    _registry(project_root).append_operator_task_log(task_id, rendered)


def _create_task(
    task_type: str,
    label: str,
    metadata: Optional[Dict[str, Any]] = None,
    *,
    project_root: str | Path | None = None,
) -> str:
    task_id = f"task-{uuid.uuid4().hex[:8]}"
    started_at = _now()
    _registry(project_root).create_operator_task(
        task_id=task_id,
        task_type=task_type,
        label=label,
        status="running",
        started_at=started_at,
        metadata=metadata or {},
    )
    _set_task(
        task_id,
        project_root=project_root,
        task_id=task_id,
        task_type=task_type,
        label=label,
        status="running",
        started_at=started_at,
        finished_at=None,
        result=None,
        error=None,
        logs=[],
        metadata=metadata or {},
    )
    _append_task_log(task_id, f"Task created: {label}", project_root=project_root)
    return task_id


def _launch_subprocess_task(
    *,
    project_root: str | Path,
    task_type: str,
    label: str,
    command: List[str],
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """Launch a Python subprocess in the background and stream logs into the task log."""
    root = Path(project_root)
    task_id = _create_task(task_type, label, metadata, project_root=root)

    def _runner() -> None:
        try:
            _append_task_log(task_id, f"Running command: {' '.join(command)}", project_root=root)
            proc = subprocess.Popen(
                command,
                cwd=root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            _set_task(
                task_id,
                project_root=root,
                status="running",
                metadata={
                    **(_task_snapshot(task_id, root).get("metadata") or {}),
                    "pid": int(proc.pid),
                    "command": " ".join(command),
                },
                result={"pid": int(proc.pid)},
            )
            if proc.stdout is not None:
                for line in proc.stdout:
                    message = line.rstrip()
                    if message:
                        _append_task_log(task_id, message, project_root=root)

            exit_code = proc.wait()
            if exit_code == 0:
                _set_task(task_id, project_root=root, status="completed", finished_at=_now())
                _append_task_log(task_id, f"Task completed with exit_code={exit_code}", project_root=root)
            else:
                _set_task(task_id, project_root=root, status="failed", finished_at=_now(), error=f"Process exited with code {exit_code}")
                _append_task_log(task_id, f"Task failed with exit_code={exit_code}", project_root=root)
        except Exception as exc:
            _append_task_log(task_id, f"Task failed: {exc.__class__.__name__}: {exc}", project_root=root)
            _set_task(
                task_id,
                project_root=root,
                status="failed",
                finished_at=_now(),
                error=f"{exc.__class__.__name__}: {exc}",
                metadata={
                    **(_task_snapshot(task_id, root).get("metadata") or {}),
                    "traceback": traceback.format_exc(),
                },
            )

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def launch_pipeline_task(
    *,
    project_root: str | Path,
    label: str,
    stage_names: List[str],
    params: Optional[Dict[str, Any]] = None,
    run_id: Optional[str] = None,
    run_date: Optional[str] = None,
) -> str:
    """Run a pipeline flow in the background and track it for the UI."""
    root = Path(project_root)
    task_id = _create_task(
        "pipeline",
        label,
        {"stages": list(stage_names), "params": params or {}, "run_id": run_id, "run_date": run_date, "current_stage": stage_names[0] if stage_names else None},
        project_root=root,
    )

    def _runner() -> None:
        try:
            resolved_run_id = run_id or f"ui-{datetime.now().date().isoformat()}-{uuid.uuid4().hex[:8]}"
            _set_task(
                task_id,
                project_root=root,
                metadata={**(_task_snapshot(task_id, root).get("metadata") or {}), "run_id": resolved_run_id},
            )
            _append_task_log(task_id, f"Starting pipeline run {resolved_run_id} for stages={stage_names}", project_root=root)
            orchestrator_cls = _load_pipeline_orchestrator_class()
            orchestrator = orchestrator_cls(root)
            result = orchestrator.run_pipeline(
                run_id=resolved_run_id,
                stage_names=stage_names,
                run_date=run_date,
                params=params or {},
            )
            for stage in result.get("stages", []):
                _append_task_log(
                    task_id,
                    f"Stage {stage['stage_name']} attempt={stage['attempt_number']} status={stage['status']}",
                    project_root=root,
                )
            final_status = str(result.get("status") or "completed")
            task_status = final_status if final_status in {"completed", "completed_with_publish_errors"} else "completed"
            _set_task(task_id, project_root=root, status=task_status, finished_at=_now(), result=result)
            _append_task_log(task_id, f"Pipeline completed with status={result.get('status')}", project_root=root)
        except Exception as exc:
            _append_task_log(task_id, f"Pipeline failed: {exc.__class__.__name__}: {exc}", project_root=root)
            _set_task(
                task_id,
                project_root=root,
                status="failed",
                finished_at=_now(),
                error=f"{exc.__class__.__name__}: {exc}",
                metadata={
                    **(_task_snapshot(task_id, root).get("metadata") or {}),
                    "traceback": traceback.format_exc(),
                },
            )

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def launch_shadow_monitor_task(
    *,
    label: str,
    backfill_days: int = 0,
    prediction_date: Optional[str] = None,
) -> str:
    """Run the shadow-monitor updater in the background and track it for the UI."""
    shadow_monitor_module = _load_shadow_monitor_module()
    project_root = Path(shadow_monitor_module.__file__).resolve().parents[1]
    task_id = _create_task(
        "shadow_monitor",
        label,
        {"backfill_days": int(backfill_days), "prediction_date": prediction_date},
        project_root=project_root,
    )

    def _runner() -> None:
        try:
            _append_task_log(task_id, "Loading latest trained 5d and 20d LightGBM models", project_root=project_root)
            args = shadow_monitor_module.build_parser().parse_args([])
            args.backfill_days = int(backfill_days)
            args.prediction_date = prediction_date
            args.exchange = "NSE"
            args.lookback_days = 420
            args.technical_weight = 0.75
            args.ml_weight = 0.25

            # Reuse the module's main flow with a light inline adaptation.
            operational_paths = shadow_monitor_module.ensure_domain_layout(project_root=project_root, data_domain="operational")
            research_paths = shadow_monitor_module.ensure_domain_layout(project_root=project_root, data_domain="research")

            _, model_5d_meta = shadow_monitor_module.find_latest_model_metadata(research_paths.model_dir, horizon=5)
            _, model_20d_meta = shadow_monitor_module.find_latest_model_metadata(research_paths.model_dir, horizon=20)

            scorer = shadow_monitor_module.LightGBMAlphaEngine(
                ohlcv_db_path=str(operational_paths.ohlcv_db_path),
                feature_store_dir=str(operational_paths.feature_store_dir),
                model_dir=str(research_paths.model_dir),
                data_domain="operational",
            )
            model_5d = scorer.load_model_from_uri(model_5d_meta["_model_path"])
            model_20d = scorer.load_model_from_uri(model_20d_meta["_model_path"])

            if args.backfill_days > 0:
                _append_task_log(task_id, f"Preparing historical shadow frames for backfill_days={args.backfill_days}", project_root=project_root)
                latest_df, prediction_ts = shadow_monitor_module.prepare_current_universe_dataset(
                    project_root=project_root,
                    prediction_date=args.prediction_date,
                    exchange=args.exchange,
                    lookback_days=args.lookback_days,
                )
                backfill_start = (
                    prediction_ts - shadow_monitor_module.pd.Timedelta(days=int(args.backfill_days))
                ).date().isoformat()
                history_df = shadow_monitor_module.prepare_shadow_history_dataset(
                    project_root=project_root,
                    from_prediction_date=backfill_start,
                    to_prediction_date=prediction_ts.date().isoformat(),
                    exchange=args.exchange,
                    lookback_days=args.lookback_days,
                )
                prediction_frames = {
                    shadow_monitor_module.pd.Timestamp(date): frame.copy()
                    for date, frame in history_df.groupby(history_df["timestamp"].dt.normalize())
                }
            else:
                _append_task_log(task_id, "Preparing current-universe shadow frame", project_root=project_root)
                latest_df, prediction_ts = shadow_monitor_module.prepare_current_universe_dataset(
                    project_root=project_root,
                    prediction_date=args.prediction_date,
                    exchange=args.exchange,
                    lookback_days=args.lookback_days,
                )
                prediction_frames = {prediction_ts: latest_df.copy()}

            reports_dir = research_paths.reports_dir
            reports_dir.mkdir(parents=True, exist_ok=True)
            latest_overlay_path = reports_dir / "ml_rank_overlay.csv"
            dated_overlay_path = reports_dir / f"ml_rank_overlay_{prediction_ts.date().isoformat()}.csv"
            registry = RegistryStore(project_root)

            inserted_predictions = 0
            for prediction_day, frame in sorted(prediction_frames.items()):
                _append_task_log(task_id, f"Scoring overlay for {prediction_day.date().isoformat()} ({len(frame)} symbols)", project_root=project_root)
                overlay_df = shadow_monitor_module.build_shadow_overlay(
                    frame,
                    scorer=scorer,
                    model_5d=model_5d,
                    model_20d=model_20d,
                    technical_weight=args.technical_weight,
                    ml_weight=args.ml_weight,
                )
                overlay_metadata = {
                    "prediction_date": prediction_day.date().isoformat(),
                    "technical_weight": args.technical_weight,
                    "ml_weight": args.ml_weight,
                    "exchange": args.exchange,
                    "model_5d_path": model_5d_meta["_model_path"],
                    "model_20d_path": model_20d_meta["_model_path"],
                    "model_5d_metadata": model_5d_meta["_metadata_path"],
                    "model_20d_metadata": model_20d_meta["_metadata_path"],
                }
                prediction_rows = shadow_monitor_module.overlay_rows_for_registry(overlay_df, metadata=overlay_metadata)
                artifact_uri = None
                if prediction_day.normalize() == prediction_ts.normalize():
                    overlay_df.to_csv(latest_overlay_path, index=False)
                    overlay_df.to_csv(dated_overlay_path, index=False)
                    artifact_uri = str(latest_overlay_path)
                inserted_predictions += registry.replace_shadow_predictions(
                    prediction_day.date().isoformat(),
                    prediction_rows,
                    artifact_uri=artifact_uri,
                )

            matured_counts: dict[int, int] = {}
            for horizon in (5, 20):
                pending = registry.get_unscored_shadow_predictions(horizon)
                if not pending:
                    matured_counts[horizon] = 0
                    _append_task_log(task_id, f"No pending matured outcomes for {horizon}d horizon", project_root=project_root)
                    continue
                from_date = min(row["prediction_date"] for row in pending)
                _append_task_log(task_id, f"Evaluating {len(pending)} pending outcomes for {horizon}d horizon from {from_date}", project_root=project_root)
                price_history = shadow_monitor_module.load_operational_price_history(
                    ohlcv_db_path=operational_paths.ohlcv_db_path,
                    exchange=args.exchange,
                    from_date=from_date,
                )
                outcome_rows = shadow_monitor_module.compute_matured_outcomes(price_history, pending, horizon=horizon)
                matured_counts[horizon] = registry.replace_shadow_outcomes(outcome_rows)

            result = {
                "prediction_date": prediction_ts.date().isoformat(),
                "prediction_rows": inserted_predictions,
                "matured_outcomes": matured_counts,
                "overlay_uri": str(latest_overlay_path),
                "dated_overlay_uri": str(dated_overlay_path),
                "backfill_days": int(backfill_days),
            }
            _set_task(task_id, project_root=project_root, status="completed", finished_at=_now(), result=result)
            _append_task_log(task_id, f"Shadow monitor completed: rows={inserted_predictions}, outcomes={matured_counts}", project_root=project_root)
        except Exception as exc:
            _append_task_log(task_id, f"Shadow monitor failed: {exc.__class__.__name__}: {exc}", project_root=project_root)
            _set_task(
                task_id,
                project_root=project_root,
                status="failed",
                finished_at=_now(),
                error=f"{exc.__class__.__name__}: {exc}",
                metadata={
                    **(_task_snapshot(task_id, project_root).get("metadata") or {}),
                    "traceback": traceback.format_exc(),
                },
            )

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def get_recent_runs(project_root: str | Path, limit: int = 12) -> List[Dict[str, Any]]:
    """Return recent pipeline runs for the execution console."""
    root = Path(project_root)
    db_path = root / "data" / "control_plane.duckdb"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT run_id, run_date, status, current_stage, started_at, ended_at, error_class, error_message
            FROM pipeline_run
            ORDER BY started_at DESC NULLS LAST
            LIMIT ?
            """,
            [int(limit)],
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "run_id": row[0],
            "run_date": str(row[1]) if row[1] is not None else None,
            "status": row[2],
            "current_stage": row[3],
            "started_at": str(row[4]) if row[4] is not None else None,
            "ended_at": str(row[5]) if row[5] is not None else None,
            "error_class": row[6],
            "error_message": row[7],
        }
        for row in rows
    ]


def find_latest_publishable_run(project_root: str | Path, limit: int = 50) -> Dict[str, Any] | None:
    """Return the most recent run that still has a usable rank artifact for publish retry."""
    root = Path(project_root)
    db_path = root / "data" / "control_plane.duckdb"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT r.run_id, r.run_date, r.status, r.current_stage, r.started_at, r.ended_at, r.error_class, r.error_message, a.uri
            FROM pipeline_run r
            JOIN pipeline_artifact a
              ON a.run_id = r.run_id
             AND a.stage_name = 'rank'
             AND a.artifact_type = 'ranked_signals'
            ORDER BY r.started_at DESC NULLS LAST
            LIMIT ?
            """,
            [int(limit)],
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        artifact_path = Path(str(row[8]))
        if not artifact_path.exists():
            continue
        return {
            "run_id": row[0],
            "run_date": str(row[1]) if row[1] is not None else None,
            "status": row[2],
            "current_stage": row[3],
            "started_at": str(row[4]) if row[4] is not None else None,
            "ended_at": str(row[5]) if row[5] is not None else None,
            "error_class": row[6],
            "error_message": row[7],
            "ranked_signals_uri": str(artifact_path),
        }
    return None


def get_run_details(project_root: str | Path, run_id: str) -> Dict[str, Any]:
    """Return stage runs, alerts, and delivery logs for one pipeline run."""
    registry = RegistryStore(project_root)
    return {
        "run": registry.get_run(run_id),
        "stages": registry.get_stage_runs(run_id),
        "alerts": registry.get_alerts(run_id),
        "delivery_logs": registry.get_delivery_logs(run_id),
    }


def get_task_logs(task_id: str, project_root: str | Path | None = None, after: int = 0, limit: int = 300) -> List[str]:
    _ = reconcile_operator_task(task_id, project_root, persist=False)
    logs = _registry(project_root).get_operator_task_logs(task_id, after=after, limit=limit)
    if logs:
        return [str(row["message"]) for row in logs]
    with _TASK_LOCK:
        task = _TASKS.get(task_id, {})
        return list(task.get("logs", []))


def terminate_operator_task(task_id: str, project_root: str | Path | None = None) -> Dict[str, Any]:
    task = reconcile_operator_task(task_id, project_root, persist=True)
    status = str(task.get("status") or "").lower()
    if status in {"completed", "completed_with_publish_errors", "failed", "terminated"}:
        return {"ok": False, "message": f"Task {task_id} is already {status}.", "task": task}

    metadata = dict(task.get("metadata") or {})
    pid = metadata.get("pid")
    if pid is not None:
        try:
            resolved_pid = int(pid)
        except (TypeError, ValueError):
            resolved_pid = None
        if resolved_pid:
            try:
                os.kill(resolved_pid, signal.SIGTERM)
                _append_task_log(task_id, f"Task terminated by operator (pid={resolved_pid})", project_root=project_root)
                _set_task(
                    task_id,
                    project_root=project_root,
                    status="terminated",
                    finished_at=_now(),
                    error="Terminated by operator.",
                )
                return {"ok": True, "message": f"Sent SIGTERM to pid={resolved_pid}.", "task": get_operator_task(task_id, project_root)}
            except ProcessLookupError:
                _set_task(
                    task_id,
                    project_root=project_root,
                    status="failed",
                    finished_at=_now(),
                    error="Background process is no longer running.",
                )
                return {"ok": True, "message": f"Task {task_id} was already stale and is now marked failed.", "task": get_operator_task(task_id, project_root)}
            except PermissionError:
                return {"ok": False, "message": f"Permission denied terminating pid={resolved_pid}.", "task": task}

    derived = _derive_task_terminal_state(task_id, task, project_root)
    if derived:
        _append_task_log(task_id, "Task state reconciled from terminal run/log evidence.", project_root=project_root)
        _set_task(task_id, project_root=project_root, **derived)
        return {"ok": True, "message": f"Task {task_id} was stale and has been reconciled to {derived.get('status')}.", "task": get_operator_task(task_id, project_root)}

    return {
        "ok": False,
        "message": "This task runs in-process and cannot be safely terminated from the dashboard.",
        "task": task,
    }


def launch_prepare_dataset_task(
    *,
    project_root: str | Path,
    label: str,
    engine: str,
    dataset_name: str,
    from_date: str,
    to_date: str,
    horizon: int,
    validation_fraction: float = 0.2,
) -> str:
    """Run dataset preparation in the background for the ML workbench."""
    return _launch_subprocess_task(
        project_root=project_root,
        task_type="ml_prepare_dataset",
        label=label,
        command=[
            sys.executable,
            "-m",
            "ai_trading_system.research.prepare_training_dataset",
            "--engine",
            engine,
            "--dataset-name",
            dataset_name,
            "--from-date",
            from_date,
            "--to-date",
            to_date,
            "--horizon",
            str(int(horizon)),
            "--validation-fraction",
            str(float(validation_fraction)),
        ],
        metadata={
            "engine": engine,
            "dataset_name": dataset_name,
            "from_date": from_date,
            "to_date": to_date,
            "horizon": int(horizon),
            "validation_fraction": float(validation_fraction),
        },
    )


def launch_train_model_task(
    *,
    project_root: str | Path,
    label: str,
    engine: str,
    model_name: str,
    model_version: str,
    horizon: int,
    from_date: str,
    to_date: str,
    progress_interval: int = 25,
    min_train_years: int = 5,
    dataset_uri: Optional[str] = None,
) -> str:
    """Run research training in the background for the ML workbench."""
    command = [
        sys.executable,
        "-m",
        "ai_trading_system.research.train_pipeline",
        "--engine",
        engine,
        "--model-name",
        model_name,
        "--model-version",
        model_version,
        "--horizon",
        str(int(horizon)),
        "--from-date",
        from_date,
        "--to-date",
        to_date,
        "--progress-interval",
        str(int(progress_interval)),
        "--min-train-years",
        str(int(min_train_years)),
    ]
    if dataset_uri:
        command.extend(["--dataset-uri", dataset_uri])

    return _launch_subprocess_task(
        project_root=project_root,
        task_type="ml_train_model",
        label=label,
        command=command,
        metadata={
            "engine": engine,
            "model_name": model_name,
            "model_version": model_version,
            "horizon": int(horizon),
            "from_date": from_date,
            "to_date": to_date,
            "progress_interval": int(progress_interval),
            "min_train_years": int(min_train_years),
            "dataset_uri": dataset_uri,
        },
    )


def launch_recipe_run_task(
    *,
    project_root: str | Path,
    label: str,
    recipe: str,
    auto_approve: bool = False,
    auto_deploy: bool = False,
) -> str:
    """Run a simplified research recipe in the background for the ML workbench."""
    command = [
        sys.executable,
        "-m",
        "ai_trading_system.research.run_recipe",
        "--recipe",
        recipe,
    ]
    if auto_approve:
        command.append("--auto-approve")
    if auto_deploy:
        command.append("--auto-deploy")

    return _launch_subprocess_task(
        project_root=project_root,
        task_type="ml_recipe_run",
        label=label,
        command=command,
        metadata={
            "recipe": recipe,
            "auto_approve": bool(auto_approve),
            "auto_deploy": bool(auto_deploy),
        },
    )


def launch_recipe_bundle_task(
    *,
    project_root: str | Path,
    label: str,
    bundle: str,
    auto_approve: bool = False,
    auto_deploy: bool = False,
) -> str:
    """Run a bundled research preset in the background for the ML workbench."""
    command = [
        sys.executable,
        "-m",
        "ai_trading_system.research.run_recipe",
        "--bundle",
        bundle,
    ]
    if auto_approve:
        command.append("--auto-approve")
    if auto_deploy:
        command.append("--auto-deploy")

    return _launch_subprocess_task(
        project_root=project_root,
        task_type="ml_recipe_bundle",
        label=label,
        command=command,
        metadata={
            "bundle": bundle,
            "auto_approve": bool(auto_approve),
            "auto_deploy": bool(auto_deploy),
        },
    )


def list_project_processes(project_root: str | Path) -> List[Dict[str, Any]]:
    """List running OS processes that appear related to this project."""
    root = str(Path(project_root).resolve())
    current_pid = os.getpid()
    result = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,etime=,command="],
        capture_output=True,
        text=True,
        check=False,
    )
    rows: List[Dict[str, Any]] = []
    for line in result.stdout.splitlines():
        raw = line.strip()
        if not raw:
            continue
        parts = raw.split(None, 3)
        if len(parts) < 4:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        etime = parts[2]
        command = parts[3]
        if pid == current_pid:
            continue
        if root not in command and "ai-trading-system" not in command:
            continue
        if "ps -axo" in command:
            continue
        kind = "other"
        if "ai_trading_system.pipeline.orchestrator" in command:
            kind = "pipeline"
        elif "ai_trading_system.research.shadow_monitor" in command:
            kind = "shadow_monitor"
        port_match = re.search(r"(?:--server\.port|--port)\s+(\d+)", command)
        rows.append(
            {
                "pid": pid,
                "ppid": ppid,
                "etime": etime,
                "kind": kind,
                "port": int(port_match.group(1)) if port_match else None,
                "command": command,
            }
        )
    return sorted(rows, key=lambda row: (row["kind"], row["pid"]))


def terminate_project_process(project_root: str | Path, pid: int) -> Dict[str, Any]:
    """Terminate a project-related process safely with SIGTERM."""
    processes = {row["pid"]: row for row in list_project_processes(project_root)}
    target = processes.get(int(pid))
    if not target:
        return {"ok": False, "message": f"PID {pid} is not a recognised project process."}
    try:
        os.kill(int(pid), signal.SIGTERM)
        return {"ok": True, "message": f"Sent SIGTERM to pid={pid}.", "process": target}
    except ProcessLookupError:
        return {"ok": False, "message": f"PID {pid} no longer exists."}
    except PermissionError:
        return {"ok": False, "message": f"Permission denied terminating pid={pid}."}
