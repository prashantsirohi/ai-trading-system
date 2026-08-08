"""Authenticated execution-API routes for the Actual Trading Journal."""

from __future__ import annotations

import base64
import binascii
import json
import sys
import tempfile
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse

from ai_trading_system.domains.trade_journal.analytics import behaviour_rate
from ai_trading_system.domains.trade_journal.enrichment import JournalMarketDataReader
from ai_trading_system.domains.trade_journal.identity import stable_id
from ai_trading_system.domains.trade_journal.service import TradeJournalService
from ai_trading_system.domains.trade_journal.store import (
    JournalMigrationRequiredError,
    TradeJournalStore,
    rows_as_dicts,
    utc_now,
)
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.control_center import _launch_subprocess_task


router = APIRouter(prefix="/api/trade-journal", tags=["trade-journal"])
MAX_UPLOAD_BYTES = 25 * 1024 * 1024


def _store() -> TradeJournalStore:
    return TradeJournalStore(project_root())


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        value = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _response(value: Any, *, status_code: int = 200) -> JSONResponse:
    return JSONResponse(_json_value(value), status_code=status_code)


def _encode_cursor(timestamp: Any, row_id: Any) -> str:
    raw = json.dumps([_json_value(timestamp), str(row_id)], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str | None) -> tuple[datetime, str] | None:
    if not cursor:
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        timestamp, row_id = json.loads(base64.urlsafe_b64decode(padded).decode())
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None), str(row_id)
    except (ValueError, TypeError, json.JSONDecodeError, binascii.Error) as exc:
        raise HTTPException(status_code=422, detail="Invalid pagination cursor") from exc


def _page(
    rows: list[dict[str, Any]], *, limit: int, timestamp_key: str, id_key: str
) -> dict[str, Any]:
    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = (
        _encode_cursor(items[-1][timestamp_key], items[-1][id_key])
        if has_more and items else None
    )
    return {
        "items": items,
        "pagination": {"limit": limit, "has_more": has_more, "next_cursor": next_cursor},
    }


def _market_reader() -> JournalMarketDataReader:
    paths = get_domain_paths(project_root(), data_domain="operational")
    return JournalMarketDataReader(
        paths.ohlcv_db_path,
        control_plane_db_path=paths.root_dir / "control_plane.duckdb",
        master_db_path=paths.master_db_path,
    )


async def _persist_upload(upload: UploadFile) -> Path:
    suffix = Path(upload.filename or "upload").suffix
    handle = tempfile.NamedTemporaryFile(prefix="trade-journal-", suffix=suffix, delete=False)
    path = Path(handle.name)
    size = 0
    try:
        while chunk := await upload.read(1024 * 1024):
            size += len(chunk)
            if size > MAX_UPLOAD_BYTES:
                raise HTTPException(status_code=413, detail="Upload exceeds the 25 MiB limit")
            handle.write(chunk)
        handle.close()
        return path
    except Exception:
        handle.close()
        path.unlink(missing_ok=True)
        raise


def _service() -> TradeJournalService:
    try:
        store = _store()
        store.verify_schema()
        return TradeJournalService(store)
    except JournalMigrationRequiredError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _launch_journal_task(
    *, action: str, account_ref: str, snapshot_id: str | None = None
) -> dict[str, str]:
    service = _service()
    journal_run_id = service.enqueue_task(
        action=action, account=account_ref, snapshot_id=snapshot_id
    )
    root = project_root()
    task_id = _launch_subprocess_task(
        project_root=root,
        task_type=f"trade_journal_{action}",
        label=f"Trade journal {action}",
        command=[
            sys.executable, "-m", "ai_trading_system.domains.trade_journal",
            "--project-root", str(root), "--db-path", str(service.store.db_path),
            "worker", "--journal-run-id", journal_run_id,
        ],
        metadata={"journal_run_id": journal_run_id, "operator_action_type": f"trade_journal_{action}"},
    )
    service.attach_operator_task(journal_run_id, task_id)
    return {"journal_run_id": journal_run_id, "task_id": task_id, "status": "QUEUED"}


async def _with_upload(upload: UploadFile, operation: Any) -> Any:
    path = await _persist_upload(upload)
    try:
        return operation(path)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except JournalMigrationRequiredError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    finally:
        path.unlink(missing_ok=True)


@router.post("/imports/tradebook/preview")
async def preview_tradebook(file: UploadFile = File(...)) -> JSONResponse:
    result = await _with_upload(file, lambda path: _service().preview_tradebook(path))
    return _response(result)


@router.post("/imports/tradebook/commit", status_code=201)
async def commit_tradebook(
    file: UploadFile = File(...),
    broker: str = Form(...),
    account_ref: str = Form(...),
    expected_sha256: str = Form(...),
) -> JSONResponse:
    result = await _with_upload(
        file,
        lambda path: _service().import_tradebook(
            path=path,
            broker=broker,
            account_ref=account_ref,
            expected_sha256=expected_sha256,
        ),
    )
    return _response(asdict(result), status_code=201)


@router.post("/imports/holdings/preview")
async def preview_holdings(file: UploadFile = File(...)) -> JSONResponse:
    result = await _with_upload(file, lambda path: _service().preview_holdings(path))
    return _response(result)


@router.post("/imports/holdings/commit", status_code=201)
async def commit_holdings(
    file: UploadFile = File(...),
    broker: str = Form(...),
    account_ref: str = Form(...),
    as_of: date = Form(...),
    market_state: str = Form("eod"),
    mode: str = Form("reconciliation_only"),
    expected_sha256: str = Form(...),
) -> JSONResponse:
    if mode not in {"reconciliation_only", "opening_anchor"}:
        raise HTTPException(status_code=422, detail="Unsupported holdings mode")
    result = await _with_upload(
        file,
        lambda path: _service().import_holdings(
            path=path,
            broker=broker,
            account_ref=account_ref,
            as_of=as_of,
            market_state=market_state,
            mode=mode,  # type: ignore[arg-type]
            expected_sha256=expected_sha256,
        ),
    )
    return _response(asdict(result), status_code=201)


@router.get("/imports")
def list_imports(
    account_ref: str | None = None,
    limit: int = Query(50, ge=1, le=250),
    cursor: str | None = None,
) -> JSONResponse:
    decoded = _decode_cursor(cursor)
    clauses: list[str] = []
    params: list[Any] = []
    if account_ref:
        clauses.append("account_ref=?")
        params.append(account_ref)
    if decoded:
        clauses.append("(created_at<? OR (created_at=? AND import_id<?))")
        params.extend([decoded[0], decoded[0], decoded[1]])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _service().store.reader() as conn:
        rows = rows_as_dicts(conn.execute(
            f"""SELECT * FROM journal_import_file {where}
                ORDER BY created_at DESC,import_id DESC LIMIT ?""",
            [*params, limit + 1],
        ))
    return _response(_page(rows, limit=limit, timestamp_key="created_at", id_key="import_id"))


@router.get("/dq-issues")
def list_dq_issues(
    account_ref: str | None = None,
    limit: int = Query(50, ge=1, le=250),
    cursor: str | None = None,
) -> JSONResponse:
    decoded = _decode_cursor(cursor)
    clauses: list[str] = []
    params: list[Any] = []
    if account_ref:
        clauses.append("account_ref=?")
        params.append(account_ref)
    if decoded:
        clauses.append("(created_at<? OR (created_at=? AND issue_id<?))")
        params.extend([decoded[0], decoded[0], decoded[1]])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _service().store.reader() as conn:
        rows = rows_as_dicts(conn.execute(
            f"""SELECT * FROM journal_dq_issue {where}
                ORDER BY created_at DESC,issue_id DESC LIMIT ?""",
            [*params, limit + 1],
        ))
    return _response(_page(rows, limit=limit, timestamp_key="created_at", id_key="issue_id"))


@router.get("/accounts")
def list_accounts() -> JSONResponse:
    return _response({"items": _service().store.accounts()})


@router.get("/as-of-dates")
def list_as_of_dates(account_ref: str) -> JSONResponse:
    with _service().store.reader() as conn:
        rows = conn.execute(
            """SELECT as_of_date,market_state,trust_status,snapshot_id
               FROM portfolio_snapshot WHERE account_ref=?
               ORDER BY as_of_date DESC,created_at DESC""", [account_ref]
        ).fetchall()
    return _response({"items": [
        {"as_of_date": row[0], "market_state": row[1], "trust_status": row[2], "snapshot_id": row[3]}
        for row in rows
    ]})


@router.get("/positions")
def list_positions(account_ref: str) -> JSONResponse:
    with _service().store.reader() as conn:
        rows = rows_as_dicts(conn.execute(
            """SELECT p.*,(SELECT a.symbol FROM instrument_alias a
                   WHERE a.instrument_id=p.instrument_id
                   ORDER BY a.valid_from DESC NULLS LAST,a.created_at DESC LIMIT 1) symbol
               FROM journal_current_positions p WHERE p.account_ref=?
               ORDER BY symbol,p.instrument_id""",
            [account_ref],
        ))
    return _response({"scope": "securities_only", "items": rows})


@router.get("/episodes")
def list_episodes(
    account_ref: str,
    limit: int = Query(50, ge=1, le=250),
    cursor: str | None = None,
) -> JSONResponse:
    decoded = _decode_cursor(cursor)
    params: list[Any] = [account_ref]
    cursor_clause = ""
    if decoded:
        cursor_clause = "AND (e.opened_at<? OR (e.opened_at=? AND e.episode_id<?))"
        params.extend([decoded[0], decoded[0], decoded[1]])
    with _service().store.reader() as conn:
        rows = rows_as_dicts(conn.execute(
            f"""SELECT e.* FROM trade_episode e JOIN journal_latest_analysis a USING(analysis_run_id)
                WHERE e.account_ref=? AND a.analysis_type='reconstruction' AND a.status='COMPLETED'
                {cursor_clause} ORDER BY e.opened_at DESC,e.episode_id DESC LIMIT ?""",
            [*params, limit + 1],
        ))
    page = _page(rows, limit=limit, timestamp_key="opened_at", id_key="episode_id")
    return _response({"scope": "gross", **page})


@router.get("/episodes/{episode_id}")
def episode_detail(episode_id: str) -> JSONResponse:
    store = _service().store
    with store.reader() as conn:
        episodes = rows_as_dicts(conn.execute(
            """SELECT * FROM trade_episode WHERE episode_id=?
               ORDER BY generated_at DESC LIMIT 1""", [episode_id]
        ))
        if not episodes:
            raise HTTPException(status_code=404, detail="Episode not found")
        fills = rows_as_dicts(conn.execute(
            """SELECT f.*,l.link_type FROM episode_fill_link l
               JOIN journal_fill f USING(fill_id)
               WHERE l.episode_id=? AND l.analysis_run_id=? ORDER BY f.executed_at,f.fill_id""",
            [episode_id, episodes[0]["analysis_run_id"]],
        ))
        annotations = rows_as_dicts(conn.execute(
            "SELECT * FROM journal_annotation WHERE episode_id=? ORDER BY revision", [episode_id]
        ))
    return _response({"scope": "gross", "episode": episodes[0], "fills": fills, "annotations": annotations})


@router.get("/episodes/{episode_id}/chart-markers")
def episode_chart_markers(episode_id: str) -> JSONResponse:
    detail = episode_detail(episode_id)
    payload = json.loads(detail.body)
    markers = [{
        "fill_id": row["fill_id"], "time": row["executed_at"], "side": row["side"],
        "exchange_date": row["trade_date"], "price": row["price"],
        "quantity": row["quantity"], "link_type": row["link_type"],
    } for row in payload["fills"]]
    return _response({"items": markers})


@router.get("/episodes/{episode_id}/chart")
def episode_chart(episode_id: str) -> JSONResponse:
    detail = json.loads(episode_detail(episode_id).body)
    episode = detail["episode"]
    fills = detail["fills"]
    symbols = {str(row["symbol"]) for row in fills}
    exchange = str(fills[0]["exchange"]) if fills else "NSE"
    opened = date.fromisoformat(str(episode["opened_at"])[:10])
    closed = (
        date.fromisoformat(str(episode["closed_at"])[:10])
        if episode.get("closed_at") else date.today()
    )
    chart = _market_reader().candles(
        symbols=symbols, exchange=exchange,
        from_date=opened - timedelta(days=300),
        to_date=min(date.today(), closed + timedelta(days=90)), limit=1000,
    )
    markers = [{
        "fill_id": row["fill_id"], "time": row["trade_date"],
        "executed_at": row["executed_at"], "side": row["side"],
        "price": row["price"], "quantity": row["quantity"],
        "link_type": row["link_type"],
    } for row in fills]
    return _response({"episode_id": episode_id, "bars": chart["items"], "markers": markers,
                      "trust_status": chart["trust_status"], "source_snapshot": chart.get("source_snapshot")})


@router.get("/evaluations")
def list_evaluations(
    account_ref: str, limit: int = Query(50, ge=1, le=250), cursor: str | None = None
) -> JSONResponse:
    decoded = _decode_cursor(cursor)
    params: list[Any] = [account_ref]
    cursor_clause = ""
    if decoded:
        cursor_clause = "AND (p.generated_at<? OR (p.generated_at=? AND p.evaluation_id<?))"
        params.extend([decoded[0], decoded[0], decoded[1]])
    store = _service().store
    with store.reader() as conn:
        rows = rows_as_dicts(conn.execute(
            f"""SELECT p.* FROM portfolio_evaluation p JOIN journal_latest_analysis a USING(analysis_run_id)
               WHERE a.account_ref=? AND a.analysis_type='point_in_time_analysis' AND a.status='COMPLETED'
               {cursor_clause} ORDER BY p.generated_at DESC,p.evaluation_id DESC LIMIT ?""",
            [*params, limit + 1],
        ))
    page = _page(rows, limit=limit, timestamp_key="generated_at", id_key="evaluation_id")
    return _response({"scope": "holdings_only", **page})


@router.get("/portfolio-series")
def portfolio_series(
    account_ref: str,
    from_date: date | None = None,
    to_date: date | None = None,
    limit: int = Query(250, ge=1, le=250),
) -> JSONResponse:
    store = _service().store
    clauses = ["a.account_ref=?", "a.analysis_type='point_in_time_analysis'", "a.status='COMPLETED'"]
    params: list[Any] = [account_ref]
    if from_date:
        clauses.append("r.as_of_date>=?")
        params.append(from_date)
    if to_date:
        clauses.append("r.as_of_date<=?")
        params.append(to_date)
    with store.reader() as conn:
        rows = rows_as_dicts(conn.execute(
            f"""SELECT r.as_of_date,r.scope_label,r.metrics_json,r.logic_version,r.generated_at
                FROM portfolio_risk_snapshot r JOIN journal_latest_analysis a USING(analysis_run_id)
                WHERE {' AND '.join(clauses)} ORDER BY r.as_of_date DESC LIMIT ?""",
            [*params, limit],
        ))
    items = []
    for row in rows:
        metrics = json.loads(row.pop("metrics_json"))
        items.append({**row, **metrics})
    return _response({"scope": "holdings_only", "items": list(reversed(items))})


@router.get("/exposures")
def portfolio_exposures(account_ref: str) -> JSONResponse:
    store = _service().store
    with store.reader() as conn:
        row = conn.execute(
            """SELECT p.analysis_run_id,p.as_of_date,p.metrics_json,p.trust_status,p.logic_version,p.generated_at
               FROM portfolio_evaluation p JOIN journal_latest_analysis a USING(analysis_run_id)
               WHERE a.account_ref=? AND a.analysis_type='point_in_time_analysis'
               AND a.status='COMPLETED' ORDER BY p.generated_at DESC LIMIT 1""",
            [account_ref],
        ).fetchone()
    if row is None:
        return _response({"scope": "holdings_only", "status": "EMPTY", "metrics": {}})
    with store.reader() as conn:
        breaches = rows_as_dicts(conn.execute(
            """SELECT b.* FROM portfolio_policy_breach b
               JOIN portfolio_risk_snapshot r USING(risk_snapshot_id)
               WHERE r.analysis_run_id=? ORDER BY b.rule_code,b.breach_id""",
            [row[0]],
        ))
    return _response({
        "scope": "holdings_only", "as_of_date": row[1], "metrics": json.loads(row[2]),
        "trust_status": row[3], "logic_version": row[4], "generated_at": row[5],
        "policy_breaches": breaches,
    })


@router.get("/trade-evaluations")
def trade_evaluations(
    account_ref: str, evaluation_type: str | None = None,
    limit: int = Query(50, ge=1, le=250), cursor: str | None = None,
) -> JSONResponse:
    clauses = ["a.account_ref=?", "a.analysis_type='point_in_time_analysis'", "a.status='COMPLETED'"]
    params: list[Any] = [account_ref]
    if evaluation_type:
        clauses.append("e.evaluation_type=?")
        params.append(evaluation_type)
    decoded = _decode_cursor(cursor)
    if decoded:
        clauses.append("(e.generated_at<? OR (e.generated_at=? AND e.evaluation_id<?))")
        params.extend([decoded[0], decoded[0], decoded[1]])
    with _service().store.reader() as conn:
        rows = rows_as_dicts(conn.execute(
            f"""SELECT e.*,f.symbol,f.trade_date,f.executed_at,f.side,f.quantity,f.price
                FROM trade_evaluation e JOIN journal_latest_analysis a USING(analysis_run_id)
                LEFT JOIN journal_fill f USING(fill_id) WHERE {' AND '.join(clauses)}
                ORDER BY e.generated_at DESC,e.evaluation_id DESC LIMIT ?""",
            [*params, limit + 1],
        ))
    for row in rows:
        row["components"] = json.loads(row.pop("components_json"))
    page = _page(rows, limit=limit, timestamp_key="generated_at", id_key="evaluation_id")
    return _response({"scope": "gross", **page})


@router.get("/behaviour")
def behaviour_findings(account_ref: str) -> JSONResponse:
    store = _service().store
    with store.reader() as conn:
        rows = conn.execute(
            """SELECT classification,count(*) FROM trade_evaluation e
               JOIN journal_latest_analysis a USING(analysis_run_id)
               WHERE a.account_ref=? AND a.analysis_type='point_in_time_analysis'
               AND a.status='COMPLETED' AND classification IS NOT NULL GROUP BY classification""",
            [account_ref],
        ).fetchall()
    eligible = sum(int(row[1]) for row in rows)
    findings = [
        {"classification": row[0], **behaviour_rate(int(row[1]), eligible)} for row in rows
    ]
    return _response({"scope": "gross", "eligible": eligible, "items": findings})


@router.get("/tasks/{journal_run_id}")
def journal_task_status(journal_run_id: str) -> JSONResponse:
    store = _service().store
    with store.reader() as conn:
        rows = rows_as_dicts(conn.execute(
            """SELECT journal_run_id,action,status,operator_task_id,requested_at,started_at,
                      completed_at,result_json,error_summary
               FROM journal_task_request WHERE journal_run_id=?""", [journal_run_id]
        ))
    if not rows:
        raise HTTPException(status_code=404, detail="Journal task not found")
    return _response(rows[0])


@router.get("/reconciliations")
def list_reconciliations(
    account_ref: str,
    limit: int = Query(50, ge=1, le=250),
    cursor: str | None = None,
) -> JSONResponse:
    decoded = _decode_cursor(cursor)
    params: list[Any] = [account_ref]
    cursor_clause = ""
    if decoded:
        cursor_clause = "AND (generated_at<? OR (generated_at=? AND reconciliation_id<?))"
        params.extend([decoded[0], decoded[0], decoded[1]])
    store = _service().store
    with store.reader() as conn:
        rows = rows_as_dicts(conn.execute(
            f"""SELECT * FROM portfolio_reconciliation WHERE account_ref=? {cursor_clause}
               ORDER BY generated_at DESC,reconciliation_id DESC LIMIT ?""",
            [*params, limit + 1],
        ))
    page = _page(rows, limit=limit, timestamp_key="generated_at", id_key="reconciliation_id")
    return _response({"scope": "securities_only", **page})


@router.get("/reconciliations/{reconciliation_id}")
def reconciliation_detail(reconciliation_id: str) -> JSONResponse:
    with _service().store.reader() as conn:
        reconciliations = rows_as_dicts(conn.execute(
            "SELECT * FROM portfolio_reconciliation WHERE reconciliation_id=?",
            [reconciliation_id],
        ))
        if not reconciliations:
            raise HTTPException(status_code=404, detail="Reconciliation not found")
        items = rows_as_dicts(conn.execute(
            """SELECT * FROM portfolio_reconciliation_item WHERE reconciliation_id=?
               ORDER BY classification,instrument""", [reconciliation_id]
        ))
    return _response({"scope": "securities_only", "reconciliation": reconciliations[0], "items": items})


@router.get("/governance/requests")
def governance_requests(account_ref: str | None = None) -> JSONResponse:
    with _service().store.reader() as conn:
        if account_ref:
            adjustments = rows_as_dicts(conn.execute(
                """SELECT * FROM journal_adjustment_request WHERE account_ref=?
                   ORDER BY proposed_at DESC""", [account_ref]
            ))
        else:
            adjustments = rows_as_dicts(conn.execute(
                "SELECT * FROM journal_adjustment_request ORDER BY proposed_at DESC"
            ))
        actions = rows_as_dicts(conn.execute(
            "SELECT * FROM corporate_action_event ORDER BY created_at DESC"
        ))
    return _response({"adjustments": adjustments, "corporate_actions": actions})


@router.post("/reconstructions", status_code=202)
def reconstruct(account_ref: str = Form(...)) -> JSONResponse:
    # Only the opaque journal run ID is placed on the worker command line.
    return _response(_launch_journal_task(action="reconstruct", account_ref=account_ref), status_code=202)


@router.post("/reconciliations", status_code=202)
def run_reconciliation(
    account_ref: str = Form(...), snapshot_id: str | None = Form(None)
) -> JSONResponse:
    return _response(
        _launch_journal_task(action="reconcile", account_ref=account_ref, snapshot_id=snapshot_id),
        status_code=202,
    )


@router.post("/analyses", status_code=202)
def run_analysis(account_ref: str = Form(...)) -> JSONResponse:
    return _response(_launch_journal_task(action="analyze", account_ref=account_ref), status_code=202)


@router.post("/adjustments/propose", status_code=201)
def propose_adjustment(
    account_ref: str = Form(...),
    instrument_id: str = Form(...),
    adjustment_type: str = Form(...),
    effective_at: datetime = Form(...),
    quantity: Decimal | None = Form(None),
    amount: Decimal | None = Form(None),
    reason: str = Form(...),
) -> JSONResponse:
    try:
        result = _service().propose_adjustment(
            account=account_ref, instrument_id=instrument_id,
            adjustment_type=adjustment_type, effective_at=effective_at,
            quantity=quantity, amount=amount, reason=reason,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _response(result, status_code=201)


@router.post("/opening-lots/propose", status_code=201)
def propose_opening_lot(
    account_ref: str = Form(...),
    instrument_id: str = Form(...),
    effective_at: datetime = Form(...),
    quantity: Decimal = Form(...),
    total_cost: Decimal = Form(...),
    reason: str = Form(...),
) -> JSONResponse:
    return propose_adjustment(
        account_ref=account_ref, instrument_id=instrument_id,
        adjustment_type="opening_lot", effective_at=effective_at,
        quantity=quantity, amount=total_cost, reason=reason,
    )


@router.post("/adjustments/{adjustment_id}/approve")
def approve_adjustment(adjustment_id: str, reviewer: str = Form(...)) -> JSONResponse:
    try:
        result = _service().approve_adjustment(adjustment_id, reviewer=reviewer)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _response(result)


@router.post("/opening-lots/{adjustment_id}/approve")
def approve_opening_lot(adjustment_id: str, reviewer: str = Form(...)) -> JSONResponse:
    return approve_adjustment(adjustment_id, reviewer)


@router.post("/corporate-actions/propose", status_code=201)
def propose_corporate_action(
    instrument_id: str = Form(...),
    action_type: str = Form(...),
    effective_date: date = Form(...),
    quantity_factor: Decimal = Form(...),
    cost_factor: Decimal | None = Form(None),
    source_ref: str = Form(...),
) -> JSONResponse:
    try:
        result = _service().propose_corporate_action(
            instrument_id=instrument_id, action_type=action_type,
            effective_date=effective_date, quantity_factor=quantity_factor,
            cost_factor=cost_factor, source_ref=source_ref,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _response(result, status_code=201)


@router.post("/corporate-actions/{action_id}/approve")
def approve_corporate_action(action_id: str, reviewer: str = Form(...)) -> JSONResponse:
    try:
        result = _service().approve_corporate_action(action_id, reviewer=reviewer)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _response(result)


@router.post("/annotations", status_code=201)
def append_annotation(
    episode_id: str = Form(...),
    thesis: str | None = Form(None),
    setup: str | None = Form(None),
    intended_stop: Decimal | None = Form(None),
    target: Decimal | None = Form(None),
    exit_reason: str | None = Form(None),
    lesson: str | None = Form(None),
    tags_json: str = Form("[]"),
) -> JSONResponse:
    store = _service().store
    created_at = utc_now()
    with store.writer() as conn:
        version_row = conn.execute(
            "SELECT coalesce(max(revision),0)+1 FROM journal_annotation WHERE episode_id=?",
            [episode_id],
        ).fetchone()
        if version_row is None:  # pragma: no cover - aggregate always returns one row
            raise RuntimeError("annotation version query returned no row")
        revision = int(version_row[0])
        annotation_id = stable_id("annotation", episode_id, revision)
        conn.execute(
            "INSERT INTO journal_annotation VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [annotation_id, episode_id, revision, thesis, setup, intended_stop, target,
             exit_reason, lesson, tags_json, "operator", created_at],
        )
    return _response({"annotation_id": annotation_id, "revision": revision}, status_code=201)
