"""FastAPI application for the strictly read-only Phase 4A service."""

from __future__ import annotations

import hmac
import json
import logging
import re
import time
import uuid
from collections import defaultdict, deque
from datetime import date, datetime, time as datetime_time, timezone
from typing import Any, Callable

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError

from .config import ApiSettings, SourceProfile
from .errors import Phase4ApiError
from .pagination import decode_cursor, encode_cursor
from .schemas import (
    AlertIncidentSummary,
    AlertSummary,
    ApiError,
    ApiResponse,
    CalibrationManifestResponse,
    CalibrationSummaryResponse,
    CandidateDetail,
    CandidateSnapshotResponse,
    CandidateSummary,
    CorrectionImpactResponse,
    DecisionContextResponse,
    GovernanceCorrectionResponse,
    HealthResponse,
    LineageRef,
    LineageMeta,
    MarketStageResponse,
    OutcomeAttributionResponse,
    PaginationMeta,
    PerformanceSummaryResponse,
    PositionCoverageSummary,
    ReadinessCheckResponse,
    ResponseMeta,
    RoutingDecisionDetail,
    RoutingDecisionSummary,
    SectorSummary,
    SourceFreshness,
    StageObservation,
    StockSummary,
    SystemLimitation,
    SystemReadinessResponse,
    VersionResponse,
)
from .services import LIMITATIONS, Phase4ReadService
from .telemetry import ApiMetrics


LOGGER = logging.getLogger("ai_trading_system.phase4_api")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
PUBLIC_PATHS = {"/api/v1/health/live", "/api/v1/health/ready"}
ALLOWED_QUERY_PARAMS = {
    "limit", "cursor", "sort", "order", "as_of", "exchange", "symbol",
    "sector", "stock_stage", "sector_stage", "stage_status", "scan_tier",
    "scan_reason", "candidate_state", "setup_family", "coverage_status",
    "alert_status", "status", "severity", "eligibility_status", "readiness_status",
    "date_from", "date_to", "include",
    "dimension", "bucket", "cache_mode", "replay_mode", "performance_status",
    "review_required", "calibration_eligible", "entity_type",
}


def _request_id(request: Request) -> str:
    value = (request.headers.get("X-Request-ID") or "").strip()
    return value if REQUEST_ID_RE.fullmatch(value) else str(uuid.uuid4())


def _parse_as_of(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            parsed = datetime.combine(date.fromisoformat(value), datetime_time.max, tzinfo=timezone.utc)
        else:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("RFC3339 timestamps require a timezone")
            parsed = parsed.astimezone(timezone.utc)
    except ValueError as exc:
        raise Phase4ApiError("INVALID_AS_OF", "as_of must be an ISO date or timezone-aware RFC3339 timestamp", 400) from exc
    if parsed > datetime.now(timezone.utc):
        raise Phase4ApiError("INVALID_AS_OF", "future as_of values are not supported", 400)
    return parsed


def _meta(request: Request, *, as_of: datetime | None = None, partial: bool = False, limitations: list[str] | None = None, pagination: PaginationMeta | None = None) -> ResponseMeta:
    service: Phase4ReadService = request.app.state.service
    path = request.url.path
    family = next((name for prefix, name in (("/api/v1/positions", "positions"), ("/api/v1/calibration", "calibration"), ("/api/v1/readiness", "calibration"), ("/api/v1/performance", "performance"), ("/api/v1/governance", "governance"), ("/api/v1/routing", "routing"), ("/api/v1/stocks", "stages"), ("/api/v1/sectors", "stages"), ("/api/v1/market/stage", "stages"), ("/api/v1/candidates", "candidates"), ("/api/v1/alerts", "alerts"), ("/api/v1/alert-incidents", "alerts"), ("/api/v1/system/readiness", "readiness"), ("/api/v1/system/limitations", "readiness")) if path.startswith(prefix)), "system")
    state = service.projection_state(family)
    lineage = [LineageRef.model_validate(item) for item in state.lineage]
    combined = list(dict.fromkeys([*(limitations or []), *state.limitations]))
    freshness = SourceFreshness.model_validate({**state.freshness, **({"source_as_of": as_of} if as_of and not state.freshness.get("source_as_of") else {})})
    effective_partial = partial or bool(state.limitations) or state.source_version_mismatch
    return ResponseMeta(
        request_id=request.state.request_id,
        generated_at=datetime.now(timezone.utc),
        as_of=as_of,
        source_freshness=freshness,
        freshness=freshness,
        partial=effective_partial,
        limitations=combined,
        lineage=lineage,
        lineage_meta=LineageMeta(primary=lineage[0] if lineage else None, supporting=lineage[1:] if lineage else [], source_consistent=not state.source_version_mismatch, source_version_mismatch=state.source_version_mismatch),
        pagination=pagination,
    )


def _response(request: Request, data: Any, *, meta: ResponseMeta, etag_seed: Any | None = None, status_code: int = 200) -> Response:
    payload = ApiResponse(data=data, meta=meta).model_dump(mode="json")
    headers = {"X-Request-ID": request.state.request_id}
    if meta.partial:
        route = getattr(request.scope.get("route"), "path", "__unmatched__")
        request.app.state.metrics.record_partial(route)
    if data and request.url.path in {"/api/v1/governance/conflicts", "/api/v1/routing/conflicts"}:
        request.app.state.metrics.governance_conflict_response_count += 1
    if etag_seed is not None:
        service: Phase4ReadService = request.app.state.service
        etag = f'"{service.semantic_hash(etag_seed)}"'
        headers["ETag"] = etag
        if request.headers.get("If-None-Match") == etag:
            return Response(status_code=304, headers=headers)
    return JSONResponse(payload, status_code=status_code, headers=headers)


def _item_id(row: dict[str, Any]) -> str:
    for key in ("observation_id", "decision_id", "candidate_id", "position_cycle_id", "alert_id", "governance_event_id", "impact_id", "membership_observation_id", "run_id", "recovery_proposal_id"):
        if row.get(key) is not None:
            return str(row[key])
    return Phase4ReadService.semantic_hash(row)


def _list_page(
    rows: list[dict[str, Any]], *, limit: int, cursor: str | None, sort: str,
    order: str, allowed_sort: set[str], filters: dict[str, Any], max_limit: int,
    cursor_filters: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], PaginationMeta]:
    if limit < 1 or limit > max_limit:
        raise Phase4ApiError("INVALID_ARGUMENT", f"limit must be between 1 and {max_limit}", 400)
    if sort not in allowed_sort:
        raise Phase4ApiError("INVALID_ARGUMENT", "Unknown sort field", 400, {"allowed": sorted(allowed_sort)})
    if order not in {"asc", "desc"}:
        raise Phase4ApiError("INVALID_ARGUMENT", "order must be asc or desc", 400)
    for key, value in filters.items():
        if value is not None:
            rows = [row for row in rows if str(row.get(key, "")).lower() == str(value).lower()]
    rows.sort(key=lambda row: (str(row.get(sort, "")), _item_id(row)), reverse=order == "desc")
    bound_filters = cursor_filters if cursor_filters is not None else filters
    start = 0
    if cursor:
        last = decode_cursor(cursor, sort=sort, order=order, filters=bound_filters)
        matches = [index for index, row in enumerate(rows) if _item_id(row) == last]
        if not matches:
            raise Phase4ApiError("INVALID_ARGUMENT", "Cursor is no longer valid", 400)
        start = matches[0] + 1
    page = rows[start : start + limit]
    has_more = start + limit < len(rows)
    next_cursor = encode_cursor(sort=sort, order=order, last_key=_item_id(page[-1]), filters=bound_filters) if has_more and page else None
    return page, PaginationMeta(next_cursor=next_cursor, has_more=has_more, limit=limit)


def _find(rows: list[dict[str, Any]], key: str, value: str) -> dict[str, Any]:
    for row in rows:
        if str(row.get(key)) == value:
            return row
    raise Phase4ApiError("RESOURCE_NOT_FOUND", "Resource not found", 404)


def _allow(value: str | None, allowed: set[str], field: str) -> None:
    if value is not None and value.lower() not in allowed:
        raise Phase4ApiError(
            "INVALID_ARGUMENT", f"Unknown {field}", 400, {"allowed": sorted(allowed)}
        )


def create_app(*, testing: bool = False, settings: ApiSettings | None = None) -> FastAPI:
    if settings is None:
        settings = ApiSettings(
            source_profile=SourceProfile.SMALL_FIXTURE,
            auth_enabled=False,
            local_dev_mode=True,
        ) if testing else ApiSettings.from_env()
    assert settings is not None
    resolved_settings: ApiSettings = settings
    docs_url = "/docs" if settings.include_openapi else None
    openapi_url = "/openapi.json" if settings.include_openapi else None
    app = FastAPI(
        title="AI Trading System Phase 4A Read-Only API",
        description=(
            "Strictly read-only governed Phase 3 state. No pipeline, broker, execution, "
            "recovery, acknowledgement, refresh, or mutation operations are exposed. "
            "Phase 4 development is ready; production deployment remains blocked."
        ),
        version="1.0.0",
        docs_url=docs_url,
        openapi_url=openapi_url,
    )
    app.state.settings = settings
    app.state.metrics = ApiMetrics()
    app.state.service = Phase4ReadService(settings, app.state.metrics)
    app.state.rate_windows = defaultdict(deque)

    @app.exception_handler(Phase4ApiError)
    async def phase4_error(request: Request, exc: Phase4ApiError) -> JSONResponse:
        request_id = getattr(request.state, "request_id", _request_id(request))
        body = ApiError(code=exc.code, message=exc.message, request_id=request_id, details=exc.details)
        return JSONResponse(body.model_dump(mode="json"), status_code=exc.status_code, headers={"X-Request-ID": request_id})

    @app.exception_handler(ValidationError)
    async def validation_error(request: Request, exc: ValidationError) -> JSONResponse:
        request_id = getattr(request.state, "request_id", _request_id(request))
        body = ApiError(code="INVALID_ARGUMENT", message="Response contract validation failed", request_id=request_id)
        return JSONResponse(body.model_dump(mode="json"), status_code=500, headers={"X-Request-ID": request_id})

    @app.exception_handler(Exception)
    async def internal_error(request: Request, exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", _request_id(request))
        LOGGER.exception("phase4_api_internal_error request_id=%s", request_id)
        body = ApiError(code="INTERNAL_ERROR", message="An internal error occurred", request_id=request_id)
        return JSONResponse(body.model_dump(mode="json"), status_code=500, headers={"X-Request-ID": request_id})

    @app.middleware("http")
    async def read_only_security(request: Request, call_next: Callable):
        started = time.monotonic()
        request.state.request_id = _request_id(request)
        authenticated = False
        path = request.url.path
        def finish(response: Response) -> Response:
            duration_ms = round((time.monotonic() - started) * 1000, 3)
            response.headers["X-Request-ID"] = request.state.request_id
            route_object = request.scope.get("route")
            route = getattr(route_object, "path", None)
            if route is None:
                route = next((str(getattr(item, "path")) for item in app.routes if getattr(item, "path_regex", None) and getattr(item, "path_regex").fullmatch(path)), "__unmatched__")
            app.state.metrics.record_request(route, request.method, response.status_code, duration_ms)
            LOGGER.info(json.dumps({"event": "phase4_api_request", "request_id": request.state.request_id, "request_started_at": datetime.now(timezone.utc).isoformat(), "duration_ms": duration_ms, "route": route, "status_code": response.status_code, "authenticated": authenticated}, sort_keys=True))
            return response
        if path.startswith("/api/v1") and request.method not in {"GET", "HEAD", "OPTIONS"}:
            return finish(JSONResponse(
                ApiError(code="METHOD_NOT_ALLOWED", message="Phase 4A is read-only", request_id=request.state.request_id).model_dump(),
                status_code=405, headers={"Allow": "GET, HEAD, OPTIONS", "X-Request-ID": request.state.request_id},
            ))
        unknown = set(request.query_params) - ALLOWED_QUERY_PARAMS
        if path.startswith("/api/v1") and unknown:
            return finish(JSONResponse(
                ApiError(code="INVALID_ARGUMENT", message="Unknown query parameter", request_id=request.state.request_id, details={"unknown": sorted(unknown)}).model_dump(),
                status_code=400, headers={"X-Request-ID": request.state.request_id},
            ))
        protected = path.startswith("/api/v1") and path not in PUBLIC_PATHS
        if protected and settings.auth_enabled and not settings.local_dev_mode:
            if not settings.api_key:
                app.state.metrics.authentication_failure_count += 1
                return finish(JSONResponse(ApiError(code="AUTHENTICATION_REQUIRED", message="API authentication is not configured", request_id=request.state.request_id).model_dump(), status_code=503, headers={"X-Request-ID": request.state.request_id}))
            supplied = request.headers.get("Authorization", "")
            supplied = supplied[7:].strip() if supplied.startswith("Bearer ") else (request.headers.get("X-API-Key") or "").strip()
            if not supplied:
                app.state.metrics.authentication_failure_count += 1
                return finish(JSONResponse(ApiError(code="AUTHENTICATION_REQUIRED", message="Authentication required", request_id=request.state.request_id).model_dump(), status_code=401, headers={"X-Request-ID": request.state.request_id}))
            if not hmac.compare_digest(supplied.encode(), settings.api_key.encode()):
                app.state.metrics.authorization_failure_count += 1
                return finish(JSONResponse(ApiError(code="AUTHORIZATION_DENIED", message="Invalid API credential", request_id=request.state.request_id).model_dump(), status_code=403, headers={"X-Request-ID": request.state.request_id}))
            authenticated = True
            window = app.state.rate_windows[Phase4ReadService.semantic_hash(supplied)]
            now = time.monotonic()
            while window and window[0] <= now - 60:
                window.popleft()
            if len(window) >= settings.rate_limit_per_minute:
                app.state.metrics.rate_limit_count += 1
                return finish(JSONResponse(ApiError(code="RATE_LIMITED", message="Rate limit exceeded", request_id=request.state.request_id).model_dump(), status_code=429, headers={"X-Request-ID": request.state.request_id}))
            window.append(now)
        response = await call_next(request)
        return finish(response)

    # Keep CORS outside the authentication middleware so a browser can complete
    # its credential-header preflight. The subsequent GET is still authenticated.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.cors_allowed_origins),
        allow_credentials=False,
        allow_methods=["GET", "HEAD", "OPTIONS"],
        allow_headers=["Authorization", "X-API-Key", "X-Request-ID", "If-None-Match"],
        expose_headers=["ETag", "X-Request-ID"],
        max_age=600,
    )

    service: Phase4ReadService = app.state.service

    @app.get("/api/v1/health/live", response_model=ApiResponse[dict[str, str]], tags=["system"])
    def health_live(request: Request):
        return _response(request, {"status": "live"}, meta=_meta(request))

    @app.get("/api/v1/health/ready", response_model=ApiResponse[HealthResponse], tags=["system"])
    def health_ready(request: Request):
        readable = service.source_readable()
        auth_ok = settings.auth_configured()
        ready = readable and auth_ok
        data = {"api_ready": ready, "source_readable": readable, "primary_source_readable": readable, "optional_sources": {"phase3c_tables": "optional_or_unapplied"}, "phase4_development_ready": True, "phase4_production_ready": False, "limitations": list(LIMITATIONS)}
        return _response(request, data, meta=_meta(request, partial=not ready, limitations=list(LIMITATIONS)), status_code=200 if ready else 503)

    @app.get("/api/v1/system/version", response_model=ApiResponse[VersionResponse], tags=["system"])
    def system_version(request: Request):
        data = service.version()
        return _response(request, data, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=data)

    @app.get("/api/v1/system/readiness", response_model=ApiResponse[SystemReadinessResponse], tags=["system"])
    def system_readiness(request: Request):
        data = service.readiness()
        return _response(request, data, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=data)

    @app.get("/api/v1/system/limitations", response_model=ApiResponse[list[SystemLimitation]], tags=["system"])
    def system_limitations(request: Request):
        data = service.limitations()
        return _response(request, data, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=data)

    def stage_rows(request: Request, *, scope: str, as_of: str | None, symbol: str | None = None, sector: str | None = None) -> tuple[list[dict[str, Any]], datetime, list[str]]:
        cutoff = _parse_as_of(as_of)
        rows = service.stages(scope, cutoff)
        if symbol:
            rows = [row for row in rows if str(row.get("symbol_id", "")).upper() == symbol.upper()]
        if sector:
            rows = [row for row in rows if str(row.get("sector_id", "")).upper() == sector.upper()]
        entity = symbol or sector
        if entity:
            conflict = service.entity_conflict(scope, entity)
            if conflict:
                app.state.metrics.governance_conflict_response_count += 1
                raise Phase4ApiError("GOVERNANCE_CONFLICT", "No authoritative stage can be selected", 409, conflict)
        limitations = service.partial_limitations("stages", rows)
        return rows, cutoff, limitations

    @app.get("/api/v1/market/stage", response_model=ApiResponse[MarketStageResponse], tags=["market/stages"])
    def market_stage(request: Request, as_of: str | None = None):
        rows, cutoff, limits = stage_rows(request, scope="stock", as_of=as_of)
        data = {"observations": [StageObservation.model_validate(row).model_dump() for row in rows], "conflicts": service.governance("conflicts")}
        return _response(request, data, meta=_meta(request, as_of=cutoff, partial=not rows, limitations=limits))

    @app.get("/api/v1/sectors", response_model=ApiResponse[list[SectorSummary]], tags=["market/stages"])
    def sectors(request: Request, as_of: str | None = None, sector_stage: str | None = None, stage_status: str | None = None):
        rows, cutoff, limits = stage_rows(request, scope="sector", as_of=as_of)
        if sector_stage:
            rows = [row for row in rows if row.get("effective_stage") == sector_stage]
        if stage_status:
            rows = [row for row in rows if row.get("stage_status") == stage_status]
        return _response(request, rows, meta=_meta(request, as_of=cutoff, partial=not rows, limitations=limits))

    @app.get("/api/v1/sectors/{sector_id}", response_model=ApiResponse[SectorSummary], tags=["market/stages"])
    def sector_detail(sector_id: str, request: Request, as_of: str | None = None):
        rows, cutoff, limits = stage_rows(request, scope="sector", as_of=as_of, sector=sector_id)
        row = _find(rows, "sector_id", sector_id)
        return _response(request, row, meta=_meta(request, as_of=cutoff, limitations=limits), etag_seed=row)

    @app.get("/api/v1/stocks", response_model=ApiResponse[list[StockSummary]], tags=["market/stages"])
    def stocks(request: Request, as_of: str | None = None, symbol: str | None = None, sector: str | None = None, stock_stage: str | None = None, stage_status: str | None = None):
        rows, cutoff, limits = stage_rows(request, scope="stock", as_of=as_of, symbol=symbol, sector=sector)
        if stock_stage:
            rows = [row for row in rows if row.get("effective_stage") == stock_stage]
        if stage_status:
            rows = [row for row in rows if row.get("stage_status") == stage_status]
        return _response(request, rows, meta=_meta(request, as_of=cutoff, partial=not rows, limitations=limits))

    @app.get("/api/v1/stocks/{symbol_id}", response_model=ApiResponse[StockSummary], tags=["market/stages"])
    def stock_detail(symbol_id: str, request: Request, as_of: str | None = None):
        rows, cutoff, limits = stage_rows(request, scope="stock", as_of=as_of, symbol=symbol_id)
        row = _find(rows, "symbol_id", symbol_id)
        return _response(request, row, meta=_meta(request, as_of=cutoff, limitations=limits), etag_seed=row)

    @app.get("/api/v1/stocks/{symbol_id}/stage-history", response_model=ApiResponse[list[StageObservation]], tags=["market/stages"])
    def stock_history(symbol_id: str, request: Request, as_of: str | None = None):
        rows, cutoff, limits = stage_rows(request, scope="stock", as_of=as_of, symbol=symbol_id)
        return _response(request, rows, meta=_meta(request, as_of=cutoff, partial=not rows, limitations=limits))

    def list_resource(request: Request, rows: list[dict[str, Any]], *, resource: str, limit: int, cursor: str | None, sort: str, order: str, allowed_sort: set[str], filters: dict[str, Any]):
        page, page_meta = _list_page(rows, limit=limit, cursor=cursor, sort=sort, order=order, allowed_sort=allowed_sort, filters=filters, max_limit=settings.max_page_size)
        limits = service.partial_limitations(resource, rows)
        return _response(request, page, meta=_meta(request, partial=not rows, limitations=limits, pagination=page_meta))

    @app.get("/api/v1/routing", response_model=ApiResponse[list[RoutingDecisionSummary]], tags=["routing"])
    def routing(request: Request, limit: int = Query(resolved_settings.default_page_size), cursor: str | None = None, sort: str = "as_of", order: str = "desc", symbol: str | None = None, scan_tier: str | None = None, scan_reason: str | None = None):
        _allow(scan_tier, {"stage_only", "light_pattern", "full_investigator", "position_monitor"}, "scan_tier")
        _allow(scan_reason, {"full_universe_structural", "stage_1_discovery", "stage_transition_discovery", "rank_selected", "stage_promoted", "active_position", "recent_exit", "triggered_candidate", "pending_followthrough", "manual_override"}, "scan_reason")
        return list_resource(request, service.routing(), resource="routing", limit=limit, cursor=cursor, sort=sort, order=order, allowed_sort={"as_of", "symbol_id", "decision_id"}, filters={"symbol_id": symbol, "effective_scan_tier": scan_tier, "winning_reason": scan_reason})

    @app.get("/api/v1/routing/conflicts", response_model=ApiResponse[list[dict[str, Any]]], tags=["routing"])
    def routing_conflicts(request: Request):
        rows = service.governance("conflicts")
        return _response(request, rows, meta=_meta(request, partial=not rows, limitations=service.partial_limitations("routing", rows)))

    @app.get("/api/v1/routing/{routing_decision_id}", response_model=ApiResponse[RoutingDecisionDetail], tags=["routing"])
    def routing_detail(routing_decision_id: str, request: Request):
        row = _find(service.routing(), "decision_id", routing_decision_id)
        return _response(request, row, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=row)

    @app.get("/api/v1/stocks/{symbol_id}/routing", response_model=ApiResponse[list[RoutingDecisionSummary]], tags=["routing"])
    def stock_routing(symbol_id: str, request: Request):
        rows = [row for row in service.routing() if str(row.get("symbol_id", "")).upper() == symbol_id.upper()]
        return _response(request, rows, meta=_meta(request, partial=not rows, limitations=service.partial_limitations("routing", rows)))

    @app.get("/api/v1/candidates", response_model=ApiResponse[list[CandidateSummary]], tags=["candidates"])
    def candidates(request: Request, limit: int = Query(resolved_settings.default_page_size), cursor: str | None = None, sort: str = "opened_at", order: str = "desc", symbol: str | None = None, candidate_state: str | None = None, setup_family: str | None = None):
        _allow(candidate_state, {"unseen", "discovered", "investigating", "early_accumulation", "setup_forming", "ready", "triggered", "pending_followthrough", "confirmed", "advancing", "extended", "weakening", "failed", "exited", "archived", "open", "closed"}, "candidate_state")
        return list_resource(request, service.candidates(), resource="candidates", limit=limit, cursor=cursor, sort=sort, order=order, allowed_sort={"opened_at", "symbol_id", "candidate_id"}, filters={"symbol_id": symbol, "candidate_state": candidate_state, "setup_family": setup_family})

    @app.get("/api/v1/candidates/{candidate_id}", response_model=ApiResponse[CandidateDetail], tags=["candidates"])
    def candidate_detail(candidate_id: str, request: Request):
        row = _find(service.candidates(), "candidate_id", candidate_id)
        return _response(request, row, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=row)

    def candidate_children(candidate_id: str, request: Request, resource: str, model: Any, limit: int, cursor: str | None):
        _find(service.candidates(), "candidate_id", candidate_id)
        rows = [row for row in service.candidate_children(resource) if row["candidate_id"] == candidate_id]
        id_field = {"snapshots": "snapshot_id", "decisions": "decision_context_id", "outcomes": "attribution_id"}[resource]
        page, page_meta = _list_page(rows, limit=limit, cursor=cursor, sort=id_field, order="asc", allowed_sort={id_field}, filters={"candidate_id": candidate_id}, max_limit=settings.max_page_size)
        return _response(request, [model.model_validate(row).model_dump() for row in page], meta=_meta(request, partial=not rows, limitations=service.partial_limitations("candidates", rows), pagination=page_meta))

    @app.get("/api/v1/candidates/{candidate_id}/snapshots", response_model=ApiResponse[list[CandidateSnapshotResponse]], tags=["candidates"])
    def candidate_snapshots(candidate_id: str, request: Request, limit: int = Query(resolved_settings.default_page_size), cursor: str | None = None): return candidate_children(candidate_id, request, "snapshots", CandidateSnapshotResponse, limit, cursor)

    @app.get("/api/v1/candidates/{candidate_id}/decisions", response_model=ApiResponse[list[DecisionContextResponse]], tags=["candidates"])
    def candidate_decisions(candidate_id: str, request: Request, limit: int = Query(resolved_settings.default_page_size), cursor: str | None = None): return candidate_children(candidate_id, request, "decisions", DecisionContextResponse, limit, cursor)

    @app.get("/api/v1/candidates/{candidate_id}/outcomes", response_model=ApiResponse[list[OutcomeAttributionResponse]], tags=["candidates"])
    def candidate_outcomes(candidate_id: str, request: Request, limit: int = Query(resolved_settings.default_page_size), cursor: str | None = None): return candidate_children(candidate_id, request, "outcomes", OutcomeAttributionResponse, limit, cursor)

    @app.get("/api/v1/positions/coverage", response_model=ApiResponse[list[PositionCoverageSummary]], tags=["positions"])
    def position_coverage(request: Request, coverage_status: str | None = None):
        _allow(coverage_status, {"fully_monitored", "routed_with_incomplete_data", "missing_routing", "incompatible_episode", "recovery_required", "hard_exclusion"}, "coverage_status")
        rows = service.positions()
        if coverage_status:
            rows = [row for row in rows if row["coverage_status"] == coverage_status]
        return _response(request, rows, meta=_meta(request, partial=not rows, limitations=service.partial_limitations("positions", rows)))

    @app.get("/api/v1/positions/missing-data", response_model=ApiResponse[list[dict[str, Any]]], tags=["positions"])
    def position_missing(request: Request):
        rows = service.position_missing_data()
        return _response(request, rows, meta=_meta(request, limitations=service.partial_limitations("positions", rows)))

    @app.get("/api/v1/positions/recovery-proposals", response_model=ApiResponse[list[dict[str, Any]]], tags=["positions"])
    def recovery_proposals(request: Request):
        rows = service.recovery_proposals()
        return _response(request, rows, meta=_meta(request, partial=not rows, limitations=service.partial_limitations("positions", rows)))

    @app.get("/api/v1/positions/coverage/{position_cycle_id}", response_model=ApiResponse[PositionCoverageSummary], tags=["positions"])
    def position_detail(position_cycle_id: str, request: Request):
        row = _find(service.positions(), "position_cycle_id", position_cycle_id)
        return _response(request, row, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=row)

    def alert_list(request: Request, incidents: bool, severity: str | None, status: str | None):
        _allow(severity, {"info", "warning", "critical"}, "severity")
        _allow(status, {"open", "resolved", "recurred"}, "status")
        rows = service.alerts(incidents)
        if severity:
            rows = [row for row in rows if row["severity"].lower() == severity.lower()]
        if status:
            rows = [row for row in rows if row["status"].lower() == status.lower()]
        return _response(request, rows, meta=_meta(request, partial=not rows, limitations=service.partial_limitations("alerts", rows)))

    @app.get("/api/v1/alerts", response_model=ApiResponse[list[AlertSummary]], tags=["alerts"])
    def alerts(request: Request, severity: str | None = None, status: str | None = None): return alert_list(request, False, severity, status)

    @app.get("/api/v1/alerts/{alert_id}", response_model=ApiResponse[AlertSummary], tags=["alerts"])
    def alert_detail(alert_id: str, request: Request):
        row = _find(service.alerts(False), "alert_id", alert_id)
        return _response(request, row, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=row)

    @app.get("/api/v1/alert-incidents", response_model=ApiResponse[list[AlertIncidentSummary]], tags=["alerts"])
    def incidents(request: Request, severity: str | None = None, status: str | None = None): return alert_list(request, True, severity, status)

    @app.get("/api/v1/alert-incidents/{incident_id}", response_model=ApiResponse[AlertIncidentSummary], tags=["alerts"])
    def incident_detail(incident_id: str, request: Request):
        row = _find(service.alerts(True), "alert_id", incident_id)
        return _response(request, row, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=row)

    @app.get("/api/v1/governance/stage-corrections", response_model=ApiResponse[list[GovernanceCorrectionResponse]], tags=["governance"])
    def corrections(request: Request):
        rows = service.governance("corrections")
        return _response(request, rows, meta=_meta(request, partial=not rows, limitations=service.partial_limitations("governance", rows)))

    @app.get("/api/v1/governance/correction-impacts", response_model=ApiResponse[list[CorrectionImpactResponse]], tags=["governance"])
    def impacts(request: Request, status: str | None = None, review_required: bool | None = None, calibration_eligible: bool | None = None, entity_type: str | None = None):
        rows = service.governance("impacts")
        if status:
            rows = [row for row in rows if str(row.get("impact_link_status") or row.get("impact_status")).lower() == status.lower()]
        if review_required is not None:
            rows = [row for row in rows if bool(row.get("review_required")) is review_required]
        if calibration_eligible is not None:
            rows = [row for row in rows if bool(row.get("authoritative_calibration_eligible")) is calibration_eligible]
        if entity_type:
            rows = [row for row in rows if str(row.get("entity_type", "")).lower() == entity_type.lower()]
        return _response(request, rows, meta=_meta(request, partial=not rows, limitations=service.partial_limitations("governance", rows)))

    @app.get("/api/v1/governance/conflicts", response_model=ApiResponse[list[dict[str, Any]]], tags=["governance"])
    def governance_conflicts(request: Request):
        rows = service.governance("conflicts")
        return _response(request, rows, meta=_meta(request, partial=False, limitations=service.partial_limitations("governance", rows)))

    @app.get("/api/v1/governance/membership-history", response_model=ApiResponse[list[dict[str, Any]]], tags=["governance"])
    def memberships(request: Request):
        rows = service.governance("memberships")
        return _response(request, rows, meta=_meta(request, partial=not rows, limitations=service.partial_limitations("governance", rows)))

    @app.get("/api/v1/calibration/summary", response_model=ApiResponse[CalibrationSummaryResponse], tags=["calibration/readiness"])
    def calibration_summary(request: Request):
        data = service.calibration("summary")
        return _response(request, data, meta=_meta(request, partial=data["manifest_id"] is None, limitations=list(LIMITATIONS)), etag_seed=data)

    @app.get("/api/v1/calibration/manifest", response_model=ApiResponse[CalibrationManifestResponse], tags=["calibration/readiness"])
    def calibration_manifest(request: Request):
        data = service.calibration("manifest")
        return _response(request, data, meta=_meta(request, partial=data["manifest_id"] is None, limitations=list(LIMITATIONS)), etag_seed=data)

    @app.get("/api/v1/calibration/coverage", response_model=ApiResponse[list[dict[str, Any]]], tags=["calibration/readiness"])
    def calibration_coverage(request: Request, dimension: str | None = None, bucket: str | None = None, status: str | None = None):
        rows = service.calibration("coverage")
        for key, value in (("dimension", dimension), ("value", bucket), ("status", status)):
            if value is not None:
                rows = [row for row in rows if str(row.get(key, "")).lower() == value.lower()]
        return _response(request, rows, meta=_meta(request, partial=not rows, limitations=service.partial_limitations("calibration", rows)))

    @app.get("/api/v1/calibration/exclusions", response_model=ApiResponse[list[dict[str, Any]]], tags=["calibration/readiness"])
    def calibration_exclusions(request: Request, eligibility_status: str | None = None): return _response(request, service.calibration("exclusions"), meta=_meta(request, limitations=list(LIMITATIONS)))

    @app.get("/api/v1/readiness/checks", response_model=ApiResponse[list[ReadinessCheckResponse]], tags=["calibration/readiness"])
    def readiness_checks(request: Request, readiness_status: str | None = None):
        rows = service.calibration("checks")
        if readiness_status:
            rows = [row for row in rows if row["status"] == readiness_status]
        return _response(request, rows, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=rows)

    @app.get("/api/v1/performance/latest", response_model=ApiResponse[PerformanceSummaryResponse], tags=["performance"])
    def performance_latest(request: Request):
        rows = service.performance()
        row = rows[-1] if rows else None
        if row is None:
            raise Phase4ApiError("SOURCE_UNAVAILABLE", "Performance evidence is unavailable", 503, {"limitation": "COPIED_REALISTIC_BASELINE_MISSING"})
        return _response(request, row, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=row)

    @app.get("/api/v1/performance/runs", response_model=ApiResponse[list[PerformanceSummaryResponse]], tags=["performance"])
    def performance_runs(request: Request, limit: int = Query(resolved_settings.default_page_size), cursor: str | None = None, sort: str = "as_of", order: str = "desc", cache_mode: str | None = None, replay_mode: str | None = None, performance_status: str | None = None, date_from: date | None = None, date_to: date | None = None):
        rows = service.performance()
        for key, value in (("cache_mode", cache_mode), ("replay_mode", replay_mode), ("performance_status", performance_status)):
            if value is not None:
                rows = [row for row in rows if str(row.get(key, "")).lower() == value.lower()]
        if date_from:
            rows = [row for row in rows if str(row.get("as_of", ""))[:10] >= date_from.isoformat()]
        if date_to:
            rows = [row for row in rows if str(row.get("as_of", ""))[:10] <= date_to.isoformat()]
        page, page_meta = _list_page(rows, limit=limit, cursor=cursor, sort=sort, order=order, allowed_sort={"as_of", "run_id", "performance_status"}, filters={}, cursor_filters={"cache_mode": cache_mode, "replay_mode": replay_mode, "performance_status": performance_status, "date_from": date_from, "date_to": date_to}, max_limit=settings.max_page_size)
        return _response(request, page, meta=_meta(request, partial=not rows, limitations=[*LIMITATIONS, *(["SOURCE_UNAVAILABLE"] if not rows else [])], pagination=page_meta))

    @app.get("/api/v1/performance/runs/{run_id}", response_model=ApiResponse[PerformanceSummaryResponse], tags=["performance"])
    def performance_run(run_id: str, request: Request):
        row = _find(service.performance(), "run_id", run_id)
        return _response(request, row, meta=_meta(request, limitations=list(LIMITATIONS)), etag_seed=row)

    @app.get("/api/v1/performance/baselines", response_model=ApiResponse[list[PerformanceSummaryResponse]], tags=["performance"])
    def performance_baselines(request: Request):
        rows = service.performance_baselines()
        return _response(request, rows, meta=_meta(request, partial=not rows, limitations=[*LIMITATIONS, *([] if rows else ["COPIED_REALISTIC_BASELINE_MISSING"])]))

    def phase4_openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )
        schemes = schema.setdefault("components", {}).setdefault("securitySchemes", {})
        schemes["BearerAuth"] = {"type": "http", "scheme": "bearer"}
        schemes["ApiKeyAuth"] = {"type": "apiKey", "in": "header", "name": "X-API-Key"}
        for path, methods in schema.get("paths", {}).items():
            if path not in PUBLIC_PATHS:
                for operation in methods.values():
                    if isinstance(operation, dict):
                        operation["security"] = [{"BearerAuth": []}, {"ApiKeyAuth": []}]
        app.openapi_schema = schema
        return schema

    app.openapi = phase4_openapi  # type: ignore[method-assign]

    return app


app = create_app()
