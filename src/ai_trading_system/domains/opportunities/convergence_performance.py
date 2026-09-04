"""Shadow-only maturation for unified Investigator/fundamental/pattern evidence."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb

from ai_trading_system.domains.opportunities.orchestration.contracts import (
    INVESTIGATOR_SECTOR_INDEX_ALIASES,
)
from ai_trading_system.domains.opportunities.performance_evaluation import (
    BENCHMARK_SYMBOL,
    SHADOW_FILL_POLICY_VERSION,
    SHADOW_FILL_SLIPPAGE_BPS,
)
from ai_trading_system.pipeline.registry import RegistryStore


CONVERGENCE_PERFORMANCE_POLICY_VERSION = "opportunity-convergence-performance-v1"
CONVERGENCE_PERFORMANCE_HORIZONS = (3, 5, 10, 20)
CONVERGENCE_WINDOW_SESSIONS = 10
CONVERGENCE_ANCHOR_TYPES = (
    "DISCOVERY_CLOSE",
    "DISCOVERY_NEXT_OPEN_FILL",
    "CONFIRMATION_CLOSE",
    "EXECUTABLE_SHADOW_FILL",
)
TERMINAL_HORIZON_STATES = {"MATURED", "PARTIAL_MATURED"}


class ConvergencePerformanceConflictError(RuntimeError):
    """Raised when an immutable observation or anchor changes in place."""


def evaluate_convergence_performance(
    registry: RegistryStore,
    *,
    convergence_rows: Iterable[Mapping[str, Any]],
    run_id: str,
    stage_attempt: int,
    observed_at: datetime,
    ohlcv_db_path: str | Path | None,
    persist: bool,
) -> dict[str, list[dict[str, Any]]]:
    """Persist and mature convergence evidence without creating trading authority."""

    source_rows = [dict(row) for row in convergence_rows]
    if persist and any(
        not str(row.get("policy_snapshot_id") or "").strip() for row in source_rows
    ):
        raise ValueError("persistent convergence performance requires policy snapshots")
    current = [
        _observation(row, run_id, stage_attempt, observed_at) for row in source_rows
    ]
    path = Path(ohlcv_db_path) if ohlcv_db_path is not None else None
    if persist:
        with registry._writer() as conn:  # noqa: SLF001
            _append_observations(conn, current)
            observations = _records(
                conn.execute(
                    """
                    SELECT * FROM opportunity_convergence_observation
                    ORDER BY session_date, exchange, symbol_id, observation_id
                    """
                )
            )
            existing_anchor_ids = {
                str(row[0])
                for row in conn.execute(
                    "SELECT anchor_id FROM opportunity_convergence_anchor"
                ).fetchall()
            }
            prices, indices, sector_map = _load_market_data(path, observations)
            anchors = [
                row
                for row in _anchor_candidates(
                    observations,
                    prices=prices,
                    performance_events=_performance_events(conn),
                    run_id=run_id,
                )
                if row["anchor_id"] not in existing_anchor_ids
            ]
            _append_anchors(conn, anchors)
            stored_anchors = _records(
                conn.execute(
                    """
                    SELECT * FROM opportunity_convergence_anchor
                    ORDER BY anchor_session_date, observation_id, anchor_type, anchor_id
                    """
                )
            )
            updates = [
                _mature_horizon(
                    anchor,
                    horizon,
                    observations={row["observation_id"]: row for row in observations},
                    prices=prices,
                    indices=indices,
                    sector_map=sector_map,
                )
                for anchor in stored_anchors
                for horizon in CONVERGENCE_PERFORMANCE_HORIZONS
            ]
            _upsert_horizons(conn, updates)
            return _build_outputs(
                conn,
                current_observation_ids={row["observation_id"] for row in current},
            )

    observations = current
    prices, indices, sector_map = _load_market_data(path, observations)
    with registry._reader() as conn:  # noqa: SLF001
        performance_events = _performance_events(conn)
    anchors = _anchor_candidates(
        observations,
        prices=prices,
        performance_events=performance_events,
        run_id=run_id,
    )
    horizons = [
        _mature_horizon(
            anchor,
            horizon,
            observations={row["observation_id"]: row for row in observations},
            prices=prices,
            indices=indices,
            sector_map=sector_map,
        )
        for anchor in anchors
        for horizon in CONVERGENCE_PERFORMANCE_HORIZONS
    ]
    return _outputs_from_rows(
        observations,
        anchors,
        horizons,
        current_observation_ids={row["observation_id"] for row in current},
        persistence_state="PREVIEW",
    )


def _observation(
    row: Mapping[str, Any],
    run_id: str,
    stage_attempt: int,
    observed_at: datetime,
) -> dict[str, Any]:
    exchange = str(row.get("exchange") or "NSE").strip().upper()
    symbol = str(row.get("symbol_id") or "").strip().upper()
    session = _date(row.get("session_date"))
    policy_snapshot_id = str(
        row.get("policy_snapshot_id") or "UNREGISTERED_POLICY_SNAPSHOT"
    ).strip()
    if not symbol:
        raise ValueError("convergence performance requires symbol")
    identity = {
        "exchange": exchange,
        "symbol_id": symbol,
        "session_date": session.isoformat(),
        "policy_snapshot_id": policy_snapshot_id,
    }
    observation_id = f"convergence-observation-{_digest(identity)}"
    snapshot = dict(row)
    semantic = {
        **identity,
        "convergence_policy_version": row.get("convergence_policy_version"),
        "convergence_cohort": row.get("convergence_cohort"),
        "investigator_member": _bool(row.get("investigator_member")),
        "fundamental_member": _bool(row.get("fundamental_member")),
        "pattern_member": _bool(row.get("pattern_member")),
        "sector_name": row.get("sector_name"),
        "sector_evaluation_state": row.get("sector_evaluation_state"),
        "invalidation_price": _float(row.get("investigator_invalidation_price")),
        "evidence_hash": row.get("evidence_hash"),
    }
    return {
        "observation_id": observation_id,
        **identity,
        "observed_at": _db_time(observed_at),
        "convergence_policy_version": str(
            row.get("convergence_policy_version") or "UNKNOWN"
        ),
        "convergence_cohort": str(row.get("convergence_cohort") or "NONE"),
        "investigator_member": semantic["investigator_member"],
        "fundamental_member": semantic["fundamental_member"],
        "pattern_member": semantic["pattern_member"],
        "sector_name": _text(row.get("sector_name")),
        "sector_evaluation_state": str(
            row.get("sector_evaluation_state") or "NOT_EVALUATED"
        ),
        "invalidation_price": semantic["invalidation_price"],
        "evidence_snapshot_json": _canonical_json(snapshot),
        "evidence_hash": str(row.get("evidence_hash") or _digest(snapshot)),
        "source_run_id": run_id,
        "source_stage_attempt": int(stage_attempt),
        "semantic_payload_hash": _digest(semantic),
        "idempotency_key": f"convergence-observation-{_digest(identity)}",
    }


def _append_observations(
    conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]
) -> None:
    columns = tuple(rows[0]) if rows else ()
    for row in rows:
        existing = conn.execute(
            """
            SELECT observation_id, semantic_payload_hash
            FROM opportunity_convergence_observation
            WHERE exchange = ? AND symbol_id = ? AND session_date = ?
              AND policy_snapshot_id = ?
            """,
            [
                row["exchange"],
                row["symbol_id"],
                row["session_date"],
                row["policy_snapshot_id"],
            ],
        ).fetchone()
        if existing:
            if str(existing[1]) != row["semantic_payload_hash"]:
                raise ConvergencePerformanceConflictError(
                    "immutable convergence observation changed for "
                    f"{row['exchange']}:{row['symbol_id']}:{row['session_date']}"
                )
            continue
        placeholders = ", ".join("?" for _ in columns)
        conn.execute(
            f"""
            INSERT INTO opportunity_convergence_observation ({", ".join(columns)})
            VALUES ({placeholders})
            """,
            [row[column] for column in columns],
        )


def _anchor_candidates(
    observations: list[dict[str, Any]],
    *,
    prices: dict[tuple[str, str], list[dict[str, Any]]],
    performance_events: list[dict[str, Any]],
    run_id: str,
) -> list[dict[str, Any]]:
    origins: dict[tuple[str, str, date], set[str]] = defaultdict(set)
    later_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in performance_events:
        candidate_id = str(event.get("candidate_id") or "")
        if event.get("event_type") == "CANDIDATE_DISCOVERED":
            origins[
                (
                    str(event.get("exchange") or "").upper(),
                    str(event.get("symbol_id") or "").upper(),
                    _date(event.get("session_date")),
                )
            ].add(candidate_id)
        elif event.get("event_type") in {"ENTRY_CONFIRMED", "EXECUTABLE_AVAILABLE"}:
            later_events[candidate_id].append(event)

    anchors: list[dict[str, Any]] = []
    for observation in observations:
        if observation.get("convergence_cohort") == "NONE":
            continue
        key = (str(observation["exchange"]), str(observation["symbol_id"]))
        series = prices.get(key, [])
        session = _date(observation["session_date"])
        index = _series_index(series, session)
        bar = series[index] if index is not None else None
        close = _float(bar.get("close")) if bar else None
        anchors.append(
            _anchor(
                observation,
                anchor_type="DISCOVERY_CLOSE",
                session=session,
                price=close,
                basis="DECISION_SESSION_CLOSE",
                source_event_id=None,
                candidate_id=None,
                fill_policy_version=None,
                source_run_id=run_id,
                source_payload=bar or {"missing_session": session.isoformat()},
            )
        )
        if index is not None and index + 1 < len(series):
            next_bar = series[index + 1]
            next_open = _float(next_bar.get("open"))
            if next_open not in {None, 0.0}:
                anchors.append(
                    _anchor(
                        observation,
                        anchor_type="DISCOVERY_NEXT_OPEN_FILL",
                        session=_date(next_bar["session_date"]),
                        price=round(
                            float(next_open)
                            * (1.0 + SHADOW_FILL_SLIPPAGE_BPS / 10_000.0),
                            6,
                        ),
                        basis="NEXT_SESSION_OPEN_PLUS_SLIPPAGE",
                        source_event_id=None,
                        candidate_id=None,
                        fill_policy_version=SHADOW_FILL_POLICY_VERSION,
                        source_run_id=run_id,
                        source_payload=next_bar,
                    )
                )
        origin_key = (key[0], key[1], session)
        for candidate_id in sorted(origins.get(origin_key, ())):
            for event in sorted(
                later_events.get(candidate_id, ()),
                key=lambda item: (
                    _date(item.get("session_date")),
                    str(item.get("event_type")),
                    str(item.get("event_id")),
                ),
            ):
                event_type = str(event.get("event_type"))
                anchors.append(
                    _anchor(
                        observation,
                        anchor_type=(
                            "CONFIRMATION_CLOSE"
                            if event_type == "ENTRY_CONFIRMED"
                            else "EXECUTABLE_SHADOW_FILL"
                        ),
                        session=_date(event.get("session_date")),
                        price=_float(event.get("anchor_price")),
                        basis=str(event.get("anchor_price_basis") or "UNKNOWN"),
                        source_event_id=str(event.get("event_id") or ""),
                        candidate_id=candidate_id,
                        fill_policy_version=_text(event.get("fill_policy_version")),
                        source_run_id=str(event.get("source_run_id") or run_id),
                        source_payload={
                            "event_id": event.get("event_id"),
                            "semantic_payload_hash": event.get("semantic_payload_hash"),
                            "anchor_price": event.get("anchor_price"),
                            "anchor_price_basis": event.get("anchor_price_basis"),
                        },
                    )
                )
    return anchors


def _anchor(
    observation: Mapping[str, Any],
    *,
    anchor_type: str,
    session: date,
    price: float | None,
    basis: str,
    source_event_id: str | None,
    candidate_id: str | None,
    fill_policy_version: str | None,
    source_run_id: str,
    source_payload: Any,
) -> dict[str, Any]:
    identity = {
        "observation_id": observation["observation_id"],
        "anchor_type": anchor_type,
        "source_event_id": source_event_id,
    }
    semantic = {
        **identity,
        "candidate_id": candidate_id,
        "anchor_session_date": session.isoformat(),
        "anchor_price": price,
        "anchor_price_basis": basis,
        "invalidation_price": observation.get("invalidation_price"),
        "fill_policy_version": fill_policy_version,
        "source_data_hash": _digest(source_payload),
    }
    digest = _digest(identity)
    return {
        "anchor_id": f"convergence-anchor-{digest}",
        "observation_id": observation["observation_id"],
        "anchor_type": anchor_type,
        "candidate_id": candidate_id,
        "source_event_id": source_event_id,
        "anchor_session_date": session,
        "anchor_price": price,
        "anchor_price_basis": basis,
        "invalidation_price": observation.get("invalidation_price"),
        "fill_policy_version": fill_policy_version,
        "source_run_id": source_run_id,
        "source_data_hash": semantic["source_data_hash"],
        "semantic_payload_hash": _digest(semantic),
        "idempotency_key": f"convergence-anchor-{digest}",
    }


def _append_anchors(
    conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]
) -> None:
    columns = tuple(rows[0]) if rows else ()
    for row in rows:
        existing = conn.execute(
            """
            SELECT semantic_payload_hash
            FROM opportunity_convergence_anchor
            WHERE anchor_id = ?
            """,
            [row["anchor_id"]],
        ).fetchone()
        if existing:
            if str(existing[0]) != row["semantic_payload_hash"]:
                raise ConvergencePerformanceConflictError(
                    f"immutable convergence anchor changed: {row['anchor_id']}"
                )
            continue
        placeholders = ", ".join("?" for _ in columns)
        conn.execute(
            f"""
            INSERT INTO opportunity_convergence_anchor ({", ".join(columns)})
            VALUES ({placeholders})
            """,
            [row[column] for column in columns],
        )


def _mature_horizon(
    anchor: Mapping[str, Any],
    horizon: int,
    *,
    observations: Mapping[str, Mapping[str, Any]],
    prices: Mapping[tuple[str, str], list[dict[str, Any]]],
    indices: Mapping[str, list[dict[str, Any]]],
    sector_map: Mapping[str, str],
) -> dict[str, Any]:
    observation = observations[str(anchor["observation_id"])]
    series = prices.get(
        (str(observation["exchange"]), str(observation["symbol_id"])), []
    )
    anchor_session = _date(anchor["anchor_session_date"])
    anchor_index = _series_index(series, anchor_session)
    anchor_price = _float(anchor.get("anchor_price"))
    sector_code = sector_map.get(_normalize_sector(observation.get("sector_name")))
    base = {
        "anchor_id": anchor["anchor_id"],
        "horizon_sessions": horizon,
        "observed_sessions": 0,
        "target_session_date": None,
        "partial_return_pct": None,
        "return_pct": None,
        "maximum_favourable_excursion_pct": None,
        "maximum_adverse_excursion_pct": None,
        "days_to_2pct": None,
        "days_to_5pct": None,
        "days_to_stop": None,
        "benchmark_symbol": BENCHMARK_SYMBOL,
        "benchmark_return_pct": None,
        "benchmark_relative_return_pct": None,
        "sector_index_code": sector_code,
        "sector_return_pct": None,
        "sector_relative_return_pct": None,
        "data_quality_status": "PENDING",
        "data_quality_reason": None,
        "outcome_source_hash": None,
        "matured_at": None,
        "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }
    if anchor_index is None or anchor_price in {None, 0.0}:
        return {
            **base,
            "data_quality_status": "INSUFFICIENT_PRICE_DATA",
            "data_quality_reason": "anchor_price_or_session_missing",
        }
    available = series[anchor_index + 1 : anchor_index + horizon + 1]
    observed_sessions = len(available)
    partial_close = _float(available[-1].get("close")) if available else None
    partial_return = _return_pct(partial_close, anchor_price)
    if observed_sessions < horizon:
        return {
            **base,
            "observed_sessions": observed_sessions,
            "partial_return_pct": partial_return,
            "data_quality_reason": f"awaiting_{horizon - observed_sessions}_sessions",
            "outcome_source_hash": _digest(available) if available else None,
        }
    target = available[-1]
    target_close = _float(target.get("close"))
    if target_close is None:
        return {
            **base,
            "observed_sessions": observed_sessions,
            "data_quality_status": "INSUFFICIENT_PRICE_DATA",
            "data_quality_reason": "target_close_missing",
            "outcome_source_hash": _digest(available),
        }
    highs = [_float(row.get("high")) for row in available]
    lows = [_float(row.get("low")) for row in available]
    valid_highs = [value for value in highs if value is not None]
    valid_lows = [value for value in lows if value is not None]
    target_session = _date(target["session_date"])
    return_pct = _return_pct(target_close, anchor_price)
    use_open = "OPEN" in str(anchor.get("anchor_price_basis") or "").upper()
    benchmark_return = _index_return(
        indices.get(BENCHMARK_SYMBOL, []),
        anchor_session,
        target_session,
        use_open=use_open,
    )
    sector_return = (
        _index_return(
            indices.get(str(sector_code), []),
            anchor_session,
            target_session,
            use_open=use_open,
        )
        if sector_code
        else None
    )
    reasons: list[str] = []
    if benchmark_return is None:
        reasons.append("benchmark_history_missing")
    if not sector_code:
        reasons.append("sector_index_mapping_missing")
    elif sector_return is None:
        reasons.append("sector_index_history_missing")
    return {
        **base,
        "observed_sessions": observed_sessions,
        "target_session_date": target_session,
        "partial_return_pct": return_pct,
        "return_pct": return_pct,
        "maximum_favourable_excursion_pct": (
            _return_pct(max(valid_highs), anchor_price) if valid_highs else None
        ),
        "maximum_adverse_excursion_pct": (
            _return_pct(min(valid_lows), anchor_price) if valid_lows else None
        ),
        "days_to_2pct": _first_touch(highs, anchor_price, 2.0),
        "days_to_5pct": _first_touch(highs, anchor_price, 5.0),
        "days_to_stop": _first_stop_touch(
            lows, _float(anchor.get("invalidation_price"))
        ),
        "benchmark_return_pct": benchmark_return,
        "benchmark_relative_return_pct": _difference(return_pct, benchmark_return),
        "sector_return_pct": sector_return,
        "sector_relative_return_pct": _difference(return_pct, sector_return),
        "data_quality_status": "PARTIAL_MATURED" if reasons else "MATURED",
        "data_quality_reason": ";".join(reasons) or None,
        "outcome_source_hash": _digest(
            {
                "stock": available,
                "benchmark": benchmark_return,
                "sector_code": sector_code,
                "sector_return": sector_return,
            }
        ),
        "matured_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }


def _upsert_horizons(
    conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]
) -> None:
    columns = tuple(rows[0]) if rows else ()
    for row in rows:
        existing = conn.execute(
            """
            SELECT data_quality_status
            FROM opportunity_convergence_horizon
            WHERE anchor_id = ? AND horizon_sessions = ?
            """,
            [row["anchor_id"], row["horizon_sessions"]],
        ).fetchone()
        if existing and str(existing[0]) in TERMINAL_HORIZON_STATES:
            continue
        if existing:
            conn.execute(
                """
                DELETE FROM opportunity_convergence_horizon
                WHERE anchor_id = ? AND horizon_sessions = ?
                """,
                [row["anchor_id"], row["horizon_sessions"]],
            )
        placeholders = ", ".join("?" for _ in columns)
        conn.execute(
            f"""
            INSERT INTO opportunity_convergence_horizon ({", ".join(columns)})
            VALUES ({placeholders})
            """,
            [row[column] for column in columns],
        )


def _build_outputs(
    conn: duckdb.DuckDBPyConnection,
    *,
    current_observation_ids: set[str],
) -> dict[str, list[dict[str, Any]]]:
    observations = _records(
        conn.execute(
            """
            SELECT * FROM opportunity_convergence_observation
            ORDER BY session_date, exchange, symbol_id, observation_id
            """
        )
    )
    anchors = _records(
        conn.execute(
            """
            SELECT * FROM opportunity_convergence_anchor
            ORDER BY anchor_session_date, observation_id, anchor_type, anchor_id
            """
        )
    )
    horizons = _records(
        conn.execute(
            """
            SELECT h.*, a.observation_id, a.anchor_type, a.candidate_id,
                   a.anchor_session_date, a.anchor_price, a.anchor_price_basis,
                   o.exchange, o.symbol_id, o.session_date,
                   o.convergence_cohort, o.investigator_member,
                   o.fundamental_member, o.pattern_member, o.sector_name
            FROM opportunity_convergence_horizon h
            JOIN opportunity_convergence_anchor a USING (anchor_id)
            JOIN opportunity_convergence_observation o USING (observation_id)
            ORDER BY o.session_date, a.anchor_type, h.horizon_sessions,
                     o.exchange, o.symbol_id, a.anchor_id
            """
        )
    )
    return _outputs_from_rows(
        observations,
        anchors,
        horizons,
        current_observation_ids=current_observation_ids,
        persistence_state="PERSISTED",
    )


def _outputs_from_rows(
    observations: list[dict[str, Any]],
    anchors: list[dict[str, Any]],
    horizons: list[dict[str, Any]],
    *,
    current_observation_ids: set[str],
    persistence_state: str,
) -> dict[str, list[dict[str, Any]]]:
    joined_horizons = (
        horizons
        if not horizons or "convergence_cohort" in horizons[0]
        else _join_preview_rows(observations, anchors, horizons)
    )
    primary = _primary_cohorts(joined_horizons)
    diagnostic = _diagnostic_cohorts(joined_horizons)
    research = [
        {**row, "research_reason": "INSUFFICIENT_SAMPLE"}
        for row in primary
        if int(row["sample_count"]) < 30
    ]
    windows = _calendar_windows(joined_horizons)
    missing = _missing_reasons(joined_horizons)
    readiness = _readiness_inputs(
        observations,
        anchors,
        joined_horizons,
        windows,
        current_observation_ids=current_observation_ids,
        persistence_state=persistence_state,
    )
    return {
        "opportunity_convergence_observations": observations,
        "opportunity_convergence_anchors": anchors,
        "opportunity_convergence_horizons": joined_horizons,
        "opportunity_convergence_primary_cohorts": primary,
        "opportunity_convergence_diagnostic_cohorts": diagnostic,
        "opportunity_convergence_research_cohorts": research,
        "opportunity_convergence_calendar_windows": windows,
        "opportunity_convergence_missing_data_reasons": missing,
        "opportunity_convergence_performance_readiness": readiness,
    }


def _join_preview_rows(
    observations: list[dict[str, Any]],
    anchors: list[dict[str, Any]],
    horizons: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    observation_by_id = {row["observation_id"]: row for row in observations}
    anchor_by_id = {row["anchor_id"]: row for row in anchors}
    joined = []
    for horizon in horizons:
        anchor = anchor_by_id[horizon["anchor_id"]]
        observation = observation_by_id[anchor["observation_id"]]
        joined.append(
            {
                **horizon,
                "observation_id": observation["observation_id"],
                "anchor_type": anchor["anchor_type"],
                "candidate_id": anchor.get("candidate_id"),
                "anchor_session_date": anchor["anchor_session_date"],
                "anchor_price": anchor.get("anchor_price"),
                "anchor_price_basis": anchor["anchor_price_basis"],
                "exchange": observation["exchange"],
                "symbol_id": observation["symbol_id"],
                "session_date": observation["session_date"],
                "convergence_cohort": observation["convergence_cohort"],
                "investigator_member": observation["investigator_member"],
                "fundamental_member": observation["fundamental_member"],
                "pattern_member": observation["pattern_member"],
                "sector_name": observation.get("sector_name"),
            }
        )
    return joined


def _primary_cohorts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _aggregate(
        [
            row
            for row in rows
            if row.get("convergence_cohort") != "NONE"
            and row.get("return_pct") is not None
        ],
        dimensions=("convergence_cohort", "anchor_type", "horizon_sessions"),
        cohort_scope="PRIMARY_MUTUALLY_EXCLUSIVE",
    )


def _diagnostic_cohorts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for row in rows:
        if row.get("return_pct") is None:
            continue
        for lane, member_field in (
            ("INVESTIGATOR_ANY", "investigator_member"),
            ("FUNDAMENTAL_ANY", "fundamental_member"),
            ("PATTERN_ANY", "pattern_member"),
        ):
            if _bool(row.get(member_field)):
                expanded.append({**row, "diagnostic_lane": lane})
    return _aggregate(
        expanded,
        dimensions=("diagnostic_lane", "anchor_type", "horizon_sessions"),
        cohort_scope="OVERLAPPING_DIAGNOSTIC",
    )


def _aggregate(
    rows: list[dict[str, Any]],
    *,
    dimensions: tuple[str, ...],
    cohort_scope: str,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row.get(field) for field in dimensions)].append(row)
    output: list[dict[str, Any]] = []
    for key, group in sorted(grouped.items(), key=lambda item: str(item[0])):
        returns = [float(row["return_pct"]) for row in group]
        winners = [value for value in returns if value > 0]
        losers = [value for value in returns if value <= 0]
        avg_win = sum(winners) / len(winners) if winners else None
        avg_loss = sum(losers) / len(losers) if losers else None
        benchmark = [
            float(row["benchmark_relative_return_pct"])
            for row in group
            if row.get("benchmark_relative_return_pct") is not None
        ]
        sector = [
            float(row["sector_relative_return_pct"])
            for row in group
            if row.get("sector_relative_return_pct") is not None
        ]
        sample_count = len(group)
        output.append(
            {
                "cohort_scope": cohort_scope,
                **dict(zip(dimensions, key, strict=True)),
                "sample_count": sample_count,
                "unique_symbol_count": len(
                    {f"{row.get('exchange')}:{row.get('symbol_id')}" for row in group}
                ),
                "unique_observation_count": len(
                    {str(row.get("observation_id")) for row in group}
                ),
                "unique_episode_count": len(
                    {
                        str(row.get("candidate_id") or row.get("observation_id"))
                        for row in group
                    }
                ),
                "overlapping_episode_count": sample_count
                - len(
                    {f"{row.get('exchange')}:{row.get('symbol_id')}" for row in group}
                ),
                "avg_return_pct": _round(sum(returns) / sample_count),
                "median_return_pct": _round(_median(returns)),
                "win_rate_pct": _round(100.0 * len(winners) / sample_count),
                "avg_mfe_pct": _average(group, "maximum_favourable_excursion_pct"),
                "avg_mae_pct": _average(group, "maximum_adverse_excursion_pct"),
                "avg_benchmark_relative_return_pct": _round(
                    sum(benchmark) / len(benchmark)
                )
                if benchmark
                else None,
                "avg_sector_relative_return_pct": _round(sum(sector) / len(sector))
                if sector
                else None,
                "payoff_ratio": (
                    _round(avg_win / abs(avg_loss))
                    if avg_win is not None and avg_loss not in {None, 0.0}
                    else None
                ),
                "expectancy_pct": _round(sum(returns) / sample_count),
                "sample_confidence": _confidence_label(sample_count),
            }
        )
    return output


def _calendar_windows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible = [
        row
        for row in rows
        if row.get("convergence_cohort") != "NONE" and row.get("return_pct") is not None
    ]
    sessions = sorted({_date(row["session_date"]) for row in eligible})
    window_by_session = {
        session: index // CONVERGENCE_WINDOW_SESSIONS
        for index, session in enumerate(sessions)
    }
    grouped: dict[tuple[str, str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in eligible:
        scopes = ["ALL_ACTIVE"]
        if _bool(row.get("investigator_member")):
            scopes.append("INVESTIGATOR_ANY")
        for scope in scopes:
            grouped[
                (
                    scope,
                    str(row["anchor_type"]),
                    int(row["horizon_sessions"]),
                    window_by_session[_date(row["session_date"])],
                )
            ].append(row)
    output = []
    for (window_scope, anchor_type, horizon, window_index), group in sorted(
        grouped.items()
    ):
        returns = [float(row["return_pct"]) for row in group]
        relative = [
            float(row["benchmark_relative_return_pct"])
            for row in group
            if row.get("benchmark_relative_return_pct") is not None
        ]
        window_sessions = sorted(_date(row["session_date"]) for row in group)
        avg_return = sum(returns) / len(returns)
        avg_relative = sum(relative) / len(relative) if relative else None
        output.append(
            {
                "window_scope": window_scope,
                "anchor_type": anchor_type,
                "horizon_sessions": horizon,
                "window_index": window_index,
                "window_start": window_sessions[0],
                "window_end": window_sessions[-1],
                "sample_count": len(group),
                "investigator_sample_count": sum(
                    _bool(row.get("investigator_member")) for row in group
                ),
                "avg_return_pct": _round(avg_return),
                "avg_benchmark_relative_return_pct": _round(avg_relative),
                "positive_expectancy": avg_return > 0,
                "window_stable": bool(
                    avg_return > 0 and avg_relative is not None and avg_relative > 0
                ),
                "sample_confidence": _confidence_label(len(group)),
            }
        )
    return output


def _missing_reasons(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[tuple[str, int, str, str], int] = defaultdict(int)
    for row in rows:
        reason = str(row.get("data_quality_reason") or "").strip()
        if not reason:
            continue
        for code in reason.split(";"):
            counts[
                (
                    str(row.get("anchor_type")),
                    int(row.get("horizon_sessions") or 0),
                    str(row.get("data_quality_status")),
                    code,
                )
            ] += 1
    return [
        {
            "anchor_type": key[0],
            "horizon_sessions": key[1],
            "data_quality_status": key[2],
            "reason_code": key[3],
            "sample_count": count,
        }
        for key, count in sorted(counts.items())
    ]


def _readiness_inputs(
    observations: list[dict[str, Any]],
    anchors: list[dict[str, Any]],
    horizons: list[dict[str, Any]],
    windows: list[dict[str, Any]],
    *,
    current_observation_ids: set[str],
    persistence_state: str,
) -> list[dict[str, Any]]:
    observation_ids = {str(row["observation_id"]) for row in observations}
    current_present = len(current_observation_ids & observation_ids)
    active = [row for row in observations if row.get("convergence_cohort") != "NONE"]
    active_ids = {str(row["observation_id"]) for row in active}
    discovery_ids = {
        str(row["observation_id"])
        for row in anchors
        if row.get("anchor_type") == "DISCOVERY_CLOSE"
        and row.get("anchor_price") not in {None, 0.0}
    }
    expected_horizons = len(anchors) * len(CONVERGENCE_PERFORMANCE_HORIZONS)
    horizon_keys = {
        (str(row["anchor_id"]), int(row["horizon_sessions"])) for row in horizons
    }
    discovery_20 = [
        row
        for row in horizons
        if row.get("anchor_type") == "DISCOVERY_CLOSE"
        and int(row.get("horizon_sessions") or 0) == 20
        and str(row.get("observation_id")) in active_ids
    ]
    eligible_20 = [
        row for row in discovery_20 if int(row.get("observed_sessions") or 0) >= 20
    ]
    matured_20 = [row for row in eligible_20 if row.get("return_pct") is not None]
    discovery_sessions = len({_date(row["session_date"]) for row in active})
    stable_windows = [
        row
        for row in windows
        if row.get("anchor_type") == "DISCOVERY_NEXT_OPEN_FILL"
        and row.get("window_scope") == "INVESTIGATOR_ANY"
        and int(row.get("horizon_sessions") or 0) == 20
        and bool(row.get("window_stable"))
    ]
    checks = (
        (
            "OPPORTUNITY_CONVERGENCE_PERFORMANCE_OBSERVATION_RECONCILIATION",
            current_present,
            len(current_observation_ids),
            "PASS" if current_present == len(current_observation_ids) else "FAIL",
            f"current attempt convergence rows are {persistence_state.lower()}",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_DISCOVERY_ANCHOR_COVERAGE",
            len(active_ids & discovery_ids),
            len(active_ids),
            "PASS" if active_ids <= discovery_ids else "FAIL",
            "every active convergence observation has an immutable discovery close",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_EXPLICIT_HORIZONS",
            len(horizon_keys),
            expected_horizons,
            "PASS" if len(horizon_keys) == expected_horizons else "FAIL",
            "every available anchor has explicit 3/5/10/20-session state",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_ELIGIBLE_20D_MATURATION",
            len(matured_20),
            len(eligible_20),
            "PASS" if len(matured_20) == len(eligible_20) else "FAIL",
            "only observations with 20 available future sessions enter this denominator",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_DISCOVERY_SESSION_SAMPLE",
            discovery_sessions,
            30,
            "PASS" if discovery_sessions >= 30 else "PENDING",
            "collect at least 30 distinct active discovery sessions",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_PRIMARY_MATURED_20D_SAMPLE",
            len(matured_20),
            120,
            "PASS" if len(matured_20) >= 120 else "PENDING",
            "policy-eligible confidence requires 120 matured active observations",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_POSITIVE_NON_OVERLAP_WINDOWS",
            len(stable_windows),
            3,
            "PASS" if len(stable_windows) >= 3 else "PENDING",
            "requires positive slippage-adjusted and benchmark-relative 20D results in three windows containing weekly-momentum evidence",
        ),
    )
    policy_snapshot_ids = sorted(
        {str(row.get("policy_snapshot_id")) for row in observations}
    )
    return [
        {
            "check_id": check_id,
            "category": "opportunity_convergence_performance",
            "status": status,
            "observed": observed,
            "expected": expected,
            "production_blocking": True,
            "policy_version": CONVERGENCE_PERFORMANCE_POLICY_VERSION,
            "policy_snapshot_ids_json": json.dumps(policy_snapshot_ids),
            "details": details,
        }
        for check_id, observed, expected, status, details in checks
    ]


def _load_market_data(
    path: Path | None,
    observations: list[dict[str, Any]],
) -> tuple[
    dict[tuple[str, str], list[dict[str, Any]]],
    dict[str, list[dict[str, Any]]],
    dict[str, str],
]:
    active_observations = [
        row for row in observations if row.get("convergence_cohort") != "NONE"
    ]
    if path is None or not path.is_file() or not active_observations:
        return {}, {}, {}
    symbols = sorted({str(row["symbol_id"]).upper() for row in active_observations})
    exchanges = sorted({str(row["exchange"]).upper() for row in active_observations})
    min_date = min(_date(row["session_date"]) for row in active_observations)
    with duckdb.connect(str(path), read_only=True) as conn:
        tables = {
            str(row[0])
            for row in conn.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        if "_catalog" not in tables:
            return {}, {}, {}
        catalog_columns = {
            str(row[0])
            for row in conn.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '_catalog'
                """
            ).fetchall()
        }
        required_catalog_columns = {
            "exchange",
            "symbol_id",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
        }
        if not required_catalog_columns <= catalog_columns:
            return {}, {}, {}
        symbol_marks = ", ".join("?" for _ in symbols)
        exchange_marks = ", ".join("?" for _ in exchanges)
        benchmark_filter = (
            "AND COALESCE(is_benchmark, FALSE) = FALSE"
            if "is_benchmark" in catalog_columns
            else ""
        )
        stock_rows = _records(
            conn.execute(
                f"""
                SELECT UPPER(exchange) AS exchange, UPPER(symbol_id) AS symbol_id,
                       CAST(timestamp AS DATE) AS session_date,
                       open, high, low, close
                FROM _catalog
                WHERE UPPER(symbol_id) IN ({symbol_marks})
                  AND UPPER(exchange) IN ({exchange_marks})
                  AND CAST(timestamp AS DATE) >= ?
                  {benchmark_filter}
                ORDER BY exchange, symbol_id, session_date
                """,
                [*symbols, *exchanges, min_date],
            )
        )
        sector_rows = (
            conn.execute(
                """
                SELECT system_sector, index_code FROM sector_to_index
                WHERE COALESCE(is_primary, FALSE) = TRUE
                ORDER BY system_sector, index_code
                """
            ).fetchall()
            if "sector_to_index" in tables
            else []
        )
        sector_map = {
            _normalize_sector(sector): str(index_code)
            for sector, index_code in sector_rows
            if sector and index_code
        }
        for source, target in INVESTIGATOR_SECTOR_INDEX_ALIASES.items():
            code = sector_map.get(_normalize_sector(target))
            if code:
                sector_map[_normalize_sector(source)] = code
        index_codes = sorted({BENCHMARK_SYMBOL, *sector_map.values()})
        index_marks = ", ".join("?" for _ in index_codes)
        index_rows = (
            _records(
                conn.execute(
                    f"""
                    SELECT index_code, date AS session_date, open, high, low, close
                    FROM _index_catalog
                    WHERE index_code IN ({index_marks}) AND date >= ?
                    ORDER BY index_code, session_date
                    """,
                    [*index_codes, min_date],
                )
            )
            if "_index_catalog" in tables
            else []
        )
    prices: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in stock_rows:
        prices[(str(row["exchange"]), str(row["symbol_id"]))].append(row)
    indices: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in index_rows:
        indices[str(row["index_code"])].append(row)
    return dict(prices), dict(indices), sector_map


def _performance_events(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    return _records(
        conn.execute(
            """
            SELECT * FROM investigator_performance_event
            WHERE event_type IN (
                'CANDIDATE_DISCOVERED', 'ENTRY_CONFIRMED', 'EXECUTABLE_AVAILABLE'
            )
            ORDER BY session_date, exchange, symbol_id, event_type, event_id
            """
        )
    )


def _index_return(
    rows: list[dict[str, Any]], start: date, end: date, *, use_open: bool
) -> float | None:
    by_date = {_date(row["session_date"]): row for row in rows}
    start_row = by_date.get(start)
    end_row = by_date.get(end)
    if not start_row or not end_row:
        return None
    start_value = _float(start_row.get("open" if use_open else "close"))
    return _return_pct(_float(end_row.get("close")), start_value)


def _first_touch(
    values: list[float | None], anchor: float, threshold_pct: float
) -> int | None:
    target = anchor * (1.0 + threshold_pct / 100.0)
    return next(
        (
            index
            for index, value in enumerate(values, start=1)
            if value is not None and value >= target
        ),
        None,
    )


def _first_stop_touch(
    values: list[float | None], stop_price: float | None
) -> int | None:
    if stop_price is None:
        return None
    return next(
        (
            index
            for index, value in enumerate(values, start=1)
            if value is not None and value <= stop_price
        ),
        None,
    )


def _series_index(rows: list[dict[str, Any]], target: date) -> int | None:
    return next(
        (
            index
            for index, row in enumerate(rows)
            if _date(row["session_date"]) == target
        ),
        None,
    )


def _confidence_label(sample_count: int) -> str:
    if sample_count >= 120:
        return "POLICY_ELIGIBLE"
    if sample_count >= 60:
        return "MODERATE"
    if sample_count >= 30:
        return "PROVISIONAL"
    return "EXPLORATORY"


def _average(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [_float(row.get(field)) for row in rows]
    valid = [value for value in values if value is not None]
    return _round(sum(valid) / len(valid)) if valid else None


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def _return_pct(end: float | None, start: float | None) -> float | None:
    if end is None or start in {None, 0.0}:
        return None
    return _round((float(end) / float(start) - 1.0) * 100.0)


def _difference(left: float | None, right: float | None) -> float | None:
    return _round(left - right) if left is not None and right is not None else None


def _round(value: float | None) -> float | None:
    return round(float(value), 6) if value is not None else None


def _records(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _db_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if parsed != parsed else parsed


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes"}


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_sector(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
