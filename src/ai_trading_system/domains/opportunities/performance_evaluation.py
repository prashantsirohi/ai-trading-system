"""Point-in-time Investigator event maturation and reporting."""

from __future__ import annotations

from collections import defaultdict
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    INVESTIGATOR_ATTRIBUTION_POLICY_VERSION,
)


HORIZONS = (3, 5, 10, 20)
PRIMARY_ATTRIBUTION_MODES = {
    "OBSERVED_AT_DECISION",
    "RECONSTRUCTED_SAME_RUN",
}
SUSTAINED_STATES = {"confirmed", "advancing", "extended"}
FAILED_STATES = {"failed", "weakening", "exited", "archived"}
BENCHMARK_SYMBOL = "NIFTY_50"
SHADOW_FILL_POLICY_VERSION = "investigator-shadow-fill-v1"
SHADOW_FILL_SLIPPAGE_BPS = 5.0
COVERAGE_TARGETS = {
    "stage_attribution": 95.0,
    "pattern_evaluation_attempted": 95.0,
    "pattern_known_or_none": 90.0,
    "setup_quality": 90.0,
    "breakout_classification": 95.0,
    "regime_and_breadth_context": 100.0,
    "sector_context": 98.0,
    "run_artifact_lineage": 100.0,
}


def mature_performance_events(
    registry: RegistryStore,
    *,
    ohlcv_db_path: str | Path,
) -> dict[str, list[dict[str, Any]]]:
    """Mature immutable discovery/entry events without rewriting their context."""
    path = Path(ohlcv_db_path)
    if not path.exists():
        return _empty_outputs()
    with registry._writer() as conn:  # noqa: SLF001
        events = _records(
            conn.execute(
                """
                SELECT *
                FROM investigator_performance_event
                ORDER BY session_date, exchange, symbol_id, event_type, event_id
                """
            )
        )
        if not events:
            _append_coverage_receipts(conn)
            return _build_outputs(conn)
        prices, index_prices, sector_map = _load_market_data(path, events)
        _append_executable_events(conn, events, prices)
        events = _records(
            conn.execute(
                """
                SELECT *
                FROM investigator_performance_event
                ORDER BY session_date, exchange, symbol_id, event_type, event_id
                """
            )
        )
        transitions = _transition_history(conn)
        updates = [
            _mature_horizon(
                event,
                horizon,
                prices=prices,
                index_prices=index_prices,
                sector_map=sector_map,
                transitions=transitions,
            )
            for event in events
            for horizon in HORIZONS
        ]
        _upsert_horizons(conn, updates)
        _append_evaluation_transitions(conn)
        _append_coverage_receipts(conn)
        _project_discovery_events_to_legacy_cohort(conn)
        return _build_outputs(conn)


def _append_executable_events(
    conn: duckdb.DuckDBPyConnection,
    events: list[dict[str, Any]],
    prices: dict[tuple[str, str], list[dict[str, Any]]],
) -> None:
    """Append a deterministic next-session shadow entry without broker dispatch."""
    for event in events:
        if str(event.get("event_type")) != "ENTRY_CONFIRMED":
            continue
        series = prices.get(
            (
                str(event.get("exchange") or "").upper(),
                str(event.get("symbol_id") or "").upper(),
            ),
            [],
        )
        anchor_date = _date(event["session_date"])
        anchor_index = next(
            (
                index
                for index, row in enumerate(series)
                if _date(row["session_date"]) == anchor_date
            ),
            None,
        )
        if anchor_index is None or anchor_index + 1 >= len(series):
            continue
        next_bar = series[anchor_index + 1]
        next_open = _float(next_bar.get("open"))
        if next_open in (None, 0.0):
            continue
        fill_price = round(
            float(next_open) * (1.0 + SHADOW_FILL_SLIPPAGE_BPS / 10_000.0), 6
        )
        session_date = _date(next_bar["session_date"])
        event_at = datetime.combine(
            session_date, datetime.min.time(), tzinfo=timezone.utc
        )
        identity = {
            "candidate_id": event["candidate_id"],
            "event_type": "EXECUTABLE_AVAILABLE",
            "session_date": session_date.isoformat(),
            "fill_policy_version": SHADOW_FILL_POLICY_VERSION,
            "source_event_id": event["event_id"],
        }
        digest = _digest(identity)
        context_json = str(event.get("context_json") or "{}")
        semantic_hash = _digest(
            {
                **identity,
                "next_session_open": next_open,
                "simulated_fill_price": fill_price,
                "context_json": context_json,
            }
        )
        conn.execute(
            """
            INSERT INTO investigator_performance_event (
                event_id, candidate_id, setup_id, symbol_id, exchange,
                sector_name, overlap_group_id, event_type, event_at,
                session_date, anchor_price, anchor_price_basis,
                source_snapshot_id, source_transition_id, attribution_mode,
                primary_eligible, lifecycle_evaluable, context_as_of,
                context_json, source_run_id, source_artifact_hash,
                data_quality_status, data_quality_reason,
                semantic_payload_hash, idempotency_key, next_session_open,
                simulated_fill_price, invalidation_price,
                fill_policy_version, policy_version
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, 'EXECUTABLE_AVAILABLE', ?, ?, ?,
                'DETERMINISTIC_SHADOW_FILL', ?, ?, ?, ?, TRUE, ?, ?, ?, ?,
                'PENDING', NULL, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT DO NOTHING
            """,
            [
                f"event-{digest}",
                event["candidate_id"],
                event["setup_id"],
                event["symbol_id"],
                event["exchange"],
                event.get("sector_name"),
                event["overlap_group_id"],
                event_at.replace(tzinfo=None),
                session_date,
                fill_price,
                event["source_snapshot_id"],
                event.get("source_transition_id"),
                event["attribution_mode"],
                bool(event.get("primary_eligible")),
                event.get("context_as_of"),
                context_json,
                event["source_run_id"],
                event["source_artifact_hash"],
                semantic_hash,
                f"executable-{digest}",
                next_open,
                fill_price,
                event.get("invalidation_price"),
                SHADOW_FILL_POLICY_VERSION,
                INVESTIGATOR_ATTRIBUTION_POLICY_VERSION,
            ],
        )


def _load_market_data(
    path: Path,
    events: list[dict[str, Any]],
) -> tuple[
    dict[tuple[str, str], list[dict[str, Any]]],
    dict[str, list[dict[str, Any]]],
    dict[str, str],
]:
    symbols = sorted({str(row["symbol_id"]).upper() for row in events})
    exchanges = sorted({str(row["exchange"]).upper() for row in events})
    min_date = min(_date(row["session_date"]) for row in events)
    with duckdb.connect(str(path), read_only=True) as conn:
        available_tables = {
            str(row[0])
            for row in conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        symbol_placeholders = ", ".join("?" for _ in symbols)
        exchange_placeholders = ", ".join("?" for _ in exchanges)
        stock_rows = _records(
            conn.execute(
                f"""
                SELECT UPPER(exchange) AS exchange, UPPER(symbol_id) AS symbol_id,
                       CAST(timestamp AS DATE) AS session_date,
                       open, high, low, close
                FROM _catalog
                WHERE UPPER(symbol_id) IN ({symbol_placeholders})
                  AND UPPER(exchange) IN ({exchange_placeholders})
                  AND CAST(timestamp AS DATE) >= ?
                  AND COALESCE(is_benchmark, FALSE) = FALSE
                ORDER BY exchange, symbol_id, session_date
                """,
                [*symbols, *exchanges, min_date],
            )
        )
        sector_rows = (
            conn.execute(
                """
                SELECT system_sector, index_code
                FROM sector_to_index
                WHERE COALESCE(is_primary, FALSE) = TRUE
                ORDER BY system_sector, index_code
                """
            ).fetchall()
            if "sector_to_index" in available_tables
            else []
        )
        sector_map = {
            str(sector): str(index_code)
            for sector, index_code in sector_rows
            if sector and index_code
        }
        index_codes = sorted({BENCHMARK_SYMBOL, *sector_map.values()})
        index_placeholders = ", ".join("?" for _ in index_codes)
        index_rows = (
            _records(
                conn.execute(
                    f"""
                    SELECT index_code, date AS session_date, open, high, low, close
                    FROM _index_catalog
                    WHERE index_code IN ({index_placeholders})
                      AND date >= ?
                    ORDER BY index_code, session_date
                    """,
                    [*index_codes, min_date],
                )
            )
            if "_index_catalog" in available_tables
            else []
        )
    prices: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in stock_rows:
        prices[(str(row["exchange"]), str(row["symbol_id"]))].append(row)
    indices: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in index_rows:
        indices[str(row["index_code"])].append(row)
    return dict(prices), dict(indices), sector_map


def _transition_history(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, list[dict[str, Any]]]:
    rows = _records(
        conn.execute(
            """
            SELECT candidate_id, to_state, transition_reason,
                   CAST(transitioned_at AS DATE) AS session_date, transitioned_at
            FROM candidate_transition
            ORDER BY candidate_id, transitioned_at, transition_id
            """
        )
    )
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        result[str(row["candidate_id"])].append(row)
    return dict(result)


def _mature_horizon(
    event: dict[str, Any],
    horizon: int,
    *,
    prices: dict[tuple[str, str], list[dict[str, Any]]],
    index_prices: dict[str, list[dict[str, Any]]],
    sector_map: dict[str, str],
    transitions: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    event_id = str(event["event_id"])
    series = prices.get(
        (str(event["exchange"]).upper(), str(event["symbol_id"]).upper()), []
    )
    anchor_date = _date(event["session_date"])
    anchor_index = next(
        (
            index
            for index, row in enumerate(series)
            if _date(row["session_date"]) == anchor_date
        ),
        None,
    )
    anchor = _float(event.get("anchor_price"))
    base = {
        "event_id": event_id,
        "horizon_sessions": horizon,
        "target_session_date": None,
        "close_to_close_return_pct": None,
        "next_open_entry_return_pct": None,
        "maximum_favourable_excursion_pct": None,
        "maximum_adverse_excursion_pct": None,
        "days_to_2pct": None,
        "days_to_5pct": None,
        "days_to_stop": None,
        "drawdown_before_2pct_pct": None,
        "drawdown_before_5pct_pct": None,
        "benchmark_symbol": BENCHMARK_SYMBOL,
        "benchmark_return_pct": None,
        "benchmark_relative_return_pct": None,
        "sector_index_code": sector_map.get(str(event.get("sector_name") or "")),
        "sector_return_pct": None,
        "sector_relative_return_pct": None,
        "lifecycle_outcome": None,
        "data_quality_status": "PENDING",
        "data_quality_reason": None,
        "matured_at": None,
        "updated_at": datetime.now(timezone.utc),
    }
    if anchor_index is None or anchor in {None, 0.0}:
        return {
            **base,
            "data_quality_status": "INSUFFICIENT_PRICE_DATA",
            "data_quality_reason": "event_anchor_price_missing",
        }
    target_index = anchor_index + horizon
    if target_index >= len(series):
        return base
    target = series[target_index]
    target_close = _float(target.get("close"))
    if target_close is None:
        return {
            **base,
            "data_quality_status": "INSUFFICIENT_PRICE_DATA",
            "data_quality_reason": "target_close_missing",
        }
    window = series[anchor_index + 1 : target_index + 1]
    highs = [_float(row.get("high")) for row in window]
    lows = [_float(row.get("low")) for row in window]
    valid_highs = [value for value in highs if value is not None]
    valid_lows = [value for value in lows if value is not None]
    close_return = _return_pct(target_close, anchor)
    next_open = _float(window[0].get("open")) if window else None
    mfe = _return_pct(max(valid_highs), anchor) if valid_highs else None
    mae = _return_pct(min(valid_lows), anchor) if valid_lows else None
    day_2 = _first_touch(highs, anchor, 2.0)
    day_5 = _first_touch(highs, anchor, 5.0)
    day_stop = _first_stop_touch(
        lows, _float(event.get("invalidation_price"))
    )
    drawdown_2 = _drawdown_before(lows, anchor, day_2)
    drawdown_5 = _drawdown_before(lows, anchor, day_5)
    target_date = _date(target["session_date"])
    benchmark_return = _index_return(
        index_prices.get(BENCHMARK_SYMBOL, []), anchor_date, target_date
    )
    sector_code = base["sector_index_code"]
    sector_return = (
        _index_return(index_prices.get(str(sector_code), []), anchor_date, target_date)
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
    lifecycle_outcome = _lifecycle_outcome(
        event,
        horizon,
        target_date=target_date,
        target_close=target_close,
        transitions=transitions.get(str(event["candidate_id"]), []),
    )
    return {
        **base,
        "target_session_date": target_date,
        "close_to_close_return_pct": close_return,
        "next_open_entry_return_pct": _return_pct(target_close, next_open),
        "maximum_favourable_excursion_pct": mfe,
        "maximum_adverse_excursion_pct": mae,
        "days_to_2pct": day_2,
        "days_to_5pct": day_5,
        "days_to_stop": day_stop,
        "drawdown_before_2pct_pct": drawdown_2,
        "drawdown_before_5pct_pct": drawdown_5,
        "benchmark_return_pct": benchmark_return,
        "benchmark_relative_return_pct": _difference(close_return, benchmark_return),
        "sector_return_pct": sector_return,
        "sector_relative_return_pct": _difference(close_return, sector_return),
        "lifecycle_outcome": lifecycle_outcome,
        "data_quality_status": "PARTIAL_MATURED" if reasons else "MATURED",
        "data_quality_reason": ";".join(reasons) or None,
        "matured_at": datetime.now(timezone.utc),
    }


def _lifecycle_outcome(
    event: dict[str, Any],
    horizon: int,
    *,
    target_date: date,
    target_close: float,
    transitions: list[dict[str, Any]],
) -> str | None:
    if not bool(event.get("lifecycle_evaluable", True)):
        return None
    event_type = str(event["event_type"])
    states = [
        str(row.get("to_state") or "").lower()
        for row in transitions
        if _date(row["session_date"]) <= target_date
        and _date(row["session_date"]) >= _date(event["session_date"])
    ]
    reasons = [
        str(row.get("transition_reason") or "").lower()
        for row in transitions
        if _date(row["session_date"]) <= target_date
        and _date(row["session_date"]) >= _date(event["session_date"])
    ]
    if event_type == "CANDIDATE_DISCOVERED" and horizon == 3:
        if any(state == "confirmed" for state in states):
            return "CONFIRMED"
        if any(
            marker in reason
            for reason in reasons
            for marker in ("stagnation", "timeout", "expired", "no_longer_eligible")
        ):
            return "EXPIRED"
        if any(state in FAILED_STATES for state in states):
            return "FAILED"
        return "STILL_DEVELOPING"
    if event_type == "ENTRY_CONFIRMED" and horizon == 10:
        anchor = _float(event.get("anchor_price"))
        if any(state in FAILED_STATES for state in states):
            return "FAILED_AFTER_CONFIRMATION"
        latest = states[-1] if states else "confirmed"
        if latest in SUSTAINED_STATES and anchor is not None and target_close >= anchor:
            return "SUSTAINED_10D"
        return "FAILED_AFTER_CONFIRMATION"
    return None


def _upsert_horizons(
    conn: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
) -> None:
    columns = tuple(rows[0]) if rows else ()
    if not columns:
        return
    placeholders = ", ".join("?" for _ in columns)
    conn.executemany(
        """
        DELETE FROM investigator_performance_horizon
        WHERE event_id = ? AND horizon_sessions = ?
        """,
        [[row["event_id"], row["horizon_sessions"]] for row in rows],
    )
    conn.executemany(
        f"""
        INSERT INTO investigator_performance_horizon ({", ".join(columns)})
        VALUES ({placeholders})
        """,
        [[row[column] for column in columns] for row in rows],
    )


def _append_evaluation_transitions(conn: duckdb.DuckDBPyConnection) -> None:
    events = _records(
        conn.execute(
            """
            SELECT e.*, h3.lifecycle_outcome AS outcome_3d,
                   h3.target_session_date AS outcome_3d_date,
                   h10.lifecycle_outcome AS outcome_10d,
                   h10.target_session_date AS outcome_10d_date
            FROM investigator_performance_event e
            LEFT JOIN investigator_performance_horizon h3
              ON h3.event_id = e.event_id AND h3.horizon_sessions = 3
            LEFT JOIN investigator_performance_horizon h10
              ON h10.event_id = e.event_id AND h10.horizon_sessions = 10
            ORDER BY e.session_date, e.event_type, e.event_id
            """
        )
    )
    for event in events:
        event_type = str(event["event_type"])
        if event_type == "CANDIDATE_DISCOVERED":
            _insert_evaluation_transition(
                conn,
                event,
                from_state="DISCOVERED",
                to_state="PENDING_3D",
                transitioned_at=event["event_at"],
                session_date=_date(event["session_date"]),
                reason_code="DISCOVERY_REQUIRES_3D_FOLLOWTHROUGH",
            )
            outcome = str(event.get("outcome_3d") or "")
            if outcome:
                _insert_evaluation_transition(
                    conn,
                    event,
                    from_state="PENDING_3D",
                    to_state=outcome,
                    transitioned_at=event["outcome_3d_date"],
                    session_date=_date(event["outcome_3d_date"]),
                    reason_code={
                        "CONFIRMED": "CANONICAL_CONFIRMATION_OBSERVED",
                        "FAILED": "CANONICAL_FAILURE_OBSERVED",
                        "EXPIRED": "CANONICAL_EXPIRY_OBSERVED",
                        "STILL_DEVELOPING": "NO_TERMINAL_TRANSITION_AT_3D",
                    }.get(outcome, "EVALUATION_OUTCOME_OBSERVED"),
                )
        elif event_type == "ENTRY_CONFIRMED":
            _insert_evaluation_transition(
                conn,
                event,
                from_state="PENDING_3D",
                to_state="CONFIRMED",
                transitioned_at=event["event_at"],
                session_date=_date(event["session_date"]),
                reason_code="CANONICAL_CONFIRMATION_TRANSITION",
            )
            outcome = str(event.get("outcome_10d") or "")
            if outcome:
                _insert_evaluation_transition(
                    conn,
                    event,
                    from_state="EXECUTABLE",
                    to_state=outcome,
                    transitioned_at=event["outcome_10d_date"],
                    session_date=_date(event["outcome_10d_date"]),
                    reason_code=(
                        "SUSTAINED_THROUGH_10D"
                        if outcome == "SUSTAINED_10D"
                        else "FAILED_AFTER_CONFIRMATION_BY_10D"
                    ),
                )
        elif event_type == "EXECUTABLE_AVAILABLE":
            _insert_evaluation_transition(
                conn,
                event,
                from_state="CONFIRMED",
                to_state="EXECUTABLE",
                transitioned_at=event["event_at"],
                session_date=_date(event["session_date"]),
                reason_code="NEXT_SESSION_SHADOW_FILL_AVAILABLE",
            )


def _insert_evaluation_transition(
    conn: duckdb.DuckDBPyConnection,
    event: dict[str, Any],
    *,
    from_state: str,
    to_state: str,
    transitioned_at: Any,
    session_date: date,
    reason_code: str,
) -> None:
    if conn.execute(
        """
        SELECT COUNT(*)
        FROM investigator_evaluation_transition
        WHERE candidate_id = ? AND to_state = ? AND policy_version = ?
        """,
        [
            event["candidate_id"],
            to_state,
            INVESTIGATOR_ATTRIBUTION_POLICY_VERSION,
        ],
    ).fetchone()[0]:
        return
    identity = {
        "candidate_id": event["candidate_id"],
        "source_event_id": event["event_id"],
        "from_state": from_state,
        "to_state": to_state,
        "session_date": session_date.isoformat(),
        "policy_version": INVESTIGATOR_ATTRIBUTION_POLICY_VERSION,
    }
    digest = _digest(identity)
    context_json = str(event.get("context_json") or "{}")
    conn.execute(
        """
        INSERT INTO investigator_evaluation_transition (
            evaluation_transition_id, candidate_id, setup_id, from_state,
            to_state, transitioned_at, session_date, reason_code,
            policy_version, source_event_id, source_snapshot_id,
            source_transition_id, originating_run_id, confirming_run_id,
            price_anchor, price_anchor_basis, evidence_snapshot_json,
            evidence_snapshot_hash, idempotency_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        [
            f"evaluation-transition-{digest}",
            event["candidate_id"],
            event["setup_id"],
            from_state,
            to_state,
            transitioned_at,
            session_date,
            reason_code,
            INVESTIGATOR_ATTRIBUTION_POLICY_VERSION,
            event["event_id"],
            event.get("source_snapshot_id"),
            event.get("source_transition_id"),
            event["source_run_id"],
            event["source_run_id"] if to_state == "CONFIRMED" else None,
            event.get("anchor_price"),
            event.get("anchor_price_basis"),
            context_json,
            hashlib.sha256(context_json.encode("utf-8")).hexdigest(),
            f"evaluation-transition-{digest}",
        ],
    )


def _append_coverage_receipts(conn: duckdb.DuckDBPyConnection) -> None:
    snapshots = _records(
        conn.execute(
            """
            SELECT CAST(as_of AS DATE) AS as_of_date, run_id, review_eligible,
                   investigator_evaluation_states_json,
                   investigator_missing_fields_json
            FROM candidate_snapshot
            WHERE investigator_context_json IS NOT NULL
            ORDER BY as_of_date, as_of, snapshot_id
            """
        )
    )
    grouped: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[_date(snapshot["as_of_date"])].append(snapshot)
    for as_of_date, rows in sorted(grouped.items()):
        source_run_id = str(rows[-1].get("run_id") or "UNKNOWN")
        decoded = [
            {
                **row,
                "states": _json_mapping(
                    row.get("investigator_evaluation_states_json")
                ),
                "missing": _json_list(row.get("investigator_missing_fields_json")),
            }
            for row in rows
        ]
        eligible = [row for row in decoded if bool(row.get("review_eligible"))]
        definitions = {
            "stage_attribution": (
                decoded,
                lambda row: row["states"].get("stage") == "KNOWN",
            ),
            "pattern_evaluation_attempted": (
                decoded,
                lambda row: row["states"].get("pattern_attempted")
                in {"KNOWN", "NONE", "NOT_ELIGIBLE", "ERROR"},
            ),
            "pattern_known_or_none": (
                decoded,
                lambda row: row["states"].get("pattern") in {"KNOWN", "NONE"},
            ),
            "setup_quality": (
                eligible,
                lambda row: row["states"].get("setup_quality") == "KNOWN",
            ),
            "breakout_classification": (
                decoded,
                lambda row: row["states"].get("breakout")
                in {"KNOWN", "NONE", "NOT_ELIGIBLE"},
            ),
            "regime_and_breadth_context": (
                decoded,
                lambda row: row["states"].get("regime") == "KNOWN"
                and row["states"].get("breadth") == "KNOWN",
            ),
            "sector_context": (
                decoded,
                lambda row: row["states"].get("sector") == "KNOWN",
            ),
            "run_artifact_lineage": (
                decoded,
                lambda row: row["states"].get("lineage") == "KNOWN",
            ),
        }
        for metric_name, (population, predicate) in definitions.items():
            denominator = len(population)
            numerator = sum(1 for row in population if predicate(row))
            coverage_pct = (
                round(100.0 * numerator / denominator, 6)
                if denominator
                else 0.0
            )
            status = (
                "NOT_EVALUATED"
                if denominator == 0
                else "PASS"
                if coverage_pct >= COVERAGE_TARGETS[metric_name]
                else "FAIL"
            )
            reason_counts: dict[str, int] = defaultdict(int)
            for row in population:
                if predicate(row):
                    continue
                state = row["states"].get(
                    {
                        "stage_attribution": "stage",
                        "pattern_evaluation_attempted": "pattern_attempted",
                        "pattern_known_or_none": "pattern",
                        "setup_quality": "setup_quality",
                        "breakout_classification": "breakout",
                        "regime_and_breadth_context": "regime",
                        "sector_context": "sector",
                        "run_artifact_lineage": "lineage",
                    }[metric_name],
                    "UNKNOWN",
                )
                reason_counts[str(state)] += 1
            unknown_count = int(reason_counts.get("UNKNOWN", 0))
            payload = {
                "as_of_date": as_of_date.isoformat(),
                "source_run_id": source_run_id,
                "metric_name": metric_name,
                "numerator": numerator,
                "denominator": denominator,
                "coverage_pct": coverage_pct,
                "target_pct": COVERAGE_TARGETS[metric_name],
                "status": status,
                "reasons": dict(sorted(reason_counts.items())),
            }
            digest = _digest(payload)
            conn.execute(
                """
                INSERT INTO investigator_attribution_coverage_receipt (
                    receipt_id, as_of_date, source_run_id, policy_version,
                    policy_snapshot_id, metric_name, numerator, denominator,
                    coverage_pct, target_pct, status, exclusion_reasons_json,
                    unexplained_unknown_count, idempotency_key
                ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [
                    f"coverage-{digest}",
                    as_of_date,
                    source_run_id,
                    INVESTIGATOR_ATTRIBUTION_POLICY_VERSION,
                    metric_name,
                    numerator,
                    denominator,
                    coverage_pct,
                    COVERAGE_TARGETS[metric_name],
                    status,
                    json.dumps(dict(sorted(reason_counts.items())), sort_keys=True),
                    unknown_count,
                    f"coverage-{digest}",
                ],
            )


def _project_discovery_events_to_legacy_cohort(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Insert a lossy compatibility projection without repainting legacy rows."""
    conn.execute(
        """
        INSERT INTO investigator_cohort_performance (
            trade_date, symbol_id, exchange, sector, close,
            stage_label, stage_confidence, pattern_family, pattern_state,
            setup_quality_bucket, breakout_type, candidate_tier,
            qualified_breakout, confirmed_regime, raw_regime,
            regime_confidence, breadth_velocity_bucket,
            breadth_velocity_quantile, regime_score_chg_5d,
            sector_relative_strength_bucket, context_as_of,
            attribution_mode, missing_fields_json,
            fwd_3d_return, fwd_5d_return, fwd_10d_return, fwd_20d_return,
            fwd_3d_matured_at, fwd_5d_matured_at,
            fwd_10d_matured_at, fwd_20d_matured_at,
            data_quality_status, inserted_at, updated_at
        )
        SELECT
            e.session_date, e.symbol_id, e.exchange, e.sector_name,
            e.anchor_price,
            json_extract_string(e.context_json, '$.stage_label'),
            TRY_CAST(json_extract_string(e.context_json, '$.stage_confidence') AS DOUBLE),
            json_extract_string(e.context_json, '$.pattern_family'),
            json_extract_string(e.context_json, '$.pattern_state'),
            json_extract_string(e.context_json, '$.setup_quality_bucket'),
            json_extract_string(e.context_json, '$.breakout_type'),
            json_extract_string(e.context_json, '$.candidate_tier'),
            TRY_CAST(json_extract_string(e.context_json, '$.qualified_breakout') AS BOOLEAN),
            json_extract_string(e.context_json, '$.confirmed_regime'),
            json_extract_string(e.context_json, '$.raw_regime'),
            TRY_CAST(json_extract_string(e.context_json, '$.regime_confidence') AS DOUBLE),
            json_extract_string(e.context_json, '$.breadth_velocity_bucket'),
            json_extract_string(e.context_json, '$.breadth_velocity_quantile'),
            TRY_CAST(json_extract_string(e.context_json, '$.regime_score_chg_5d') AS DOUBLE),
            json_extract_string(e.context_json, '$.sector_relative_strength_bucket'),
            e.context_as_of, e.attribution_mode,
            json_extract(e.context_json, '$.missing_fields')::VARCHAR,
            h3.close_to_close_return_pct,
            h5.close_to_close_return_pct,
            h10.close_to_close_return_pct,
            h20.close_to_close_return_pct,
            h3.target_session_date, h5.target_session_date,
            h10.target_session_date, h20.target_session_date,
            CASE
                WHEN h20.data_quality_status = 'MATURED' THEN 'MATURED'
                WHEN COALESCE(h3.matured_at, h5.matured_at, h10.matured_at, h20.matured_at)
                     IS NOT NULL THEN 'PARTIAL_MATURED'
                ELSE e.data_quality_status
            END,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM investigator_performance_event AS e
        LEFT JOIN investigator_performance_horizon AS h3
          ON h3.event_id = e.event_id AND h3.horizon_sessions = 3
        LEFT JOIN investigator_performance_horizon AS h5
          ON h5.event_id = e.event_id AND h5.horizon_sessions = 5
        LEFT JOIN investigator_performance_horizon AS h10
          ON h10.event_id = e.event_id AND h10.horizon_sessions = 10
        LEFT JOIN investigator_performance_horizon AS h20
          ON h20.event_id = e.event_id AND h20.horizon_sessions = 20
        WHERE e.event_type = 'CANDIDATE_DISCOVERED'
          AND NOT EXISTS (
              SELECT 1
              FROM investigator_cohort_performance AS existing
              WHERE existing.trade_date = e.session_date
                AND existing.symbol_id = e.symbol_id
                AND existing.exchange = e.exchange
          )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY e.session_date, e.symbol_id, e.exchange
            ORDER BY e.event_at, e.event_id
        ) = 1
        """
    )


def _build_outputs(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, list[dict[str, Any]]]:
    events = _records(
        conn.execute(
            """
            SELECT * FROM investigator_performance_event
            ORDER BY session_date, event_type, exchange, symbol_id, event_id
            """
        )
    )
    horizons = _records(
        conn.execute(
            """
            SELECT h.*, e.event_type, e.candidate_id, e.symbol_id, e.exchange,
                   e.setup_id, e.session_date, e.primary_eligible,
                   e.attribution_mode, e.context_json
            FROM investigator_performance_horizon h
            JOIN investigator_performance_event e USING (event_id)
            ORDER BY e.event_type, h.horizon_sessions, e.session_date, e.event_id
            """
        )
    )
    scorecard = _records(
        conn.execute(
            """
            SELECT e.event_type, h.horizon_sessions,
                   COUNT(*) AS sample_count,
                   AVG(h.close_to_close_return_pct) AS avg_return_pct,
                   MEDIAN(h.close_to_close_return_pct) AS median_return_pct,
                   AVG(CASE WHEN h.close_to_close_return_pct > 0 THEN 100.0 ELSE 0.0 END) AS win_rate_pct,
                   AVG(h.maximum_favourable_excursion_pct) AS avg_mfe_pct,
                   AVG(h.maximum_adverse_excursion_pct) AS avg_mae_pct,
                   AVG(h.next_open_entry_return_pct) AS avg_next_open_return_pct,
                   AVG(h.days_to_2pct) AS avg_days_to_2pct,
                   AVG(h.days_to_5pct) AS avg_days_to_5pct,
                   AVG(h.days_to_stop) AS avg_days_to_stop,
                   AVG(h.benchmark_relative_return_pct) AS avg_benchmark_relative_return_pct,
                   AVG(h.sector_relative_return_pct) AS avg_sector_relative_return_pct,
                   COUNT(DISTINCT e.symbol_id) AS unique_symbol_count,
                   COUNT(DISTINCT e.candidate_id) AS unique_episode_count,
                   COUNT(*) - COUNT(DISTINCT e.symbol_id) AS overlapping_episode_count,
                   AVG(h.close_to_close_return_pct) AS expectancy_pct,
                   (
                       AVG(h.close_to_close_return_pct)
                           FILTER (WHERE h.close_to_close_return_pct > 0)
                   ) / NULLIF(ABS(
                       AVG(h.close_to_close_return_pct)
                           FILTER (WHERE h.close_to_close_return_pct <= 0)
                   ), 0) AS payoff_ratio,
                   CASE
                       WHEN COUNT(*) >= 120 THEN 'POLICY_ELIGIBLE'
                       WHEN COUNT(*) >= 60 THEN 'MODERATE'
                       WHEN COUNT(*) >= 30 THEN 'PROVISIONAL'
                       ELSE 'EXPLORATORY'
                   END AS sample_confidence,
                   COUNT(*) >= 30 AS subgroup_min_sample_pass,
                   COUNT(*) >= 100 AS overall_tuning_sample_pass
            FROM investigator_performance_horizon h
            JOIN investigator_performance_event e USING (event_id)
            WHERE e.primary_eligible = TRUE
              AND e.attribution_mode IN ('OBSERVED_AT_DECISION', 'RECONSTRUCTED_SAME_RUN')
              AND h.close_to_close_return_pct IS NOT NULL
            GROUP BY e.event_type, h.horizon_sessions
            ORDER BY e.event_type, h.horizon_sessions
            """
        )
    )
    transitions = _records(
        conn.execute(
            """
            SELECT e.event_type, h.horizon_sessions, h.lifecycle_outcome,
                   COUNT(*) AS sample_count
            FROM investigator_performance_horizon h
            JOIN investigator_performance_event e USING (event_id)
            WHERE e.primary_eligible = TRUE
              AND h.lifecycle_outcome IS NOT NULL
            GROUP BY e.event_type, h.horizon_sessions, h.lifecycle_outcome
            ORDER BY e.event_type, h.horizon_sessions, h.lifecycle_outcome
            """
        )
    )
    coverage = _records(
        conn.execute(
            """
            SELECT *
            FROM investigator_attribution_coverage_receipt
            QUALIFY as_of_date = MAX(as_of_date) OVER ()
                AND ROW_NUMBER() OVER (
                    PARTITION BY as_of_date, metric_name
                    ORDER BY created_at DESC, receipt_id DESC
                ) = 1
            ORDER BY metric_name, receipt_id
            """
        )
    )
    missing_reasons = _records(
        conn.execute(
            """
            SELECT event_type, 'EVENT' AS reason_scope,
                   NULL::INTEGER AS horizon_sessions,
                   data_quality_reason, COUNT(*) AS sample_count
            FROM investigator_performance_event
            WHERE data_quality_reason IS NOT NULL
            GROUP BY event_type, data_quality_reason
            UNION ALL
            SELECT e.event_type, 'HORIZON' AS reason_scope,
                   h.horizon_sessions, h.data_quality_reason,
                   COUNT(*) AS sample_count
            FROM investigator_performance_horizon h
            JOIN investigator_performance_event e USING (event_id)
            WHERE h.data_quality_reason IS NOT NULL
            GROUP BY e.event_type, h.horizon_sessions, h.data_quality_reason
            ORDER BY event_type, reason_scope, horizon_sessions, data_quality_reason
            """
        )
    )
    sensitivity = _symbol_sensitivity(events, horizons)
    evaluation_transitions = _records(
        conn.execute(
            """
            SELECT *
            FROM investigator_evaluation_transition
            ORDER BY session_date, candidate_id, transitioned_at,
                     evaluation_transition_id
            """
        )
    )
    cohort_outputs = _cohort_outputs(horizons)
    calendar_windows = _calendar_windows(horizons)
    readiness_inputs = [
        {
            "check_id": f"INVESTIGATOR_{str(row['metric_name']).upper()}",
            "category": "investigator_attribution",
            "status": row["status"],
            "observed": row["coverage_pct"],
            "expected": f">={row['target_pct']}",
            "production_blocking": True,
            "policy_version": row["policy_version"],
            "as_of_date": row["as_of_date"],
        }
        for row in coverage
    ]
    return {
        "investigator_performance_events": events,
        "investigator_performance_horizons": horizons,
        "investigator_discovery_scorecard": [
            row for row in scorecard if row["event_type"] == "CANDIDATE_DISCOVERED"
        ],
        "investigator_entry_scorecard": [
            row for row in scorecard if row["event_type"] == "ENTRY_CONFIRMED"
        ],
        "investigator_executable_scorecard": [
            row for row in scorecard if row["event_type"] == "EXECUTABLE_AVAILABLE"
        ],
        "investigator_transition_matrix": transitions,
        "investigator_evaluation_transitions": evaluation_transitions,
        "investigator_attribution_coverage": coverage,
        "investigator_coverage_receipt": coverage,
        "investigator_readiness_inputs": readiness_inputs,
        "investigator_missing_data_reasons": missing_reasons,
        "investigator_symbol_sensitivity": sensitivity,
        "investigator_primary_cohorts": cohort_outputs["primary"],
        "investigator_diagnostic_cohorts": cohort_outputs["diagnostic"],
        "investigator_research_cohorts": cohort_outputs["research"],
        "investigator_calendar_windows": calendar_windows,
    }


def _symbol_sensitivity(
    events: list[dict[str, Any]],
    horizons: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    target_20 = {
        str(row["event_id"]): _date(row["target_session_date"])
        for row in horizons
        if int(row["horizon_sessions"]) == 20 and row.get("target_session_date")
    }
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[
            (
                str(event["exchange"]),
                str(event["symbol_id"]),
                str(event["event_type"]),
            )
        ].append(event)
    rows: list[dict[str, Any]] = []
    for key, group in sorted(grouped.items()):
        retained_until: date | None = None
        for event in sorted(
            group, key=lambda item: (_date(item["session_date"]), str(item["event_id"]))
        ):
            session = _date(event["session_date"])
            included = retained_until is None or session > retained_until
            if included:
                retained_until = target_20.get(str(event["event_id"]), session)
            rows.append(
                {
                    "event_id": event["event_id"],
                    "exchange": key[0],
                    "symbol_id": key[1],
                    "event_type": key[2],
                    "session_date": session,
                    "sensitivity_included": included,
                    "exclusion_reason": None
                    if included
                    else "OVERLAPPING_SYMBOL_EPISODE",
                }
            )
    return rows


def _cohort_outputs(
    horizons: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    matured = [
        row for row in horizons if row.get("close_to_close_return_pct") is not None
    ]
    prepared = [
        {**row, "context": _json_mapping(row.get("context_json"))}
        for row in matured
    ]
    primary = _aggregate_cohorts(
        [row for row in prepared if bool(row.get("primary_eligible"))],
        dimensions=("event_type", "horizon_sessions"),
        cohort_type="PRIMARY_MUTUALLY_EXCLUSIVE",
    )
    diagnostic = _aggregate_cohorts(
        prepared,
        dimensions=(
            "event_type",
            "horizon_sessions",
            "stage_label",
            "pattern_family",
            "trigger_reason",
            "breakout_type",
        ),
        cohort_type="OVERLAPPING_DIAGNOSTIC",
    )
    research = _aggregate_cohorts(
        [row for row in prepared if not bool(row.get("primary_eligible"))],
        dimensions=("event_type", "horizon_sessions", "review_lane", "trigger_reason"),
        cohort_type="INSUFFICIENT_SAMPLE_RESEARCH",
    )
    return {"primary": primary, "diagnostic": diagnostic, "research": research}


def _aggregate_cohorts(
    rows: list[dict[str, Any]],
    *,
    dimensions: tuple[str, ...],
    cohort_type: str,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = tuple(
            row.get(dimension)
            if dimension in row
            else row["context"].get(dimension, "UNKNOWN")
            for dimension in dimensions
        )
        grouped[key].append(row)
    output = []
    for key, group in sorted(grouped.items(), key=lambda item: str(item[0])):
        returns = [float(row["close_to_close_return_pct"]) for row in group]
        winners = [value for value in returns if value > 0]
        losers = [value for value in returns if value <= 0]
        average_winner = sum(winners) / len(winners) if winners else None
        average_loser = sum(losers) / len(losers) if losers else None
        sample_count = len(group)
        output.append(
            {
                "cohort_type": cohort_type,
                **dict(zip(dimensions, key, strict=True)),
                "sample_count": sample_count,
                "unique_symbol_count": len(
                    {str(row["symbol_id"]) for row in group}
                ),
                "unique_episode_count": len(
                    {str(row["candidate_id"]) for row in group}
                ),
                "overlapping_episode_count": sample_count
                - len({str(row["symbol_id"]) for row in group}),
                "avg_return_pct": round(sum(returns) / sample_count, 6),
                "win_rate_pct": round(100.0 * len(winners) / sample_count, 6),
                "payoff_ratio": (
                    round(average_winner / abs(average_loser), 6)
                    if average_winner is not None
                    and average_loser not in (None, 0.0)
                    else None
                ),
                "expectancy_pct": round(sum(returns) / sample_count, 6),
                "sample_confidence": _confidence_label(sample_count),
            }
        )
    return output


def _calendar_windows(horizons: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible = [
        row
        for row in horizons
        if bool(row.get("primary_eligible"))
        and row.get("close_to_close_return_pct") is not None
    ]
    sessions = sorted({_date(row["session_date"]) for row in eligible})
    window_by_session = {
        session: index // 10 for index, session in enumerate(sessions)
    }
    grouped: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in eligible:
        grouped[
            (
                str(row["event_type"]),
                int(row["horizon_sessions"]),
                window_by_session[_date(row["session_date"])],
            )
        ].append(row)
    output = []
    for (event_type, horizon, window_index), group in sorted(grouped.items()):
        window_sessions = sorted(_date(row["session_date"]) for row in group)
        returns = [float(row["close_to_close_return_pct"]) for row in group]
        output.append(
            {
                "event_type": event_type,
                "horizon_sessions": horizon,
                "window_index": window_index,
                "window_start": window_sessions[0],
                "window_end": window_sessions[-1],
                "sample_count": len(group),
                "avg_return_pct": round(sum(returns) / len(returns), 6),
                "positive_expectancy": sum(returns) / len(returns) > 0,
                "sample_confidence": _confidence_label(len(group)),
            }
        )
    return output


def _confidence_label(sample_count: int) -> str:
    if sample_count >= 120:
        return "POLICY_ELIGIBLE"
    if sample_count >= 60:
        return "MODERATE"
    if sample_count >= 30:
        return "PROVISIONAL"
    return "EXPLORATORY"


def _index_return(rows: list[dict[str, Any]], start: date, end: date) -> float | None:
    by_date = {_date(row["session_date"]): _float(row.get("close")) for row in rows}
    return _return_pct(by_date.get(end), by_date.get(start))


def _first_touch(
    highs: list[float | None], anchor: float, threshold_pct: float
) -> int | None:
    target = anchor * (1.0 + threshold_pct / 100.0)
    return next(
        (
            index
            for index, value in enumerate(highs, start=1)
            if value is not None and value >= target
        ),
        None,
    )


def _first_stop_touch(
    lows: list[float | None], stop_price: float | None
) -> int | None:
    if stop_price is None:
        return None
    return next(
        (
            index
            for index, value in enumerate(lows, start=1)
            if value is not None and value <= stop_price
        ),
        None,
    )


def _drawdown_before(
    lows: list[float | None], anchor: float, touch_day: int | None
) -> float | None:
    if touch_day is None:
        return None
    values = [value for value in lows[:touch_day] if value is not None]
    return _return_pct(min(values), anchor) if values else None


def _return_pct(end: float | None, start: float | None) -> float | None:
    if end is None or start in {None, 0.0}:
        return None
    return round((float(end) / float(start) - 1.0) * 100.0, 6)


def _difference(left: float | None, right: float | None) -> float | None:
    return round(left - right, 6) if left is not None and right is not None else None


def _float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if parsed != parsed else parsed


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _records(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _empty_outputs() -> dict[str, list[dict[str, Any]]]:
    return {
        "investigator_performance_events": [],
        "investigator_performance_horizons": [],
        "investigator_discovery_scorecard": [],
        "investigator_entry_scorecard": [],
        "investigator_executable_scorecard": [],
        "investigator_transition_matrix": [],
        "investigator_evaluation_transitions": [],
        "investigator_attribution_coverage": [],
        "investigator_coverage_receipt": [],
        "investigator_readiness_inputs": [],
        "investigator_missing_data_reasons": [],
        "investigator_symbol_sensitivity": [],
        "investigator_primary_cohorts": [],
        "investigator_diagnostic_cohorts": [],
        "investigator_research_cohorts": [],
        "investigator_calendar_windows": [],
    }
