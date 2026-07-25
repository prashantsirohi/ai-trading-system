"""Point-in-time Investigator event maturation and reporting."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.pipeline.registry import RegistryStore


HORIZONS = (3, 5, 10, 20)
PRIMARY_ATTRIBUTION_MODES = {
    "OBSERVED_AT_DECISION",
    "RECONSTRUCTED_SAME_RUN",
}
SUSTAINED_STATES = {"confirmed", "advancing", "extended"}
FAILED_STATES = {"failed", "weakening", "exited", "archived"}
BENCHMARK_SYMBOL = "NIFTY_50"


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
            return _empty_outputs()
        prices, index_prices, sector_map = _load_market_data(path, events)
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
        _project_discovery_events_to_legacy_cohort(conn)
        return _build_outputs(conn)


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
    if event_type == "CANDIDATE_DISCOVERED" and horizon == 3:
        if any(state == "confirmed" for state in states):
            return "CONFIRMED"
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
                   e.primary_eligible, e.attribution_mode
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
                   AVG(h.benchmark_relative_return_pct) AS avg_benchmark_relative_return_pct,
                   AVG(h.sector_relative_return_pct) AS avg_sector_relative_return_pct,
                   CASE
                       WHEN COUNT(*) >= 50 THEN 'HIGH'
                       WHEN COUNT(*) >= 20 THEN 'MEDIUM'
                       ELSE 'LOW'
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
            SELECT investigator_attribution_mode AS attribution_mode,
                   COUNT(*) AS snapshot_count,
                   SUM(CASE WHEN stage_label NOT IN ('UNKNOWN', '') THEN 1 ELSE 0 END) AS stage_label_present,
                   SUM(CASE WHEN stage_confidence IS NOT NULL THEN 1 ELSE 0 END) AS stage_confidence_present,
                   SUM(CASE WHEN pattern_family NOT IN ('UNKNOWN', '') THEN 1 ELSE 0 END) AS pattern_family_present,
                   SUM(CASE WHEN setup_quality_bucket NOT IN ('UNKNOWN', '') THEN 1 ELSE 0 END) AS setup_quality_present,
                   SUM(CASE WHEN confirmed_regime NOT IN ('UNKNOWN', '') THEN 1 ELSE 0 END) AS regime_present,
                   SUM(CASE WHEN breadth_velocity_bucket NOT IN ('UNKNOWN', '') THEN 1 ELSE 0 END) AS breadth_velocity_present,
                   SUM(CASE WHEN sector_relative_strength_bucket NOT IN ('UNKNOWN', '') THEN 1 ELSE 0 END) AS sector_rs_present
            FROM candidate_snapshot
            GROUP BY investigator_attribution_mode
            ORDER BY investigator_attribution_mode
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
    return {
        "investigator_performance_events": events,
        "investigator_performance_horizons": horizons,
        "investigator_discovery_scorecard": [
            row for row in scorecard if row["event_type"] == "CANDIDATE_DISCOVERED"
        ],
        "investigator_entry_scorecard": [
            row for row in scorecard if row["event_type"] == "ENTRY_CONFIRMED"
        ],
        "investigator_transition_matrix": transitions,
        "investigator_attribution_coverage": coverage,
        "investigator_missing_data_reasons": missing_reasons,
        "investigator_symbol_sensitivity": sensitivity,
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


def _empty_outputs() -> dict[str, list[dict[str, Any]]]:
    return {
        "investigator_performance_events": [],
        "investigator_performance_horizons": [],
        "investigator_discovery_scorecard": [],
        "investigator_entry_scorecard": [],
        "investigator_transition_matrix": [],
        "investigator_attribution_coverage": [],
        "investigator_missing_data_reasons": [],
        "investigator_symbol_sensitivity": [],
    }
