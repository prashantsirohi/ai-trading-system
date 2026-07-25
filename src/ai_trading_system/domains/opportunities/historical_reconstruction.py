"""Same-run reconstruction for pre-attribution Investigator shadow events."""

from __future__ import annotations

import csv
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.domains.opportunities.contracts import InvestigatorContext
from ai_trading_system.domains.opportunities.orchestration.assembler import (
    _sector_rs_bucket,
)
from ai_trading_system.domains.opportunities.registry import (
    DuckDBOpportunityRegistryStore,
    PerformanceEventObservation,
    SourceLineage,
)
from ai_trading_system.domains.opportunities.performance_evaluation import (
    mature_performance_events,
)
from ai_trading_system.pipeline.registry import RegistryStore


def reconstruct_same_run_events(
    registry: RegistryStore,
    *,
    ohlcv_db_path: str | Path,
    from_date: date,
    to_date: date,
    apply: bool,
) -> dict[str, Any]:
    """Reconstruct only evidence that was available before each snapshot write."""
    store = DuckDBOpportunityRegistryStore(registry)
    with registry._connect(read_only=True) as conn:  # noqa: SLF001
        candidates = _candidate_rows(conn, from_date=from_date, to_date=to_date)
    planned = 0
    created = 0
    skipped_existing = 0
    rejected_late = 0
    missing_context = 0
    for row in candidates:
        with registry._connect(read_only=True) as conn:  # noqa: SLF001
            if conn.execute(
                """
                SELECT COUNT(*)
                FROM investigator_performance_event
                WHERE candidate_id = ? AND event_type = ?
                """,
                [row["candidate_id"], row["event_type"]],
            ).fetchone()[0]:
                skipped_existing += 1
                continue
            artifacts = _artifact_map(
                conn,
                run_id=str(row["run_id"]),
                available_at=_datetime(row["snapshot_created_at"]),
            )
        context, sector_name = _reconstructed_context(row, artifacts)
        if context.context_as_of is not None and context.context_as_of > _datetime(
            row["event_at"]
        ):
            rejected_late += 1
            continue
        missing_context += int(bool(context.missing_fields))
        planned += 1
        if not apply:
            continue
        anchor = _decision_close(
            ohlcv_db_path,
            exchange=str(row["exchange"]),
            symbol_id=str(row["symbol_id"]),
            session_date=_date(row["session_date"]),
        )
        lineage = SourceLineage(
            run_id=str(row["run_id"]),
            stage_name="opportunities",
            stage_attempt=int(row["stage_attempt"] or 1),
            source_artifact_type="same_run_reconstruction",
            source_artifact_path="|".join(
                sorted(str(item["uri"]) for item in artifacts.values())
            )
            or "reconstructed:registry-only",
            source_artifact_hash=_combined_hash(artifacts),
        )
        result = store.append_performance_event(
            PerformanceEventObservation(
                candidate_id=str(row["candidate_id"]),
                setup_id=str(row["setup_id"]),
                symbol_id=str(row["symbol_id"]),
                exchange=str(row["exchange"]),
                sector_name=sector_name,
                event_type=str(row["event_type"]),
                event_at=_datetime(row["event_at"]),
                session_date=_date(row["session_date"]),
                anchor_price=anchor,
                anchor_price_basis="DECISION_SESSION_CLOSE",
                investigator_context=context,
                lineage=lineage,
                source_snapshot_id=str(row["snapshot_id"]),
                source_transition_id=(
                    str(row["transition_id"]) if row.get("transition_id") else None
                ),
                lifecycle_evaluable=bool(row["lifecycle_evaluable"]),
                data_quality_status=(
                    "PENDING" if anchor is not None else "INSUFFICIENT_PRICE_DATA"
                ),
                data_quality_reason=(
                    "decision_session_close_missing"
                    if anchor is None
                    else (
                        None
                        if row["lifecycle_evaluable"]
                        else "pending_3d_sequence_absent"
                    )
                ),
            )
        )
        created += int(result.created)
    outputs = (
        mature_performance_events(registry, ohlcv_db_path=ohlcv_db_path)
        if apply
        else {}
    )
    return {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "apply": apply,
        "planned_events": planned,
        "created_events": created,
        "skipped_existing": skipped_existing,
        "rejected_late_sources": rejected_late,
        "events_with_missing_context": missing_context,
        "discovery_scorecard_rows": len(
            outputs.get("investigator_discovery_scorecard", [])
        ),
        "entry_scorecard_rows": len(outputs.get("investigator_entry_scorecard", [])),
        "transition_rows": len(outputs.get("investigator_transition_matrix", [])),
        "transition_conclusion": (
            "AVAILABLE"
            if outputs.get("investigator_transition_matrix")
            else "INSUFFICIENT_PENDING_3D_SEQUENCE"
        ),
    }


def _candidate_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    from_date: date,
    to_date: date,
) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        WITH first_snapshot AS (
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT s.*, ROW_NUMBER() OVER (
                    PARTITION BY s.candidate_id
                    ORDER BY s.as_of, s.created_at, s.snapshot_id
                ) AS rn
                FROM candidate_snapshot s
            )
            WHERE rn = 1
        ),
        confirmed AS (
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT t.*, ROW_NUMBER() OVER (
                    PARTITION BY t.candidate_id
                    ORDER BY t.transitioned_at, t.created_at, t.transition_id
                ) AS rn
                FROM candidate_transition t
                WHERE LOWER(t.to_state) = 'confirmed'
            )
            WHERE rn = 1
        )
        SELECT ep.candidate_id, ep.setup_id, ep.symbol_id, ep.exchange,
               first.run_id, first.stage_attempt, first.snapshot_id,
               first.as_of AS event_at, CAST(first.as_of AS DATE) AS session_date,
               first.created_at AS snapshot_created_at,
               'CANDIDATE_DISCOVERED' AS event_type,
               NULL::VARCHAR AS transition_id, first.snapshot_json,
               (
                   LOWER(json_extract_string(first.snapshot_json, '$.lifecycle_state'))
                       = 'pending_followthrough'
                   AND LOWER(json_extract_string(first.snapshot_json, '$.followthrough_status'))
                       = 'pending_3d'
               ) AS lifecycle_evaluable
        FROM candidate_episode ep
        JOIN first_snapshot first USING (candidate_id)
        JOIN pipeline_run run ON run.run_id = first.run_id
        WHERE run.status = 'completed'
          AND run.run_id LIKE 'shadow-%'
          AND CAST(first.as_of AS DATE) BETWEEN ? AND ?
          AND ep.episode_type <> 'position_state_recovery'
        UNION ALL
        SELECT ep.candidate_id, ep.setup_id, ep.symbol_id, ep.exchange,
               snap.run_id, snap.stage_attempt, snap.snapshot_id,
               confirmed.transitioned_at AS event_at,
               CAST(confirmed.transitioned_at AS DATE) AS session_date,
               snap.created_at AS snapshot_created_at,
               'ENTRY_CONFIRMED' AS event_type,
               confirmed.transition_id, snap.snapshot_json,
               TRUE AS lifecycle_evaluable
        FROM candidate_episode ep
        JOIN confirmed USING (candidate_id)
        JOIN candidate_snapshot snap
          ON snap.snapshot_id = confirmed.triggering_snapshot_id
        JOIN pipeline_run run ON run.run_id = snap.run_id
        WHERE run.status = 'completed'
          AND run.run_id LIKE 'shadow-%'
          AND CAST(confirmed.transitioned_at AS DATE) BETWEEN ? AND ?
        ORDER BY event_at, candidate_id, event_type
        """,
        [from_date, to_date, from_date, to_date],
    )
    return _records(cursor)


def _artifact_map(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    available_at: datetime,
) -> dict[str, dict[str, Any]]:
    wanted = (
        "investigator_scores",
        "pattern_scan",
        "breakout_scan",
        "stock_scan",
        "weekly_stock_stage_universe",
        "sector_dashboard",
        "weekly_sector_stage_universe",
        "dashboard_payload",
        "ranked_signals",
    )
    placeholders = ", ".join("?" for _ in wanted)
    rows = _records(
        conn.execute(
            f"""
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY artifact_type
                    ORDER BY attempt_number DESC, created_at DESC, artifact_id DESC
                ) AS rn
                FROM pipeline_artifact
                WHERE run_id = ?
                  AND artifact_type IN ({placeholders})
                  AND created_at <= ?
            )
            WHERE rn = 1
            """,
            [run_id, *wanted, available_at.replace(tzinfo=None)],
        )
    )
    return {str(row["artifact_type"]): row for row in rows}


def _reconstructed_context(
    row: dict[str, Any],
    artifacts: dict[str, dict[str, Any]],
) -> tuple[InvestigatorContext, str | None]:
    symbol = str(row["symbol_id"]).upper()
    snapshot = json.loads(str(row.get("snapshot_json") or "{}"))
    investigator = _artifact_row(artifacts.get("investigator_scores"), symbol)
    pattern_rows = _artifact_rows(artifacts.get("pattern_scan"), symbol)
    breakout_rows = _artifact_rows(artifacts.get("breakout_scan"), symbol)
    stock = _artifact_row(
        artifacts.get("weekly_stock_stage_universe") or artifacts.get("stock_scan"),
        symbol,
    )
    ranked = _artifact_row(artifacts.get("ranked_signals"), symbol)
    sector_name = _text(
        (ranked or {}).get("sector_name") or (ranked or {}).get("sector"),
        unknown=None,
    )
    sector = _sector_row(
        artifacts.get("weekly_sector_stage_universe")
        or artifacts.get("sector_dashboard"),
        sector_name,
    )
    market = _artifact_json(artifacts.get("dashboard_payload")).get("market_regime", {})
    market = market if isinstance(market, dict) else {}
    stock_snapshot = snapshot.get("stock_stage")
    stock_snapshot = stock_snapshot if isinstance(stock_snapshot, dict) else {}
    primary_pattern = _best_pattern(pattern_rows)
    primary_breakout = _best_breakout(breakout_rows)
    stage_label = _text(
        (investigator or {}).get("stage_label")
        or (stock or {}).get("stage_label")
        or (stock or {}).get("effective_stage")
        or stock_snapshot.get("effective_stage")
    )
    stage_confidence = _confidence(
        (investigator or {}).get("stage_confidence")
        or (stock or {}).get("stage_confidence_score")
        or (stock or {}).get("weekly_stage_confidence")
        or stock_snapshot.get("confidence_score")
    )
    values = {
        "stage_label": stage_label,
        "stage_confidence": stage_confidence,
        "pattern_family": _text(
            (primary_pattern or {}).get("pattern_family")
            or (investigator or {}).get("pattern_family")
        ),
        "pattern_state": _text(
            (primary_pattern or {}).get("pattern_state")
            or (investigator or {}).get("pattern_state")
        ),
        "setup_quality_bucket": _setup_bucket(
            (primary_pattern or {}).get("setup_quality_bucket")
            or (primary_pattern or {}).get("setup_quality")
            or (investigator or {}).get("setup_quality_bucket")
        ),
        "breakout_type": _text(
            (primary_breakout or {}).get("breakout_type")
            or (primary_breakout or {}).get("setup_family")
            or (investigator or {}).get("breakout_type")
        ),
        "candidate_tier": _tier(
            (primary_breakout or {}).get("candidate_tier")
            or (investigator or {}).get("candidate_tier")
        ),
        "qualified_breakout": _bool(
            (primary_breakout or {}).get("qualified")
            or (primary_breakout or {}).get("qualified_breakout")
            or (investigator or {}).get("qualified_breakout")
        ),
        "confirmed_regime": _text(
            market.get("regime") or market.get("confirmed_regime")
        ),
        "raw_regime": _text(market.get("raw_regime") or market.get("regime")),
        "regime_confidence": _float(
            market.get("regime_confidence_capped", market.get("regime_confidence"))
        ),
        "breadth_velocity_bucket": _text(market.get("breadth_velocity_bucket")),
        "breadth_velocity_quantile": _text(market.get("breadth_velocity_quantile")),
        "regime_score_chg_5d": _float(market.get("regime_score_chg_5d")),
        "sector_relative_strength_bucket": _sector_rs_bucket(
            (sector or {}).get("sector_relative_strength_state")
            or (sector or {}).get("RS_rank_pct")
            or (sector or {}).get("rs_state")
        ),
    }
    required = (
        "stage_label",
        "stage_confidence",
        "pattern_family",
        "pattern_state",
        "setup_quality_bucket",
        "breakout_type",
        "candidate_tier",
        "qualified_breakout",
        "confirmed_regime",
        "breadth_velocity_bucket",
        "sector_relative_strength_bucket",
    )
    missing = tuple(
        field
        for field in required
        if values[field] is None or str(values[field]).upper() == "UNKNOWN"
    )
    return (
        InvestigatorContext(
            **values,
            context_as_of=_datetime(row["event_at"]),
            source_run_id=str(row["run_id"]),
            source_artifact_hashes=tuple(
                sorted(str(item["content_hash"]) for item in artifacts.values())
            ),
            classifier_versions=tuple(
                value
                for value in (
                    str((investigator or {}).get("stage1_model_version") or ""),
                    str((stock or {}).get("classifier_version") or ""),
                )
                if value
            ),
            missing_fields=missing,
            attribution_mode="RECONSTRUCTED_SAME_RUN",
            pattern_events=tuple(pattern_rows),
            breakout_events=tuple(breakout_rows),
        ),
        sector_name,
    )


def _artifact_rows(
    artifact: dict[str, Any] | None,
    symbol: str,
) -> list[dict[str, Any]]:
    if not artifact:
        return []
    path = Path(str(artifact["uri"]))
    if not path.exists() or path.suffix.lower() != ".csv":
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return [
        item
        for item in rows
        if str(item.get("symbol_id") or item.get("symbol") or item.get("ticker") or "")
        .strip()
        .upper()
        == symbol
    ]


def _artifact_row(
    artifact: dict[str, Any] | None,
    symbol: str,
) -> dict[str, Any] | None:
    rows = _artifact_rows(artifact, symbol)
    return rows[0] if rows else None


def _sector_row(
    artifact: dict[str, Any] | None,
    sector_name: str | None,
) -> dict[str, Any] | None:
    if not artifact or not sector_name:
        return None
    path = Path(str(artifact["uri"]))
    if not path.exists() or path.suffix.lower() != ".csv":
        return None
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    target = sector_name.strip().lower()
    return next(
        (
            item
            for item in rows
            if str(
                item.get("sector_name")
                or item.get("sector")
                or item.get("Sector")
                or ""
            )
            .strip()
            .lower()
            == target
        ),
        None,
    )


def _artifact_json(artifact: dict[str, Any] | None) -> dict[str, Any]:
    if not artifact:
        return {}
    path = Path(str(artifact["uri"]))
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _best_pattern(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return min(
        rows,
        key=lambda item: (
            not bool(_bool(item.get("qualified") or item.get("pattern_qualified"))),
            -(_float(item.get("pattern_score") or item.get("score")) or -1),
            str(item.get("pattern_family") or ""),
        ),
        default=None,
    )


def _best_breakout(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return min(
        rows,
        key=lambda item: (
            not bool(_bool(item.get("qualified") or item.get("qualified_breakout"))),
            {"A": 0, "B": 1, "C": 2, "D": 3}.get(
                str(item.get("candidate_tier") or "").upper(), 4
            ),
            -(_float(item.get("breakout_score") or item.get("score")) or -1),
        ),
        default=None,
    )


def _decision_close(
    path: str | Path,
    *,
    exchange: str,
    symbol_id: str,
    session_date: date,
) -> float | None:
    with duckdb.connect(str(path), read_only=True) as conn:
        row = conn.execute(
            """
            SELECT close
            FROM _catalog
            WHERE UPPER(exchange) = ?
              AND UPPER(symbol_id) = ?
              AND CAST(timestamp AS DATE) = ?
              AND COALESCE(is_benchmark, FALSE) = FALSE
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            [exchange.upper(), symbol_id.upper(), session_date],
        ).fetchone()
    return _float(row[0]) if row else None


def _combined_hash(artifacts: dict[str, dict[str, Any]]) -> str:
    import hashlib

    hashes = sorted(str(item["content_hash"]) for item in artifacts.values())
    return hashlib.sha256("|".join(hashes).encode()).hexdigest()


def _setup_bucket(value: Any) -> str:
    text = _text(value)
    if text in {"HIGH", "MEDIUM", "LOW"}:
        return text
    parsed = _float(value)
    if parsed is None:
        return "UNKNOWN"
    return "HIGH" if parsed >= 70 else "MEDIUM" if parsed >= 45 else "LOW"


def _tier(value: Any) -> str:
    text = _text(value)
    return text if text in {"A", "B", "C", "D"} else "UNKNOWN"


def _text(value: Any, *, unknown: str | None = "UNKNOWN") -> str | None:
    text = str(value or "").strip().upper().replace(" ", "_").replace("-", "_")
    return text if text not in {"", "NONE", "NAN", "<NA>"} else unknown


def _confidence(value: Any) -> float | None:
    parsed = _float(value)
    if parsed is None:
        return None
    return max(0.0, min(100.0, parsed * 100.0 if parsed <= 1 else parsed))


def _float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if parsed != parsed else parsed


def _bool(value: Any) -> bool | None:
    text = str(value or "").strip().lower()
    if text in {"true", "1", "yes", "y", "qualified"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return (
        parsed.astimezone(timezone.utc)
        if parsed.tzinfo
        else parsed.replace(tzinfo=timezone.utc)
    )


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _records(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
