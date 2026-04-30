"""Read models for the per-symbol ranking detail and history.

Backs:

  * ``GET /api/execution/ranking/{symbol}`` — full ranked row + lifecycle +
    decision + curated factor block, optionally pinned to a specific
    ``run_id``.
  * ``GET /api/execution/ranking/{symbol}/history`` — historical rank
    position across the most-recent N runs (sparkline data).
  * ``GET /api/execution/workspace/snapshot`` — slim Control Tower payload
    (top-3 actions, summary cards, market state) without the heavy
    ``/workspace/pipeline`` payload.

All functions read directly from the on-disk ``pipeline_runs/`` artifact
tree (``ranked_signals.csv``, ``stock_scan.csv``, etc.). They never raise
on missing inputs — instead they return ``{"available": False}``-style
payloads so the UI can render a degraded state.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ai_trading_system.ui.execution_api.services.readmodels.latest_operational_snapshot import (
    ExecutionContext,
    LatestOperationalSnapshot,
    _load_frames,
    _load_payload,
    get_execution_context,
    load_latest_operational_snapshot,
)
from ai_trading_system.ui.execution_api.services.readmodels.rank_snapshot import (
    _enrich_operator_rank_fields,
)
from ai_trading_system.ui.execution_api.services.readmodels.stock_detail import (
    _frame_row_for_symbol,
    _isoformat,
    _lifecycle,
    _rank_position,
    _scalar_or_none,
)


# Columns we recognise as numeric factor inputs to the composite score. We
# map them onto the four-bucket Canvas display (`rs`, `volume`, `trend`,
# `sector`) by name pattern; anything else flows through as ``other``.
_FACTOR_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("rs", re.compile(r"(?:^|_)rs(?:_score)?(?:$|_)|rel_strength", re.IGNORECASE)),
    ("volume", re.compile(r"volume", re.IGNORECASE)),
    ("trend", re.compile(r"trend|stage2|stage_2", re.IGNORECASE)),
    ("sector", re.compile(r"sector", re.IGNORECASE)),
)


def _categorise_factor(column: str) -> str:
    for bucket, pattern in _FACTOR_PATTERNS:
        if pattern.search(column):
            return bucket
    return "other"


def _extract_factor_block(row: dict[str, Any]) -> dict[str, Any]:
    """Collapse all numeric ``*_score`` / factor columns into Canvas buckets.

    Each bucket holds the *first* numeric value we found whose column name
    matched its regex, plus ``contributors`` listing every column that fell
    into the bucket. Non-numeric values are dropped.
    """

    buckets: dict[str, dict[str, Any]] = {}
    for column, value in row.items():
        if column in ("symbol_id", "exchange", "rank", "composite_score", "sector_name"):
            continue
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if pd.isna(numeric):
            continue
        bucket = _categorise_factor(column)
        slot = buckets.setdefault(bucket, {"value": numeric, "contributors": []})
        slot["contributors"].append({"column": column, "value": numeric})
    # Keep the slot value as the *max* contributor — gives the bar a
    # representative magnitude without averaging away signal.
    for bucket, slot in buckets.items():
        slot["value"] = max((c["value"] for c in slot["contributors"]), default=slot["value"])
    return buckets


def _first_present(row: dict[str, Any], names: list[str]) -> Any:
    for name in names:
        value = row.get(name)
        if value is not None and not pd.isna(value):
            return value
    return None


def _operator_context(
    rank_row: Optional[dict[str, Any]],
    pattern_row: Optional[dict[str, Any]],
) -> dict[str, Any]:
    row = rank_row or {}
    pattern = pattern_row or {}
    stage_label = _first_present(row, ["stage_label", "weekly_stage_label", "stage2_label"])
    stage_transition = _first_present(row, ["stage_transition", "weekly_stage_transition"])
    top_pattern_family = _first_present(row, ["top_pattern_family"]) or _first_present(pattern, ["pattern_family", "setup_family"])
    top_pattern_state = _first_present(row, ["top_pattern_state"]) or _first_present(pattern, ["pattern_state"])
    reasons: list[str] = []
    for label, column in [
        ("Momentum acceleration", "momentum_acceleration_score"),
        ("Relative strength", "rel_strength_score"),
        ("Sector strength", "sector_strength_score"),
    ]:
        value = _scalar_or_none(row.get(column))
        if isinstance(value, (int, float)) and value >= 75:
            reasons.append(f"{label} is strong at {value:.1f}.")
    penalty = _scalar_or_none(row.get("exhaustion_penalty"))
    if isinstance(penalty, (int, float)) and penalty > 0:
        reasons.append(f"Exhaustion penalty applied: {penalty:.1f}.")
    pivot_distance = _scalar_or_none(row.get("distance_from_pivot_atr"))
    if isinstance(pivot_distance, (int, float)) and pivot_distance >= 2:
        reasons.append(f"Extended from pivot by {pivot_distance:.1f} ATR.")
    if top_pattern_family:
        reasons.append(f"Top setup: {top_pattern_family} ({top_pattern_state or 'state unknown'}).")
    return {
        "stage_label": _scalar_or_none(stage_label),
        "stage_transition": _scalar_or_none(stage_transition),
        "bars_in_stage": _scalar_or_none(row.get("bars_in_stage")),
        "stage_entry_date": _scalar_or_none(row.get("stage_entry_date")),
        "stage_freshness_bucket": _scalar_or_none(row.get("stage_freshness_bucket")),
        "momentum_acceleration_score": _scalar_or_none(row.get("momentum_acceleration_score")),
        "exhaustion_penalty": _scalar_or_none(row.get("exhaustion_penalty")),
        "exhaustion_flag": _scalar_or_none(row.get("exhaustion_flag")),
        "distance_from_pivot_atr": _scalar_or_none(row.get("distance_from_pivot_atr")),
        "top_pattern_family": _scalar_or_none(top_pattern_family),
        "top_pattern_state": _scalar_or_none(top_pattern_state),
        "top_pattern_setup_quality": _scalar_or_none(_first_present(row, ["top_pattern_setup_quality"]) or _first_present(pattern, ["setup_quality"])),
        "top_pattern_pivot_price": _scalar_or_none(_first_present(row, ["top_pattern_pivot_price"]) or _first_present(pattern, ["pivot_price", "breakout_level"])),
        "top_pattern_invalidation_price": _scalar_or_none(_first_present(row, ["top_pattern_invalidation_price"]) or _first_present(pattern, ["invalidation_price"])),
        "reclaim_signal_flag": bool(row.get("reclaim_signal_flag") or top_pattern_family == "stage2_reclaim"),
        "explanation": reasons,
    }


def _decision_from_category(category: Optional[str]) -> dict[str, Any]:
    """Translate stock_scan category into the Canvas decision pill."""

    if not category:
        return {"verdict": None, "confidence": None, "reason": None}
    norm = str(category).upper()
    if norm.startswith("BUY"):
        return {
            "verdict": "BUY CANDIDATE",
            "confidence": "HIGH" if norm == "BUY" else "MEDIUM",
            "reason": "Stock-scan category marks this as a buy candidate.",
        }
    if norm.startswith("WATCH"):
        return {
            "verdict": "HOLD / WATCH",
            "confidence": "MEDIUM",
            "reason": "Stock-scan category marks this as a watchlist candidate.",
        }
    if norm.startswith("BLOCK") or norm.startswith("REJECT"):
        return {
            "verdict": "REJECT",
            "confidence": "HIGH",
            "reason": "Stock-scan category blocks execution for this symbol.",
        }
    return {"verdict": norm, "confidence": None, "reason": None}


# ---------------------------------------------------------------------------
# Locating snapshots by run_id
# ---------------------------------------------------------------------------


def _resolve_rank_attempt_dir(
    ctx: ExecutionContext, run_id: str
) -> Optional[Path]:
    """Most recent ``rank/attempt_*/`` directory for ``run_id``, or ``None``."""

    run_root = ctx.pipeline_runs_dir / run_id / "rank"
    if not run_root.exists():
        return None
    attempts = sorted(
        (p for p in run_root.glob("attempt_*") if p.is_dir()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return attempts[0] if attempts else None


def _load_snapshot_for_run(
    ctx: ExecutionContext, run_id: str
) -> Optional[LatestOperationalSnapshot]:
    """Load a ``LatestOperationalSnapshot`` pinned to ``run_id``."""

    rank_dir = _resolve_rank_attempt_dir(ctx, run_id)
    if rank_dir is None:
        return None
    payload_path = rank_dir / "dashboard_payload.json"
    payload = _load_payload(payload_path) if payload_path.exists() else {}
    frames = _load_frames(rank_dir)
    return LatestOperationalSnapshot(
        context=ctx,
        payload_path=payload_path if payload_path.exists() else None,
        rank_attempt_dir=rank_dir,
        payload=payload,
        frames=frames,
    )


# ---------------------------------------------------------------------------
# /ranking/{symbol}
# ---------------------------------------------------------------------------


def get_ranking_detail(
    project_root: str | Path | None,
    symbol: str,
    *,
    run_id: Optional[str] = None,
) -> dict[str, Any]:
    """Return the full ranking detail payload for ``symbol``.

    When ``run_id`` is supplied, the response is pinned to that run; otherwise
    the latest operational snapshot is used. The shape stays identical so the
    UI can render either case.
    """

    ctx = get_execution_context(project_root)

    if run_id is not None:
        snap = _load_snapshot_for_run(ctx, run_id)
        if snap is None:
            return {
                "available": False,
                "symbol": symbol,
                "run_id": run_id,
                "ranking": None,
                "lifecycle": _lifecycle(
                    rank_pos=None,
                    universe_size=0,
                    breakout_row=None,
                    pattern_row=None,
                    stock_scan_row=None,
                ),
                "decision": _decision_from_category(None),
                "factors": {},
                "operator_context": _operator_context(None, None),
                "raw_row": None,
            }
    else:
        snap = load_latest_operational_snapshot(project_root)
        # Best effort to infer the run_id from the artifact path.
        if snap.rank_attempt_dir is not None:
            try:
                run_id = snap.rank_attempt_dir.parts[-3]
            except IndexError:
                run_id = None

    ranked = snap.frames.get("ranked_signals", pd.DataFrame())
    breakouts = snap.frames.get("breakout_scan", pd.DataFrame())
    patterns = snap.frames.get("pattern_scan", pd.DataFrame())
    ranked = _enrich_operator_rank_fields(ranked, patterns)
    stock_scan = snap.frames.get("stock_scan", pd.DataFrame())
    sectors = snap.frames.get("sector_dashboard", pd.DataFrame())

    rank_row = _frame_row_for_symbol(ranked, symbol)
    breakout_row = _frame_row_for_symbol(breakouts, symbol)
    pattern_row = _frame_row_for_symbol(patterns, symbol)
    scan_row = _frame_row_for_symbol(stock_scan, symbol)
    rank_pos = _rank_position(ranked, symbol)
    universe_size = int(len(ranked.index)) if ranked is not None else 0

    if rank_row is None and scan_row is None:
        return {
            "available": False,
            "symbol": symbol,
            "run_id": run_id,
            "ranking": None,
            "lifecycle": _lifecycle(
                rank_pos=None,
                universe_size=universe_size,
                breakout_row=None,
                pattern_row=None,
                stock_scan_row=None,
            ),
            "decision": _decision_from_category(None),
            "factors": {},
            "operator_context": _operator_context(None, None),
            "raw_row": None,
        }

    ranking_block = {
        "rank_position": rank_pos,
        "universe_size": universe_size,
        "composite_score": _scalar_or_none((rank_row or {}).get("composite_score")),
        "sector_name": _scalar_or_none((rank_row or {}).get("sector_name")),
        "category": _scalar_or_none((scan_row or {}).get("category")),
        "in_breakout_scan": breakout_row is not None,
        "in_pattern_scan": pattern_row is not None,
        "stage_label": _scalar_or_none(_first_present(rank_row or {}, ["stage_label", "weekly_stage_label"])),
        "stage_transition": _scalar_or_none(_first_present(rank_row or {}, ["stage_transition", "weekly_stage_transition"])),
        "bars_in_stage": _scalar_or_none((rank_row or {}).get("bars_in_stage")),
        "stage_entry_date": _scalar_or_none((rank_row or {}).get("stage_entry_date")),
    }

    lifecycle = _lifecycle(
        rank_pos=rank_pos,
        universe_size=universe_size,
        breakout_row=breakout_row,
        pattern_row=pattern_row,
        stock_scan_row=scan_row,
    )

    decision = _decision_from_category((scan_row or {}).get("category"))

    sector_name = ranking_block["sector_name"]
    sector_context: Optional[dict[str, Any]] = None
    if sector_name:
        sector_row = _frame_row_for_symbol(
            sectors.rename(columns={"Sector": "symbol_id"}) if "Sector" in sectors.columns else sectors,
            sector_name,
        )
        if sector_row is not None:
            sector_context = {k: _scalar_or_none(v) for k, v in sector_row.items()}

    factors = _extract_factor_block(rank_row or {})

    return {
        "available": True,
        "symbol": symbol,
        "run_id": run_id,
        "ranking": ranking_block,
        "lifecycle": lifecycle,
        "decision": decision,
        "factors": factors,
        "sector_context": sector_context,
        "breakout_row": breakout_row,
        "pattern_row": pattern_row,
        "operator_context": _operator_context(rank_row, pattern_row),
        "raw_row": rank_row,
    }


# ---------------------------------------------------------------------------
# /ranking/{symbol}/history
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _HistoricalRunRef:
    run_id: str
    rank_attempt_dir: Path
    mtime: float


def _walk_historical_runs(ctx: ExecutionContext) -> list[_HistoricalRunRef]:
    """All ``rank/attempt_*/`` dirs under ``pipeline_runs/`` newest-first."""

    if not ctx.pipeline_runs_dir.exists():
        return []
    refs: list[_HistoricalRunRef] = []
    for run_root in ctx.pipeline_runs_dir.iterdir():
        if not run_root.is_dir():
            continue
        rank_root = run_root / "rank"
        if not rank_root.exists():
            continue
        attempts = [p for p in rank_root.glob("attempt_*") if p.is_dir()]
        if not attempts:
            continue
        # One entry per run — the most recent attempt represents the run's
        # final state. Earlier attempts inflate the history without adding
        # signal.
        latest = max(attempts, key=lambda p: p.stat().st_mtime)
        refs.append(
            _HistoricalRunRef(
                run_id=run_root.name,
                rank_attempt_dir=latest,
                mtime=latest.stat().st_mtime,
            )
        )
    refs.sort(key=lambda r: r.mtime, reverse=True)
    return refs


def get_ranking_history(
    project_root: str | Path | None,
    symbol: str,
    *,
    limit: int = 20,
) -> dict[str, Any]:
    """Return the most recent ``limit`` runs' rank position for ``symbol``.

    Output is ordered newest-first. Each entry has:

    .. code-block:: json

      {
        "run_id": "pipeline-2026-04-10-...",
        "run_date": "2026-04-10",
        "rank_position": 3,
        "composite_score": 88.5
      }

    Runs where the symbol is not present surface as ``rank_position: null``,
    so the UI can still render a continuous timeline with gaps.
    """

    ctx = get_execution_context(project_root)
    refs = _walk_historical_runs(ctx)[:limit]

    history: list[dict[str, Any]] = []
    for ref in refs:
        ranked_path = ref.rank_attempt_dir / "ranked_signals.csv"
        rank_pos: Optional[int] = None
        composite: Optional[float] = None
        if ranked_path.exists():
            try:
                frame = pd.read_csv(ranked_path)
            except Exception:
                frame = pd.DataFrame()
            rank_pos = _rank_position(frame, symbol)
            row = _frame_row_for_symbol(frame, symbol)
            if row is not None:
                composite = _scalar_or_none(row.get("composite_score"))
        history.append(
            {
                "run_id": ref.run_id,
                "run_date": _infer_run_date(ref.run_id),
                "rank_position": rank_pos,
                "composite_score": composite,
                "rank_attempt_mtime": _isoformat(
                    datetime.fromtimestamp(ref.mtime)
                ),
            }
        )

    return {
        "available": ctx.pipeline_runs_dir.exists(),
        "symbol": symbol,
        "history": history,
        "limit": limit,
    }


_RUN_DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _infer_run_date(run_id: str) -> Optional[str]:
    match = _RUN_DATE_PATTERN.search(run_id)
    return match.group(1) if match else None


# ---------------------------------------------------------------------------
# /workspace/snapshot — Control Tower compact payload
# ---------------------------------------------------------------------------


def get_workspace_snapshot_compact(
    project_root: str | Path | None,
    *,
    top_n: int = 3,
) -> dict[str, Any]:
    """Slim payload tailored to the Canvas Control Tower view.

    Includes only what the landing page renders — top-N actions, output
    summary card counts, sector leaders strip, and the trust banner. Heavier
    detail (full ranked tables, full sector heatmap) stays on the existing
    ``/workspace/pipeline`` endpoint.
    """

    snap = load_latest_operational_snapshot(project_root)
    ranked = snap.frames.get("ranked_signals", pd.DataFrame())
    breakouts = snap.frames.get("breakout_scan", pd.DataFrame())
    patterns = snap.frames.get("pattern_scan", pd.DataFrame())
    sectors = snap.frames.get("sector_dashboard", pd.DataFrame())
    stock_scan = snap.frames.get("stock_scan", pd.DataFrame())
    summary = snap.payload.get("summary", {}) if isinstance(snap.payload, dict) else {}

    top_actions: list[dict[str, Any]] = []
    if not ranked.empty and "symbol_id" in ranked.columns:
        for _, row in ranked.head(top_n).iterrows():
            symbol = str(row.get("symbol_id"))
            scan_row = _frame_row_for_symbol(stock_scan, symbol)
            decision = _decision_from_category((scan_row or {}).get("category"))
            top_actions.append(
                {
                    "symbol": symbol,
                    "composite_score": _scalar_or_none(row.get("composite_score")),
                    "sector_name": _scalar_or_none(row.get("sector_name")),
                    "verdict": decision["verdict"],
                    "confidence": decision["confidence"],
                }
            )

    sector_leaders: list[dict[str, Any]] = []
    if not sectors.empty:
        # ``Sector`` column comes capitalised from the producer. We surface
        # the first ``top_n`` rows assuming the producer already sorted them
        # by leadership score.
        for _, row in sectors.head(top_n).iterrows():
            record = {k: _scalar_or_none(v) for k, v in row.to_dict().items()}
            sector_leaders.append(record)

    counts = {
        "ranked": int(len(ranked.index)) if ranked is not None else 0,
        "breakouts": int(len(breakouts.index)) if breakouts is not None else 0,
        "patterns": int(len(patterns.index)) if patterns is not None else 0,
        "sectors": int(len(sectors.index)) if sectors is not None else 0,
    }

    return {
        "available": snap.payload_path is not None,
        "artifact_path": str(snap.payload_path) if snap.payload_path else None,
        "summary": summary,
        "top_actions": top_actions,
        "sector_leaders": sector_leaders,
        "counts": counts,
    }


__all__ = [
    "get_ranking_detail",
    "get_ranking_history",
    "get_workspace_snapshot_compact",
]
