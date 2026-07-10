"""Unambiguous symbol-level and evidence-level Investigator trap metrics."""

from __future__ import annotations

from typing import Any

import pandas as pd


def attach_trap_category(frame: pd.DataFrame | None) -> pd.DataFrame:
    out = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    if out.empty:
        return out
    reason = _text(out, "drop_reason") + " " + _text(out, "move_tag") + " " + _text(out, "verdict")
    price = _num(out, "price_progression_pct").fillna(_num(out, "price_vs_first_trigger_pct"))
    rank = _num(out, "rank_change_20d")
    low_delivery = _bool(out, "low_delivery_flag")
    volume_declining = _bool(out, "volume_ratio_declining")
    category = pd.Series("Trap evidence", index=out.index, dtype=object)
    category = category.mask(reason.str.contains("ONE_CANDLE|OPERATOR|SPIKE", case=False, na=False), "One-day spike")
    category = category.mask(price.lt(0).fillna(False), "Price fade")
    category = category.mask(rank.gt(25).fillna(False), "Rank collapse")
    category = category.mask(volume_declining, "Volume not sustaining")
    category = category.mask(
        low_delivery | reason.str.contains("LOW_DELIVERY|ILLIQUID|LIQUIDITY", case=False, na=False),
        "Low delivery / liquidity",
    )
    out.loc[:, "trap_category"] = category
    return out


def build_trap_summary_metrics(
    *,
    current_traps: pd.DataFrame | None,
    archive: pd.DataFrame | None,
    run_date: str,
    candidate_union_rows: int,
) -> dict[str, Any]:
    current = attach_trap_category(
        _normalise_symbols(
            current_traps.copy() if isinstance(current_traps, pd.DataFrame) else pd.DataFrame()
        )
    )
    historical = attach_trap_category(
        _normalise_symbols(archive.copy() if isinstance(archive, pd.DataFrame) else pd.DataFrame())
    )
    current = _deduplicate_columns(current)
    historical = _deduplicate_columns(historical)
    unique_traps = int(current["symbol_id"].nunique()) if not current.empty else 0

    if current.empty:
        fresh_traps = 0
    else:
        evidence_date = _evidence_date(current)
        fresh_traps = int(current.loc[evidence_date.eq(str(run_date)), "symbol_id"].nunique()) if run_date else 0

    evidence = pd.concat(
        [_evidence_events(current), _evidence_events(historical)],
        ignore_index=True,
        sort=False,
    )
    if evidence.empty:
        evidence_events = 0
    else:
        evidence_events = int(
            evidence[["symbol_id", "_trap_category", "_evidence_date"]]
            .drop_duplicates()
            .shape[0]
        )

    candidate_rows = max(0, int(candidate_union_rows or 0))
    raw_rate = (unique_traps / candidate_rows) if candidate_rows > 0 else 0.0
    errors: list[str] = []
    if unique_traps > candidate_rows:
        errors.append("unique_trap_symbols exceeds candidate_union_rows")
    if fresh_traps > candidate_rows:
        errors.append("fresh_trap_symbols_today exceeds candidate_union_rows")
    if evidence_events < unique_traps:
        errors.append("trap_evidence_events is below unique_trap_symbols")
    if not 0.0 <= raw_rate <= 1.0:
        errors.append("trap_candidate_rate outside [0, 1]")
    rate = min(1.0, max(0.0, float(raw_rate)))
    return {
        "unique_trap_symbols": unique_traps,
        "fresh_trap_symbols_today": fresh_traps,
        "trap_evidence_events": evidence_events,
        "trap_candidate_rate": round(rate, 6),
        "trap_summary_valid": not errors,
        "trap_summary_validation_errors": errors,
        "trap_count": unique_traps,
        "fresh_trap_today": fresh_traps,
        "trap_evidence_count": evidence_events,
        "trap_rate": round(rate, 6),
    }


def _normalise_symbols(frame: pd.DataFrame) -> pd.DataFrame:
    frame = _deduplicate_columns(frame)
    if frame.empty or "symbol_id" not in frame.columns:
        return pd.DataFrame(columns=["symbol_id", *frame.columns])
    out = frame.copy()
    out.loc[:, "symbol_id"] = out["symbol_id"].fillna("").astype(str).str.strip().str.upper()
    return out.loc[out["symbol_id"].ne("")].reset_index(drop=True)


def _deduplicate_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep the first occurrence of each label using positional selection.

    Pandas label-based selection can expand a duplicated label back into more
    than one column, so trap metrics deliberately select by integer position.
    """
    seen: set[Any] = set()
    keep: list[int] = []
    for position, column in enumerate(frame.columns):
        if column in seen:
            continue
        seen.add(column)
        keep.append(position)
    return frame.iloc[:, keep].copy()


def _evidence_date(frame: pd.DataFrame) -> pd.Series:
    out = pd.Series("UNKNOWN", index=frame.index, dtype=object)
    for column in ("trade_date", "last_seen_date", "archived_at", "created_at", "first_seen_date"):
        if column not in frame.columns:
            continue
        values = pd.to_datetime(frame[column], errors="coerce").dt.date.astype("string")
        usable = values.notna() & out.eq("UNKNOWN")
        out.loc[usable] = values.loc[usable].astype(str)
    return out


def _evidence_events(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["symbol_id", "_trap_category", "_evidence_date"])
    return pd.DataFrame(
        {
            "symbol_id": frame["symbol_id"].to_numpy(copy=False),
            "_trap_category": _text(frame, "trap_category")
            .replace("", "Trap evidence")
            .to_numpy(copy=False),
            "_evidence_date": _evidence_date(frame).to_numpy(copy=False),
        }
    )


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series("", index=frame.index, dtype=object)
    return frame[column].fillna("").astype(str)


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _bool(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].astype("string").str.lower().isin({"true", "1", "yes", "y"}).fillna(False)
