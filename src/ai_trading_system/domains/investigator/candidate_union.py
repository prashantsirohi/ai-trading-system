"""Deterministic Investigator candidate admission and source attribution."""

from __future__ import annotations

from typing import Any

import pandas as pd


SOURCE_PRIORITY = (
    "DAILY_GAINER",
    "WEEKLY_GAINER",
    "STEALTH_ACCUMULATION",
    "STAGE1_SCAN",
    "EARLY_ACCUMULATION",
    "PREVIOUS_WATCHLIST",
    "BREAKOUT_CONTEXT",
    "RANK_CONTEXT",
    "STOCK_SCAN_CONTEXT",
)
ADMISSION_SOURCES = frozenset(SOURCE_PRIORITY[:6])
CURRENT_ADMISSION_SOURCES = frozenset(SOURCE_PRIORITY[:5])
ACTIVE_STATUSES = frozenset({"NEW_TRIGGER", "TRACKING", "ACTIVE_RESEARCH", "HIGH_CONVICTION", "WATCHLIST"})


def build_candidate_union(
    *,
    event_intake: pd.DataFrame,
    early_accumulation: pd.DataFrame,
    previous_watchlist: pd.DataFrame | None = None,
    ranked: pd.DataFrame | None = None,
    stock_scan: pd.DataFrame | None = None,
    breakout_scan: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Union admission sources and attach matched context without context-only admission."""

    event = _normalise(event_intake)
    early = _normalise(early_accumulation)
    early_context = early.copy()
    if (
        "stage1_maturity_score" in early.columns
        and pd.to_numeric(early["stage1_maturity_score"], errors="coerce").notna().any()
        and "stage1_eligible" in early.columns
    ):
        early = early.loc[_boolish(early, "stage1_eligible")].reset_index(drop=True)
    previous = eligible_previous_watchlist(previous_watchlist)
    ranked_context = _normalise(ranked)
    stock_context = _normalise(stock_scan)
    breakout_context = _normalise(breakout_scan)

    event_sources = _event_sources(event)
    early_symbols = _symbols(early)
    previous_symbols = _symbols(previous)
    seed_symbols = set(event_sources) | early_symbols | previous_symbols

    frames = {
        "event": _collapse(event),
        # All Stage-1 rows remain enrichments for independently admitted event
        # candidates; only eligible rows are permitted to admit by STAGE1_SCAN.
        "early": _collapse(early_context),
        "stock": _collapse(stock_context),
        "ranked": _collapse(ranked_context),
        "breakout": _collapse(breakout_context),
        "previous": _collapse(previous),
    }
    lookups = {name: _records_by_symbol(frame) for name, frame in frames.items()}
    rows: list[dict[str, Any]] = []
    source_counts = {source: 0 for source in SOURCE_PRIORITY}

    for symbol in sorted(seed_symbols):
        # Current observations and contexts win; the prior watchlist is fallback only.
        row = _coalesce_records(
            lookups["event"].get(symbol),
            lookups["early"].get(symbol),
            lookups["stock"].get(symbol),
            lookups["ranked"].get(symbol),
            lookups["breakout"].get(symbol),
            lookups["previous"].get(symbol),
        )
        row["symbol_id"] = symbol
        sources = set(event_sources.get(symbol, ()))
        if symbol in early_symbols:
            eligible = lookups["early"].get(symbol, {}).get("stage1_eligible")
            maturity = lookups["early"].get(symbol, {}).get("stage1_maturity_score")
            is_stage1 = pd.notna(pd.to_numeric(pd.Series([maturity]), errors="coerce").iloc[0])
            if is_stage1 and str(eligible).strip().lower() in {"true", "1", "yes", "y"}:
                sources.add("STAGE1_SCAN")
            elif not is_stage1:
                sources.add("EARLY_ACCUMULATION")
        if symbol in previous_symbols:
            sources.add("PREVIOUS_WATCHLIST")
        if symbol in _symbols(breakout_context):
            sources.add("BREAKOUT_CONTEXT")
        if symbol in _symbols(ranked_context):
            sources.add("RANK_CONTEXT")
        if symbol in _symbols(stock_context):
            sources.add("STOCK_SCAN_CONTEXT")
        ordered = [source for source in SOURCE_PRIORITY if source in sources]
        for source in ordered:
            source_counts[source] += 1
        primary = ordered[0] if ordered else ""
        row["candidate_sources"] = "|".join(ordered)
        row["primary_candidate_source"] = primary
        row["candidate_source_count"] = len(ordered)
        row["new_candidate_today"] = symbol not in previous_symbols and bool(sources & CURRENT_ADMISSION_SOURCES)
        if primary in CURRENT_ADMISSION_SOURCES:
            row["trigger_reason"] = primary
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(
            columns=[
                "symbol_id",
                "candidate_sources",
                "primary_candidate_source",
                "candidate_source_count",
                "new_candidate_today",
            ]
        )
    else:
        out = out.sort_values("symbol_id", kind="stable").reset_index(drop=True)

    event_symbols = set(event_sources)
    diagnostics = {
        "candidate_union_rows": int(len(out)),
        "event_candidate_rows": int(len(event_symbols)),
        "early_accumulation_candidate_rows": int(len(early_symbols)),
        "early_accumulation_only_rows": int(
            sum(symbol not in event_symbols and symbol not in previous_symbols for symbol in early_symbols)
        ),
        "previous_watchlist_rows": int(len(previous_symbols)),
        "multi_source_candidate_rows": int(
            pd.to_numeric(out.get("candidate_source_count"), errors="coerce").fillna(0).gt(1).sum()
        ),
        "candidate_source_counts": {key: int(value) for key, value in source_counts.items() if value},
    }
    return out, diagnostics


def eligible_previous_watchlist(frame: pd.DataFrame | None) -> pd.DataFrame:
    """Return active Stage-1 rows that are safe to carry into a new run."""

    out = _normalise(frame)
    if out.empty:
        return out
    status = _text(out, "status").str.upper()
    verdict = _text(out, "verdict").str.upper()
    stage = _text(out, "stage_label").str.upper()
    s1_state = _text(out, "s1_promotion_state").str.upper()
    pattern_lifecycle = _text(out, "pattern_lifecycle_state").str.lower()
    drop_reason = _text(out, "drop_reason")
    sources = _text(out, "candidate_sources").str.upper()
    primary = _text(out, "primary_candidate_source").str.upper()

    safe = (
        (status.eq("") | status.isin(ACTIVE_STATUSES))
        & ~verdict.eq("NOISE_TRAP")
        & ~_boolish(out, "hard_trap_flag")
        & drop_reason.eq("")
        & ~s1_state.isin({"FAILED_S1", "S2_CONFIRMED"})
        & ~pattern_lifecycle.isin({"invalidated", "expired"})
        & ~stage.isin({"STAGE_2_CONFIRMED", "STAGE_3_DISTRIBUTION", "STAGE_4_DECLINE"})
    )
    has_stage_evidence = stage.ne("") | s1_state.ne("") | sources.ne("") | primary.ne("")
    stage1 = (
        stage.isin({"STAGE_1_BASE", "STAGE_2_EARLY"})
        | (s1_state.str.startswith("S1_") & ~s1_state.eq("FAILED_S1"))
        | sources.str.contains(r"(?:^|\|)(?:STAGE1_SCAN|EARLY_ACCUMULATION)(?:\||$)", regex=True)
        | primary.isin({"STAGE1_SCAN", "EARLY_ACCUMULATION"})
        | ~has_stage_evidence
    )
    return _collapse(out.loc[safe & stage1].copy())


def is_trigger_observation(candidate_sources: object) -> bool:
    """Whether a score row represents a current trigger rather than a quiet refresh."""

    sources = {part.strip().upper() for part in str(candidate_sources or "").split("|") if part.strip()}
    return not sources or bool(sources & CURRENT_ADMISSION_SOURCES)


def _normalise(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol_id"])
    out = frame.copy()
    if "symbol_id" not in out.columns:
        for candidate in ("symbol", "Symbol"):
            if candidate in out.columns:
                out.loc[:, "symbol_id"] = out[candidate]
                break
    if "symbol_id" not in out.columns:
        return pd.DataFrame(columns=["symbol_id"])
    out.loc[:, "symbol_id"] = out["symbol_id"].fillna("").astype(str).str.strip().str.upper()
    return out.loc[out["symbol_id"].ne("")].reset_index(drop=True)


def _collapse(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "symbol_id" not in frame.columns:
        return pd.DataFrame(columns=list(frame.columns) if isinstance(frame, pd.DataFrame) else ["symbol_id"])
    safe = frame.copy().reset_index(drop=True)
    safe.loc[:, "_ordinal"] = range(len(safe))
    value_columns = [column for column in safe.columns if column not in {"symbol_id", "_ordinal"}]
    safe.loc[:, "_completeness"] = safe[value_columns].apply(
        lambda row: sum(not _missing(value) for value in row), axis=1
    ) if value_columns else 0
    safe = safe.sort_values(
        ["symbol_id", "_completeness", "_ordinal"],
        ascending=[True, False, True],
        kind="stable",
    )
    rows: list[dict[str, Any]] = []
    for symbol, group in safe.groupby("symbol_id", sort=True):
        record: dict[str, Any] = {"symbol_id": symbol}
        for column in value_columns:
            for value in group[column]:
                if not _missing(value):
                    record[column] = value
                    break
        rows.append(record)
    return pd.DataFrame(rows)


def _event_sources(frame: pd.DataFrame) -> dict[str, tuple[str, ...]]:
    found: dict[str, set[str]] = {}
    if frame.empty:
        return {}
    for _, row in frame.iterrows():
        source = str(row.get("trigger_reason") or "").strip().upper()
        if source not in CURRENT_ADMISSION_SOURCES - {"EARLY_ACCUMULATION", "STAGE1_SCAN"}:
            continue
        found.setdefault(str(row["symbol_id"]), set()).add(source)
    return {
        symbol: tuple(source for source in SOURCE_PRIORITY if source in values)
        for symbol, values in found.items()
    }


def _symbols(frame: pd.DataFrame) -> set[str]:
    if frame is None or frame.empty or "symbol_id" not in frame.columns:
        return set()
    return set(frame["symbol_id"].fillna("").astype(str).str.strip().str.upper().loc[lambda value: value.ne("")])


def _records_by_symbol(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if frame.empty:
        return {}
    return {str(row["symbol_id"]): row.to_dict() for _, row in frame.iterrows()}


def _coalesce_records(*records: dict[str, Any] | None) -> dict[str, Any]:
    output: dict[str, Any] = {}
    columns: list[str] = []
    for record in records:
        if record:
            columns.extend(column for column in record if column not in columns)
    for column in columns:
        for record in records:
            if record and column in record and not _missing(record[column]):
                output[column] = record[column]
                break
    return output


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series("", index=frame.index, dtype=object)
    return frame[column].fillna("").astype(str).str.strip()


def _boolish(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].astype("string").str.lower().isin({"true", "1", "yes", "y"}).fillna(False)


def _missing(value: object) -> bool:
    if value is None:
        return True
    try:
        if bool(pd.isna(value)):
            return True
    except (TypeError, ValueError):
        pass
    return isinstance(value, str) and not value.strip()
