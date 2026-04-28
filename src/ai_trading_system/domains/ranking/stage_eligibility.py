"""Eligibility filter that overlays weekly stage on the daily ranker.

Designed to be called inside the ranker — joins the latest weekly stage
snapshot to a daily candidate frame and returns the filtered subset.

Intentionally pure: takes frames in, returns frames out. The ranker decides
when (and whether) to call it.
"""
from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd


DEFAULT_ALLOWED_STAGES: frozenset[str] = frozenset({"S2"})


def filter_by_stage(
    candidates: pd.DataFrame,
    stage_snapshot: pd.DataFrame,
    *,
    allowed_stages: Iterable[str] = DEFAULT_ALLOWED_STAGES,
    min_confidence: float = 0.6,
    symbol_col: str = "symbol",
    require_snapshot: bool = False,
) -> pd.DataFrame:
    """Return rows of `candidates` whose latest weekly stage is allowed.

    Parameters
    ----------
    candidates : daily ranking frame; must contain `symbol_col`.
    stage_snapshot : output of `stage_store.read_latest_snapshot`.
    allowed_stages : set of accepted labels (default: {S2}).
    min_confidence : drop labels below this confidence.
    require_snapshot : if False (default), candidates with no snapshot pass
        through (ranker stays usable while backfill catches up). If True,
        only symbols present in the snapshot are kept.
    """
    if candidates.empty:
        return candidates

    allowed = frozenset(allowed_stages)
    if stage_snapshot is None or stage_snapshot.empty:
        return candidates if not require_snapshot else candidates.iloc[0:0]

    snap = stage_snapshot[["symbol", "stage_label", "stage_confidence"]].copy()
    snap = snap.rename(columns={"symbol": symbol_col})

    merged = candidates.merge(snap, on=symbol_col, how="left")
    has_label = merged["stage_label"].notna()
    confident = merged["stage_confidence"].fillna(0.0) >= min_confidence
    in_allowed = merged["stage_label"].isin(allowed)

    if require_snapshot:
        mask = has_label & confident & in_allowed
    else:
        # Keep rows that either pass the gate or have no snapshot yet.
        mask = (~has_label) | (confident & in_allowed)

    return merged[mask].drop(columns=["stage_label", "stage_confidence"])


def annotate_with_stage(
    candidates: pd.DataFrame,
    stage_snapshot: pd.DataFrame,
    *,
    symbol_col: str = "symbol",
) -> pd.DataFrame:
    """Left-join stage columns onto `candidates` without filtering."""
    if candidates.empty or stage_snapshot is None or stage_snapshot.empty:
        return candidates.assign(stage_label=pd.NA, stage_confidence=pd.NA)
    snap = stage_snapshot[["symbol", "stage_label", "stage_confidence",
                           "stage_transition"]].copy()
    snap = snap.rename(columns={"symbol": symbol_col})
    return candidates.merge(snap, on=symbol_col, how="left")
