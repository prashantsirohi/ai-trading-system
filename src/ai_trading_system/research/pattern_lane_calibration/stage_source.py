"""Governed weekly-stage source contract.

Primary source is the governed observation store
``control_plane.duckdb::weekly_stock_stage_history`` plus the append-only
``weekly_stage_backfill_observation`` table written by the historical
backfill. The legacy ``ohlcv.duckdb::weekly_stage_snapshot`` table is a
compatibility fallback only: a snapshot row is admitted solely for a
(symbol, week_end) pair that has no governed observation, and every admitted
row is stamped ``stage_source_fallback_used = True``. Sources are never
combined silently — every loaded observation carries ``stage_source``.

Two source modes serve two different consumers:

- ``governed_current`` (live/R1a reads): precedence per (symbol, exchange,
  week_end) is ``governed_live`` (locked over provisional, later ``as_of``
  wins) > ``governed_backfill`` > ``snapshot_fallback`` (only when the
  governed sources have no row for the pair).
- ``frozen_backfill`` (historical calibration): backfill rows only, a single
  ``stage_policy_version`` (and optionally a single ``backfill_run_id``).
  Live rows never override backfilled rows in the analytical dataset; use
  ``reconcile_backfill_vs_live`` to surface differences as conflicts. No
  snapshot fallback.

Current stage and transition are separate facts and stay separate columns:
``stage_label`` is the current stage only (``S1``..``S4``/``UNDEFINED``),
``stage_transition`` carries the normalised transition (``S1_TO_S2``, ...,
``NONE``) and ``stage_transition_as_of`` the week that recorded it. The
production provisional label ``transition_1_to_2`` denotes a week whose
current classification is S2 arrived from a locked S1, so it normalises to
``stage_label = S2`` with ``stage_transition = S1_TO_S2``. Raw labels are
preserved in ``stage_label_raw``.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

GOVERNED_LIVE = "governed_live"
GOVERNED_BACKFILL = "governed_backfill"
SNAPSHOT_FALLBACK = "snapshot_fallback"

MODE_GOVERNED_CURRENT = "governed_current"
MODE_FROZEN_BACKFILL = "frozen_backfill"

BACKFILL_TABLE = "weekly_stage_backfill_observation"

_LABEL_MAP = {
    "STAGE_1_BASING": "S1",
    "STAGE_2_ADVANCING": "S2",
    "STAGE_3_TOPPING": "S3",
    "STAGE_4_DECLINING": "S4",
    # Provisional transition labels denote the week's *current* stage having
    # moved from the previous locked stage; the current stage is the target.
    "TRANSITION_1_TO_2": "S2",
    "TRANSITION_2_TO_3": "S3",
    "TRANSITION_3_TO_4": "S4",
    "TRANSITION_4_TO_1": "S1",
    "UNKNOWN": "UNDEFINED",
    "S1": "S1", "S2": "S2", "S3": "S3", "S4": "S4",
    "UNDEFINED": "UNDEFINED",
}

_TRANSITION_MAP = {
    "STAGE_1_BASING_TO_STAGE_2_ADVANCING": "S1_TO_S2",
    "STAGE_2_ADVANCING_TO_STAGE_3_TOPPING": "S2_TO_S3",
    "STAGE_3_TOPPING_TO_STAGE_4_DECLINING": "S3_TO_S4",
    "STAGE_4_DECLINING_TO_STAGE_1_BASING": "S4_TO_S1",
    "TRANSITION_1_TO_2": "S1_TO_S2",
    "TRANSITION_2_TO_3": "S2_TO_S3",
    "TRANSITION_3_TO_4": "S3_TO_S4",
    "TRANSITION_4_TO_1": "S4_TO_S1",
    "S1_TO_S2": "S1_TO_S2", "S2_TO_S3": "S2_TO_S3",
    "S3_TO_S4": "S3_TO_S4", "S4_TO_S1": "S4_TO_S1",
}

OUTPUT_COLUMNS = [
    "symbol", "exchange", "week_end_date", "stage_label", "stage_label_raw",
    "stage_transition", "stage_transition_as_of", "stage_confidence",
    "stage_source", "stage_observation_id", "stage_policy_version",
    "stage_as_of_date", "stage_source_fallback_used",
]


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()[0])


def normalize_stage_label(raw_label: object) -> str:
    """Normalise a raw label to the current-stage S-vocabulary only."""
    text = str(raw_label or "").strip().upper()
    # A raw label that itself is a transition label carries the transition in
    # normalize_stage_transition; here it resolves to the current stage.
    return _LABEL_MAP.get(text, "UNDEFINED")


def normalize_stage_transition(raw_transition: object, *, raw_label: object = None) -> str:
    """Normalise the recorded transition; the raw label may itself carry it."""
    for candidate in (raw_transition, raw_label):
        text = str(candidate or "").strip().upper()
        for marker, normalised in _TRANSITION_MAP.items():
            if marker in text:
                return normalised
    return "NONE"


def _load_governed_live(control_plane_db: Path, *, through_date: str) -> pd.DataFrame:
    conn = duckdb.connect(str(control_plane_db), read_only=True)
    try:
        if not _table_exists(conn, "weekly_stock_stage_history"):
            return pd.DataFrame()
        frame = conn.execute(
            """
            SELECT symbol_id AS symbol, exchange,
                   CAST(source_week_end AS DATE) AS week_end_date,
                   effective_stage AS stage_label_raw,
                   observation_json,
                   stage_status,
                   observation_id AS stage_observation_id,
                   classifier_version AS stage_policy_version,
                   CAST(as_of AS DATE) AS stage_as_of_date
            FROM weekly_stock_stage_history
            WHERE CAST(source_week_end AS DATE) <= CAST(? AS DATE)
            ORDER BY symbol_id, source_week_end, as_of
            """,
            [through_date],
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        return frame
    def _json_field(payload: object, key: str) -> object:
        try:
            return json.loads(payload).get(key)
        except (TypeError, ValueError):
            return None
    frame["stage_transition"] = [_json_field(p, "stage_transition") for p in frame["observation_json"]]
    frame["stage_confidence"] = [_json_field(p, "stage_confidence_score") for p in frame["observation_json"]]
    frame = frame.drop(columns=["observation_json"])
    frame["_locked"] = (frame["stage_status"].astype(str) == "locked").astype(int)
    frame = (
        frame.sort_values(["symbol", "week_end_date", "_locked", "stage_as_of_date"], kind="mergesort")
        .drop_duplicates(subset=["symbol", "exchange", "week_end_date"], keep="last")
        .drop(columns=["_locked", "stage_status"])
    )
    frame["stage_source"] = GOVERNED_LIVE
    return frame


def _load_governed_backfill(
    control_plane_db: Path, *, through_date: str, backfill_run_id: str | None = None
) -> pd.DataFrame:
    conn = duckdb.connect(str(control_plane_db), read_only=True)
    try:
        if not _table_exists(conn, BACKFILL_TABLE):
            return pd.DataFrame()
        run_clause = "AND backfill_run_id = ?" if backfill_run_id else ""
        params: list[object] = [through_date]
        if backfill_run_id:
            params.append(backfill_run_id)
        frame = conn.execute(
            f"""
            SELECT symbol_id AS symbol, exchange,
                   CAST(week_end AS DATE) AS week_end_date,
                   stage_label AS stage_label_raw,
                   stage_transition,
                   stage_score AS stage_confidence,
                   observation_id AS stage_observation_id,
                   stage_policy_version,
                   CAST(week_end AS DATE) AS stage_as_of_date
            FROM {BACKFILL_TABLE}
            WHERE CAST(week_end AS DATE) <= CAST(? AS DATE)
            {run_clause}
            ORDER BY symbol_id, week_end
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame = frame.drop_duplicates(subset=["symbol", "exchange", "week_end_date"], keep="last")
    frame["stage_source"] = GOVERNED_BACKFILL
    return frame


def _load_snapshot(ohlcv_db: Path, *, through_date: str) -> pd.DataFrame:
    conn = duckdb.connect(str(ohlcv_db), read_only=True)
    try:
        if not _table_exists(conn, "weekly_stage_snapshot"):
            return pd.DataFrame()
        frame = conn.execute(
            """
            SELECT symbol, CAST(week_end_date AS DATE) AS week_end_date,
                   stage_label AS stage_label_raw,
                   stage_transition,
                   stage_confidence,
                   CAST(week_end_date AS DATE) AS stage_as_of_date
            FROM weekly_stage_snapshot
            WHERE CAST(week_end_date AS DATE) <= CAST(? AS DATE)
            ORDER BY symbol, week_end_date
            """,
            [through_date],
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame["exchange"] = "NSE"
    frame["stage_observation_id"] = (
        "snapshot:" + frame["symbol"].astype(str) + ":" + frame["week_end_date"].astype(str)
    )
    frame["stage_policy_version"] = "weekly_stage_snapshot:unversioned"
    frame["stage_source"] = SNAPSHOT_FALLBACK
    return frame


def _finalize(combined: pd.DataFrame) -> pd.DataFrame:
    raw_transitions = combined["stage_transition"] if "stage_transition" in combined.columns else [None] * len(combined)
    normalised_transitions = [
        normalize_stage_transition(transition, raw_label=raw)
        for raw, transition in zip(combined["stage_label_raw"], raw_transitions)
    ]
    combined = combined.copy()
    combined["stage_label"] = [normalize_stage_label(raw) for raw in combined["stage_label_raw"]]
    combined["stage_transition"] = normalised_transitions
    combined["week_end_date"] = pd.to_datetime(combined["week_end_date"])
    combined["stage_transition_as_of"] = combined["week_end_date"].where(
        combined["stage_transition"] != "NONE", pd.NaT
    )
    combined["stage_source_fallback_used"] = combined["stage_source"] == SNAPSHOT_FALLBACK
    combined["stage_as_of_date"] = pd.to_datetime(combined["stage_as_of_date"])
    return combined[OUTPUT_COLUMNS].sort_values(["symbol", "week_end_date"], kind="mergesort").reset_index(drop=True)


def _require_policy(frame: pd.DataFrame, required: str | None, *, strict: bool) -> pd.DataFrame:
    if required is None or frame.empty:
        return frame
    mismatched = frame.loc[frame["stage_policy_version"].astype(str) != str(required)]
    if mismatched.empty:
        return frame
    if strict:
        versions = sorted(mismatched["stage_policy_version"].astype(str).unique())
        raise RuntimeError(
            f"stage policy mismatch: required {required}, found {versions} "
            f"in {int(len(mismatched))} governed rows"
        )
    return frame


def load_weekly_stage_observations(
    *,
    control_plane_db: Path | str,
    ohlcv_db: Path | str,
    through_date: str,
    mode: str = MODE_GOVERNED_CURRENT,
    require_stage_policy_version: str | None = None,
    backfill_run_id: str | None = None,
    allow_snapshot_fallback: bool = True,
) -> pd.DataFrame:
    """Load provenance-stamped weekly-stage observations up to ``through_date``."""
    if mode == MODE_FROZEN_BACKFILL:
        backfill = _load_governed_backfill(
            Path(control_plane_db), through_date=through_date, backfill_run_id=backfill_run_id
        )
        if backfill.empty:
            raise RuntimeError(
                f"frozen_backfill mode found no rows in {BACKFILL_TABLE}"
                + (f" for run {backfill_run_id}" if backfill_run_id else "")
            )
        backfill = _require_policy(backfill, require_stage_policy_version, strict=True)
        return _finalize(backfill)
    if mode != MODE_GOVERNED_CURRENT:
        raise ValueError(f"unknown weekly-stage source mode: {mode}")

    live = _require_policy(
        _load_governed_live(Path(control_plane_db), through_date=through_date),
        require_stage_policy_version, strict=False,
    )
    backfill = _require_policy(
        _load_governed_backfill(Path(control_plane_db), through_date=through_date),
        require_stage_policy_version, strict=False,
    )
    governed_frames = [live, backfill]
    governed = pd.concat([f for f in governed_frames if not f.empty], ignore_index=True) \
        if any(not f.empty for f in governed_frames) else pd.DataFrame()
    if not governed.empty:
        source_rank = {GOVERNED_LIVE: 1, GOVERNED_BACKFILL: 0}
        governed["_rank"] = governed["stage_source"].map(source_rank)
        governed = (
            governed.sort_values(["symbol", "week_end_date", "_rank"], kind="mergesort")
            .drop_duplicates(subset=["symbol", "exchange", "week_end_date"], keep="last")
            .drop(columns=["_rank"])
        )

    frames = [governed] if not governed.empty else []
    if allow_snapshot_fallback:
        snapshot = _load_snapshot(Path(ohlcv_db), through_date=through_date)
        if not snapshot.empty:
            if governed.empty:
                frames = [snapshot]
            else:
                governed_keys = set(zip(governed["symbol"], governed["week_end_date"]))
                admitted = snapshot.loc[
                    [
                        (symbol, week) not in governed_keys
                        for symbol, week in zip(snapshot["symbol"], snapshot["week_end_date"])
                    ]
                ]
                if not admitted.empty:
                    frames.append(admitted)

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    return _finalize(pd.concat(frames, ignore_index=True))


def reconcile_backfill_vs_live(
    *, control_plane_db: Path | str, through_date: str
) -> pd.DataFrame:
    """Differences between backfilled and live governed rows on shared weeks.

    Used with ``frozen_backfill`` mode: live rows never replace backfilled
    rows in the analytical dataset, so disagreements are surfaced here.
    """
    live = _load_governed_live(Path(control_plane_db), through_date=through_date)
    backfill = _load_governed_backfill(Path(control_plane_db), through_date=through_date)
    if live.empty or backfill.empty:
        return pd.DataFrame(columns=[
            "symbol", "exchange", "week_end_date", "backfill_stage", "live_stage",
        ])
    merged = backfill.merge(
        live, on=["symbol", "exchange", "week_end_date"], how="inner", suffixes=("_backfill", "_live")
    )
    merged["backfill_stage"] = [normalize_stage_label(v) for v in merged["stage_label_raw_backfill"]]
    merged["live_stage"] = [normalize_stage_label(v) for v in merged["stage_label_raw_live"]]
    conflicts = merged.loc[merged["backfill_stage"] != merged["live_stage"]]
    return conflicts[["symbol", "exchange", "week_end_date", "backfill_stage", "live_stage"]].reset_index(drop=True)


def annotate_stage_age(
    observations: pd.DataFrame,
    *,
    as_of_date: str,
    exchange_sessions: pd.DatetimeIndex,
) -> pd.DataFrame:
    """Return the latest observation per symbol at ``as_of_date`` with trading-day age."""
    as_of = pd.Timestamp(as_of_date).normalize()
    scoped = observations.loc[observations["week_end_date"] <= as_of].copy()
    if scoped.empty:
        return scoped.assign(stage_age_trading_days=pd.Series(dtype=int))
    latest = (
        scoped.sort_values(["symbol", "week_end_date"], kind="mergesort")
        .drop_duplicates(subset=["symbol"], keep="last")
        .copy()
    )
    sessions = exchange_sessions.sort_values()
    as_of_position = sessions.searchsorted(as_of, side="right")
    ages = []
    for week_end in latest["week_end_date"]:
        week_position = sessions.searchsorted(pd.Timestamp(week_end).normalize(), side="right")
        ages.append(int(as_of_position - week_position))
    latest["stage_age_trading_days"] = ages
    return latest
