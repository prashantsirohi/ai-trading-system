"""Offline cross-shadow reconciliation: pattern-lane evidence vs registry shadow.

Compares R1a ``pattern_lane_scan.csv`` signals against the opportunity-registry
shadow (``candidate_episode``) for the same run date, **read-only** — it never
writes pattern evidence into the registry and never mutates any store. It
answers whether the lane scan adds genuine *early* discovery or merely confirms
opportunities already found by rank / Investigator.

Categories (per symbol, comparing pattern ``signal_date`` to the registry
``episode_started_at``):

- ``pattern_before_registry``   — pattern led (earliest signal < earliest episode)
- ``same_day_discovery``        — same day
- ``pattern_after_registry``    — pattern confirms (signal > episode)
- ``pattern_only``              — pattern signal, no registry episode
- ``registry_only``             — episode, no pattern signal
- ``suppression_conflict``      — R1a head_shoulders suppression coincides with an OPEN episode
- ``possible_duplicate_episode``— same symbol+setup_family, multiple episode_started_at

The output is an immutable bundle bound by hash to its sources.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.research.pattern_lane_calibration.r0_analysis import _git_commit, _sha256_file

CROSS_SHADOW_POLICY_VERSION = "pattern-cross-shadow-policy-v1"

_EPISODE_COLUMNS = (
    "symbol_id", "exchange", "candidate_id", "episode_started_at",
    "opening_reason", "setup_family", "episode_status",
)


def load_pattern_signals(pattern_lane_csv: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(pattern_lane_csv, dtype=str, keep_default_na=False)
    for col in ("symbol_id", "exchange", "as_of_date", "signal_date",
                "scan_lane_as_of", "pattern_family", "r1a_evidence_class", "evidence_origin"):
        if col not in frame.columns:
            frame[col] = ""
    frame["symbol_id"] = frame["symbol_id"].astype(str).str.upper()
    frame["exchange"] = frame["exchange"].astype(str).str.upper()
    frame["signal_date"] = pd.to_datetime(frame["signal_date"], errors="coerce").dt.date
    return frame


def load_registry_episodes(control_plane_db: str | Path, *, through_date: str) -> pd.DataFrame:
    conn = duckdb.connect(str(control_plane_db), read_only=True)
    try:
        exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'candidate_episode'"
        ).fetchone()[0]
        if not exists:
            return pd.DataFrame(columns=list(_EPISODE_COLUMNS))
        frame = conn.execute(
            """
            SELECT symbol_id, exchange, candidate_id,
                   CAST(episode_started_at AS DATE) AS episode_started_at,
                   opening_reason, setup_family, episode_status
            FROM candidate_episode
            WHERE CAST(episode_started_at AS DATE) <= CAST(? AS DATE)
            ORDER BY symbol_id, episode_started_at
            """,
            [through_date],
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame["symbol_id"] = frame["symbol_id"].astype(str).str.upper()
    frame["exchange"] = frame["exchange"].astype(str).str.upper()
    frame["episode_started_at"] = pd.to_datetime(frame["episode_started_at"], errors="coerce").dt.date
    return frame


def reconcile(signals: pd.DataFrame, episodes: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Split into the seven reconciliation categories."""
    signals = signals.copy()
    episodes = episodes.copy()
    if not signals.empty:
        signals["symbol_id"] = signals["symbol_id"].astype(str).str.upper()
        signals["signal_date"] = pd.to_datetime(signals["signal_date"], errors="coerce").dt.date
    if not episodes.empty:
        episodes["symbol_id"] = episodes["symbol_id"].astype(str).str.upper()
        episodes["episode_started_at"] = pd.to_datetime(episodes["episode_started_at"], errors="coerce").dt.date
    sig_syms = set(signals["symbol_id"]) if not signals.empty else set()
    epi_syms = set(episodes["symbol_id"]) if not episodes.empty else set()

    earliest_sig = (
        signals.groupby("symbol_id")["signal_date"].min() if not signals.empty else pd.Series(dtype="object")
    )
    earliest_epi = (
        episodes.groupby("symbol_id")["episode_started_at"].min() if not episodes.empty else pd.Series(dtype="object")
    )
    # earliest episode's opening_reason/setup_family for lead/lag breakdown
    epi_first = (
        episodes.sort_values(["symbol_id", "episode_started_at"]).drop_duplicates("symbol_id", keep="first")
        if not episodes.empty else episodes
    )
    epi_first_map = epi_first.set_index("symbol_id") if not epi_first.empty else pd.DataFrame()

    before, same, after = [], [], []
    for symbol in sorted(sig_syms & epi_syms):
        sd, ed = earliest_sig.get(symbol), earliest_epi.get(symbol)
        if sd is None or ed is None or pd.isna(sd) or pd.isna(ed):
            continue
        row = {
            "symbol_id": symbol,
            "earliest_signal_date": sd,
            "earliest_episode_started_at": ed,
            "lead_lag_days": (ed - sd).days,
            "opening_reason": epi_first_map.loc[symbol, "opening_reason"] if symbol in epi_first_map.index else None,
            "setup_family": epi_first_map.loc[symbol, "setup_family"] if symbol in epi_first_map.index else None,
            "episode_status": epi_first_map.loc[symbol, "episode_status"] if symbol in epi_first_map.index else None,
        }
        if sd < ed:
            before.append(row)
        elif sd == ed:
            same.append(row)
        else:
            after.append(row)

    pattern_only = signals.loc[~signals["symbol_id"].isin(epi_syms)].copy() if not signals.empty else signals
    registry_only = episodes.loc[~episodes["symbol_id"].isin(sig_syms)].copy() if not episodes.empty else episodes

    # suppression conflict: head_shoulders / suppression_only coinciding with an OPEN episode
    open_syms = set(episodes.loc[episodes["episode_status"].astype(str).str.upper() == "OPEN", "symbol_id"]) if not episodes.empty else set()
    suppression = (
        signals.loc[
            (signals["r1a_evidence_class"] == "suppression_only")
            & (signals["symbol_id"].isin(open_syms))
        ].copy()
        if not signals.empty else signals
    )

    # possible duplicate episodes: same symbol+setup_family, >1 distinct start date
    if not episodes.empty:
        grp = (
            episodes.groupby(["symbol_id", "setup_family"])["episode_started_at"]
            .nunique().reset_index(name="distinct_start_dates")
        )
        dup_keys = grp.loc[grp["distinct_start_dates"] > 1]
        duplicates = episodes.merge(dup_keys[["symbol_id", "setup_family"]], on=["symbol_id", "setup_family"], how="inner")
        duplicates = duplicates.sort_values(["symbol_id", "setup_family", "episode_started_at"])
    else:
        duplicates = episodes

    return {
        "pattern_before_registry": pd.DataFrame(before),
        "same_day_discovery": pd.DataFrame(same),
        "pattern_after_registry": pd.DataFrame(after),
        "pattern_only": pattern_only,
        "registry_only": registry_only,
        "suppression_conflict": suppression,
        "possible_duplicate_episode": duplicates,
    }


def _summary(categories: dict[str, pd.DataFrame], signals: pd.DataFrame, episodes: pd.DataFrame) -> dict[str, Any]:
    counts = {name: int(len(frame)) for name, frame in categories.items()}
    before = categories["pattern_before_registry"]
    lead_by_reason = (
        before.groupby("opening_reason").size().astype(int).to_dict() if not before.empty else {}
    )
    return {
        "policy_version": CROSS_SHADOW_POLICY_VERSION,
        "signal_symbols": int(signals["symbol_id"].nunique()) if not signals.empty else 0,
        "episode_symbols": int(episodes["symbol_id"].nunique()) if not episodes.empty else 0,
        "category_counts": counts,
        "pattern_lead_by_episode_opening_reason": {str(k): int(v) for k, v in lead_by_reason.items()},
        "median_lead_lag_days": (
            float(before["lead_lag_days"].median()) if not before.empty else None
        ),
        "operational_side_effects": False,
    }


def write_cross_shadow_bundle(
    *,
    pattern_lane_csv: str | Path,
    control_plane_db: str | Path,
    output_dir: str | Path,
    through_date: str,
    project_root: str | Path,
    pattern_manifest: str | Path | None = None,
) -> dict[str, Any]:
    """Write an immutable cross-shadow reconciliation bundle (never overwrites)."""
    out = Path(output_dir).resolve()
    if out.exists() and any(out.iterdir()):
        raise FileExistsError(f"cross-shadow output already exists: {out}")
    out.mkdir(parents=True, exist_ok=True)

    signals = load_pattern_signals(pattern_lane_csv)
    episodes = load_registry_episodes(control_plane_db, through_date=through_date)
    categories = reconcile(signals, episodes)

    dataset_hashes: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    for name, frame in categories.items():
        path = out / f"cross_shadow_{name}.csv"
        frame.to_csv(path, index=False)
        dataset_hashes[path.name] = _sha256_file(path)
        row_counts[path.name] = int(len(frame))

    summary = _summary(categories, signals, episodes)
    summary_path = out / "cross_shadow_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")

    source_pattern_hash = None
    if pattern_manifest is not None and Path(pattern_manifest).exists():
        source_pattern_hash = _sha256_file(Path(pattern_manifest))
    manifest = {
        "schema_version": "pattern-cross-shadow-manifest-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "policy_version": CROSS_SHADOW_POLICY_VERSION,
        "through_date": through_date,
        "code_commit": _git_commit(Path(project_root)),
        "source_pattern_lane_csv_sha256": _sha256_file(Path(pattern_lane_csv)),
        "source_pattern_manifest_sha256": source_pattern_hash,
        "registry_episode_rows": int(len(episodes)),
        "dataset_hashes": dataset_hashes,
        "row_counts": row_counts,
        "operational_side_effects": False,
    }
    manifest_path = out / "cross_shadow_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"summary": summary, "manifest": manifest, "output_dir": str(out)}
