"""Versioned field-parity comparison policy.

Classifies every pipeline artifact column into one of four field classes and
compares two runs' artifacts accordingly. This generalizes the ad-hoc
comparison script in ``docs/runbooks/shadow_stage_ab_parity.md`` and the
fixed-key ``compare_manifests`` into a single reusable, tested policy consumed
by the shadow A/B proof builder and the daily session gate.

Field classes:

- ``STRICT``       — must be byte-identical (whole artifact).
- ``CONTENT``      — compared after canonicalization: drop RUN_SCOPED columns,
                     round float-jitter columns, sort columns, sort rows by all
                     remaining columns, normalize null representation.
- ``RUN_SCOPED``   — expected to differ (generated ids, wall-clock timestamps,
                     run-scoped hashes/lineage, rank tie/row-order jitter);
                     excluded from decision parity.
- ``TELEMETRY``    — additive per-stage metrics; row-count growth is allowed and
                     only the column schema is compared.

The RUN_SCOPED / FLOAT catalogs are the machine-readable form of the
"accepted nondeterministic fields" identified by the R1a A/B/C proof
(2026-07-17). Treat this module as the single source of truth for that list.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

PARITY_POLICY_VERSION = "shadow-parity-policy-v1"

FLOAT_ROUND_DECIMALS = 6


class FieldClass(str, Enum):
    STRICT = "STRICT"
    CONTENT = "CONTENT"
    RUN_SCOPED = "RUN_SCOPED"
    TELEMETRY = "TELEMETRY"


# Columns that are expected to differ between two otherwise-identical runs and
# are therefore excluded from decision parity (the "accepted nondeterministic
# fields" from the R1a A/B proof).
RUN_SCOPED_COLUMNS: frozenset[str] = frozenset({
    # generated identifiers
    "fill_id", "order_id", "routing_decision_id", "scan_run_id", "run_id",
    "pipeline_run_id", "decision_id", "transition_id",
    # wall-clock timestamps
    "filled_at", "submitted_at", "updated_at", "created_at", "archived_at",
    "observed_at", "recorded_at", "ingested_at", "last_seen_at", "generated_at",
    "completed_at", "started_at", "ended_at", "timestamp", "as_of_timestamp",
    # run-scoped hashes / lineage strings
    "routing_input_hash", "row_identity", "source_lineage", "artifact_uri",
    "source_artifact_hash", "content_hash", "attempt_number",
    # rank tie-break / row-order jitter (scores are the decision, not the tie rank)
    "rank", "rank_position", "rank_current", "rank_change_20d",
    "rank_current_repeat", "rank_change_20d_repeat",
})

# Float aggregate columns whose summation order jitters across parallel workers;
# compared after rounding to FLOAT_ROUND_DECIMALS rather than byte-exact.
FLOAT_TOLERANCE_COLUMNS: frozenset[str] = frozenset({
    "pe_ttm", "pe_trimmed_avg", "pe_200dma", "pe_1y_median", "pe_3y_median",
    "pe_5y_median", "pe_zscore_3y", "pe_zscore_5y", "pe_distance_from_200dma",
    "earnings_yield", "loss_mcap_pct", "total_market_cap_cr",
    "total_ttm_profit_cr", "positive_profit_market_cap_cr",
    "loss_making_market_cap_cr",
})

# Artifacts asserted byte-identical (matched by basename). These are the
# decision datasets proven byte-identical A vs B in the R1a proof.
STRICT_ARTIFACTS: frozenset[str] = frozenset({
    "pattern_scan.csv",
    "final_candidates.csv",
    "positions.csv",
    "trade_actions.csv",
    "candidate_tracker_current.csv",
    "candidate_tracker_alerts.csv",
    "stage1_current_state.csv",
    "stage1_transitions.csv",
    "stage1_watchlist.csv",
    "stage1_invalidations.csv",
    "stage1_regressions.csv",
    "stage1_stale_candidates.csv",
    "candidate_admissions.csv",
    "candidate_transitions.csv",
    "candidate_closures.csv",
    "current_candidate_state.csv",
    "stage_discovery_candidates.csv",
})

# Path fragments whose artifacts are additive telemetry.
_TELEMETRY_FRAGMENTS: tuple[str, ...] = ("performance/", "perf_tracker/")


@dataclass(frozen=True)
class ArtifactPolicy:
    rel_path: str
    artifact_class: FieldClass
    run_scoped_columns: frozenset[str] = field(default_factory=frozenset)
    float_columns: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class ArtifactComparison:
    rel_path: str
    artifact_class: FieldClass
    raw_match: bool
    normalized_match: bool
    differing_columns: tuple[str, ...]
    note: str = ""

    @property
    def verdict(self) -> str:
        if self.raw_match:
            return "IDENTICAL"
        if self.artifact_class is FieldClass.TELEMETRY:
            return "TELEMETRY"
        if self.normalized_match:
            return "CONTENT_EQUIVALENT"
        return "DATA_DIFF"


@dataclass(frozen=True)
class RunComparison:
    policy_version: str
    artifacts: tuple[ArtifactComparison, ...]
    # columns that differ in A~B but are NOT reproduced in the A~C control,
    # i.e. candidate flag-caused differences (empty => parity holds).
    flag_caused: tuple[tuple[str, tuple[str, ...]], ...] = ()

    def data_diffs(self) -> tuple[ArtifactComparison, ...]:
        return tuple(a for a in self.artifacts if a.verdict == "DATA_DIFF")


def classify_artifact(rel_path: str) -> ArtifactPolicy:
    """Classify one artifact by its relative path / basename."""
    normalized = rel_path.replace("\\", "/")
    name = normalized.rsplit("/", 1)[-1]
    if any(fragment in normalized for fragment in _TELEMETRY_FRAGMENTS):
        return ArtifactPolicy(rel_path, FieldClass.TELEMETRY)
    if name in STRICT_ARTIFACTS:
        return ArtifactPolicy(
            rel_path, FieldClass.STRICT,
            run_scoped_columns=RUN_SCOPED_COLUMNS, float_columns=FLOAT_TOLERANCE_COLUMNS,
        )
    return ArtifactPolicy(
        rel_path, FieldClass.CONTENT,
        run_scoped_columns=RUN_SCOPED_COLUMNS, float_columns=FLOAT_TOLERANCE_COLUMNS,
    )


def raw_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_csv(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or not p.stat().st_size:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, dtype=str, keep_default_na=False)
    except EmptyDataError:
        return pd.DataFrame()


def canonicalize(frame: pd.DataFrame, policy: ArtifactPolicy) -> pd.DataFrame:
    """Drop run-scoped columns, round floats, sort columns and rows."""
    if frame.empty:
        return frame
    out = frame.drop(columns=[c for c in frame.columns if c in policy.run_scoped_columns], errors="ignore").copy()
    for col in out.columns:
        if col in policy.float_columns:
            numeric = pd.to_numeric(out[col], errors="coerce").round(FLOAT_ROUND_DECIMALS)
            out[col] = numeric.map(lambda v: "" if pd.isna(v) else f"{v:.{FLOAT_ROUND_DECIMALS}f}")
    out = out.reindex(sorted(out.columns), axis=1)
    if len(out.columns):
        out = out.sort_values(list(out.columns), kind="stable", na_position="last").reset_index(drop=True)
    return out


def canonical_sha256(frame: pd.DataFrame, policy: ArtifactPolicy) -> str:
    canon = canonicalize(frame, policy)
    payload = canon.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def normalized_sha256(path: str | Path, policy: ArtifactPolicy | None = None) -> str:
    policy = policy or classify_artifact(str(path))
    return canonical_sha256(_read_csv(path), policy)


def _canonical_differing_columns(ca: pd.DataFrame, cb: pd.DataFrame) -> list[str]:
    """Columns that differ positionally between two already-canonicalized frames.

    Both frames are sorted by all (non-run-scoped) columns, so tied-only row
    reorderings vanish and this surfaces genuine content diffs — including ones
    embedded in JSON-blob columns (e.g. ``selection_details`` carrying a
    run-scoped ``rank_position``), which a column-value multiset check misses.
    """
    if list(ca.columns) != list(cb.columns):
        return ["__COLUMN_SET__"]
    if len(ca) != len(cb):
        return [f"__ROW_COUNT__({len(ca)}v{len(cb)})"]
    return [c for c in ca.columns if not ca[c].reset_index(drop=True).equals(cb[c].reset_index(drop=True))]


def compare_artifact(
    path_a: str | Path, path_b: str | Path, policy: ArtifactPolicy | None = None,
) -> ArtifactComparison:
    policy = policy or classify_artifact(str(path_a))
    raw = raw_sha256(path_a) == raw_sha256(path_b)
    name = policy.rel_path.replace("\\", "/").rsplit("/", 1)[-1]
    if not name.endswith(".csv"):
        # Non-tabular (json/html/md): raw comparison only.
        return ArtifactComparison(
            policy.rel_path, policy.artifact_class, raw, raw, (), note="non-tabular",
        )
    da, db = _read_csv(path_a), _read_csv(path_b)
    if policy.artifact_class is FieldClass.TELEMETRY:
        # Additive rows allowed; only a column-set change is a real diff.
        schema_ok = list(da.columns) == list(db.columns) or da.empty or db.empty
        return ArtifactComparison(
            policy.rel_path, policy.artifact_class, raw, schema_ok,
            () if schema_ok else ("__COLUMN_SET__",), note="telemetry-additive",
        )
    ca, cb = canonicalize(da, policy), canonicalize(db, policy)
    differing = _canonical_differing_columns(ca, cb)
    normalized = not differing
    return ArtifactComparison(
        policy.rel_path, policy.artifact_class, raw, normalized, tuple(differing),
    )


def _iter_csvs(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.csv") if p.is_file())


def compare_runs(
    dir_a: str | Path, dir_b: str | Path, control_c: str | Path | None = None,
) -> RunComparison:
    """Compare all CSV artifacts under two run dirs, optionally subtracting the
    A~C control so only genuinely flag-caused column diffs remain.
    """
    root_a, root_b = Path(dir_a), Path(dir_b)
    comparisons: list[ArtifactComparison] = []
    control_diffs: dict[str, set[str]] = {}
    root_c = Path(control_c) if control_c else None
    for pa in _iter_csvs(root_a):
        rel = str(pa.relative_to(root_a))
        pb = root_b / rel
        if not pb.exists():
            continue
        policy = classify_artifact(rel)
        cmp_ab = compare_artifact(pa, pb, policy)
        comparisons.append(cmp_ab)
        if root_c is not None:
            pc = root_c / rel
            if pc.exists():
                cmp_ac = compare_artifact(pa, pc, policy)
                control_diffs[rel] = set(cmp_ac.differing_columns)
    flag_caused: list[tuple[str, tuple[str, ...]]] = []
    for cmp_ab in comparisons:
        if cmp_ab.verdict != "DATA_DIFF":
            continue
        reproduced = control_diffs.get(cmp_ab.rel_path, set())
        extra = tuple(c for c in cmp_ab.differing_columns if c not in reproduced)
        if extra and root_c is not None:
            flag_caused.append((cmp_ab.rel_path, extra))
        elif root_c is None:
            flag_caused.append((cmp_ab.rel_path, cmp_ab.differing_columns))
    return RunComparison(
        policy_version=PARITY_POLICY_VERSION,
        artifacts=tuple(comparisons),
        flag_caused=tuple(flag_caused),
    )
