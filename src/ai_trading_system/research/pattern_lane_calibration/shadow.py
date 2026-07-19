"""R1a shadow-only, single-date live lane scan (no outcomes).

R0/R0.1 established the calibration; R1a runs the lane-aware scanner in
production on a strictly observational basis. This module holds the
R1a-specific logic — a single-date scan that produces *evidence*, never
outcomes, plus the source diagnostics, legacy parity capture, evidence
classification, and human-readable report the pipeline stage writes as
artifacts. It reuses the public building blocks in
:mod:`~ai_trading_system.research.pattern_lane_calibration.harness` and never
performs any operational write.

Nothing here reads or mutates rank, candidate, opportunity, execution or
lifecycle state; the scan is non-actionable by construction.
"""

from __future__ import annotations

import html
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from .harness import (
    build_point_in_time_context,
    classify_lanes,
    scan_lane_patterns,
)
from .policy import R0Policy, default_r0_policy
from .stage_source import GOVERNED_BACKFILL, GOVERNED_LIVE, SNAPSHOT_FALLBACK

SHADOW_BANNER = (
    "SHADOW — NON-ACTIONABLE — DOES NOT AFFECT RANKING, CANDIDATES, "
    "OPPORTUNITIES OR EXECUTION"
)

# Evidence-class taxonomy (labels only — deliberately no buy/watchlist/score
# semantics). Precedence: suppression family first, then per-family evidence,
# then per-lane evidence, then the observational default.
EVIDENCE_CLASS: dict[str, Any] = {
    "suppression": {
        "head_shoulders": "suppression_only",
    },
    "family": {
        "flat_base": "evidence_supported",
        "vcp": "evidence_supported_smaller_sample",
        "flag": "negative_evidence",
        "high_tight_flag": "insufficient_evidence",
        "three_weeks_tight": "insufficient_evidence",
    },
    "lane": {
        "stage1_base": "evidence_supported_low_volume",
        "young_listing_base": "observational",
        "ipo_early_base": "observational_non_promotable",
    },
    "default": "observational",
}


def classify_evidence(lane: str, family: str) -> str:
    """Map a (lane, family) pair to its R1a evidence class label."""
    if family in EVIDENCE_CLASS["suppression"]:
        return EVIDENCE_CLASS["suppression"][family]
    if family in EVIDENCE_CLASS["family"]:
        return EVIDENCE_CLASS["family"][family]
    if lane in EVIDENCE_CLASS["lane"]:
        return EVIDENCE_CLASS["lane"][lane]
    return EVIDENCE_CLASS["default"]


def run_lane_shadow_scan(
    market: pd.DataFrame,
    *,
    as_of_date: str,
    weekly_stage_frame: pd.DataFrame | None,
    policy: R0Policy | None = None,
    exclusion_frame: pd.DataFrame | None = None,
    workers: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, float]]:
    """Single-date live scan producing evidence only (no forward outcomes).

    Runs ``build_point_in_time_context`` → ``classify_lanes`` →
    ``scan_lane_patterns`` (all lanes) and omits every outcome/control/metric
    step, because live scanning has no future data. Returns the classified
    context, emitted signals, detector invocations, and per-phase timings.
    """
    active = policy or default_r0_policy()
    timings: dict[str, float] = {}
    started = perf_counter()

    context = build_point_in_time_context(
        market,
        as_of_date=as_of_date,
        weekly_stage_frame=weekly_stage_frame,
        policy=active,
        exclusion_frame=exclusion_frame,
    )
    timings["context_load_seconds"] = perf_counter() - started

    classify_started = perf_counter()
    classified = classify_lanes(context, policy=active)
    timings["classify_seconds"] = perf_counter() - classify_started

    scan_started = perf_counter()
    signals, invocations = scan_lane_patterns(
        market,
        classified,
        as_of_date=as_of_date,
        policy=active,
        lane_filter=None,
        workers=workers,
    )
    timings["scan_seconds"] = perf_counter() - scan_started
    timings["total_seconds"] = perf_counter() - started
    return classified, signals, invocations, timings


def attach_evidence(signals: pd.DataFrame, classified: pd.DataFrame) -> pd.DataFrame:
    """Join signals with lane/context provenance and add the evidence class.

    Adds ``r1a_evidence_class`` and ``stage_source`` and preserves the
    detector-supplied ``evidence_origin`` (fresh/carry_forward).
    """
    if signals is None or signals.empty:
        return signals if signals is not None else pd.DataFrame()
    output = signals.copy()
    output.loc[:, "symbol_id"] = output["symbol_id"].astype(str).str.upper()
    provenance_cols = [
        "symbol_id", "exchange", "weekly_stage_source",
        "weekly_stage_observation_id", "weekly_stage_policy_version",
        "weekly_stage_source_fallback_used", "weekly_stage_is_fresh",
        "weekly_stage_age_trading_days",
    ]
    if classified is not None and not classified.empty:
        available = [col for col in provenance_cols if col in classified.columns]
        provenance = classified.loc[:, available].copy()
        provenance.loc[:, "symbol_id"] = provenance["symbol_id"].astype(str).str.upper()
        if "exchange" in provenance.columns:
            provenance.loc[:, "exchange"] = provenance["exchange"].astype(str).str.upper()
        provenance = provenance.drop_duplicates(subset=[c for c in ("symbol_id", "exchange") if c in provenance.columns])
        if "exchange" in output.columns and "exchange" in provenance.columns:
            output = output.merge(provenance, on=["symbol_id", "exchange"], how="left")
        else:
            output = output.merge(provenance, on="symbol_id", how="left")
    output.loc[:, "stage_source"] = output.get("weekly_stage_source")
    output.loc[:, "r1a_evidence_class"] = [
        classify_evidence(str(lane), str(family))
        for lane, family in zip(
            output.get("scan_lane_as_of", pd.Series([""] * len(output))),
            output.get("pattern_family", pd.Series([""] * len(output))),
        )
    ]
    if "evidence_origin" not in output.columns:
        output.loc[:, "evidence_origin"] = None
    return output


def build_source_diagnostics(
    context: pd.DataFrame,
    *,
    policy: R0Policy | None = None,
    require_stage_policy_version: str | None = None,
) -> dict[str, Any]:
    """Aggregate weekly-stage source usage, fallback rate, and freshness.

    Raises ``ValueError`` when a governed row carries a stage policy version
    other than ``require_stage_policy_version`` (a contract violation that must
    fail only this stage). ``stale_admitted_as_fresh_count`` must be 0.
    """
    active = policy or default_r0_policy()
    max_age = active.weekly_freshness.max_age_trading_days
    if context is None or context.empty or "weekly_stage_source" not in context.columns:
        return {
            "rows_total": int(0 if context is None else len(context)),
            "rows_with_weekly_stage": 0,
            "rows_without_weekly_stage": int(0 if context is None else len(context)),
            "source_counts": {},
            "fallback_rows": 0,
            "fallback_rate": 0.0,
            "policy_version_distribution": {},
            "freshness_fresh": 0,
            "freshness_stale": 0,
            "stage_age_trading_days_min": None,
            "stage_age_trading_days_median": None,
            "stage_age_trading_days_max": None,
            "stale_admitted_as_fresh_count": 0,
            "policy_mismatch_count": 0,
            "require_stage_policy_version": require_stage_policy_version,
        }
    sourced = context.loc[context["weekly_stage_source"].notna()].copy()
    source_counts = (
        sourced["weekly_stage_source"].astype(str).value_counts().to_dict()
        if not sourced.empty else {}
    )
    fallback_series = sourced.get("weekly_stage_source_fallback_used", pd.Series(dtype=bool))
    fallback_rows = int(fallback_series.fillna(False).astype(bool).sum()) if not sourced.empty else 0
    policy_versions = (
        sourced["weekly_stage_policy_version"].astype(str).value_counts().to_dict()
        if "weekly_stage_policy_version" in sourced.columns and not sourced.empty else {}
    )
    age = pd.to_numeric(sourced.get("weekly_stage_age_trading_days"), errors="coerce")
    fresh_series = sourced.get("weekly_stage_is_fresh", pd.Series(dtype=bool)).fillna(False).astype(bool)
    stale_as_fresh = int((fresh_series & (age > max_age)).sum())

    # Policy-version contract: governed rows must match the required version.
    # Snapshot-fallback rows are unversioned by design and excluded.
    policy_mismatch_count = 0
    if require_stage_policy_version is not None and not sourced.empty:
        governed = sourced.loc[
            sourced["weekly_stage_source"].astype(str).isin([GOVERNED_LIVE, GOVERNED_BACKFILL])
        ]
        if "weekly_stage_policy_version" in governed.columns:
            mismatched = governed.loc[
                governed["weekly_stage_policy_version"].astype(str) != str(require_stage_policy_version)
            ]
            policy_mismatch_count = int(len(mismatched))
            if policy_mismatch_count:
                found = sorted(mismatched["weekly_stage_policy_version"].astype(str).unique())
                raise ValueError(
                    f"weekly-stage policy mismatch: required {require_stage_policy_version}, "
                    f"found {found} in {policy_mismatch_count} governed scanned rows"
                )
    return {
        "rows_total": int(len(context)),
        "rows_with_weekly_stage": int(len(sourced)),
        "rows_without_weekly_stage": int(len(context) - len(sourced)),
        "source_counts": {str(k): int(v) for k, v in source_counts.items()},
        "fallback_rows": fallback_rows,
        "fallback_rate": round(fallback_rows / len(sourced), 6) if len(sourced) else 0.0,
        "policy_version_distribution": {str(k): int(v) for k, v in policy_versions.items()},
        "freshness_fresh": int(fresh_series.sum()),
        "freshness_stale": int((~fresh_series).sum()),
        "stage_age_trading_days_min": None if age.dropna().empty else int(age.min()),
        "stage_age_trading_days_median": None if age.dropna().empty else float(age.median()),
        "stage_age_trading_days_max": None if age.dropna().empty else int(age.max()),
        "stale_admitted_as_fresh_count": stale_as_fresh,
        "policy_mismatch_count": policy_mismatch_count,
        "require_stage_policy_version": require_stage_policy_version,
    }


def source_diagnostics_frame(diagnostics: dict[str, Any]) -> pd.DataFrame:
    """One row per weekly-stage source plus a TOTAL row for the CSV artifact."""
    source_counts = diagnostics.get("source_counts", {})
    rows: list[dict[str, Any]] = []
    for source in (GOVERNED_LIVE, GOVERNED_BACKFILL, SNAPSHOT_FALLBACK):
        rows.append({
            "stage_source": source,
            "row_count": int(source_counts.get(source, 0)),
            "is_fallback": source == SNAPSHOT_FALLBACK,
        })
    for source, count in source_counts.items():
        if source not in {GOVERNED_LIVE, GOVERNED_BACKFILL, SNAPSHOT_FALLBACK}:
            rows.append({"stage_source": str(source), "row_count": int(count), "is_fallback": False})
    rows.append({
        "stage_source": "TOTAL",
        "row_count": int(diagnostics.get("rows_with_weekly_stage", 0)),
        "is_fallback": False,
    })
    frame = pd.DataFrame(rows)
    frame.loc[:, "fallback_rate"] = diagnostics.get("fallback_rate", 0.0)
    frame.loc[:, "stale_admitted_as_fresh_count"] = diagnostics.get("stale_admitted_as_fresh_count", 0)
    frame.loc[:, "policy_mismatch_count"] = diagnostics.get("policy_mismatch_count", 0)
    return frame


def _read_csv(uri: str | Path) -> pd.DataFrame:
    path = Path(uri)
    if not path.exists() or not path.stat().st_size:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _symbols(frame: pd.DataFrame) -> set[str]:
    if frame is None or frame.empty or "symbol_id" not in frame.columns:
        return set()
    return set(frame["symbol_id"].dropna().astype(str).str.upper())


def build_parity_report(lane_signals: pd.DataFrame, legacy_pattern_scan_artifact: Any) -> dict[str, Any]:
    """Capture the legacy pattern-scan hash (read-only proof it is untouched)
    and the lane-vs-legacy symbol overlap. The byte-identical parity gate is a
    separate with/without-flag run; here we only record identity and overlap.
    """
    legacy: dict[str, Any] = {
        "present": False, "uri": None, "content_hash": None, "row_count": None,
    }
    legacy_symbols: set[str] = set()
    if legacy_pattern_scan_artifact is not None:
        legacy = {
            "present": True,
            "uri": getattr(legacy_pattern_scan_artifact, "uri", None),
            "content_hash": getattr(legacy_pattern_scan_artifact, "content_hash", None),
            "row_count": getattr(legacy_pattern_scan_artifact, "row_count", None),
        }
        legacy_symbols = _symbols(_read_csv(legacy["uri"])) if legacy["uri"] else set()
    lane_symbols = _symbols(lane_signals)
    both = lane_symbols & legacy_symbols
    return {
        "legacy_pattern_scan": legacy,
        "lane_symbol_count": len(lane_symbols),
        "legacy_symbol_count": len(legacy_symbols),
        "overlap_count": len(both),
        "both": sorted(both),
        "lane_only": sorted(lane_symbols - legacy_symbols),
        "legacy_only": sorted(legacy_symbols - lane_symbols),
        "operational_side_effects": False,
    }


VALID_EVIDENCE_CLASSES: frozenset[str] = frozenset(
    [EVIDENCE_CLASS["default"]]
    + list(EVIDENCE_CLASS["suppression"].values())
    + list(EVIDENCE_CLASS["family"].values())
    + list(EVIDENCE_CLASS["lane"].values())
)
VALID_EVIDENCE_ORIGINS: frozenset[str] = frozenset({"fresh", "carry_forward"})
_SIGNAL_REQUIRED_COLUMNS: tuple[str, ...] = (
    "symbol_id", "exchange", "as_of_date", "pattern_family",
    "scan_lane_as_of", "evidence_origin", "r1a_evidence_class",
)


def validate_signal_rows(frame: pd.DataFrame) -> tuple[bool, list[str], int]:
    """Validate the evidence-attached signal frame; return (ok, issues, count).

    A malformed row is one missing a required key, carrying an out-of-domain
    ``evidence_origin`` / ``r1a_evidence_class``, or a ``head_shoulders``
    suppression row not classified ``suppression_only`` (regression guard for
    the suppression-provenance bug fixed earlier). Non-blocking: surfaced as a
    count, never raised here.
    """
    issues: list[str] = []
    if frame is None or frame.empty:
        return True, issues, 0
    missing_cols = [c for c in _SIGNAL_REQUIRED_COLUMNS if c not in frame.columns]
    if missing_cols:
        return False, [f"missing_columns:{missing_cols}"], int(len(frame))

    def _blank(series: pd.Series) -> pd.Series:
        return series.isna() | series.astype(str).str.strip().isin({"", "nan", "None"})

    malformed = pd.Series(False, index=frame.index)
    for key in ("symbol_id", "pattern_family", "scan_lane_as_of", "as_of_date"):
        blank = _blank(frame[key])
        if blank.any():
            issues.append(f"blank_{key}:{int(blank.sum())}")
        malformed |= blank
    bad_origin = ~frame["evidence_origin"].astype(str).isin(VALID_EVIDENCE_ORIGINS)
    if bad_origin.any():
        issues.append(f"invalid_evidence_origin:{int(bad_origin.sum())}")
    malformed |= bad_origin
    bad_class = ~frame["r1a_evidence_class"].astype(str).isin(VALID_EVIDENCE_CLASSES)
    if bad_class.any():
        issues.append(f"invalid_r1a_evidence_class:{int(bad_class.sum())}")
    malformed |= bad_class
    hs = frame["pattern_family"].astype(str) == "head_shoulders"
    bad_suppression = hs & (frame["r1a_evidence_class"].astype(str) != "suppression_only")
    if bad_suppression.any():
        issues.append(f"suppression_misclassified:{int(bad_suppression.sum())}")
    malformed |= bad_suppression
    count = int(malformed.sum())
    return count == 0, issues, count


def build_shadow_summary(
    classified: pd.DataFrame,
    signals: pd.DataFrame,
    *,
    diagnostics: dict[str, Any],
    parity: dict[str, Any],
    status: str = "completed",
) -> dict[str, Any]:
    """Roll up lane/family/evidence counts for pattern_lane_summary.json."""
    lane_series = (
        classified["scan_lane_as_of"].astype(str)
        if classified is not None and not classified.empty and "scan_lane_as_of" in classified.columns
        else pd.Series(dtype=str)
    )
    symbols_scanned = int((lane_series != "no_lane").sum()) if not lane_series.empty else 0
    signal_lane = signals.get("scan_lane_as_of", pd.Series(dtype=str)) if signals is not None else pd.Series(dtype=str)
    signal_family = signals.get("pattern_family", pd.Series(dtype=str)) if signals is not None else pd.Series(dtype=str)
    origin = signals.get("evidence_origin", pd.Series(dtype=str)) if signals is not None else pd.Series(dtype=str)
    evidence = signals.get("r1a_evidence_class", pd.Series(dtype=str)) if signals is not None else pd.Series(dtype=str)
    signals_ok, signal_issues, malformed_signal_rows = validate_signal_rows(signals)
    return {
        "status": status,
        "symbols_scanned": symbols_scanned,
        "universe_rows": int(0 if classified is None else len(classified)),
        "signal_rows": int(0 if signals is None else len(signals)),
        "lane_counts": {str(k): int(v) for k, v in lane_series.value_counts().to_dict().items()} if not lane_series.empty else {},
        "signal_lane_counts": {str(k): int(v) for k, v in signal_lane.value_counts().to_dict().items()},
        "family_counts": {str(k): int(v) for k, v in signal_family.value_counts().to_dict().items()},
        "evidence_class_counts": {str(k): int(v) for k, v in evidence.value_counts().to_dict().items()},
        "fresh_signals": int((origin.astype(str) == "fresh").sum()),
        "carry_forward_signals": int((origin.astype(str) == "carry_forward").sum()),
        "malformed_signal_rows": int(malformed_signal_rows),
        "signal_rows_valid": bool(signals_ok),
        "signal_validation_issues": signal_issues,
        "fallback_rate": diagnostics.get("fallback_rate", 0.0),
        "stale_admitted_as_fresh_count": diagnostics.get("stale_admitted_as_fresh_count", 0),
        "policy_mismatch_count": diagnostics.get("policy_mismatch_count", 0),
        "legacy_parity_captured": bool(parity.get("legacy_pattern_scan", {}).get("present")),
        "operational_side_effects": False,
    }


def build_runtime_report(
    timings: dict[str, float],
    *,
    symbols_scanned: int,
    invocations: pd.DataFrame,
    classified: pd.DataFrame,
) -> dict[str, Any]:
    """Assemble pattern_lane_runtime.json from the phase timings."""
    total = float(timings.get("total_seconds", 0.0))
    detector_by_family: dict[str, int] = {}
    if invocations is not None and not invocations.empty and "pattern_family" in invocations.columns:
        detector_by_family = {
            str(k): int(v) for k, v in invocations["pattern_family"].value_counts().to_dict().items()
        }
    time_by_lane: dict[str, int] = {}
    if classified is not None and not classified.empty and "scan_lane_as_of" in classified.columns:
        lanes = classified.loc[classified["scan_lane_as_of"].astype(str) != "no_lane", "scan_lane_as_of"]
        time_by_lane = {str(k): int(v) for k, v in lanes.astype(str).value_counts().to_dict().items()}
    return {
        "total_wall_seconds": round(total, 6),
        "context_load_seconds": round(float(timings.get("context_load_seconds", 0.0)), 6),
        "classify_seconds": round(float(timings.get("classify_seconds", 0.0)), 6),
        "scan_seconds": round(float(timings.get("scan_seconds", 0.0)), 6),
        "symbols_scanned": int(symbols_scanned),
        "seconds_per_symbol": round(total / symbols_scanned, 6) if symbols_scanned else None,
        "detector_calls_by_family": detector_by_family,
        "symbols_by_lane": time_by_lane,
    }


def _kv_rows(pairs: list[tuple[str, Any]]) -> str:
    return "".join(
        f"<tr><th>{html.escape(str(label))}</th><td>{html.escape(str(value))}</td></tr>"
        for label, value in pairs
    )


def _dict_rows(mapping: dict[str, Any]) -> str:
    if not mapping:
        return "<tr><td colspan='2'><em>none</em></td></tr>"
    return "".join(
        f"<tr><td>{html.escape(str(k))}</td><td>{html.escape(str(v))}</td></tr>"
        for k, v in sorted(mapping.items())
    )


def render_shadow_report_html(
    summary: dict[str, Any],
    diagnostics: dict[str, Any],
    parity: dict[str, Any],
    runtime: dict[str, Any],
    *,
    run_date: str,
    errors: list[str] | None = None,
    artifact_names: list[str] | None = None,
) -> str:
    """Self-contained, dependency-free HTML report (labels only, no actions)."""
    legacy = parity.get("legacy_pattern_scan", {})
    errors = errors or []
    artifact_names = artifact_names or []
    banner = html.escape(SHADOW_BANNER)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Pattern Lane Shadow Scan — {html.escape(run_date)}</title>
<style>
 body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; padding: 0 0 3rem; color: #1c2530; background: #f6f8fa; }}
 .banner {{ background: #7a3b00; color: #fff; font-weight: 700; letter-spacing: .02em; padding: 14px 20px; text-align: center; }}
 main {{ max-width: 960px; margin: 0 auto; padding: 0 20px; }}
 h1 {{ font-size: 1.4rem; margin: 24px 0 4px; }}
 h2 {{ font-size: 1.05rem; margin: 28px 0 8px; border-bottom: 1px solid #d0d7de; padding-bottom: 4px; }}
 table {{ border-collapse: collapse; width: 100%; background: #fff; font-size: .9rem; }}
 th, td {{ border: 1px solid #d0d7de; padding: 6px 10px; text-align: left; vertical-align: top; }}
 th {{ background: #eef1f4; width: 42%; }}
 .note {{ color: #57606a; font-size: .82rem; margin-top: 6px; }}
 .err {{ color: #b31d28; }}
</style></head><body>
<div class="banner">{banner}</div>
<main>
<h1>Pattern Lane Shadow Scan</h1>
<p class="note">Observational evidence only. Labels below carry no buy, watchlist,
score or action meaning and do not influence ranking, candidates, opportunities
or execution.</p>

<h2>Run</h2>
<table>{_kv_rows([
    ("Run date", run_date),
    ("Status", summary.get("status")),
    ("Symbols scanned", summary.get("symbols_scanned")),
    ("Universe rows", summary.get("universe_rows")),
    ("Signal rows", summary.get("signal_rows")),
    ("Fresh signals", summary.get("fresh_signals")),
    ("Carry-forward signals", summary.get("carry_forward_signals")),
    ("Operational side effects", summary.get("operational_side_effects")),
])}</table>

<h2>Signals by lane</h2>
<table><tr><th>Lane</th><th>Count</th></tr>{_dict_rows(summary.get("signal_lane_counts", {}))}</table>

<h2>Signals by family</h2>
<table><tr><th>Family</th><th>Count</th></tr>{_dict_rows(summary.get("family_counts", {}))}</table>

<h2>Evidence classifications</h2>
<table><tr><th>Evidence class</th><th>Count</th></tr>{_dict_rows(summary.get("evidence_class_counts", {}))}</table>

<h2>Weekly-stage source usage</h2>
<table>{_kv_rows([
    ("Rows with weekly stage", diagnostics.get("rows_with_weekly_stage")),
    ("Fallback rows", diagnostics.get("fallback_rows")),
    ("Fallback rate", diagnostics.get("fallback_rate")),
    ("Stale admitted as fresh", diagnostics.get("stale_admitted_as_fresh_count")),
    ("Policy mismatches", diagnostics.get("policy_mismatch_count")),
])}</table>
<table><tr><th>Source</th><th>Rows</th></tr>{_dict_rows(diagnostics.get("source_counts", {}))}</table>

<h2>Weekly-stage freshness</h2>
<table>{_kv_rows([
    ("Fresh", diagnostics.get("freshness_fresh")),
    ("Stale", diagnostics.get("freshness_stale")),
    ("Stage age (min)", diagnostics.get("stage_age_trading_days_min")),
    ("Stage age (median)", diagnostics.get("stage_age_trading_days_median")),
    ("Stage age (max)", diagnostics.get("stage_age_trading_days_max")),
])}</table>

<h2>Runtime</h2>
<table>{_kv_rows([
    ("Total wall seconds", runtime.get("total_wall_seconds")),
    ("Seconds per symbol", runtime.get("seconds_per_symbol")),
    ("Context load seconds", runtime.get("context_load_seconds")),
    ("Scan seconds", runtime.get("scan_seconds")),
])}</table>

<h2>Legacy parity</h2>
<table>{_kv_rows([
    ("Legacy pattern_scan present", legacy.get("present")),
    ("Legacy content hash", legacy.get("content_hash")),
    ("Legacy row count", legacy.get("row_count")),
    ("Lane symbols", parity.get("lane_symbol_count")),
    ("Legacy symbols", parity.get("legacy_symbol_count")),
    ("Overlap", parity.get("overlap_count")),
])}</table>

<h2>Errors</h2>
<table>{("".join(f"<tr><td class='err'>{html.escape(str(e))}</td></tr>" for e in errors)) or "<tr><td><em>none</em></td></tr>"}</table>

<h2>Artifacts</h2>
<table>{("".join(f"<tr><td>{html.escape(str(a))}</td></tr>" for a in artifact_names)) or "<tr><td><em>none</em></td></tr>"}</table>
</main></body></html>
"""
