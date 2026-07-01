"""Final investigator conviction scoring."""

from __future__ import annotations

import pandas as pd


FINAL_GATE_COLUMNS = ["symbol_id", "trade_date", "verdict", "final_score", "thesis", "invalidation_level", "exit_plan", "gate_status"]
FINAL_GATE_EXIT_PLAN = "Exit on invalidation breach, failed 3-session follow-through, or investigator score below 55."


def apply_rank_overlay(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    composite = pd.to_numeric(_series(out, "composite_score"), errors="coerce")
    overlay = pd.Series(0, index=out.index, dtype=float)
    overlay = overlay.mask(composite.lt(35), -10)
    overlay = overlay.mask(composite.ge(45) & composite.lt(60), 3)
    overlay = overlay.mask(composite.ge(60) & composite.lt(75), 8)
    overlay = overlay.mask(composite.ge(75), 15)
    out.loc[:, "ranking_overlay_score"] = overlay
    return out


def finalize_scores(frame: pd.DataFrame) -> pd.DataFrame:
    out = apply_rank_overlay(frame)
    components = [
        "price_structure_score",
        "volume_delivery_score",
        "fundamental_score",
        "trigger_quality_score",
        "sector_support_score",
        "buyer_fingerprint_score",
        "ranking_overlay_score",
    ]
    for column in components:
        if column not in out.columns:
            out.loc[:, column] = 0
    score = sum(pd.to_numeric(out[column], errors="coerce").fillna(0) for column in components)
    out.loc[:, "final_score"] = score.clip(lower=0, upper=100)
    out.loc[:, "verdict"] = out["final_score"].map(_verdict)
    composite = pd.to_numeric(_series(out, "composite_score"), errors="coerce")
    rank_known = composite.notna()
    credible = out.get("credible_trigger", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    hard_trap = out.get("hard_trap_flag", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    fa_missing = out.get("fa_missing", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    out.loc[rank_known & composite.lt(45) & ~out["verdict"].eq("NOISE_TRAP"), "verdict"] = "WATCH_ONLY"
    out.loc[rank_known & composite.lt(35) & ~credible, "verdict"] = "NOISE_TRAP"
    out.loc[hard_trap, "verdict"] = "NOISE_TRAP"
    out.loc[fa_missing & out["verdict"].eq("HIGH_CONVICTION"), "verdict"] = "MEDIUM_CONVICTION"
    out.loc[:, "execution_eligible"] = False
    return out


def final_gate(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=FINAL_GATE_COLUMNS)
    score = pd.to_numeric(frame.get("final_score"), errors="coerce").fillna(0)
    verdict = _series(frame, "verdict").fillna("").astype(str).str.upper()
    hard_trap = _series(frame, "hard_trap_flag").map(_truthy).fillna(False)
    credible = _series(frame, "credible_trigger").map(_truthy).fillna(False)
    eligible = frame.loc[
        score.ge(55)
        & verdict.isin({"MEDIUM_CONVICTION", "HIGH_CONVICTION"})
        & ~hard_trap
        & (credible | verdict.eq("HIGH_CONVICTION"))
    ].copy()
    if eligible.empty:
        return pd.DataFrame(columns=FINAL_GATE_COLUMNS)
    eligible.loc[:, "thesis"] = eligible.apply(_default_thesis, axis=1)
    eligible.loc[:, "invalidation_level"] = eligible.apply(_default_invalidation_level, axis=1)
    eligible.loc[:, "exit_plan"] = FINAL_GATE_EXIT_PLAN
    eligible.loc[:, "gate_status"] = "PENDING"
    return eligible[FINAL_GATE_COLUMNS]


def _verdict(score: float) -> str:
    if score >= 80:
        return "HIGH_CONVICTION"
    if score >= 55:
        return "MEDIUM_CONVICTION"
    if score >= 35:
        return "WATCH_ONLY"
    return "NOISE_TRAP"


def _series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series(pd.NA, index=frame.index)


def _default_thesis(row: pd.Series) -> str:
    trigger = _clean_label(row.get("trigger_reason")) or "investigator trigger"
    move = _clean_label(row.get("move_tag"))
    sector = _clean_label(row.get("sector")) or _clean_label(row.get("sector_name"))
    verdict = _clean_label(row.get("verdict")) or "review"
    score = _format_number(row.get("final_score"))
    parts = [trigger]
    if move and move != trigger:
        parts.append(move)
    if sector:
        parts.append(f"sector {sector}")
    parts.append(f"score {score}" if score else "score review")
    parts.append(verdict)
    return "; ".join(parts)


def _default_invalidation_level(row: pd.Series) -> str:
    for column in ("invalidation_price", "pattern_invalidation_price", "pattern_invalidation", "invalidation"):
        value = _as_float(row.get(column))
        if value is not None:
            return _format_number(value)
    low = _as_float(row.get("low"))
    if low is not None:
        return _format_number(low)
    close = _as_float(row.get("close"))
    if close is not None:
        return _format_number(close * 0.93)
    return "manual review"


def _clean_label(value: object) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "null", "<na>"}:
        return ""
    return text.replace("_", " ").title()


def _as_float(value: object) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(out) else out


def _format_number(value: object) -> str:
    number = _as_float(value)
    if number is None:
        return ""
    rounded = round(number, 2)
    return str(int(rounded)) if rounded.is_integer() else f"{rounded:.2f}"


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}
