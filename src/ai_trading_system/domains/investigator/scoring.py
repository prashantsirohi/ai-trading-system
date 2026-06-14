"""Final investigator conviction scoring."""

from __future__ import annotations

import pandas as pd


def apply_rank_overlay(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    composite = pd.to_numeric(_series(out, "composite_score"), errors="coerce").fillna(0)
    overlay = pd.Series(-10, index=out.index, dtype=float)
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
    composite = pd.to_numeric(_series(out, "composite_score"), errors="coerce").fillna(0)
    credible = out.get("credible_trigger", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    hard_trap = out.get("hard_trap_flag", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    fa_missing = out.get("fa_missing", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    out.loc[composite.lt(45) & ~out["verdict"].eq("NOISE_TRAP"), "verdict"] = "WATCH_ONLY"
    out.loc[composite.lt(35) & ~credible, "verdict"] = "NOISE_TRAP"
    out.loc[hard_trap, "verdict"] = "NOISE_TRAP"
    out.loc[fa_missing & out["verdict"].eq("HIGH_CONVICTION"), "verdict"] = "MEDIUM_CONVICTION"
    out.loc[:, "execution_eligible"] = False
    return out


def final_gate(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["symbol_id", "trade_date", "verdict", "final_score", "thesis", "invalidation_level", "exit_plan", "gate_status"])
    eligible = frame.loc[pd.to_numeric(frame.get("final_score"), errors="coerce").fillna(0).ge(55)].copy()
    if eligible.empty:
        return pd.DataFrame(columns=["symbol_id", "trade_date", "verdict", "final_score", "thesis", "invalidation_level", "exit_plan", "gate_status"])
    eligible.loc[:, "thesis"] = ""
    eligible.loc[:, "invalidation_level"] = ""
    eligible.loc[:, "exit_plan"] = ""
    eligible.loc[:, "gate_status"] = "PENDING"
    return eligible[["symbol_id", "trade_date", "verdict", "final_score", "thesis", "invalidation_level", "exit_plan", "gate_status"]]


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
