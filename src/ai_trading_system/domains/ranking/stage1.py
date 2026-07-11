"""Versioned, explainable Stage-1 maturity and emerging-leader scoring."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from typing import Any

import numpy as np
import pandas as pd


MATURITY_COMPONENTS = (
    "structural_repair",
    "accumulation",
    "rs_acceleration",
    "base_quality",
    "sector_rotation",
    "pattern_readiness",
    "golden_cross_progression",
)
EMERGING_COMPONENTS = (
    "rs_acceleration",
    "structural_repair_velocity",
    "accumulation_improvement",
    "sector_rotation",
    "base_improvement",
    "golden_cross_progression",
    "pattern_progression",
)


@dataclass(frozen=True)
class Stage1ModelConfig:
    model_version: str = "v1"
    formula_name: str = "STAGE1_MATURITY"
    model_status: str = "RESEARCH_ONLY"
    maturity_weights: dict[str, float] = field(default_factory=lambda: {
        "structural_repair": 25.0, "accumulation": 20.0, "rs_acceleration": 20.0,
        "base_quality": 15.0, "sector_rotation": 10.0, "pattern_readiness": 5.0,
        "golden_cross_progression": 5.0,
    })
    emerging_weights: dict[str, float] = field(default_factory=lambda: {
        "rs_acceleration": 25.0, "structural_repair_velocity": 20.0,
        "accumulation_improvement": 20.0, "sector_rotation": 15.0,
        "base_improvement": 10.0, "golden_cross_progression": 5.0,
        "pattern_progression": 5.0,
    })
    insufficient_data_pct: float = 45.0
    low_confidence_pct: float = 65.0
    high_confidence_pct: float = 85.0
    severe_liquidity_score: float = 0.0
    far_below_gap_pct: float = -10.0
    imminent_gap_lower_pct: float = -3.0
    recent_cross_sessions: int = 20
    extreme_positive_gap_pct: float = 50.0
    extreme_negative_gap_pct: float = -30.0
    death_cross_gap_delta_20d_pct: float = -0.5

    def validate(self) -> "Stage1ModelConfig":
        _validate_weights(self.maturity_weights, MATURITY_COMPONENTS, "maturity_weights")
        _validate_weights(self.emerging_weights, EMERGING_COMPONENTS, "emerging_weights")
        if not 0 <= self.insufficient_data_pct < self.low_confidence_pct < self.high_confidence_pct <= 100:
            raise ValueError("Stage-1 completeness thresholds must be strictly increasing within 0..100")
        if self.model_status not in {"RESEARCH_ONLY", "SHADOW_READY"}:
            raise ValueError("Stage-1 model_status must be RESEARCH_ONLY or SHADOW_READY")
        if not self.far_below_gap_pct < self.imminent_gap_lower_pct < 0:
            raise ValueError("Stage-1 Golden Cross gap thresholds must satisfy far_below < imminent_lower < 0")
        if self.extreme_negative_gap_pct >= self.far_below_gap_pct or self.extreme_positive_gap_pct <= 0:
            raise ValueError("Stage-1 extreme MA-gap thresholds are invalid")
        return self

    @classmethod
    def from_params(cls, params: dict[str, Any] | None) -> "Stage1ModelConfig":
        params = params or {}
        section = params.get("stage1_maturity") if isinstance(params.get("stage1_maturity"), dict) else {}
        maturity = section.get("weights", params.get("stage1_maturity_weights"))
        emerging = section.get("emerging_weights", params.get("stage1_emerging_weights"))
        return cls(
            model_version=str(section.get("model_version", params.get("stage1_model_version", "v1"))),
            formula_name=str(section.get("formula_name", "STAGE1_MATURITY")),
            model_status=str(section.get("model_status", "RESEARCH_ONLY")),
            maturity_weights=dict(maturity) if isinstance(maturity, dict) else cls().maturity_weights,
            emerging_weights=dict(emerging) if isinstance(emerging, dict) else cls().emerging_weights,
            far_below_gap_pct=float(section.get("far_below_gap_pct", params.get("stage1_far_below_gap_pct", -10.0))),
            imminent_gap_lower_pct=float(section.get("imminent_gap_lower_pct", params.get("stage1_imminent_gap_lower_pct", -3.0))),
            recent_cross_sessions=int(section.get("recent_cross_sessions", params.get("stage1_recent_cross_sessions", 20))),
            extreme_positive_gap_pct=float(section.get("extreme_positive_gap_pct", params.get("stage1_ma_gap_extreme_positive_pct", 50.0))),
            extreme_negative_gap_pct=float(section.get("extreme_negative_gap_pct", params.get("stage1_ma_gap_extreme_negative_pct", -30.0))),
            death_cross_gap_delta_20d_pct=float(section.get("death_cross_gap_delta_20d_pct", params.get("stage1_death_cross_gap_delta_20d_pct", -0.5))),
        ).validate()

    @property
    def config_hash(self) -> str:
        encoded = json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()[:16]


def _validate_weights(weights: dict[str, float], expected: tuple[str, ...], label: str) -> None:
    if set(weights) != set(expected):
        unknown = sorted(set(weights) - set(expected))
        missing = sorted(set(expected) - set(weights))
        raise ValueError(f"Invalid {label}; unknown={unknown}, missing={missing}")
    values = [float(weights[name]) for name in expected]
    if any(value < 0 for value in values):
        raise ValueError(f"{label} must be non-negative")
    if not np.isclose(sum(values), 100.0):
        raise ValueError(f"{label} must sum to 100")


def _num(frame: pd.DataFrame, name: str, default: float = np.nan) -> pd.Series:
    if name not in frame.columns:
        return pd.Series(default, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[name], errors="coerce")


def _bool(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame.columns:
        return pd.Series(False, index=frame.index, dtype=bool)
    value = frame[name]
    if pd.api.types.is_bool_dtype(value):
        return value.fillna(False).astype(bool)
    return value.astype("string").str.lower().isin({"1", "true", "yes", "y"}).fillna(False)


def _scaled(value: pd.Series, low: float, high: float) -> pd.Series:
    return ((value - low) / (high - low) * 100.0).clip(0.0, 100.0)


def _golden_cross_features(frame: pd.DataFrame, config: Stage1ModelConfig) -> pd.DataFrame:
    out = frame.copy()
    sma20, sma50, sma150, sma200 = (_num(out, name) for name in ("sma_20", "sma_50", "sma_150", "sma_200"))
    gap = (sma50 / sma200.replace(0, np.nan) - 1.0) * 100.0
    out.loc[:, "sma50_above_sma200"] = sma50 > sma200
    out.loc[:, "sma50_sma200_gap_pct"] = gap
    for days in (5, 20, 60):
        name = f"sma50_sma200_gap_delta_{days}d"
        if name not in out:
            out.loc[:, name] = np.nan
    out.loc[:, "sma20_above_sma50"] = sma20 > sma50
    out.loc[:, "sma20_sma50_gap_pct"] = (sma20 / sma50.replace(0, np.nan) - 1.0) * 100.0
    if "sma20_sma50_gap_delta_10d" not in out:
        out.loc[:, "sma20_sma50_gap_delta_10d"] = np.nan
    out.loc[:, "sma150_above_sma200"] = sma150 > sma200
    out.loc[:, "long_term_ma_alignment"] = np.select(
        [out["sma50_above_sma200"] & out["sma150_above_sma200"], out["sma150_above_sma200"]],
        ["CONFIRMED", "CONSTRUCTIVE"], default="UNCONFIRMED",
    )
    days_since = _num(out, "golden_cross_days_since")
    gap_velocity = _num(out, "sma50_sma200_gap_delta_20d")
    slope50 = _num(out, "sma50_slope_20d_pct")
    slope200 = _num(out, "sma200_slope_20d_pct")
    inputs_known = sma50.notna() & sma200.notna() & sma50.gt(0) & sma200.gt(0)
    imminent = (~out["sma50_above_sma200"]) & gap.ge(config.imminent_gap_lower_pct) & gap.lt(0) & gap_velocity.gt(0) & slope50.gt(slope200)
    approaching = (~out["sma50_above_sma200"]) & gap.ge(config.far_below_gap_pct) & gap.lt(config.imminent_gap_lower_pct) & gap_velocity.gt(0)
    recent = out["sma50_above_sma200"] & days_since.between(0, config.recent_cross_sessions)
    failed = _bool(out, "golden_cross_failed")
    death_risk = out["sma50_above_sma200"] & gap_velocity.lt(config.death_cross_gap_delta_20d_pct) & slope50.lt(slope200)
    out.loc[:, "golden_cross_imminent"] = imminent
    out.loc[:, "golden_cross_status_legacy"] = np.select(
        [failed, recent, imminent, out["sma50_above_sma200"]],
        ["FAILED", "RECENT", "IMMINENT", "CONFIRMED"], default="DEVELOPING",
    )
    out.loc[:, "golden_cross_status"] = np.select(
        [~inputs_known, failed, death_risk, recent, out["sma50_above_sma200"], imminent, approaching],
        ["UNKNOWN", "FAILED_CROSS", "DEATH_CROSS_RISK", "CROSSED_RECENTLY", "CROSSED_ESTABLISHED", "IMMINENT", "APPROACHING"],
        default="FAR_BELOW",
    )
    velocity = _scaled(_num(out, "sma50_sma200_gap_delta_20d", 0), -2.0, 3.0)
    alignment = out["sma50_above_sma200"].astype(float) * 100.0
    early = out["sma20_above_sma50"].astype(float) * 100.0
    quality = 0.45 * velocity + 0.35 * alignment + 0.20 * early
    out.loc[:, "golden_cross_quality"] = quality.mask(failed, 0.0).clip(0.0, 100.0)
    out.loc[:, "ma_gap_quality_flag"] = np.select(
        [~inputs_known, sma200.le(0), gap.gt(config.extreme_positive_gap_pct), gap.lt(config.extreme_negative_gap_pct)],
        ["INSUFFICIENT_HISTORY", "SOURCE_MISMATCH_SUSPECTED", "EXTREME_POSITIVE_GAP", "EXTREME_NEGATIVE_GAP"],
        default="NORMAL",
    )
    return out


def _json_codes(codes: list[str]) -> str:
    return json.dumps(sorted(set(code for code in codes if code)), separators=(",", ":"))


def _score_band(score: pd.Series) -> pd.Series:
    return pd.Series(np.select(
        [score.ge(90), score.ge(80), score.ge(65), score.ge(50), score.ge(30)],
        ["STAGE2_REVIEW_RANGE", "BREAKOUT_READY_RANGE", "LATE_RANGE", "ACCUMULATION_RANGE", "BASE_RANGE"],
        default="REPAIR_RANGE",
    ), index=score.index, dtype=object)


def build_stage1_scan(
    candidates: pd.DataFrame,
    *,
    config: Stage1ModelConfig | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    config = (config or Stage1ModelConfig()).validate()
    if candidates is None or candidates.empty:
        return pd.DataFrame(), {"rows": 0, "model_version": config.model_version, "config_hash": config.config_hash}
    out = _golden_cross_features(candidates, config)

    raw: dict[str, pd.Series] = {
        "structural_repair": _num(out, "trend_repair_score"),
        "accumulation": (0.60 * _num(out, "delivery_accumulation_score") + 0.40 * _num(out, "volume_confirmation_score")),
        "rs_acceleration": _num(out, "relative_strength_score_early"),
        "base_quality": _num(out, "base_pattern_freshness_score"),
        "sector_rotation": _num(out, "sector_strength_score", 50.0),
        "pattern_readiness": _num(out, "base_pattern_freshness_score"),
        "golden_cross_progression": _num(out, "golden_cross_quality"),
        "structural_repair_velocity": _scaled(_num(out, "sma50_sma200_gap_delta_20d"), -2, 3),
        "accumulation_improvement": (0.50 * _num(out, "delivery_accumulation_score") + 0.50 * _num(out, "volume_confirmation_score")),
        "base_improvement": _num(out, "base_pattern_freshness_score"),
        "pattern_progression": _num(out, "base_pattern_freshness_score"),
    }
    for name, values in raw.items():
        out.loc[:, f"stage1_{name}_raw"] = values.clip(0.0, 100.0)

    maturity = pd.Series(0.0, index=out.index)
    emerging = pd.Series(0.0, index=out.index)
    maturity_complete = []
    for name in MATURITY_COMPONENTS:
        maximum = float(config.maturity_weights[name])
        values = raw[name]
        contribution = (values.clip(0, 100) / 100.0 * maximum).where(values.notna(), 0.0)
        out.loc[:, f"stage1_{name}_score"] = contribution
        out.loc[:, f"stage1_{name}_max"] = maximum
        out.loc[:, f"stage1_{name}_complete"] = values.notna()
        maturity += contribution
        maturity_complete.append(values.notna())
    for name in EMERGING_COMPONENTS:
        maximum = float(config.emerging_weights[name])
        values = raw[name]
        contribution = (values.clip(0, 100) / 100.0 * maximum).where(values.notna(), 0.0)
        out.loc[:, f"stage1_emerging_{name}_score"] = contribution
        out.loc[:, f"stage1_emerging_{name}_max"] = maximum
        out.loc[:, f"stage1_emerging_{name}_complete"] = values.notna()
        emerging += contribution

    completeness = pd.concat(maturity_complete, axis=1).mean(axis=1) * 100.0
    missing = pd.Series("", index=out.index, dtype=object)
    for name in MATURITY_COMPONENTS:
        missing = missing + np.where(raw[name].isna(), np.where(missing.eq(""), name, "|" + name), "")
    out.loc[:, "stage1_data_completeness_pct"] = completeness.round(1)
    out.loc[:, "stage1_missing_components"] = missing
    out.loc[:, "stage1_score_confidence"] = np.select(
        [completeness >= config.high_confidence_pct, completeness >= config.low_confidence_pct, completeness >= config.insufficient_data_pct],
        ["HIGH", "MEDIUM", "LOW"], default="INSUFFICIENT_DATA",
    )

    bonus = np.where(
        _bool(out, "golden_cross_imminent") & (raw["rs_acceleration"] > 50) & (raw["accumulation"] > 50), 3.0, 0.0,
    )
    falling = (_num(out, "sma50_slope_20d_pct") < -1.0) & (_num(out, "sma200_slope_20d_pct") < -1.0)
    gap_closing = _num(out, "sma50_sma200_gap_delta_20d") > 0
    distribution = _bool(out, "distribution_flag") | _bool(out, "hard_trap_flag")
    pattern_mature = out.get("top_pattern_state", pd.Series("", index=out.index)).astype(str).str.lower().isin({"mature", "confirmed", "breakout_ready"})
    penalty = np.where(falling & gap_closing, 5.0, 0.0) + np.where(pattern_mature & distribution, 8.0, 0.0)
    out.loc[:, "stage1_bonus_score"] = bonus
    out.loc[:, "stage1_penalty_score"] = penalty
    out.loc[:, "stage1_adjustment_reasons"] = [
        _json_codes((["GOLDEN_CROSS_CONFLUENCE"] if positive else []) + (["STRUCTURAL_PENALTY"] if negative else []))
        for positive, negative in zip(bonus > 0, penalty > 0, strict=True)
    ]
    out.loc[:, "stage1_maturity_score"] = (maturity + bonus - penalty).clip(0, 100)
    out.loc[:, "stage1_emerging_score"] = (emerging + bonus - penalty).clip(0, 100)

    out.loc[:, "stage1_score_band"] = _score_band(out["stage1_maturity_score"])

    symbol_invalid = out["symbol_id"].fillna("").astype(str).str.strip().eq("")
    insufficient = completeness < config.insufficient_data_pct
    liquidity = _num(out, "liquidity_score", 1.0).fillna(1.0) <= config.severe_liquidity_score
    stage4 = out.get("weekly_stage_label", pd.Series("", index=out.index)).astype(str).str.upper().isin({"S4", "STAGE_4", "STAGE_4_DECLINE"})
    stage3 = out.get("weekly_stage_label", pd.Series("", index=out.index)).astype(str).str.upper().isin({"S3", "STAGE_3", "STAGE_3_DISTRIBUTION"})
    block_lists: list[list[str]] = []
    for idx in out.index:
        row_blocks: list[str] = []
        if bool(symbol_invalid.loc[idx]): row_blocks.append("INVALID_SYMBOL")
        if bool(insufficient.loc[idx]): row_blocks.append("INSUFFICIENT_DATA")
        if bool(liquidity.loc[idx]): row_blocks.append("SEVERE_LIQUIDITY_FAILURE")
        if bool(stage4.loc[idx]): row_blocks.append("STAGE4_HARD_GUARD")
        if bool((stage3 | distribution).loc[idx]): row_blocks.append("DISTRIBUTION_GUARD")
        block_lists.append(row_blocks)
    out.loc[:, "stage1_block_reasons"] = [_json_codes(codes) for codes in block_lists]
    out.loc[:, "stage1_eligible"] = [not codes for codes in block_lists]

    score = out["stage1_maturity_score"]
    eligible = out["stage1_eligible"].astype(bool)
    band_to_substate = {
        "REPAIR_RANGE": "STAGE_1_REPAIR", "BASE_RANGE": "STAGE_1_BASE",
        "ACCUMULATION_RANGE": "STAGE_1_ACCUMULATION", "LATE_RANGE": "STAGE_1_LATE",
        "BREAKOUT_READY_RANGE": "STAGE_1_BREAKOUT_READY", "STAGE2_REVIEW_RANGE": "STAGE_1_BREAKOUT_READY",
    }
    out.loc[:, "stage1_substate"] = out["stage1_score_band"].map(band_to_substate).where(eligible, "NOT_STAGE1")
    pattern_state = out.get("top_pattern_state", pd.Series("", index=out.index)).fillna("").astype(str).str.upper()
    out.loc[:, "pattern_promotion_state"] = pattern_state.replace("", "NONE")
    invalidated = pattern_state.isin({"INVALIDATED"})
    failed_pattern = pattern_state.isin({"FAILED", "INVALIDATED"})
    mature_pattern = pattern_state.isin({"MATURE", "CONFIRMED", "BREAKOUT_READY", "BREAKOUT_ATTEMPT", "PENDING_3D"})
    promotion_pattern = pattern_state.isin({"MATURE", "CONFIRMED", "BREAKOUT_READY", "BREAKOUT_ATTEMPT", "PENDING_3D"})
    late_or_ready = out["stage1_substate"].isin({"STAGE_1_LATE", "STAGE_1_BREAKOUT_READY"})
    death_risk = out["golden_cross_status"].eq("DEATH_CROSS_RISK")
    promotion_lists: list[list[str]] = []
    for idx, structural in enumerate(block_lists):
        codes = list(structural)
        if not bool(eligible.iloc[idx]) and not codes:
            codes.append("DATA_QUALITY_BLOCK")
        if bool(eligible.iloc[idx]) and not bool(late_or_ready.iloc[idx]): codes.append("SUBSTATE_TOO_EARLY")
        if not bool(promotion_pattern.iloc[idx]): codes.append("PATTERN_NOT_READY")
        if bool(failed_pattern.iloc[idx]): codes.append("FAILED_PATTERN")
        if raw["rs_acceleration"].fillna(0).iloc[idx] <= 50: codes.append("RS_NOT_ACCELERATING")
        if bool(death_risk.iloc[idx]): codes.append("DEATH_CROSS_RISK")
        promotion_lists.append(codes)
    out.loc[:, "promotion_block_reasons"] = [_json_codes(codes) for codes in promotion_lists]
    out.loc[:, "promotion_eligibility"] = [not codes for codes in promotion_lists]
    promotion_ok = out["promotion_eligibility"].astype(bool)
    out.loc[:, "stage1_operational_status"] = np.select(
        [~eligible, invalidated, failed_pattern, promotion_ok & pattern_state.eq("CONFIRMED"), promotion_ok & pattern_state.isin({"BREAKOUT_ATTEMPT", "PENDING_3D"}), promotion_ok & late_or_ready & mature_pattern, eligible & out["stage1_substate"].eq("STAGE_1_ACCUMULATION") & mature_pattern, eligible & out["stage1_substate"].eq("STAGE_1_BASE") & mature_pattern],
        ["STRUCTURALLY_BLOCKED", "INVALIDATED", "REGRESSED", "PROMOTED", "PROMOTION_PENDING", "BREAKOUT_WATCH", "HIGH_PRIORITY", "DEVELOPING"], default="MONITOR",
    )
    out.loc[:, "stage2_review_candidate"] = score.ge(90) & out["promotion_eligibility"]
    out.loc[:, "execution_eligible"] = False
    out.loc[:, "stage1_formula_name"] = config.formula_name
    out.loc[:, "stage1_model_version"] = config.model_version
    out.loc[:, "stage1_config_hash"] = config.config_hash
    out.loc[:, "model_status"] = config.model_status

    out.loc[:, "stage1_emerging_rank"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    ranked_eligible = out.loc[out["stage1_eligible"]].sort_values(
        ["stage1_score_confidence", "stage1_emerging_score", "stage1_maturity_score", "stage1_data_completeness_pct", "symbol_id"],
        ascending=[True, False, False, False, True], kind="stable",
        key=lambda s: s.map({"HIGH": 0, "MEDIUM": 1, "LOW": 2}).fillna(s) if s.name == "stage1_score_confidence" else s,
    )
    out.loc[ranked_eligible.index, "stage1_emerging_rank"] = pd.Series(range(1, len(ranked_eligible) + 1), index=ranked_eligible.index, dtype="Int64")
    out = out.sort_values(["stage1_emerging_rank", "symbol_id"], na_position="last", kind="stable").reset_index(drop=True)
    eligible_mask = out["stage1_eligible"].astype(bool)
    reason_counts: dict[str, int] = {}
    promotion_reason_counts: dict[str, int] = {}
    for value, target in ((out["stage1_block_reasons"], reason_counts), (out["promotion_block_reasons"], promotion_reason_counts)):
        for raw_codes in value.fillna("[]"):
            for code in json.loads(raw_codes):
                target[code] = target.get(code, 0) + 1
    golden_total = _counts(out, "golden_cross_status")
    golden_eligible = _counts(out.loc[eligible_mask], "golden_cross_status")
    golden_blocked = _counts(out.loc[~eligible_mask], "golden_cross_status")
    invariant_errors: list[str] = []
    if int(eligible_mask.sum()) + int((~eligible_mask).sum()) != len(out): invariant_errors.append("eligible_plus_blocked_mismatch")
    if int(out.loc[~eligible_mask, "stage1_substate"].ne("NOT_STAGE1").sum()): invariant_errors.append("blocked_positive_substate")
    if int(out.loc[~eligible_mask, "stage1_operational_status"].isin({"HIGH_PRIORITY", "BREAKOUT_WATCH", "PROMOTION_PENDING", "PROMOTED"}).sum()): invariant_errors.append("blocked_operational_promotion")
    if int(out.loc[out["stage1_emerging_rank"].notna() & ~eligible_mask].shape[0]): invariant_errors.append("ranked_blocked_row")
    for status, total in golden_total.items():
        if int(total) != int(golden_eligible.get(status, 0)) + int(golden_blocked.get(status, 0)):
            invariant_errors.append(f"golden_cross_count_mismatch:{status}")
    summary = {
        "rows": int(len(out)), "eligible_rows": int(eligible_mask.sum()), "blocked_rows": int((~eligible_mask).sum()),
        "model_version": config.model_version, "formula_name": config.formula_name,
        "model_status": config.model_status, "config_hash": config.config_hash,
        "stage1_score_band_counts": _counts(out, "stage1_score_band"),
        "stage1_substate_counts": _counts(out, "stage1_substate"),
        "substate_counts": _counts(out, "stage1_substate"),
        "stage1_block_reason_counts": reason_counts,
        "stage1_operational_status_counts": _counts(out, "stage1_operational_status"),
        "promotion_eligible_rows": int(out["promotion_eligibility"].astype(bool).sum()),
        "promotion_blocked_rows": int((~out["promotion_eligibility"].astype(bool)).sum()),
        "promotion_block_reason_counts": promotion_reason_counts,
        "golden_cross_status_counts": golden_total,
        "golden_cross_status_eligible_counts": golden_eligible,
        "golden_cross_status_blocked_counts": golden_blocked,
        "ma_gap_quality_flag_counts": _counts(out, "ma_gap_quality_flag"),
        "extreme_ma_gap_count": int(out["ma_gap_quality_flag"].isin({"EXTREME_POSITIVE_GAP", "EXTREME_NEGATIVE_GAP"}).sum()),
        "median_ma_gap_pct": _quantile(out["sma50_sma200_gap_pct"], 0.5),
        "p10_ma_gap_pct": _quantile(out["sma50_sma200_gap_pct"], 0.1),
        "p90_ma_gap_pct": _quantile(out["sma50_sma200_gap_pct"], 0.9),
        "invariant_validation": not invariant_errors,
        "invariant_validation_errors": invariant_errors,
    }
    return out, summary


def _counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame[column].fillna("UNKNOWN").value_counts().to_dict().items()}


def _quantile(values: pd.Series, quantile: float) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return None if numeric.empty else round(float(numeric.quantile(quantile)), 6)
