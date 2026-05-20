"""Derived market-direction recommendation from regime level + breadth impulse."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Mapping

from ai_trading_system.analytics.regime.profiles import (
    BreadthImpulseRiskMatrix,
    RegimeProfile,
    regime_age_multiplier,
)


def _mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "to_dict"):
        try:
            return dict(value.to_dict())
        except Exception:
            return {}
    return {}


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _direction_bias(regime: str, bucket: str, leadership_confirmed: bool) -> str:
    improving = bucket in {"positive", "very_positive"}
    weakening = bucket in {"negative", "very_negative"}
    if regime == "risk_off" and weakening:
        base = "Bearish / capital protection"
    elif regime == "risk_off" and improving:
        base = "Recovery attempt"
    elif regime == "neutral" and improving:
        base = "Early risk-on"
    elif regime == "cautious_bull" and improving:
        base = "Healthy expansion"
    elif regime == "bull" and improving:
        base = "Confirmed uptrend"
    elif regime == "strong_bull" and weakening:
        base = "Late-cycle warning"
    else:
        base = "Mixed / wait for confirmation"
    if improving and not leadership_confirmed:
        return f"{base} (selective; leadership unconfirmed)"
    return base


def build_market_direction(
    *,
    market_regime: Mapping[str, Any] | Any | None,
    regime_profile: Mapping[str, Any] | RegimeProfile | None = None,
    risk_matrix: BreadthImpulseRiskMatrix | None = None,
) -> dict[str, Any]:
    """Return the advisory market-direction block used by dashboards/publish.

    This is intentionally read-only: it recommends exposure/action for audit
    and UI surfaces, but never changes live sizing.
    """
    snap = _mapping(market_regime)
    profile = _mapping(regime_profile)
    regime = str(snap.get("regime") or profile.get("regime") or "neutral")
    bucket = str(snap.get("breadth_velocity_bucket") or "neutral")
    age_days = _int(snap.get("regime_age_days"), 0)
    age_mult = regime_age_multiplier(age_days)
    confidence = _float(snap.get("regime_confidence"), 0.0)
    confidence_capped = _float(
        snap.get("regime_confidence_capped"),
        round(confidence * age_mult, 4),
    )
    leadership_confirmed = bool(snap.get("leadership_velocity_confirmed"))

    matrix_active = risk_matrix is not None
    action = "hold"
    allowed_exposure = _float(profile.get("max_exposure"), 0.0)
    new_buys_allowed = allowed_exposure > 0 and _int(profile.get("max_positions"), 0) > 0
    required_min_score = _float(profile.get("min_score"), 0.0)
    required_breakout_tier = profile.get("breakout_mode")
    required_setup_quality_gte: float | None = None

    if risk_matrix is not None:
        try:
            cell = risk_matrix.lookup(regime, bucket)
        except KeyError:
            matrix_active = False
            action = "matrix_cell_missing"
        else:
            action = cell.action
            allowed_exposure = cell.gross_exposure * age_mult
            new_buys_allowed = bool(cell.allow_new_buys)
            required_min_score = float(cell.min_score)
            required_breakout_tier = cell.require_breakout_tier
            required_setup_quality_gte = cell.require_setup_quality_gte
    elif profile:
        action = "legacy_profile"

    return {
        "market_state": regime,
        "breadth_velocity": bucket,
        "direction_bias": _direction_bias(regime, bucket, leadership_confirmed),
        "action": action,
        "allowed_exposure": round(float(allowed_exposure), 6),
        "new_buys_allowed": bool(new_buys_allowed),
        "required_min_score": round(float(required_min_score), 4),
        "required_breakout_tier": str(required_breakout_tier) if required_breakout_tier else None,
        "required_setup_quality_gte": required_setup_quality_gte,
        "regime_age_days": age_days,
        "age_multiplier": age_mult,
        "confidence": confidence,
        "confidence_capped": confidence_capped,
        "leadership_velocity_confirmed": leadership_confirmed,
        "matrix_active": matrix_active,
        "applied_live": False,
    }
