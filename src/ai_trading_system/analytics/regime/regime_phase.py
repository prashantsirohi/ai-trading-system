"""Human-readable regime phase derived from existing regime signals."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


class RegimePhase(str, Enum):
    BEAR_STAGE4 = "bear_stage4"
    BASE_FORMING_STAGE1 = "base_forming_stage1"
    TRANSITION_S1_TO_S2 = "transition_stage1_to_stage2"
    CONFIRMED_STAGE2_BULL = "confirmed_stage2_bull"
    MIXED_WAIT = "mixed_wait"


PHASE_LABELS: dict[RegimePhase, str] = {
    RegimePhase.BEAR_STAGE4: "Bear / Stage 4",
    RegimePhase.BASE_FORMING_STAGE1: "Base forming (S1)",
    RegimePhase.TRANSITION_S1_TO_S2: "Transition S1 → S2",
    RegimePhase.CONFIRMED_STAGE2_BULL: "Confirmed bull (S2)",
    RegimePhase.MIXED_WAIT: "Mixed / wait",
}


PHASE_EMOJI: dict[RegimePhase, str] = {
    RegimePhase.BEAR_STAGE4: "🔴",
    RegimePhase.BASE_FORMING_STAGE1: "🟡",
    RegimePhase.TRANSITION_S1_TO_S2: "🟢",
    RegimePhase.CONFIRMED_STAGE2_BULL: "🟢",
    RegimePhase.MIXED_WAIT: "⚪",
}


@dataclass(frozen=True)
class RegimePhaseResult:
    regime_phase: RegimePhase
    phase_label: str
    phase_emoji: str
    driven_by: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["regime_phase"] = self.regime_phase.value
        return payload


def compute_regime_phase(
    *,
    market_stage: str | None,
    regime: str | None,
    breadth_velocity_bucket: str | None,
    s2_pct: float = 0.0,
    pct_above_200dma: float | None = None,
    transition_s2_threshold: float = 0.30,
) -> RegimePhaseResult:
    """
    Derive a human-readable regime phase from already-computed market-stage
    and breadth-regime outputs.

    This function is intentionally pure:
    - no DB access
    - no file access
    - no imports from ranking service
    - no mutation of MarketRegimeSnapshot

    Inputs:
    - market_stage: from market_stage.py, usually S2/S3/S4/MIXED/S1
    - regime: from breadth.py, usually risk_off/neutral/cautious_bull/bull/strong_bull
    - breadth_velocity_bucket: very_negative/negative/neutral/positive/very_positive
    - s2_pct: fraction of classified universe in Stage 2
    - pct_above_200dma: market breadth level as a 0-1 fraction
    """

    market_stage_norm = str(market_stage or "").strip().upper()
    regime_norm = str(regime or "").strip().lower()
    velocity_norm = str(breadth_velocity_bucket or "").strip().lower()

    invalid_s2_pct = False
    try:
        s2_value = float(s2_pct or 0.0)
    except (TypeError, ValueError):
        invalid_s2_pct = True
        s2_value = 0.0

    invalid_pct_above_200dma = False
    pct200_value: float | None
    if pct_above_200dma is None:
        pct200_value = None
    else:
        try:
            pct200_value = float(pct_above_200dma)
        except (TypeError, ValueError):
            invalid_pct_above_200dma = True
            pct200_value = None

    breadth_level_zone = _breadth_level_zone(pct200_value)
    positive_velocity = velocity_norm in {"positive", "very_positive"}

    driven_by = {
        "market_stage": market_stage_norm or None,
        "regime": regime_norm or None,
        "breadth_velocity_bucket": velocity_norm or None,
        "s2_pct": round(s2_value, 4),
        "pct_above_200dma": (
            round(pct200_value, 4)
            if pct200_value is not None and not invalid_pct_above_200dma
            else None
        ),
        "breadth_level_zone": breadth_level_zone,
        "transition_s2_threshold": float(transition_s2_threshold),
    }

    if invalid_s2_pct:
        phase = RegimePhase.MIXED_WAIT

    elif (
        market_stage_norm == "S4"
        or regime_norm == "risk_off"
        or (pct200_value is not None and pct200_value < 0.20)
    ):
        phase = RegimePhase.BEAR_STAGE4

    elif (
        regime_norm in {"bull", "strong_bull"}
        and market_stage_norm == "S2"
        and (pct200_value is None or pct200_value > 0.55)
    ):
        phase = RegimePhase.CONFIRMED_STAGE2_BULL

    elif (
        (
            (pct200_value is None and regime_norm == "neutral")
            or (pct200_value is not None and 0.20 <= pct200_value < 0.50)
        )
        and positive_velocity
        and s2_value < transition_s2_threshold
    ):
        phase = RegimePhase.BASE_FORMING_STAGE1

    elif (
        (
            (pct200_value is None and regime_norm in {"neutral", "cautious_bull"})
            or (pct200_value is not None and pct200_value >= 0.30)
        )
        and positive_velocity
        and s2_value >= transition_s2_threshold
    ):
        phase = RegimePhase.TRANSITION_S1_TO_S2

    else:
        phase = RegimePhase.MIXED_WAIT

    return RegimePhaseResult(
        regime_phase=phase,
        phase_label=PHASE_LABELS[phase],
        phase_emoji=PHASE_EMOJI[phase],
        driven_by=driven_by,
    )


def _breadth_level_zone(pct_above_200dma: float | None) -> str | None:
    if pct_above_200dma is None:
        return None
    if pct_above_200dma < 0.20:
        return "capitulation"
    if pct_above_200dma < 0.30:
        return "bear_bottom_zone"
    if pct_above_200dma < 0.50:
        return "base_recovery_zone"
    if pct_above_200dma < 0.75:
        return "risk_on_zone"
    if pct_above_200dma < 0.90:
        return "broad_bull_zone"
    return "overheated_breadth"
