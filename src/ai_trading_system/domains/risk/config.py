"""Risk-policy config + YAML profile loader."""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Literal

import yaml

StopMethod = Literal["atr", "percent", "swing_low", "breakout_candle_low", "hybrid"]
SizingMethod = Literal["equal_weight", "atr_risk"]
DMAExitWindow = Literal[11, 20, 50]


@dataclass(frozen=True)
class EntryConfig:
    require_stage_2: bool = True
    require_price_above_sma200: bool = True
    require_price_above_sma50: bool = False
    require_price_above_ema20: bool = False
    require_sma50_above_sma200_or_rising_20d: bool = False
    require_sector_positive: bool = True
    min_volume_ratio: float = 1.5
    require_delivery_above_sector_median: bool = False
    min_close_to_52w_high: float | None = None
    min_return_20_pct: float | None = None
    min_return_50_pct: float | None = None
    max_drawdown_from_recent_high_pct: float | None = None
    max_below_ema20_days_20: int | None = None


@dataclass(frozen=True)
class StopConfig:
    method: StopMethod = "atr"
    atr_multiple: float = 2.0
    stop_pct: float = 0.05
    hybrid_atr_multiple: float = 2.5


@dataclass(frozen=True)
class ExitConfig:
    emergency_exit_below_sma200: bool = True
    dma_exit_window: DMAExitWindow | None = 20
    dma_whipsaw_buffer_pct: float = 0.5
    exit_on_rank_deterioration: bool = True
    max_hold_rank: int = 50
    rank_deterioration_bars: int = 3
    exit_on_score_deterioration: bool = True
    min_hold_score: float = 60.0
    score_deterioration_bars: int = 3
    time_stop_days: int | None = 60


@dataclass(frozen=True)
class SizingConfig:
    method: SizingMethod = "equal_weight"
    risk_per_trade_pct: float = 1.0
    max_position_pct: float = 12.0


@dataclass(frozen=True)
class ConstraintsConfig:
    max_concurrent_positions: int = 8
    max_stock_weight_pct: float = 12.0
    max_sector_exposure_pct: float = 30.0


@dataclass(frozen=True)
class RiskPolicyConfig:
    """Bundle of all risk knobs the engine needs."""

    name: str = "balanced_swing"
    entry: EntryConfig = field(default_factory=EntryConfig)
    stop: StopConfig = field(default_factory=StopConfig)
    exit: ExitConfig = field(default_factory=ExitConfig)
    sizing: SizingConfig = field(default_factory=SizingConfig)
    constraints: ConstraintsConfig = field(default_factory=ConstraintsConfig)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RiskPolicyConfig":
        def _coerce(section_cls, raw: dict | None):
            if not raw:
                return section_cls()
            allowed = {f.name for f in fields(section_cls)}
            return section_cls(**{k: v for k, v in raw.items() if k in allowed})

        return cls(
            name=str(payload.get("name") or "unnamed"),
            entry=_coerce(EntryConfig, payload.get("entry")),
            stop=_coerce(StopConfig, payload.get("stop")),
            exit=_coerce(ExitConfig, payload.get("exit")),
            sizing=_coerce(SizingConfig, payload.get("sizing")),
            constraints=_coerce(ConstraintsConfig, payload.get("constraints")),
        )


_PROFILE_DIR_CANDIDATES = (
    Path(__file__).resolve().parents[4] / "config" / "risk_profiles",  # repo-root/config
    Path.cwd() / "config" / "risk_profiles",
)


def profile_search_dirs() -> tuple[Path, ...]:
    """Public accessor for the candidate dirs the loader scans."""
    return _PROFILE_DIR_CANDIDATES


def _resolve_profile_path(name: str) -> Path | None:
    for base in _PROFILE_DIR_CANDIDATES:
        candidate = base / f"{name}.yaml"
        if candidate.exists():
            return candidate
    return None


def load_profile(name: str, *, strict: bool = False) -> RiskPolicyConfig:
    """Load a named risk profile from ``config/risk_profiles/<name>.yaml``."""
    path = _resolve_profile_path(name)
    if path is None:
        if strict:
            raise FileNotFoundError(f"Risk profile not found: {name}")
        if name == "balanced_swing":
            return RiskPolicyConfig(name="balanced_swing")
        return load_profile("balanced_swing", strict=True)
    payload = yaml.safe_load(path.read_text()) or {}
    payload.setdefault("name", name)
    return RiskPolicyConfig.from_dict(payload)
