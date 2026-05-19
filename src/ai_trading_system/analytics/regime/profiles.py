"""Regime aggression profile loader."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml


# 5-tier ladder. cautious_bull sits between neutral and bull: 200DMA breadth
# is healthy but new-high leadership is thin — allow entries on top-ranked
# breakouts only, smaller exposure than full bull.
REGIMES = ("risk_off", "neutral", "cautious_bull", "bull", "strong_bull")


@dataclass(frozen=True)
class RegimeProfile:
    regime: str
    min_score: float
    rank_top_n: int
    max_exposure: float
    max_positions: int
    max_sector_exposure: float
    max_single_stock_weight: float
    atr_stop_mult: float
    breakout_mode: str
    allow_pyramiding: bool
    name: str = "unnamed"

    @classmethod
    def from_mapping(cls, regime: str, payload: dict[str, Any], *, name: str = "unnamed") -> "RegimeProfile":
        return cls(
            name=name,
            regime=regime,
            min_score=float(payload["min_score"]),
            rank_top_n=int(payload["rank_top_n"]),
            max_exposure=float(payload["max_exposure"]),
            max_positions=int(payload["max_positions"]),
            max_sector_exposure=float(payload["max_sector_exposure"]),
            max_single_stock_weight=float(payload["max_single_stock_weight"]),
            atr_stop_mult=float(payload["atr_stop_mult"]),
            breakout_mode=str(payload.get("breakout_mode", "normal")),
            allow_pyramiding=bool(payload.get("allow_pyramiding", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def resolve_regime_profile_path(project_root: Path | str, value: str | Path | None = None) -> Path:
    """Resolve an explicit profile path or the active profile symlink."""
    root = Path(project_root)
    if value:
        path = Path(value)
        return path if path.is_absolute() else root / path
    return root / "config" / "active_regime_profile.yaml"


def load_regime_profile(
    regime: str,
    *,
    project_root: Path | str,
    profile_path: str | Path | None = None,
) -> RegimeProfile | None:
    """Load the active regime profile section for ``regime``.

    Missing files return ``None`` so rank/execute can continue with their
    existing defaults when the overlay has not been enabled.
    """
    path = resolve_regime_profile_path(project_root, profile_path)
    if not path.exists():
        return None
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    section = payload.get(regime)
    if not isinstance(section, dict):
        return None
    return RegimeProfile.from_mapping(regime, section, name=str(payload.get("name") or path.stem))


def load_all_profiles(project_root: Path | str, profile_path: str | Path | None = None) -> dict[str, RegimeProfile]:
    path = resolve_regime_profile_path(project_root, profile_path)
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    name = str(payload.get("name") or path.stem)
    return {
        regime: RegimeProfile.from_mapping(regime, payload[regime], name=name)
        for regime in REGIMES
        if isinstance(payload.get(regime), dict)
    }
