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
    # Phase 6: per-regime per-trade risk cap. None means "no profile-driven
    # value — fall back to whatever the signal payload / execution caller
    # specifies." Existing profiles without this field stay backward-compat.
    risk_per_trade_pct: float | None = None
    name: str = "unnamed"

    @classmethod
    def from_mapping(cls, regime: str, payload: dict[str, Any], *, name: str = "unnamed") -> "RegimeProfile":
        risk_raw = payload.get("risk_per_trade_pct")
        risk_value: float | None
        if risk_raw is None:
            risk_value = None
        else:
            try:
                risk_value = float(risk_raw)
            except (TypeError, ValueError):
                risk_value = None
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
            risk_per_trade_pct=risk_value,
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


# ── Phase 8: Breadth-impulse risk matrix ──────────────────────────────────
#
# Two-dimensional sizing keyed by (confirmed_regime, breadth_velocity_bucket).
# Live sizing is OFF — execute.py reads this matrix only when the
# ``config/active_risk_matrix.yaml`` symlink exists, and writes the proposed
# cell into a dry-run audit block. ``regime_age_multiplier`` decays the
# matrix exposure as a confirmed regime ages, so a stale strong_bull doesn't
# unlock max risk indefinitely.

VELOCITY_BUCKETS: tuple[str, ...] = (
    "very_negative",
    "negative",
    "neutral",
    "positive",
    "very_positive",
)


def regime_age_multiplier(age_days: int) -> float:
    """Decay factor applied to regime_confidence and matrix exposure.

    A regime is most informative immediately after confirmation. The longer
    a band has been in force, the more likely the underlying breadth is
    drifting toward a transition the confirmation filter hasn't yet
    surfaced. Buckets are intentionally coarse — there's no signal to
    distinguish day 25 from day 35.
    """
    age = max(int(age_days), 0)
    if age <= 20:
        return 1.00
    if age <= 40:
        return 0.85
    if age <= 60:
        return 0.70
    return 0.50


@dataclass(frozen=True)
class RiskCell:
    regime: str
    velocity_bucket: str
    gross_exposure: float
    allow_new_buys: bool
    min_score: float
    require_breakout_tier: str | None
    require_setup_quality_gte: float | None
    allow_pyramiding: bool
    action: str

    @classmethod
    def from_mapping(cls, regime: str, bucket: str, payload: dict[str, Any]) -> "RiskCell":
        breakout = payload.get("require_breakout_tier")
        setup_q = payload.get("require_setup_quality_gte")
        return cls(
            regime=regime,
            velocity_bucket=bucket,
            gross_exposure=float(payload["gross_exposure"]),
            allow_new_buys=bool(payload["allow_new_buys"]),
            min_score=float(payload["min_score"]),
            require_breakout_tier=str(breakout) if breakout is not None else None,
            require_setup_quality_gte=float(setup_q) if setup_q is not None else None,
            allow_pyramiding=bool(payload.get("allow_pyramiding", False)),
            action=str(payload.get("action", "hold")),
        )


@dataclass(frozen=True)
class BreadthImpulseRiskMatrix:
    name: str
    cells: dict[tuple[str, str], RiskCell]

    def lookup(self, regime: str, velocity_bucket: str) -> RiskCell:
        return self.cells[(regime, velocity_bucket)]


def resolve_risk_matrix_path(project_root: Path | str, value: str | Path | None = None) -> Path:
    root = Path(project_root)
    if value:
        path = Path(value)
        return path if path.is_absolute() else root / path
    return root / "config" / "active_risk_matrix.yaml"


def load_risk_matrix(
    *,
    project_root: Path | str,
    matrix_path: str | Path | None = None,
) -> BreadthImpulseRiskMatrix | None:
    """Load the active 2-D risk matrix. Returns None when the symlink is absent.

    Validation: every (regime, bucket) in REGIMES × VELOCITY_BUCKETS must
    be present (25 cells). Missing cells raise at load time — Phase 4 style
    fail-loud, no silent fall-throughs.
    """
    path = resolve_risk_matrix_path(project_root, matrix_path)
    if not path.exists():
        return None
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    name = str(payload.get("name") or path.stem)
    cells: dict[tuple[str, str], RiskCell] = {}
    for regime in REGIMES:
        block = payload.get(regime)
        if not isinstance(block, dict):
            raise ValueError(
                f"risk matrix '{name}': missing regime block '{regime}'"
            )
        for bucket in VELOCITY_BUCKETS:
            cell_payload = block.get(bucket)
            if not isinstance(cell_payload, dict):
                raise ValueError(
                    f"risk matrix '{name}': missing cell '{regime}.{bucket}'"
                )
            cells[(regime, bucket)] = RiskCell.from_mapping(regime, bucket, cell_payload)
    return BreadthImpulseRiskMatrix(name=name, cells=cells)
