"""Rule-based sector health classification.

Aggregates per-symbol weekly stage labels into a sector verdict using
explicit numeric rules (not pixel-derived from any UI mock).

Inputs:
  - frame: rows with `symbol`, `sector`, `stage_label`, `stage_transition`
  - sector_rs: optional dict[sector -> rs_score] (latest)
  - sector_rs_trend: optional dict[sector -> rs_change_4w]
  - s2_share_trend: optional dict[sector -> change in S2 share over 4 weeks]
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional

import pandas as pd


@dataclass(frozen=True)
class SectorHealth:
    sector: str
    total: int
    s1: int
    s2: int
    s3: int
    s4: int
    s1_to_s2: int
    health: str   # Healthy / Improving / Neutral / Weakening / Unhealthy

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        for k in ("s1", "s2", "s3", "s4", "s1_to_s2", "total"):
            d[f"{k}_pct"] = (d[k] / d["total"]) if d["total"] else 0.0
        return d


def classify_sector_health(
    frame: pd.DataFrame,
    *,
    sector_rs: Optional[Mapping[str, float]] = None,
    sector_rs_trend: Optional[Mapping[str, float]] = None,
    s2_share_trend: Optional[Mapping[str, float]] = None,
) -> list[SectorHealth]:
    """Return a SectorHealth row per sector found in `frame`."""
    if frame.empty:
        return []
    required = {"sector", "stage_label"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"frame missing columns: {sorted(missing)}")

    sector_rs = sector_rs or {}
    sector_rs_trend = sector_rs_trend or {}
    s2_share_trend = s2_share_trend or {}

    out: list[SectorHealth] = []
    for sector, sub in frame.groupby("sector", dropna=False):
        counts = sub["stage_label"].value_counts()
        s1 = int(counts.get("S1", 0))
        s2 = int(counts.get("S2", 0))
        s3 = int(counts.get("S3", 0))
        s4 = int(counts.get("S4", 0))
        total = s1 + s2 + s3 + s4
        s1_to_s2 = int((sub.get("stage_transition") == "S1_TO_S2").sum()) \
            if "stage_transition" in sub.columns else 0

        rs = float(sector_rs.get(sector, 0.0))
        rs_trend = float(sector_rs_trend.get(sector, 0.0))
        s2_trend = float(s2_share_trend.get(sector, 0.0))

        health = _verdict(
            total=total, s1=s1, s2=s2, s3=s3, s4=s4,
            s1_to_s2=s1_to_s2, rs=rs, rs_trend=rs_trend, s2_trend=s2_trend,
        )
        out.append(SectorHealth(
            sector=str(sector), total=total,
            s1=s1, s2=s2, s3=s3, s4=s4,
            s1_to_s2=s1_to_s2, health=health,
        ))
    return out


def _verdict(*, total, s1, s2, s3, s4, s1_to_s2, rs, rs_trend, s2_trend) -> str:
    if total == 0:
        return "Neutral"
    pct = lambda k: k / total
    # Healthy: dominant S2 (or S1->S2 turnover) and positive sector RS.
    if (pct(s2) + pct(s1_to_s2)) > 0.60 and rs > 0:
        return "Healthy"
    # Unhealthy: heavy S4 share or strongly negative RS.
    if pct(s4) >= 0.35 or rs < -1.0:
        return "Unhealthy"
    # Improving: S2 share rising and RS rising (even if absolute share modest).
    if s2_trend > 0 and rs_trend > 0 and pct(s2) >= 0.20:
        return "Improving"
    # Weakening: S2 share falling, or S3 share creeping above 20%.
    if s2_trend < 0 or pct(s3) > 0.20:
        return "Weakening"
    return "Neutral"
