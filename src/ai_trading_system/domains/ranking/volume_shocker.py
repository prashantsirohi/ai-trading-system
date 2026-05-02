"""Volume-shocker detector.

Identifies symbols whose daily volume exceeds N standard deviations above the
20-day rolling mean. Reuses ``volume_zscore_20`` already computed by
``apply_volume_intensity`` in ``factors.py``.

Output is a DataFrame ranked by ``shock_intensity`` (z-score normalized to the
configured threshold) suitable for projection into a ``Trigger`` row.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Iterable

import numpy as np
import pandas as pd

from ai_trading_system.domains.events.triggers import Trigger

logger = logging.getLogger(__name__)


# Defaults are intentionally conservative; tune via config/events_filters.yaml.
DEFAULT_Z_THRESHOLD = 3.0
DEFAULT_MIN_TURNOVER_CR = 1.0
DEFAULT_MIN_MARKET_CAP_CR = 500.0


@dataclass(frozen=True)
class VolumeShockerConfig:
    z_threshold: float = DEFAULT_Z_THRESHOLD
    min_turnover_cr: float = DEFAULT_MIN_TURNOVER_CR
    min_market_cap_cr: float = DEFAULT_MIN_MARKET_CAP_CR
    universe_symbols: frozenset[str] | None = None


def detect_volume_shockers(
    feature_frame: pd.DataFrame,
    *,
    config: VolumeShockerConfig | None = None,
) -> pd.DataFrame:
    """Return rows whose ``volume_zscore_20`` clears the threshold gates.

    Expected columns:
      - ``symbol`` (or falls back to ``symbol_id``)
      - ``volume_zscore_20``
      - ``volume`` and ``close`` (used for turnover sanity-check) — optional;
        if absent, the turnover gate is skipped.
      - ``market_cap_cr`` — optional; if absent, the market-cap gate is skipped.

    Adds ``shock_intensity = volume_zscore_20 / z_threshold`` for downstream
    ranking. Result is sorted by ``shock_intensity`` descending.
    """
    cfg = config or VolumeShockerConfig()
    if feature_frame is None or feature_frame.empty:
        return _empty_result()

    df = feature_frame.copy()
    if "symbol" not in df.columns and "symbol_id" in df.columns:
        df = df.rename(columns={"symbol_id": "symbol"})
    if "symbol" not in df.columns or "volume_zscore_20" not in df.columns:
        logger.debug(
            "volume_shocker: missing required columns; have=%s",
            list(df.columns)[:20],
        )
        return _empty_result()

    z = pd.to_numeric(df["volume_zscore_20"], errors="coerce")
    df = df.assign(volume_zscore_20=z)
    df = df[z >= cfg.z_threshold]
    if df.empty:
        return _empty_result()

    if "volume" in df.columns and "close" in df.columns:
        turnover_cr = (
            pd.to_numeric(df["volume"], errors="coerce")
            * pd.to_numeric(df["close"], errors="coerce")
            / 1e7
        )
        df = df.assign(turnover_cr=turnover_cr.fillna(0.0))
        df = df[df["turnover_cr"] >= cfg.min_turnover_cr]

    if "market_cap_cr" in df.columns:
        mcap = pd.to_numeric(df["market_cap_cr"], errors="coerce")
        df = df[mcap.fillna(0.0) >= cfg.min_market_cap_cr]

    if cfg.universe_symbols is not None:
        df = df[df["symbol"].isin(cfg.universe_symbols)]

    if df.empty:
        return _empty_result()

    df = df.assign(
        shock_intensity=df["volume_zscore_20"].astype(float) / cfg.z_threshold
    )
    df = df.sort_values("shock_intensity", ascending=False).reset_index(drop=True)
    return df


def to_triggers(
    shocker_frame: pd.DataFrame,
    *,
    as_of: date,
) -> list[Trigger]:
    """Project the detector output into ``Trigger`` rows."""
    if shocker_frame is None or shocker_frame.empty:
        return []
    triggers: list[Trigger] = []
    for _, row in shocker_frame.iterrows():
        symbol = str(row["symbol"])
        z = float(row.get("volume_zscore_20") or 0.0)
        intensity = float(row.get("shock_intensity") or 0.0)
        meta: dict[str, object] = {"z_score": z}
        for col in ("volume", "vol_20_avg", "turnover_cr", "close", "market_cap_cr"):
            if col in row and not _is_nan(row[col]):
                meta[col] = float(row[col])
        triggers.append(
            Trigger(
                symbol=symbol,
                trigger_type="volume_shock",
                as_of_date=as_of,
                trigger_strength=intensity,
                trigger_metadata=meta,
            )
        )
    return triggers


def _empty_result() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["symbol", "volume_zscore_20", "shock_intensity"]
    )


def _is_nan(value: object) -> bool:
    try:
        return bool(np.isnan(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return value is None
