"""NSE bhavcopy SERIES whitelist and trading-segment classification.

Centralizes the policy of which NSE series codes the ingest layer accepts.
Historically the parser was hard-filtered to ``EQ`` only, which silently
dropped symbols moved to the Trade-to-Trade segment (``BE``) or under
Surveillance Measures (``BZ``).
"""

from __future__ import annotations

from typing import Iterable


SUPPORTED_SERIES: tuple[str, ...] = ("EQ", "BE", "BZ")
INTRADAY_SERIES: tuple[str, ...] = ("EQ",)

_SEGMENT_BY_SERIES: dict[str, str] = {
    "EQ": "regular",
    "BE": "t2t",
    "BZ": "trade_to_trade_z",
}


def normalize_series(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def is_supported(series: object, *, allowed: Iterable[str] | None = None) -> bool:
    code = normalize_series(series)
    whitelist = tuple(normalize_series(item) for item in allowed) if allowed is not None else SUPPORTED_SERIES
    return code in whitelist


def is_intraday_eligible(series: object) -> bool:
    return normalize_series(series) in INTRADAY_SERIES


def trading_segment(series: object) -> str:
    return _SEGMENT_BY_SERIES.get(normalize_series(series), "unknown")
