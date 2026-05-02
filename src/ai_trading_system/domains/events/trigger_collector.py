"""Trigger collector — projects trigger sources into the common ``Trigger`` shape.

Three sources today:
  1. ``volume_shock``  — pre-detected by ``ranking.volume_shocker`` (already
                         a list of Triggers; passed through here for
                         universe filtering and bookkeeping).
  2. ``bulk_deal``     — read from ``market_intel`` via ``EventQueryService.get_bulk_deals``.
  3. ``breakout``      — read from the ``breakout_scan.csv`` artifact emitted
                         by the rank stage; filters to Tier A / B rows.

Each producer is small and side-effect-free so it can be unit-tested
without touching the orchestrator or the publish layer.
"""

from __future__ import annotations

import csv
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from ai_trading_system.domains.events.triggers import Trigger

logger = logging.getLogger(__name__)


# Defaults are conservative and tunable through config. We keep them here as
# module constants so tests don't have to construct a config object.
DEFAULT_BULK_LOOKBACK_DAYS = 3
DEFAULT_BULK_MIN_VALUE_CR = 5.0
DEFAULT_BREAKOUT_TIERS: frozenset[str] = frozenset({"A", "B"})


# --------------------------------------------------------------------------- bulk deals


def collect_bulk_deal_triggers(
    *,
    as_of_date: date,
    query_service,                      # EventQueryService — duck-typed for tests
    universe_symbols: Iterable[str] | None = None,
    lookback_days: int = DEFAULT_BULK_LOOKBACK_DAYS,
    min_value_cr: float = DEFAULT_BULK_MIN_VALUE_CR,
    block_only: bool = False,
) -> list[Trigger]:
    """Query market_intel for recent bulk/block deals and project to Triggers.

    ``query_service`` is the read-side service from
    ``ai_trading_system.integrations.market_intel_client``; we accept any
    duck-typed object exposing ``get_bulk_deals(...)`` to make tests trivial.

    ``trigger_strength`` is a soft normalization — for bulk deals we use
    ``deal_value_cr / 100`` clipped to [0.1, 5.0]. A ₹100Cr deal scores 1.0,
    a ₹500Cr+ deal saturates at 5.0.
    """
    since = as_of_date - timedelta(days=lookback_days)
    universe_set = set(universe_symbols) if universe_symbols else None

    try:
        deals = query_service.get_bulk_deals(
            symbols=list(universe_set) if universe_set else None,
            since=since,
            until=as_of_date,
            min_value_cr=min_value_cr,
            block_only=block_only,
        )
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("Bulk-deal query failed: %s", exc)
        return []

    triggers: list[Trigger] = []
    seen: set[tuple[str, str, str]] = set()
    for deal in deals:
        if universe_set is not None and deal.symbol not in universe_set:
            continue
        # Collapse multiple deal rows for the same (symbol, trade_date) to a
        # single trigger; downstream events stage doesn't need every fill.
        trigger_type = "bulk_deal"  # block deals also flow through the same trigger type
        key = (deal.symbol, trigger_type, deal.trade_date.isoformat())
        if key in seen:
            continue
        seen.add(key)

        value_cr = float(deal.deal_value_cr or 0.0)
        strength = max(0.1, min(value_cr / 100.0, 5.0)) if value_cr else 0.5

        meta: dict[str, object] = {
            "trade_date": deal.trade_date.isoformat(),
            "exchange": deal.exchange,
            "side": deal.side,
            "is_block": deal.is_block,
            "deal_value_cr": value_cr or None,
            "client_name": deal.client_name,
        }
        if deal.quantity is not None:
            meta["quantity"] = int(deal.quantity)
        if deal.avg_price is not None:
            meta["avg_price"] = float(deal.avg_price)

        triggers.append(
            Trigger(
                symbol=deal.symbol,
                trigger_type=trigger_type,
                as_of_date=as_of_date,
                trigger_strength=strength,
                trigger_metadata=meta,
            )
        )

    logger.info(
        "Collected %d bulk-deal triggers (since=%s, min_value_cr=%.1f)",
        len(triggers), since.isoformat(), min_value_cr,
    )
    return triggers


# --------------------------------------------------------------------------- breakout


def _row_get(row: dict[str, str], *keys: str) -> str | None:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return None


def _to_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def collect_breakout_triggers(
    breakout_csv_path: str | Path,
    *,
    as_of_date: date,
    universe_symbols: Iterable[str] | None = None,
    tiers: Iterable[str] = DEFAULT_BREAKOUT_TIERS,
) -> list[Trigger]:
    """Read ``breakout_scan.csv`` and project Tier A / B rows into Triggers.

    The CSV is the artifact written by ``ranking.breakout.compute_breakout_v2_scores``;
    columns are tolerated case-insensitively and we accept several aliases
    for ``symbol`` and ``tier`` because the artifact format has shifted
    across sprints.
    """
    path = Path(breakout_csv_path)
    if not path.exists():
        logger.info("Breakout CSV not found at %s — no breakout triggers", path)
        return []

    tier_set = {t.upper() for t in tiers}
    universe_set = set(universe_symbols) if universe_symbols else None
    triggers: list[Trigger] = []

    try:
        with path.open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                return []
            # Build a lowercase index so we can read columns case-insensitively.
            lower_field = {f.lower().strip(): f for f in reader.fieldnames}

            def col(*aliases: str) -> str | None:
                for alias in aliases:
                    if alias.lower() in lower_field:
                        return lower_field[alias.lower()]
                return None

            symbol_col = col("symbol", "symbol_id", "ticker")
            tier_col = col("tier", "breakout_tier", "stage_tier")
            score_col = col(
                "score", "breakout_score", "stage2_score", "composite_score",
            )
            if symbol_col is None or tier_col is None:
                logger.warning(
                    "Breakout CSV at %s missing symbol/tier columns; got %s",
                    path, list(reader.fieldnames),
                )
                return []

            for row in reader:
                symbol = (row.get(symbol_col) or "").strip().upper()
                if not symbol:
                    continue
                tier = (row.get(tier_col) or "").strip().upper()
                if tier not in tier_set:
                    continue
                if universe_set is not None and symbol not in universe_set:
                    continue
                score = _to_float(row.get(score_col)) if score_col else None
                strength = (score / 100.0) if score is not None else 1.0
                meta: dict[str, object] = {"tier": tier}
                if score is not None:
                    meta["score"] = score
                triggers.append(
                    Trigger(
                        symbol=symbol,
                        trigger_type="breakout",
                        as_of_date=as_of_date,
                        trigger_strength=strength,
                        trigger_metadata=meta,
                    )
                )
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("Failed to read breakout CSV at %s: %s", path, exc)
        return []

    logger.info(
        "Collected %d breakout triggers (tiers=%s) from %s",
        len(triggers), sorted(tier_set), path,
    )
    return triggers


# --------------------------------------------------------------------------- merge


def merge_triggers(*sources: Iterable[Trigger]) -> list[Trigger]:
    """Concatenate multiple trigger streams, removing exact dedupe-key duplicates.

    A symbol can have several distinct trigger types in the same run (e.g.
    a volume shock + a bulk deal); those are kept. Duplicates within a
    single source are collapsed via ``Trigger.dedupe_key()``.
    """
    seen: set[tuple[str, str, str]] = set()
    out: list[Trigger] = []
    for stream in sources:
        for trig in stream:
            key = trig.dedupe_key()
            if key in seen:
                continue
            seen.add(key)
            out.append(trig)
    return out
