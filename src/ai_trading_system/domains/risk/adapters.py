"""Adapters: build engine inputs from the data shapes the rest of the codebase uses.

Keeping these outside the pure rule modules means ``rule_engine.py`` never has to
know about pandas, the execution ledger, or the ranked CSV schema. Both
backtesting and paper trading import from here.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Mapping

from ai_trading_system.domains.risk.contracts import (
    CandidateSignal,
    MarketSnapshot,
    PortfolioSnapshot,
    PositionSnapshot,
)


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:  # NaN
        return None
    return out


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _coerce_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value[:19].replace("Z", "")).date()
    return date.today()


def _coerce_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def candidate_from_row(row: Mapping[str, Any]) -> CandidateSignal:
    """Build a ``CandidateSignal`` from one ranked_signals.csv row."""
    rank = _coerce_int(
        row.get("eligible_rank") or row.get("rank_position") or row.get("rank") or 0
    )
    return CandidateSignal(
        symbol_id=str(row.get("symbol_id") or ""),
        exchange=str(row.get("exchange") or "NSE"),
        rank=rank,
        composite_score=_coerce_float(
            row.get("composite_score_adjusted") or row.get("composite_score")
        )
        or 0.0,
        is_stage_2=_coerce_bool(row.get("is_stage2_uptrend") or row.get("is_stage_2")),
        sector=str(row.get("sector_name") or row.get("sector") or ""),
        sector_strength=_coerce_float(
            row.get("sector_strength_score")
            or row.get("sector_strength")
            or row.get("sector_rs_value")
        )
        or 0.0,
        watchlist_bucket=row.get("watchlist_bucket"),
    )


def market_from_row(
    row: Mapping[str, Any],
    *,
    as_of: date | None = None,
    atr_override: float | None = None,
    extra: Mapping[str, Any] | None = None,
) -> MarketSnapshot:
    """Build a ``MarketSnapshot``. ``extra`` supplies fields not in ranked_signals."""
    extra = dict(extra or {})
    close = _coerce_float(row.get("close")) or 0.0

    vol_ratio = _coerce_float(row.get("volume_ratio_20") or extra.get("volume_ratio_20"))
    if vol_ratio is None:
        raw_vol = _coerce_float(row.get("volume"))
        avg_vol = _coerce_float(row.get("vol_20_avg"))
        if raw_vol is not None and avg_vol and avg_vol > 0:
            vol_ratio = raw_vol / avg_vol

    atr_14 = atr_override
    if atr_14 is None:
        atr_14 = _coerce_float(row.get("atr_14") or extra.get("atr_14")) or 0.0

    return MarketSnapshot(
        symbol_id=str(row.get("symbol_id") or ""),
        exchange=str(row.get("exchange") or "NSE"),
        date=as_of or _coerce_date(row.get("timestamp") or row.get("date")),
        close=close,
        atr_14=atr_14 or 0.0,
        open=_coerce_float(row.get("open") or extra.get("open")),
        high=_coerce_float(row.get("high") or extra.get("high")),
        low=_coerce_float(row.get("low") or extra.get("low")),
        sma_11=_coerce_float(row.get("sma_11") or extra.get("sma_11")),
        sma_20=_coerce_float(row.get("sma_20")),
        sma_50=_coerce_float(row.get("sma_50")),
        sma_200=_coerce_float(row.get("sma_200") or row.get("sma_150") or extra.get("sma_200")),
        ema_20=_coerce_float(row.get("ema_20") or row.get("ema20") or extra.get("ema_20")),
        high_52w=_coerce_float(row.get("high_52w") or row.get("hi_52w") or extra.get("high_52w")),
        return_20_pct=_coerce_float(row.get("return_20") or row.get("return_20_pct")),
        return_50_pct=_coerce_float(row.get("return_50") or row.get("return_50_pct")),
        sma50_rising_20d=_coerce_optional_bool(row.get("sma50_rising_20d")),
        drawdown_from_recent_high_pct=_coerce_float(
            row.get("drawdown_from_recent_high_pct") or row.get("drawdown_recent_high_pct")
        ),
        below_ema20_days_20=(
            int(v)
            if (v := _coerce_float(row.get("below_ema20_days_20") or row.get("below_ema20_20"))) is not None
            else None
        ),
        volume_ratio_20=vol_ratio,
        delivery_pct=_coerce_float(row.get("delivery_pct")),
        sector_delivery_median=_coerce_float(
            row.get("sector_delivery_median") or row.get("delivery_sector_median_20d")
        ),
        swing_low_20=_coerce_float(row.get("swing_low_20") or extra.get("swing_low_20")),
        breakout_candle_low=_coerce_float(
            row.get("breakout_candle_low") or extra.get("breakout_candle_low")
        ),
    )


def position_from_execution_snapshot(
    snapshot: Any,
    *,
    sector: str = "",
    stop_record: Mapping[str, Any] | None = None,
    rank_at_entry: int | None = None,
    score_at_entry: float | None = None,
) -> PositionSnapshot:
    """Convert an ``execution.portfolio.PositionSnapshot`` + optional stop row."""
    metadata: dict[str, Any] = {}
    if stop_record:
        raw = stop_record.get("metadata")
        if isinstance(raw, dict):
            metadata = dict(raw)
        else:
            raw_json = stop_record.get("metadata_json")
            if isinstance(raw_json, str) and raw_json:
                try:
                    import json
                    metadata = json.loads(raw_json)
                except (json.JSONDecodeError, ValueError):
                    metadata = {}
    entry_date_value = (
        stop_record.get("created_at") if stop_record else None
    ) or metadata.get("entry_date")
    return PositionSnapshot(
        symbol_id=str(getattr(snapshot, "symbol_id", "") or ""),
        exchange=str(getattr(snapshot, "exchange", "NSE") or "NSE"),
        entry_date=_coerce_date(entry_date_value or date.today()),
        entry_price=float(
            (stop_record.get("entry_price") if stop_record else None)
            or getattr(snapshot, "avg_entry_price", 0.0)
            or 0.0
        ),
        shares=int(getattr(snapshot, "quantity", 0) or 0),
        sector=sector or metadata.get("sector") or "",
        stop_price=_coerce_float(stop_record.get("stop_price")) if stop_record else None,
        stop_method=metadata.get("stop_method") if isinstance(metadata, dict) else None,
        rank_at_entry=rank_at_entry if rank_at_entry is not None else metadata.get("rank_at_entry"),
        score_at_entry=score_at_entry
        if score_at_entry is not None
        else metadata.get("score_at_entry"),
        bars_held=_coerce_int(metadata.get("bars_held"), 0),
        rank_above_threshold_streak=_coerce_int(
            metadata.get("rank_above_threshold_streak"), 0
        ),
        score_below_threshold_streak=_coerce_int(
            metadata.get("score_below_threshold_streak"), 0
        ),
    )


def portfolio_snapshot(
    *,
    positions: Iterable[PositionSnapshot],
    equity: float,
    cash: float | None = None,
) -> PortfolioSnapshot:
    """Aggregate positions into a portfolio snapshot. Sector exposure derived."""
    pos_tuple = tuple(positions)
    if equity <= 0:
        return PortfolioSnapshot(cash=cash or 0.0, equity=equity, positions=pos_tuple)
    sector_exposure: dict[str, float] = {}
    invested = 0.0
    for p in pos_tuple:
        value = float(p.entry_price) * int(p.shares)
        invested += value
        if not p.sector:
            continue
        sector_exposure[p.sector] = sector_exposure.get(p.sector, 0.0) + value / equity
    return PortfolioSnapshot(
        cash=cash if cash is not None else max(equity - invested, 0.0),
        equity=equity,
        positions=pos_tuple,
        sector_exposure=sector_exposure,
    )
