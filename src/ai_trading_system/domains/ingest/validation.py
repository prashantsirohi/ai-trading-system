"""Shared write-boundary validation for ingest paths."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

VALID_EXCHANGES = {"NSE", "BSE"}


class IngestValidationError(ValueError):
    """Raised when ingest rows fail schema or data contract validation."""


@dataclass(frozen=True)
class ValidationContext:
    source_label: str
    dataset_name: str


def _sample_rows(frame: pd.DataFrame, mask: pd.Series, *, limit: int = 3) -> list[dict]:
    if not mask.any():
        return []
    return frame.loc[mask].head(limit).to_dict("records")


def _raise_invalid(message: str, *, ctx: ValidationContext, sample: list[dict] | None = None) -> None:
    sample_suffix = f" sample={sample}" if sample else ""
    raise IngestValidationError(f"[{ctx.source_label}] {ctx.dataset_name}: {message}{sample_suffix}")


def _require_columns(frame: pd.DataFrame, *, required: set[str], ctx: ValidationContext) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        _raise_invalid(f"missing required columns: {missing}", ctx=ctx)


def _normalize_symbol_exchange(frame: pd.DataFrame, *, ctx: ValidationContext) -> pd.DataFrame:
    normalized = frame.copy(deep=True)
    normalized.loc[:, "symbol_id"] = normalized["symbol_id"].astype(str).str.strip()
    normalized.loc[:, "exchange"] = normalized["exchange"].astype(str).str.strip().str.upper()

    null_keys = normalized["symbol_id"].eq("") | normalized["exchange"].eq("")
    if null_keys.any():
        _raise_invalid(
            "empty symbol_id/exchange not allowed",
            ctx=ctx,
            sample=_sample_rows(normalized, null_keys),
        )

    swapped = normalized["symbol_id"].isin(VALID_EXCHANGES) & ~normalized["exchange"].isin(VALID_EXCHANGES)
    if swapped.any():
        _raise_invalid(
            "detected swapped symbol_id/exchange rows (repair with ai-trading-repair-ingest-schema --apply)",
            ctx=ctx,
            sample=_sample_rows(normalized, swapped),
        )

    invalid_exchange = ~normalized["exchange"].isin(VALID_EXCHANGES)
    if invalid_exchange.any():
        _raise_invalid(
            "invalid exchange values",
            ctx=ctx,
            sample=_sample_rows(normalized, invalid_exchange),
        )
    return normalized


def _coerce_timestamp(frame: pd.DataFrame, *, ctx: ValidationContext) -> pd.DataFrame:
    coerced = frame.copy(deep=True)
    coerced.loc[:, "timestamp"] = pd.to_datetime(coerced["timestamp"], errors="coerce")
    invalid_ts = coerced["timestamp"].isna()
    if invalid_ts.any():
        _raise_invalid("invalid timestamp values", ctx=ctx, sample=_sample_rows(coerced, invalid_ts))
    return coerced


def validate_ohlcv_frame(frame: pd.DataFrame, *, source_label: str) -> pd.DataFrame:
    """Validate and normalize an OHLCV frame prior to any DB write."""
    ctx = ValidationContext(source_label=source_label, dataset_name="_catalog")
    if frame.empty:
        return frame.copy()

    _require_columns(
        frame,
        required={"symbol_id", "security_id", "exchange", "timestamp", "open", "high", "low", "close", "volume"},
        ctx=ctx,
    )
    normalized = _normalize_symbol_exchange(frame, ctx=ctx)
    normalized = _coerce_timestamp(normalized, ctx=ctx)

    for column in ("open", "high", "low", "close", "volume"):
        normalized.loc[:, column] = pd.to_numeric(normalized[column], errors="coerce")

    invalid_price_nulls = normalized[["open", "high", "low", "close"]].isna().any(axis=1)
    if invalid_price_nulls.any():
        _raise_invalid(
            "OHLC values must be numeric and non-null",
            ctx=ctx,
            sample=_sample_rows(normalized, invalid_price_nulls),
        )

    volume = pd.to_numeric(normalized["volume"], errors="coerce")
    normalized.loc[:, "volume"] = volume.where(volume.notna(), 0)
    invalid_volume = normalized["volume"] < 0
    if invalid_volume.any():
        _raise_invalid("volume cannot be negative", ctx=ctx, sample=_sample_rows(normalized, invalid_volume))

    invalid_ohlc = (
        (normalized["high"] < normalized["low"])
        | (normalized["high"] < normalized["open"])
        | (normalized["high"] < normalized["close"])
        | (normalized["low"] > normalized["open"])
        | (normalized["low"] > normalized["close"])
    )
    if invalid_ohlc.any():
        _raise_invalid(
            "OHLC consistency check failed",
            ctx=ctx,
            sample=_sample_rows(normalized, invalid_ohlc),
        )
    return normalized


def validate_delivery_frame(frame: pd.DataFrame, *, source_label: str) -> pd.DataFrame:
    """Validate and normalize a delivery frame prior to DB write."""
    ctx = ValidationContext(source_label=source_label, dataset_name="_delivery")
    if frame.empty:
        return frame.copy()

    _require_columns(frame, required={"symbol_id", "exchange", "timestamp", "delivery_pct"}, ctx=ctx)
    normalized = _normalize_symbol_exchange(frame, ctx=ctx)
    normalized = _coerce_timestamp(normalized, ctx=ctx)

    normalized.loc[:, "delivery_pct"] = pd.to_numeric(normalized["delivery_pct"], errors="coerce")
    invalid_delivery_pct = normalized["delivery_pct"].isna() | (normalized["delivery_pct"] < 0) | (
        normalized["delivery_pct"] > 100
    )
    if invalid_delivery_pct.any():
        _raise_invalid(
            "delivery_pct must be numeric between 0 and 100",
            ctx=ctx,
            sample=_sample_rows(normalized, invalid_delivery_pct),
        )

    for column in ("volume", "delivery_qty"):
        if column not in normalized.columns:
            normalized.loc[:, column] = 0
        numeric = pd.to_numeric(normalized[column], errors="coerce")
        normalized.loc[:, column] = numeric.where(numeric.notna(), 0)
        invalid = normalized[column] < 0
        if invalid.any():
            _raise_invalid(f"{column} cannot be negative", ctx=ctx, sample=_sample_rows(normalized, invalid))

    return normalized
