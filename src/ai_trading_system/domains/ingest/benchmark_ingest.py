"""Benchmark/index ingest helpers for market-context enrichment."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

import pandas as pd

from ai_trading_system.domains.ingest.providers.nse import NSECollector


@dataclass(frozen=True)
class BenchmarkSpec:
    symbol: str
    label: str
    provider: str = "nse_bhavcopy"
    instrument_type: str = "index"


BENCHMARKS = [
    "NIFTY 50",
    "NIFTY 100",
    "NIFTY 200",
    "NIFTY 500",
    "NIFTY NEXT 50",
    "NIFTY MIDCAP 100",
    "NIFTY MIDCAP 150",
    "NIFTY SMLCAP 100",
    "NIFTY SMLCAP 250",
    "NIFTY MIDSML 400",
    "NIFTY BANK",
    "NIFTY FINSRV25 50",
    "NIFTY IT",
    "NIFTY AUTO",
    "NIFTY PHARMA",
    "NIFTY FMCG",
    "NIFTY ENERGY",
    "NIFTY INFRA",
    "NIFTY REALTY",
    "NIFTY METAL",
    "NIFTY PSU BANK",
    "NIFTY PSE",
    "NIFTY COMMODITIES",
    "NIFTY CONSUMPTION",
    "NIFTY MEDIA",
    "NIFTY SERV SECTOR",
    "NIFTY MNC",
]

DEFAULT_BENCHMARKS = [BenchmarkSpec(symbol=label.replace(" ", "_"), label=label) for label in BENCHMARKS]


def _normalized_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out.columns = [str(col).replace("\ufeff", "").strip().replace(" ", "") for col in out.columns]
    return out


def _fetch_spec_rows_for_date(
    *,
    spec: BenchmarkSpec,
    trade_date: str,
    nse_collector: NSECollector,
) -> pd.DataFrame:
    raw = nse_collector.get_bhavcopy(trade_date)
    if raw.empty:
        return pd.DataFrame()
    frame = _normalized_columns(raw)
    if "SYMBOL" not in frame.columns:
        return pd.DataFrame()
    candidates = {spec.label.upper().replace(" ", ""), spec.label.upper(), spec.symbol.upper().replace("_", "")}
    symbol_series = frame["SYMBOL"].astype(str).str.strip()
    symbol_norm = symbol_series.str.upper().str.replace(" ", "", regex=False)
    matched = frame[symbol_norm.isin(candidates)].copy()
    if matched.empty:
        return pd.DataFrame()

    rename_map = {
        "OPEN_PRICE": "open",
        "HIGH_PRICE": "high",
        "LOW_PRICE": "low",
        "CLOSE_PRICE": "close",
        "TTL_TRD_QNTY": "volume",
    }
    matched = matched.rename(columns=rename_map)
    for field in ("open", "high", "low", "close", "volume"):
        matched.loc[:, field] = pd.to_numeric(matched.get(field), errors="coerce")
    matched = matched.dropna(subset=["open", "high", "low", "close"]).copy()
    if matched.empty:
        return pd.DataFrame()

    timestamp = pd.to_datetime(trade_date)
    return pd.DataFrame(
        {
            "symbol_id": spec.symbol,
            "security_id": None,
            "exchange": "NSE",
            "timestamp": timestamp,
            "open": matched["open"].iloc[0],
            "high": matched["high"].iloc[0],
            "low": matched["low"].iloc[0],
            "close": matched["close"].iloc[0],
            "volume": int(matched["volume"].fillna(0).iloc[0]),
            "provider": spec.provider,
            "instrument_type": spec.instrument_type,
            "is_benchmark": True,
            "benchmark_label": spec.label,
        },
        index=[0],
    )


def fetch_benchmark_rows(spec: BenchmarkSpec, date_range: Iterable[str], *, nse_collector: NSECollector | None = None) -> pd.DataFrame:
    collector = nse_collector or NSECollector()
    frames: list[pd.DataFrame] = []
    for trade_date in date_range:
        if not trade_date:
            continue
        frames.append(_fetch_spec_rows_for_date(spec=spec, trade_date=str(trade_date), nse_collector=collector))
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def ingest_benchmarks(
    date_range: Iterable[str],
    specs: list[BenchmarkSpec] | None = None,
    *,
    nse_collector: NSECollector | None = None,
) -> pd.DataFrame:
    active_specs = specs or DEFAULT_BENCHMARKS
    if not active_specs:
        return pd.DataFrame()
    collector = nse_collector or NSECollector()
    frames = [fetch_benchmark_rows(spec, date_range, nse_collector=collector) for spec in active_specs]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    output = pd.concat(frames, ignore_index=True)
    return output.drop_duplicates(subset=["symbol_id", "exchange", "timestamp"], keep="last")


def benchmark_lookup(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol_id", "timestamp", "benchmark_close"])
    subset = frame[["symbol_id", "timestamp", "close"]].copy()
    subset.rename(columns={"close": "benchmark_close"}, inplace=True)
    subset.loc[:, "timestamp"] = pd.to_datetime(subset["timestamp"])
    subset = subset.sort_values(["symbol_id", "timestamp"]).copy()
    return subset
