"""Candidate-universe selection and evidence loading for catalysts."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.fundamentals.enrich_rank import normalize_symbol


def select_catalyst_universe(
    ranked: pd.DataFrame,
    *,
    watchlist: pd.DataFrame | None = None,
    breakout: pd.DataFrame | None = None,
    trends: pd.DataFrame | None = None,
    top_n: int = 50,
) -> list[str]:
    """Limit catalyst work to final candidates and useful near-candidates."""

    symbols: list[str] = []

    ranked_symbols = _ranked_symbols(ranked, top_n=top_n)
    _extend_unique(symbols, ranked_symbols)

    if watchlist is not None and not watchlist.empty and "watchlist_bucket" in watchlist.columns:
        add_rows = watchlist.loc[watchlist["watchlist_bucket"].astype(str).str.upper().eq("ADD_TO_WATCHLIST")]
        _extend_unique(symbols, _symbols(add_rows))

    if breakout is not None and not breakout.empty:
        qualified = _qualified_breakouts(breakout)
        _extend_unique(symbols, _symbols(qualified))

    if trends is not None and not trends.empty and "fundamental_trend_label" in trends.columns:
        improving = trends.loc[trends["fundamental_trend_label"].astype(str).str.upper().eq("IMPROVING")]
        _extend_unique(symbols, _symbols(improving))

    return symbols


def load_evidence_csvs(paths: Iterable[str | Path]) -> pd.DataFrame:
    """Load optional file-based catalyst evidence without scraping."""

    frames: list[pd.DataFrame] = []
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists() or path.stat().st_size == 0:
            continue
        try:
            frame = pd.read_csv(path)
        except (EmptyDataError, OSError):
            continue
        if frame.empty:
            continue
        if "symbol" not in frame.columns:
            for candidate in ("symbol_id", "NSE Code", "ticker"):
                if candidate in frame.columns:
                    frame = frame.assign(symbol=frame[candidate])
                    break
        if "symbol" not in frame.columns:
            continue
        frame = frame.copy()
        frame.loc[:, "symbol"] = frame["symbol"].map(normalize_symbol)
        frames.append(frame.loc[frame["symbol"].ne("")])
    if not frames:
        return pd.DataFrame(columns=["symbol"])
    return pd.concat(frames, ignore_index=True)


def _ranked_symbols(ranked: pd.DataFrame, *, top_n: int) -> list[str]:
    if ranked is None or ranked.empty:
        return []
    frame = ranked.copy()
    if "composite_score" in frame.columns:
        frame.loc[:, "composite_score"] = pd.to_numeric(frame["composite_score"], errors="coerce")
        frame = frame.sort_values("composite_score", ascending=False, na_position="last", kind="stable")
    return _symbols(frame.head(int(top_n)))


def _qualified_breakouts(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    qualified = pd.Series(False, index=output.index)
    if "qualified" in output.columns:
        text = output["qualified"].astype(str).str.strip().str.lower()
        numeric = pd.to_numeric(output["qualified"], errors="coerce")
        qualified = qualified | text.isin({"true", "1", "yes", "y", "qualified"}) | numeric.gt(0).fillna(False)
    for column in ("breakout_state", "candidate_tier", "breakout_type"):
        if column in output.columns:
            text = output[column].astype(str).str.strip().str.upper()
            qualified = qualified | text.isin({"A", "B", "CONFIRMED", "QUALIFIED", "BREAKOUT"})
    return output.loc[qualified]


def _symbols(frame: pd.DataFrame) -> list[str]:
    if frame is None or frame.empty:
        return []
    if "symbol" in frame.columns:
        raw = frame["symbol"]
    elif "symbol_id" in frame.columns:
        raw = frame["symbol_id"]
    elif "NSE Code" in frame.columns:
        raw = frame["NSE Code"]
    else:
        return []
    return [symbol for symbol in raw.map(normalize_symbol).tolist() if symbol]


def _extend_unique(target: list[str], values: Iterable[str]) -> None:
    seen = set(target)
    for value in values:
        symbol = normalize_symbol(value)
        if symbol and symbol not in seen:
            target.append(symbol)
            seen.add(symbol)
