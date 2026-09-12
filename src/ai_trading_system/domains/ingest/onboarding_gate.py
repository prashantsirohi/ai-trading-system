"""Read-only admission gate for incomplete operational universe onboarding."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths


def pending_identities(
    *, project_root=None, data_domain="operational", state_path=None
):
    if data_domain != "operational":
        return set()
    path = (
        Path(state_path)
        if state_path is not None
        else (
            get_domain_paths(
                project_root=project_root, data_domain=data_domain
            ).stage_store_dir
            / "universe_refresh"
            / "state.json"
        )
    )
    if not path.exists():
        return set()
    payload = json.loads(path.read_text())
    pending = payload.get("pending")
    if not isinstance(pending, dict):
        raise ValueError(
            "Invalid universe onboarding checkpoint: pending must be a mapping"
        )
    identities = set()
    for symbol, item in pending.items():
        exchange = item.get("exchange")
        if not symbol or exchange not in {"NSE", "BSE"}:
            raise ValueError("Invalid pending onboarding identity")
        identities.add((str(symbol).upper(), exchange))
    return identities


def exclude_pending(frame: pd.DataFrame, *, identities: set) -> pd.DataFrame:
    if frame is None or frame.empty or not identities:
        return frame
    column = next(
        (name for name in ("symbol_id", "symbol") if name in frame.columns), None
    )
    if column is None:
        raise ValueError("Cannot enforce onboarding admission without symbol identity")
    symbols = frame[column].astype(str).str.strip().str.upper()
    if "exchange" in frame.columns:
        exchanges = frame["exchange"].fillna("").astype(str).str.strip().str.upper()
        mask = pd.Series(
            [
                (symbol, exchange) in identities
                or (
                    exchange not in {"NSE", "BSE"}
                    and any(s == symbol for s, _ in identities)
                )
                for symbol, exchange in zip(symbols, exchanges)
            ],
            index=frame.index,
        )
    else:
        mask = symbols.isin({symbol for symbol, _ in identities})
    return frame.loc[~mask].copy()
