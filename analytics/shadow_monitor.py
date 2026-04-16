"""Shadow monitoring helpers for ML-vs-technical comparison."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd

from analytics.lightgbm_engine import LightGBMAlphaEngine
from analytics.lightgbm_research import add_technical_baseline_scores
from analytics.training_dataset import TrainingDatasetBuilder
from core.paths import ensure_domain_layout


def _descending_rank(series: pd.Series) -> pd.Series:
    return series.rank(method="first", ascending=False).astype(int)


def _top_decile_flag(rank_series: pd.Series) -> pd.Series:
    top_n = max(1, int(np.ceil(len(rank_series) * 0.1)))
    return rank_series <= top_n


def find_latest_model_metadata(model_dir: str | Path, horizon: int) -> tuple[Path, Dict[str, Any]]:
    """Return the newest LightGBM metadata file for the requested horizon."""
    root = Path(model_dir)
    candidates = sorted(root.glob("*.metadata.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    for path in candidates:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("engine") != "lightgbm":
            continue
        if int(payload.get("horizon", -1)) != int(horizon):
            continue
        model_path = path.with_name(path.name.replace(".metadata.json", ".txt"))
        if not model_path.exists():
            continue
        payload["_metadata_path"] = str(path)
        payload["_model_path"] = str(model_path)
        return path, payload
    raise FileNotFoundError(f"No LightGBM metadata found for horizon={horizon} in {root}")


def prepare_current_universe_dataset(
    *,
    project_root: str | Path,
    prediction_date: Optional[str] = None,
    exchange: str = "NSE",
    lookback_days: int = 420,
) -> tuple[pd.DataFrame, pd.Timestamp]:
    """Build the latest operational feature frame aligned to the research training schema."""
    history_df = prepare_shadow_history_dataset(
        project_root=project_root,
        from_prediction_date=prediction_date,
        to_prediction_date=prediction_date,
        exchange=exchange,
        lookback_days=lookback_days,
    )
    if history_df.empty:
        raise ValueError(f"No operational feature frame available for exchange={exchange}")
    latest = pd.Timestamp(history_df["timestamp"].max()).normalize()
    latest_df = history_df[history_df["timestamp"] == history_df["timestamp"].max()].copy()
    return latest_df.reset_index(drop=True), latest


def prepare_shadow_history_dataset(
    *,
    project_root: str | Path,
    from_prediction_date: Optional[str] = None,
    to_prediction_date: Optional[str] = None,
    exchange: str = "NSE",
    lookback_days: int = 420,
) -> pd.DataFrame:
    """Build an operational feature frame for one or more prediction dates."""
    paths = ensure_domain_layout(project_root=project_root, data_domain="operational")
    conn = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
    try:
        if to_prediction_date is None:
            row = conn.execute(
                """
                SELECT MAX(CAST(timestamp AS DATE))
                FROM _catalog
                WHERE exchange = ?
                """,
                [exchange],
            ).fetchone()
            if row is None or row[0] is None:
                raise ValueError(f"No operational OHLCV data found for exchange={exchange}")
            prediction_end_ts = pd.Timestamp(row[0]).normalize()
        else:
            prediction_end_ts = pd.Timestamp(to_prediction_date).normalize()
    finally:
        conn.close()

    prediction_start_ts = (
        pd.Timestamp(from_prediction_date).normalize()
        if from_prediction_date is not None
        else prediction_end_ts
    )
    raw_from_date = (prediction_start_ts - pd.Timedelta(days=int(lookback_days))).date().isoformat()
    to_date = prediction_end_ts.date().isoformat()

    engine = LightGBMAlphaEngine(
        ohlcv_db_path=str(paths.ohlcv_db_path),
        feature_store_dir=str(paths.feature_store_dir),
        model_dir=str(paths.model_dir),
        data_domain="operational",
    )
    builder = TrainingDatasetBuilder(project_root=project_root, data_domain="operational")

    raw = engine.prepare_training_data(
        from_date=raw_from_date,
        to_date=to_date,
        exchange=exchange,
        horizons=[],
    ).copy()
    raw["timestamp"] = pd.to_datetime(raw["timestamp"])
    enriched = builder._enrich_features(raw, horizon=5)
    enriched["timestamp"] = pd.to_datetime(enriched["timestamp"])
    enriched = add_technical_baseline_scores(enriched)
    prediction_mask = (
        enriched["timestamp"].dt.normalize() >= prediction_start_ts
    ) & (
        enriched["timestamp"].dt.normalize() <= prediction_end_ts
    )
    return enriched.loc[prediction_mask].reset_index(drop=True)


def build_shadow_overlay(
    current_df: pd.DataFrame,
    *,
    scorer: LightGBMAlphaEngine,
    model_5d: Any,
    model_20d: Any,
    technical_weight: float = 0.75,
    ml_weight: float = 0.25,
) -> pd.DataFrame:
    """Build the current-universe overlay with technical, ML, and blended ranks."""
    if current_df.empty:
        return current_df.copy()

    scored_5d = scorer.score_frame(current_df.copy(), model=model_5d, horizon=5)
    scored_20d = scorer.score_frame(current_df.copy(), model=model_20d, horizon=20)

    overlay = current_df[
        [
            "symbol_id",
            "exchange",
            "timestamp",
            "close",
            "technical_score",
        ]
    ].copy()
    overlay["ml_5d_prob"] = scored_5d["probability"].to_numpy()
    overlay["ml_20d_prob"] = scored_20d["probability"].to_numpy()
    overlay["ml_5d_pct"] = overlay["ml_5d_prob"].rank(pct=True) * 100
    overlay["ml_20d_pct"] = overlay["ml_20d_prob"].rank(pct=True) * 100
    overlay["blend_5d_score"] = (
        overlay["technical_score"] * float(technical_weight)
        + overlay["ml_5d_pct"] * float(ml_weight)
    )
    overlay["blend_20d_score"] = (
        overlay["technical_score"] * float(technical_weight)
        + overlay["ml_20d_pct"] * float(ml_weight)
    )

    overlay["technical_rank"] = _descending_rank(overlay["technical_score"])
    overlay["ml_5d_rank"] = _descending_rank(overlay["ml_5d_prob"])
    overlay["ml_20d_rank"] = _descending_rank(overlay["ml_20d_prob"])
    overlay["blend_5d_rank"] = _descending_rank(overlay["blend_5d_score"])
    overlay["blend_20d_rank"] = _descending_rank(overlay["blend_20d_score"])

    overlay["technical_top_decile"] = _top_decile_flag(overlay["technical_rank"])
    overlay["ml_5d_top_decile"] = _top_decile_flag(overlay["ml_5d_rank"])
    overlay["ml_20d_top_decile"] = _top_decile_flag(overlay["ml_20d_rank"])
    overlay["blend_5d_top_decile"] = _top_decile_flag(overlay["blend_5d_rank"])
    overlay["blend_20d_top_decile"] = _top_decile_flag(overlay["blend_20d_rank"])

    return overlay.sort_values("technical_rank").reset_index(drop=True)


def overlay_rows_for_registry(
    overlay_df: pd.DataFrame,
    *,
    metadata: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Convert the overlay frame into registry-write payloads."""
    rows: List[Dict[str, Any]] = []
    for record in overlay_df.to_dict(orient="records"):
        record["metadata"] = metadata or {}
        rows.append(record)
    return rows


def load_operational_price_history(
    *,
    ohlcv_db_path: str | Path,
    exchange: str = "NSE",
    from_date: Optional[str] = None,
) -> pd.DataFrame:
    """Load operational price history for realized-return scoring."""
    conn = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        date_filter = ""
        params: list[Any] = [exchange]
        if from_date:
            date_filter = "AND CAST(timestamp AS DATE) >= ?"
            params.append(from_date)
        prices = conn.execute(
            f"""
            SELECT symbol_id, exchange, CAST(timestamp AS DATE) AS trade_date, close
            FROM _catalog
            WHERE exchange = ?
            {date_filter}
            ORDER BY symbol_id, trade_date
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()
    prices["trade_date"] = pd.to_datetime(prices["trade_date"]).dt.normalize()
    return prices


def compute_matured_outcomes(
    price_history: pd.DataFrame,
    predictions: Iterable[Dict[str, Any]],
    *,
    horizon: int,
) -> List[Dict[str, Any]]:
    """Compute realized forward returns for prediction rows once the horizon has matured."""
    prediction_rows = list(predictions)
    if not prediction_rows or price_history.empty:
        return []

    prices = price_history.copy()
    prices["trade_date"] = pd.to_datetime(prices["trade_date"]).dt.normalize()
    prices = prices.sort_values(["symbol_id", "exchange", "trade_date"]).reset_index(drop=True)
    prices["future_date"] = prices.groupby(["symbol_id", "exchange"])["trade_date"].shift(-int(horizon))
    prices["future_close"] = prices.groupby(["symbol_id", "exchange"])["close"].shift(-int(horizon))

    prediction_df = pd.DataFrame(prediction_rows).copy()
    prediction_df["prediction_date"] = pd.to_datetime(prediction_df["prediction_date"]).dt.normalize()

    merged = prediction_df.merge(
        prices,
        left_on=["symbol_id", "exchange", "prediction_date"],
        right_on=["symbol_id", "exchange", "trade_date"],
        how="left",
        suffixes=("", "_price"),
    )
    matured = merged.dropna(subset=["future_close"]).copy()
    if matured.empty:
        return []

    matured["realized_return"] = matured["future_close"] / matured["close"] - 1.0
    matured["hit"] = matured["realized_return"] > 0

    rows: List[Dict[str, Any]] = []
    for row in matured.to_dict(orient="records"):
        rows.append(
            {
                "prediction_id": row["prediction_id"],
                "prediction_date": pd.Timestamp(row["prediction_date"]).date().isoformat(),
                "symbol_id": row["symbol_id"],
                "exchange": row["exchange"],
                "horizon": int(horizon),
                "future_date": pd.Timestamp(row["future_date"]).date().isoformat(),
                "realized_return": float(row["realized_return"]),
                "hit": bool(row["hit"]),
            }
        )
    return rows
