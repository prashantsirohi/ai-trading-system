"""Winner-capture analysis for research dynamic rankings."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.backtesting.research_loader import (
    RANKING_METHOD_VERSION,
    load_research_ranked_by_date,
)


BENCHMARK_SYMBOLS = {"NIFTY50", "NIFTY", "NIFTY 50", "NIFTYBANK", "BANKNIFTY"}


@dataclass(frozen=True)
class WinnerCaptureConfig:
    year: int
    exchange: str = "NSE"
    top_gainers: int = 50
    rank_cutoff: int = 50
    persist: bool = True


def run_winner_capture_analysis(
    project_root: Path | str,
    *,
    config: WinnerCaptureConfig,
    data_quality: dict[str, Any] | None = None,
    sync_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Measure whether current research rankings captured a year's winners."""
    root = Path(project_root)
    start = date(config.year, 1, 1)
    end = date(config.year, 12, 31)
    winners = _load_yearly_winners(
        root,
        exchange=config.exchange,
        start=start,
        end=end,
        limit=config.top_gainers,
    )
    if winners.empty:
        return {
            "status": "no_data",
            "year": config.year,
            "exchange": config.exchange,
            "top_gainers": config.top_gainers,
            "rank_cutoff": config.rank_cutoff,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "summary": _empty_summary(config),
            "winners": [],
            "sync": sync_summary,
            "data_quality": data_quality,
            "run_metadata": _metadata(config),
            "message": "no valid research OHLCV rows found for the completed calendar year",
        }

    ranked_by_date = load_research_ranked_by_date(
        root,
        from_date=start,
        to_date=end,
        exchange=config.exchange,
    )
    result_rows = _capture_rows(
        winners,
        ranked_by_date=ranked_by_date,
        rank_cutoff=config.rank_cutoff,
    )
    summary = _summary(result_rows, config=config)
    payload = {
        "status": "ok",
        "year": config.year,
        "exchange": config.exchange,
        "top_gainers": config.top_gainers,
        "rank_cutoff": config.rank_cutoff,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "summary": summary,
        "winners": result_rows,
        "sync": sync_summary,
        "data_quality": data_quality,
        "run_metadata": _metadata(config),
    }
    if config.persist:
        out_dir = (
            root
            / "data"
            / "research"
            / "winner_capture"
            / str(config.year)
            / datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(result_rows).to_csv(out_dir / "winners.csv", index=False)
        (out_dir / "summary.json").write_text(json.dumps(payload, indent=2, default=str))
        (out_dir / "metadata.json").write_text(
            json.dumps(payload["run_metadata"], indent=2, default=str)
        )
        payload["artifact_dir"] = str(out_dir.relative_to(root))
    return payload


def _load_yearly_winners(
    project_root: Path,
    *,
    exchange: str,
    start: date,
    end: date,
    limit: int,
) -> pd.DataFrame:
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    if not paths.ohlcv_db_path.exists():
        return pd.DataFrame()
    conn = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        if "_catalog" not in tables:
            return pd.DataFrame()
        df = conn.execute(
            """
            WITH daily AS (
                SELECT
                    UPPER(symbol_id) AS symbol_id,
                    exchange,
                    CAST(timestamp AS DATE) AS trade_date,
                    close,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(symbol_id), exchange, CAST(timestamp AS DATE)
                        ORDER BY ingestion_ts DESC NULLS LAST, timestamp DESC
                    ) AS rn
                FROM _catalog
                WHERE exchange = ?
                  AND CAST(timestamp AS DATE) >= ?
                  AND CAST(timestamp AS DATE) <= ?
                  AND close IS NOT NULL
                  AND close > 0
            ),
            clean AS (
                SELECT symbol_id, exchange, trade_date, close
                FROM daily
                WHERE rn = 1
            ),
            endpoints AS (
                SELECT
                    symbol_id,
                    exchange,
                    MIN(trade_date) AS start_date,
                    MAX(trade_date) AS end_date
                FROM clean
                GROUP BY symbol_id, exchange
            )
            SELECT
                e.symbol_id,
                e.exchange,
                e.start_date,
                e.end_date,
                s.close AS start_close,
                n.close AS end_close,
                (n.close - s.close) / s.close AS yearly_return
            FROM endpoints e
            JOIN clean s
              ON s.symbol_id = e.symbol_id
             AND s.exchange = e.exchange
             AND s.trade_date = e.start_date
            JOIN clean n
              ON n.symbol_id = e.symbol_id
             AND n.exchange = e.exchange
             AND n.trade_date = e.end_date
            WHERE s.close > 0
              AND n.close > 0
              AND e.symbol_id NOT IN ('NIFTY50', 'NIFTY', 'NIFTY 50', 'NIFTYBANK', 'BANKNIFTY')
            ORDER BY yearly_return DESC, e.symbol_id ASC
            LIMIT ?
            """,
            [exchange, start, end, int(limit)],
        ).fetchdf()
    except duckdb.Error:
        return pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return df
    df = df[~df["symbol_id"].astype(str).str.upper().isin(BENCHMARK_SYMBOLS)].copy()
    return df.head(limit).reset_index(drop=True)


def _capture_rows(
    winners: pd.DataFrame,
    *,
    ranked_by_date: dict[date, pd.DataFrame],
    rank_cutoff: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rank_history: dict[str, list[dict[str, Any]]] = {str(s).upper(): [] for s in winners["symbol_id"]}
    for day, frame in sorted(ranked_by_date.items()):
        if frame is None or frame.empty or "symbol_id" not in frame.columns:
            continue
        view = frame.copy()
        view.loc[:, "symbol_id"] = view["symbol_id"].astype(str).str.upper()
        view = view[view["symbol_id"].isin(rank_history.keys())]
        capture_day = _as_date(day)
        if capture_day is None:
            continue
        for raw in view.to_dict(orient="records"):
            symbol = str(raw.get("symbol_id", "")).upper()
            rank_history.setdefault(symbol, []).append(
                {
                    "date": capture_day,
                    "rank": _optional_int(raw.get("eligible_rank")),
                    "score": _optional_float(
                        raw.get("composite_score_adjusted", raw.get("composite_score"))
                    ),
                    "close": _optional_float(raw.get("close")),
                }
            )

    for index, winner in winners.reset_index(drop=True).iterrows():
        symbol = str(winner["symbol_id"]).upper()
        history = [h for h in rank_history.get(symbol, []) if h["rank"] is not None]
        captures = [h for h in history if h["rank"] is not None and h["rank"] <= rank_cutoff]
        first = captures[0] if captures else None
        best = min(history, key=lambda h: h["rank"]) if history else None
        start_date = _as_date(winner["start_date"])
        end_date = _as_date(winner["end_date"])
        start_close = float(winner["start_close"])
        end_close = float(winner["end_close"])
        capture_close = first["close"] if first else None
        return_at_capture = (
            ((capture_close - start_close) / start_close)
            if capture_close is not None and start_close > 0
            else None
        )
        remaining_return = (
            ((end_close - capture_close) / capture_close)
            if capture_close is not None and capture_close > 0
            else None
        )
        rows.append(
            {
                "rank_in_year": int(index + 1),
                "symbol_id": symbol,
                "exchange": str(winner["exchange"]),
                "yearly_return": float(winner["yearly_return"]),
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "start_close": start_close,
                "end_close": end_close,
                "captured": bool(first),
                "first_capture_date": first["date"].isoformat() if first else None,
                "first_capture_rank": first["rank"] if first else None,
                "first_capture_score": first["score"] if first else None,
                "first_capture_close": capture_close,
                "best_rank": best["rank"] if best else None,
                "best_rank_date": best["date"].isoformat() if best else None,
                "days_to_capture": (
                    (first["date"] - start_date).days
                    if first and start_date is not None
                    else None
                ),
                "return_at_capture": return_at_capture,
                "remaining_return_after_capture": remaining_return,
            }
        )
    return rows


def _summary(rows: list[dict[str, Any]], *, config: WinnerCaptureConfig) -> dict[str, Any]:
    captured = [r for r in rows if r["captured"]]
    missed = [r for r in rows if not r["captured"]]
    return {
        "winner_count": len(rows),
        "rank_cutoff": config.rank_cutoff,
        "captured_count": len(captured),
        "missed_count": len(missed),
        "capture_rate": (len(captured) / len(rows)) if rows else 0.0,
        "median_days_to_capture": _median([r["days_to_capture"] for r in captured]),
        "median_first_capture_rank": _median([r["first_capture_rank"] for r in captured]),
        "average_yearly_return_captured": _mean([r["yearly_return"] for r in captured]),
        "average_yearly_return_missed": _mean([r["yearly_return"] for r in missed]),
    }


def _empty_summary(config: WinnerCaptureConfig) -> dict[str, Any]:
    return {
        "winner_count": 0,
        "rank_cutoff": config.rank_cutoff,
        "captured_count": 0,
        "missed_count": 0,
        "capture_rate": 0.0,
        "median_days_to_capture": None,
        "median_first_capture_rank": None,
        "average_yearly_return_captured": None,
        "average_yearly_return_missed": None,
    }


def _metadata(config: WinnerCaptureConfig) -> dict[str, Any]:
    return {
        "analysis": "winner_capture",
        "config": asdict(config),
        "ranking_method_version": RANKING_METHOD_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
    }


def _median(values: list[Any]) -> float | None:
    nums = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return float(nums.median()) if not nums.empty else None


def _mean(values: list[Any]) -> float | None:
    nums = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return float(nums.mean()) if not nums.empty else None


def _optional_float(value: Any) -> float | None:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    try:
        if pd.isna(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.to_datetime(value).date()
