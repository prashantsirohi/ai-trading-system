"""Read-only loaders for single-symbol system performance reports."""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd


RUN_DIR_RE = re.compile(r"^pipeline-(\d{4}-\d{2}-\d{2})-.+$")
ATTEMPT_RE = re.compile(r"^attempt_(\d+)$")

FEATURE_GROUPS: tuple[str, ...] = (
    "rsi",
    "adx",
    "sma",
    "ema",
    "macd",
    "atr",
    "bb",
    "roc",
    "supertrend",
)


def normalize_symbol(symbol: str) -> str:
    """Return the repository's canonical uppercase symbol string."""
    return str(symbol or "").strip().upper()


def latest_ohlcv_date(
    ohlcv_db_path: Path | str,
    *,
    symbol: str,
    exchange: str = "NSE",
) -> date | None:
    """Return the latest OHLCV date available for a symbol."""
    db_path = Path(ohlcv_db_path)
    if not db_path.exists():
        return None
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT MAX(CAST(timestamp AS DATE))
            FROM _catalog
            WHERE symbol_id = ? AND exchange = ?
            """,
            [normalize_symbol(symbol), exchange],
        ).fetchone()
    finally:
        conn.close()
    value = row[0] if row else None
    if value is None:
        return None
    return pd.Timestamp(value).date()


def load_ohlcv(
    ohlcv_db_path: Path | str,
    *,
    symbol: str,
    exchange: str = "NSE",
    from_date: str | date,
    to_date: str | date,
) -> pd.DataFrame:
    """Load daily OHLCV rows for one symbol and date range."""
    conn = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        frame = conn.execute(
            """
            SELECT
                symbol_id,
                exchange,
                CAST(timestamp AS DATE) AS timestamp,
                open,
                high,
                low,
                close,
                volume,
                provider,
                validation_status
            FROM _catalog
            WHERE symbol_id = ?
              AND exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY timestamp
            """,
            [normalize_symbol(symbol), exchange, str(from_date), str(to_date)],
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame = frame.assign(timestamp=pd.to_datetime(frame["timestamp"]).dt.normalize().astype("datetime64[ns]"))
    return frame


def load_feature_history(
    feature_store_dir: Path | str,
    *,
    symbol: str,
    exchange: str = "NSE",
    from_date: str | date,
    to_date: str | date,
    feature_groups: Iterable[str] = FEATURE_GROUPS,
) -> pd.DataFrame:
    """Load and merge per-symbol feature parquet groups.

    Missing feature files are intentionally ignored so an otherwise useful
    diagnostic report can still render.
    """
    symbol_id = normalize_symbol(symbol)
    merged: pd.DataFrame | None = None
    start = pd.Timestamp(from_date).normalize()
    end = pd.Timestamp(to_date).normalize()

    for group in feature_groups:
        path = Path(feature_store_dir) / group / exchange / f"{symbol_id}.parquet"
        if not path.exists():
            continue
        frame = pd.read_parquet(path)
        if frame.empty or "timestamp" not in frame.columns:
            continue
        frame = frame.copy()
        frame = frame.assign(
            timestamp=pd.to_datetime(frame["timestamp"], errors="coerce").dt.normalize().astype("datetime64[ns]")
        )
        frame = frame[(frame["timestamp"] >= start) & (frame["timestamp"] <= end)]
        if frame.empty:
            continue

        keep = [
            column
            for column in frame.columns
            if column not in {"date", "close"}
            and not (column in {"symbol_id", "exchange"} and merged is not None)
        ]
        frame = frame[keep].drop_duplicates("timestamp", keep="last")
        merged = frame if merged is None else merged.merge(frame, on="timestamp", how="outer")

    if merged is None:
        return pd.DataFrame(columns=["timestamp"])
    merged = merged.assign(
        timestamp=pd.to_datetime(merged["timestamp"], errors="coerce").dt.normalize().astype("datetime64[ns]")
    )
    merged = merged.sort_values("timestamp").reset_index(drop=True)
    if "symbol_id" not in merged.columns:
        merged.insert(0, "symbol_id", symbol_id)
    if "exchange" not in merged.columns:
        merged.insert(1, "exchange", exchange)
    return merged


def load_weekly_stage_history(
    ohlcv_db_path: Path | str,
    *,
    symbol: str,
    from_date: str | date,
    to_date: str | date,
) -> pd.DataFrame:
    """Load Weinstein weekly stage snapshots for one symbol."""
    db_path = Path(ohlcv_db_path)
    columns = [
        "symbol",
        "week_end_date",
        "stage_label",
        "stage_confidence",
        "stage_transition",
        "bars_in_stage",
        "stage_entry_date",
        "ma10w",
        "ma30w",
        "ma40w",
        "ma30w_slope_4w",
        "weekly_rs_score",
        "weekly_volume_ratio",
        "support_level",
        "resistance_level",
    ]
    if not db_path.exists():
        return pd.DataFrame(columns=columns)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        exists = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            ["weekly_stage_snapshot"],
        ).fetchone()
        if not exists:
            return pd.DataFrame(columns=columns)
        frame = conn.execute(
            """
            SELECT
                symbol,
                CAST(week_end_date AS DATE) AS week_end_date,
                stage_label,
                stage_confidence,
                stage_transition,
                bars_in_stage,
                CAST(stage_entry_date AS DATE) AS stage_entry_date,
                ma10w,
                ma30w,
                ma40w,
                ma30w_slope_4w,
                weekly_rs_score,
                weekly_volume_ratio,
                support_level,
                resistance_level
            FROM weekly_stage_snapshot
            WHERE symbol = ?
              AND CAST(week_end_date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY week_end_date
            """,
            [normalize_symbol(symbol), str(from_date), str(to_date)],
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame = frame.assign(
        week_end_date=pd.to_datetime(frame["week_end_date"]).dt.normalize().astype("datetime64[ns]")
    )
    if "stage_entry_date" in frame.columns:
        frame = frame.assign(
            stage_entry_date=pd.to_datetime(frame["stage_entry_date"], errors="coerce")
            .dt.normalize()
            .astype("datetime64[ns]")
        )
    return frame


def _parse_run_date(run_dir_name: str) -> date | None:
    match = RUN_DIR_RE.fullmatch(run_dir_name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def _attempt_number(path: Path) -> int:
    match = ATTEMPT_RE.fullmatch(path.name)
    return int(match.group(1)) if match else -1


def latest_rank_attempts_by_date(
    pipeline_runs_dir: Path | str,
    *,
    from_date: str | date,
    to_date: str | date,
) -> dict[date, Path]:
    """Return the freshest rank attempt dir per run date with ranked_signals."""
    base = Path(pipeline_runs_dir)
    if not base.is_dir():
        return {}
    start = pd.Timestamp(from_date).date()
    end = pd.Timestamp(to_date).date()
    candidates: dict[date, list[tuple[float, int, Path]]] = {}
    for run_dir in base.iterdir():
        if not run_dir.is_dir():
            continue
        run_date = _parse_run_date(run_dir.name)
        if run_date is None or run_date < start or run_date > end:
            continue
        rank_dir = run_dir / "rank"
        if not rank_dir.is_dir():
            continue
        for attempt_dir in rank_dir.glob("attempt_*"):
            ranked = attempt_dir / "ranked_signals.csv"
            if ranked.exists():
                candidates.setdefault(run_date, []).append(
                    (ranked.stat().st_mtime, _attempt_number(attempt_dir), attempt_dir)
                )
    return {
        run_date: max(items, key=lambda item: (item[0], item[1]))[2]
        for run_date, items in candidates.items()
        if items
    }


def read_csv_if_nonempty(path: Path) -> pd.DataFrame:
    """Read a CSV artifact, returning an empty frame for absent/empty files."""
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        return pd.DataFrame()


def _symbol_rows(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if frame.empty or "symbol_id" not in frame.columns:
        return pd.DataFrame()
    mask = frame["symbol_id"].astype(str).str.upper() == normalize_symbol(symbol)
    return frame.loc[mask].copy()


def _best_pattern_row(rows: pd.DataFrame) -> dict:
    if rows.empty:
        return {}
    sort_columns = [
        column
        for column in ("pattern_priority_rank", "pattern_rank", "setup_quality", "pattern_score")
        if column in rows.columns
    ]
    if sort_columns:
        ascending = [True if "rank" in column else False for column in sort_columns]
        rows = rows.sort_values(sort_columns, ascending=ascending, na_position="last")
    return rows.iloc[0].to_dict()


def load_artifact_timeline(
    pipeline_runs_dir: Path | str,
    *,
    symbol: str,
    from_date: str | date,
    to_date: str | date,
) -> pd.DataFrame:
    """Load per-run emitted rank/scan rows for a symbol."""
    attempts = latest_rank_attempts_by_date(
        pipeline_runs_dir,
        from_date=from_date,
        to_date=to_date,
    )
    rows: list[dict] = []
    for run_date, attempt_dir in sorted(attempts.items()):
        ranked = read_csv_if_nonempty(attempt_dir / "ranked_signals.csv")
        ranked_rows = _symbol_rows(ranked, symbol)
        if not ranked_rows.empty:
            ranked = ranked.reset_index(drop=True)
            ranked.loc[:, "_rank_position"] = ranked.index + 1
            ranked_rows = _symbol_rows(ranked, symbol)

        stock_scan_rows = _symbol_rows(read_csv_if_nonempty(attempt_dir / "stock_scan.csv"), symbol)
        pattern_rows = _symbol_rows(read_csv_if_nonempty(attempt_dir / "pattern_scan.csv"), symbol)
        breakout_rows = _symbol_rows(read_csv_if_nonempty(attempt_dir / "breakout_scan.csv"), symbol)
        stage1_rows = _symbol_rows(read_csv_if_nonempty(attempt_dir / "stage1_scan.csv"), symbol)

        record: dict[str, object] = {
            "timestamp": pd.Timestamp(run_date),
            "run_date": run_date.isoformat(),
            "run_id": attempt_dir.parent.parent.name,
            "attempt": attempt_dir.name,
            "artifact_dir": str(attempt_dir),
            "ranked_emitted": not ranked_rows.empty,
            "stock_scan_emitted": not stock_scan_rows.empty,
            "pattern_emitted": not pattern_rows.empty,
            "breakout_emitted": not breakout_rows.empty,
            "stage1_emitted": not stage1_rows.empty,
        }

        if not ranked_rows.empty:
            rank_row = ranked_rows.iloc[0].to_dict()
            record.update(
                {
                    "rank_position": rank_row.get("rank", rank_row.get("_rank_position")),
                    "composite_score": rank_row.get("composite_score"),
                    "composite_score_adjusted": rank_row.get("composite_score_adjusted"),
                    "rank_mode": rank_row.get("rank_mode"),
                    "eligible_rank": rank_row.get("eligible_rank"),
                    "rejection_reasons": rank_row.get("rejection_reasons"),
                    "rank_confidence": rank_row.get("rank_confidence"),
                    "rel_strength_score": rank_row.get("rel_strength_score"),
                    "vol_intensity_score": rank_row.get("vol_intensity_score"),
                    "trend_score_score": rank_row.get("trend_score_score"),
                    "momentum_acceleration_score": rank_row.get("momentum_acceleration_score"),
                    "prox_high_score": rank_row.get("prox_high_score"),
                    "delivery_pct_score": rank_row.get("delivery_pct_score"),
                    "sector_strength_score": rank_row.get("sector_strength_score"),
                    "above_200dma_score": rank_row.get("above_200dma_score"),
                    "delivery_pct": rank_row.get("delivery_pct"),
                    "sector_name": rank_row.get("sector_name"),
                    "stage2_score": rank_row.get("stage2_score"),
                    "stage2_label": rank_row.get("stage2_label"),
                    "stage2_fail_reason": rank_row.get("stage2_fail_reason"),
                    "weekly_stage_label": rank_row.get("weekly_stage_label"),
                    "weekly_stage_confidence": rank_row.get("weekly_stage_confidence"),
                    "weekly_stage_transition": rank_row.get("weekly_stage_transition"),
                    "bars_in_stage": rank_row.get("bars_in_stage"),
                    "stage_entry_date": rank_row.get("stage_entry_date"),
                    "volume_zscore_20": rank_row.get("volume_zscore_20"),
                    "distance_from_pivot_atr": rank_row.get("distance_from_pivot_atr"),
                }
            )

        if not stock_scan_rows.empty:
            scan_row = stock_scan_rows.iloc[0].to_dict()
            record.update(
                {
                    "stock_category": scan_row.get("category"),
                    "stock_why": scan_row.get("why"),
                    "stock_score": scan_row.get("score"),
                }
            )

        pattern_row = _best_pattern_row(pattern_rows)
        if pattern_row:
            record.update(
                {
                    "pattern_family": pattern_row.get("pattern_family"),
                    "pattern_state": pattern_row.get("pattern_state"),
                    "signal_date": pattern_row.get("signal_date"),
                    "pattern_start": pattern_row.get("pattern_start"),
                    "pattern_end": pattern_row.get("pattern_end"),
                    "breakout_level": pattern_row.get("breakout_level"),
                    "watchlist_trigger_level": pattern_row.get("watchlist_trigger_level"),
                    "invalidation_price": pattern_row.get("invalidation_price"),
                    "setup_quality": pattern_row.get("setup_quality"),
                    "pattern_score": pattern_row.get("pattern_score"),
                    "pattern_priority_rank": pattern_row.get("pattern_priority_rank", pattern_row.get("pattern_rank")),
                }
            )

        if not breakout_rows.empty:
            breakout_row = breakout_rows.iloc[0].to_dict()
            record.update(
                {
                    "breakout_state": breakout_row.get("breakout_state"),
                    "breakout_score": breakout_row.get("breakout_score"),
                }
            )

        if not stage1_rows.empty:
            stage1_row = stage1_rows.iloc[0].to_dict()
            for column in (
                "stage1_score_band", "stage1_substate", "stage1_maturity_score", "stage1_emerging_score",
                "stage1_emerging_rank", "stage1_eligible", "stage1_block_reasons",
                "stage1_data_completeness_pct", "stage1_score_confidence", "stage1_bonus_score",
                "stage1_penalty_score", "stage1_adjustment_reasons", "ma_gap_quality_flag",
                "pattern_promotion_state", "stage1_operational_status",
                "promotion_eligibility", "promotion_block_reasons",
                "golden_cross_status", "golden_cross_status_legacy", "golden_cross_quality", "stage1_model_version",
                "stage1_config_hash", "model_status", "execution_eligible",
            ):
                record[column] = stage1_row.get(column)

        rows.append(record)

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "run_date", "ranked_emitted"])
    frame.loc[:, "timestamp"] = pd.to_datetime(frame["timestamp"]).dt.normalize()
    return frame.sort_values("timestamp").reset_index(drop=True)
