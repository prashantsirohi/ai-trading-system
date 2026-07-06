"""Historical validation for the early accumulation sidecar scan."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.domains.ranking.early_accumulation import (
    EarlyAccumulationConfig,
    build_early_accumulation_scan,
)
from ai_trading_system.platform.db.paths import get_domain_paths


BENCHMARK_SYMBOLS = {"NIFTY50", "NIFTY", "NIFTY 50", "NIFTYBANK", "BANKNIFTY"}
REGIME_BY_YEAR = {
    2021: "bear_recovery",
    2022: "bear_recovery",
    2023: "bull",
    2024: "bull",
    2025: "bear_consolidation",
    2026: "bear_consolidation",
}


@dataclass(frozen=True)
class EarlyAccumulationValidationConfig:
    data_domain: str = "research"
    exchange: str = "NSE"
    project_root: Path | str | None = None
    output_dir: Path | None = None
    start_date: str | None = None
    end_date: str | None = None
    cadence: str = "weekly"
    lookback_days: int = 260
    min_history_bars: int = 80
    max_snapshots: int | None = None


def _snapshot_dates(
    con: duckdb.DuckDBPyConnection,
    *,
    exchange: str,
    start_date: str | None,
    end_date: str | None,
    cadence: str,
    max_snapshots: int | None,
) -> list[pd.Timestamp]:
    bounds = con.execute(
        """
        SELECT MIN(CAST(timestamp AS DATE)), MAX(CAST(timestamp AS DATE))
        FROM _catalog
        WHERE exchange = ?
        """,
        [exchange],
    ).fetchone()
    if not bounds or bounds[0] is None or bounds[1] is None:
        return []
    start = pd.Timestamp(start_date or bounds[0])
    end = pd.Timestamp(end_date or bounds[1])
    dates = con.execute(
        """
        SELECT DISTINCT CAST(timestamp AS DATE) AS trade_date
        FROM _catalog
        WHERE exchange = ?
          AND CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE
        ORDER BY trade_date
        """,
        [exchange, start.date(), end.date()],
    ).fetchdf()
    if dates.empty:
        return []
    dates.loc[:, "trade_date"] = pd.to_datetime(dates["trade_date"])
    freq = "W-FRI" if cadence == "weekly" else "ME"
    selected = (
        dates.set_index("trade_date")
        .assign(_value=1)
        .resample(freq)
        .last()
        .dropna()
        .index
        .to_list()
    )
    available = dates["trade_date"].tolist()
    snapped = []
    for target in selected:
        prior = [item for item in available if item <= target]
        if prior:
            snapped.append(prior[-1])
    deduped = sorted(set(snapped))
    if max_snapshots is not None:
        deduped = deduped[-int(max_snapshots) :]
    return deduped


def _safe_return(close: pd.Series, n: int) -> float:
    if len(close) <= n:
        return np.nan
    prior = float(close.iloc[-n - 1])
    if prior <= 0:
        return np.nan
    return (float(close.iloc[-1]) / prior - 1.0) * 100.0


def _snapshot_factor_frame(
    con: duckdb.DuckDBPyConnection,
    *,
    snapshot_date: pd.Timestamp,
    exchange: str,
    lookback_days: int,
    min_history_bars: int,
) -> pd.DataFrame:
    start = snapshot_date - pd.Timedelta(days=int(lookback_days))
    bars = con.execute(
        """
        SELECT
            UPPER(symbol_id) AS symbol_id,
            exchange,
            CAST(timestamp AS DATE) AS trade_date,
            open,
            high,
            low,
            close,
            volume
        FROM _catalog
        WHERE exchange = ?
          AND CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE
          AND close > 0
        ORDER BY symbol_id, trade_date
        """,
        [exchange, start.date(), snapshot_date.date()],
    ).fetchdf()
    if bars.empty:
        return pd.DataFrame()
    bars.loc[:, "trade_date"] = pd.to_datetime(bars["trade_date"])
    rows: list[dict[str, Any]] = []
    for symbol_id, group in bars.groupby("symbol_id", sort=False):
        symbol = str(symbol_id).upper()
        if symbol in BENCHMARK_SYMBOLS:
            continue
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < int(min_history_bars):
            continue
        close = pd.to_numeric(group["close"], errors="coerce")
        high = pd.to_numeric(group["high"], errors="coerce")
        volume = pd.to_numeric(group["volume"], errors="coerce")
        last_close = float(close.iloc[-1])
        sma50 = close.rolling(50, min_periods=30).mean().iloc[-1]
        sma200 = close.rolling(200, min_periods=80).mean().iloc[-1]
        vol20 = volume.shift(1).rolling(20, min_periods=5).mean().iloc[-1]
        vol_mean20 = volume.rolling(20, min_periods=10).mean().iloc[-1]
        vol_std20 = volume.rolling(20, min_periods=10).std().iloc[-1]
        volume_z20 = (float(volume.iloc[-1]) - vol_mean20) / vol_std20 if vol_std20 and vol_std20 > 0 else 0.0
        volume_ratio = float(volume.iloc[-1] / vol20) if vol20 and vol20 > 0 else 1.0
        sma50_slope = (sma50 / close.rolling(50, min_periods=30).mean().shift(20).iloc[-1] - 1.0) * 100.0 if len(close) > 70 else 0.0
        sma200_prior = close.rolling(200, min_periods=80).mean().shift(20).iloc[-1] if len(close) > 220 else np.nan
        sma200_slope = (sma200 / sma200_prior - 1.0) * 100.0 if sma200_prior and sma200_prior > 0 else 0.0
        rows.append(
            {
                "symbol_id": symbol,
                "exchange": exchange,
                "close": last_close,
                "sma_50": sma50,
                "sma_200": sma200,
                "sma50_slope_20d_pct": sma50_slope,
                "sma200_slope_20d_pct": sma200_slope,
                "return_20": _safe_return(close, 20),
                "return_60": _safe_return(close, 60),
                "return_120": _safe_return(close, 120),
                "momentum_acceleration": (
                    0.6 * (_safe_return(close, 5) - _safe_return(close, 20))
                    + 0.4 * (_safe_return(close, 10) - _safe_return(close, 20))
                ),
                "trend_score": (
                    (50.0 if last_close > sma50 else 20.0)
                    + (25.0 if last_close > sma200 else 0.0)
                    + max(-20.0, min(20.0, sma50_slope))
                ),
                "volume_ratio_20": volume_ratio,
                "volume_zscore_20": volume_z20,
                "avg_value_traded_20": float(vol20 * last_close) if vol20 and vol20 > 0 else np.nan,
                "rel_strength_score": 50.0,
                "composite_score": 0.0,
                "composite_score_adjusted": 0.0,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column in ("return_20", "return_60", "return_120"):
        frame.loc[:, column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    frame.loc[:, "rel_strength_score"] = (
        0.2 * frame["return_20"].rank(pct=True) * 100.0
        + 0.5 * frame["return_60"].rank(pct=True) * 100.0
        + 0.3 * frame["return_120"].rank(pct=True) * 100.0
    )
    frame.loc[:, "composite_score"] = frame["rel_strength_score"]
    frame.loc[:, "composite_score_adjusted"] = frame["rel_strength_score"]
    try:
        delivery = con.execute(
            """
            SELECT UPPER(symbol_id) AS symbol_id, delivery_pct
            FROM _delivery
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) <= ?::DATE
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY UPPER(symbol_id)
                ORDER BY CAST(timestamp AS DATE) DESC
            ) = 1
            """,
            [exchange, snapshot_date.date()],
        ).fetchdf()
    except duckdb.Error:
        delivery = pd.DataFrame()
    if not delivery.empty:
        frame = frame.merge(delivery, on="symbol_id", how="left")
    if "delivery_pct" not in frame.columns:
        frame.loc[:, "delivery_pct"] = np.nan
    frame.loc[:, "delivery_pct_imputed"] = frame["delivery_pct"].isna()
    return frame


def _future_returns(
    con: duckdb.DuckDBPyConnection,
    *,
    exchange: str,
    snapshot_date: pd.Timestamp,
    symbols: list[str],
) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(columns=["symbol_id", "fwd_return_20d", "fwd_return_60d", "fwd_return_120d", "max_drawdown_120d"])
    bars = con.execute(
        """
        SELECT
            UPPER(symbol_id) AS symbol_id,
            CAST(timestamp AS DATE) AS trade_date,
            close
        FROM _catalog
        WHERE exchange = ?
          AND UPPER(symbol_id) = ANY(?)
          AND CAST(timestamp AS DATE) >= ?::DATE
        ORDER BY symbol_id, trade_date
        """,
        [exchange, [s.upper() for s in symbols], snapshot_date.date()],
    ).fetchdf()
    rows: list[dict[str, Any]] = []
    for symbol_id, group in bars.groupby("symbol_id", sort=False):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if group.empty:
            continue
        base = float(group.loc[0, "close"])
        row = {"symbol_id": symbol_id}
        for horizon in (20, 60, 120):
            if len(group) > horizon and base > 0:
                row[f"fwd_return_{horizon}d"] = (float(group.loc[horizon, "close"]) / base - 1.0) * 100.0
            else:
                row[f"fwd_return_{horizon}d"] = np.nan
        if base > 0 and len(group) > 1:
            row["max_drawdown_120d"] = (float(group.head(121)["close"].min()) / base - 1.0) * 100.0
        else:
            row["max_drawdown_120d"] = np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def _precision_at(frame: pd.DataFrame, n: int, *, threshold: float = 20.0) -> float:
    top = frame.sort_values("early_accumulation_score", ascending=False).head(n)
    if top.empty:
        return np.nan
    return float((pd.to_numeric(top["fwd_return_120d"], errors="coerce") >= threshold).mean())


def _decile_returns(frame: pd.DataFrame) -> pd.DataFrame:
    valid = frame.dropna(subset=["early_accumulation_score"]).copy()
    if valid.empty:
        return pd.DataFrame(columns=["score_decile", "count", "median_fwd_return_20d", "median_fwd_return_60d", "median_fwd_return_120d"])
    ranks = valid["early_accumulation_score"].rank(method="first")
    valid.loc[:, "score_decile"] = pd.qcut(ranks, q=min(10, len(valid)), labels=False, duplicates="drop") + 1
    rows = []
    for decile, group in valid.groupby("score_decile"):
        rows.append(
            {
                "score_decile": int(decile),
                "count": int(len(group)),
                "median_fwd_return_20d": float(pd.to_numeric(group["fwd_return_20d"], errors="coerce").median()),
                "median_fwd_return_60d": float(pd.to_numeric(group["fwd_return_60d"], errors="coerce").median()),
                "median_fwd_return_120d": float(pd.to_numeric(group["fwd_return_120d"], errors="coerce").median()),
            }
        )
    return pd.DataFrame(rows).sort_values("score_decile", ascending=False)


def _summary(frame: pd.DataFrame, config: EarlyAccumulationValidationConfig) -> dict[str, Any]:
    if frame.empty:
        return {"rows": 0, "config": asdict(config)}
    fwd120 = pd.to_numeric(frame["fwd_return_120d"], errors="coerce")
    summary = {
        "rows": int(len(frame)),
        "snapshots": int(frame["snapshot_date"].nunique()),
        "symbols": int(frame["symbol_id"].nunique()),
        "precision_at_25": _precision_at(frame, 25),
        "precision_at_50": _precision_at(frame, 50),
        "precision_at_100": _precision_at(frame, 100),
        "median_fwd_return_20d": float(pd.to_numeric(frame["fwd_return_20d"], errors="coerce").median()),
        "median_fwd_return_60d": float(pd.to_numeric(frame["fwd_return_60d"], errors="coerce").median()),
        "median_fwd_return_120d": float(fwd120.median()),
        "hit_rate_gt_20": float((fwd120 >= 20.0).mean()),
        "hit_rate_gt_50": float((fwd120 >= 50.0).mean()),
        "hit_rate_gt_100": float((fwd120 >= 100.0).mean()),
        "graduation_rate": float(frame["graduation_status"].astype(str).ne("early_watchlist").mean()),
        "false_positive_rate": float((fwd120 < 0.0).mean()),
        "by_regime": {},
        "by_early_purity_bucket": {},
        "config": asdict(config),
    }
    for regime, group in frame.groupby("regime_bucket"):
        regime_fwd = pd.to_numeric(group["fwd_return_120d"], errors="coerce")
        summary["by_regime"][str(regime)] = {
            "rows": int(len(group)),
            "median_fwd_return_120d": float(regime_fwd.median()),
            "hit_rate_gt_20": float((regime_fwd >= 20.0).mean()),
        }
    if "early_purity_bucket" in frame.columns:
        for bucket, group in frame.groupby("early_purity_bucket"):
            bucket_fwd = pd.to_numeric(group["fwd_return_120d"], errors="coerce")
            summary["by_early_purity_bucket"][str(bucket)] = {
                "rows": int(len(group)),
                "median_fwd_return_20d": float(pd.to_numeric(group["fwd_return_20d"], errors="coerce").median()),
                "median_fwd_return_60d": float(pd.to_numeric(group["fwd_return_60d"], errors="coerce").median()),
                "median_fwd_return_120d": float(bucket_fwd.median()),
                "hit_rate_gt_20": float((bucket_fwd >= 20.0).mean()),
                "hit_rate_gt_50": float((bucket_fwd >= 50.0).mean()),
                "false_positive_rate": float((bucket_fwd < 0.0).mean()),
            }
    return summary


def _write_markdown(summary: dict[str, Any], output_dir: Path) -> None:
    lines = [
        "# Early Accumulation Validation",
        "",
        f"- Rows: {summary.get('rows', 0)}",
        f"- Snapshots: {summary.get('snapshots', 0)}",
        f"- Precision@25: {summary.get('precision_at_25', float('nan')):.1%}",
        f"- Precision@50: {summary.get('precision_at_50', float('nan')):.1%}",
        f"- Precision@100: {summary.get('precision_at_100', float('nan')):.1%}",
        f"- Median 120D forward return: {summary.get('median_fwd_return_120d', float('nan')):.2f}%",
        f"- Hit rate >20%: {summary.get('hit_rate_gt_20', float('nan')):.1%}",
        f"- False-positive rate: {summary.get('false_positive_rate', float('nan')):.1%}",
    ]
    buckets = summary.get("by_early_purity_bucket", {})
    if isinstance(buckets, dict) and buckets:
        lines.extend(["", "## By Early Purity Bucket"])
        for bucket, values in buckets.items():
            lines.append(
                f"- {bucket}: rows={values.get('rows', 0)}, "
                f"median 120D={values.get('median_fwd_return_120d', float('nan')):.2f}%, "
                f"hit >20%={values.get('hit_rate_gt_20', float('nan')):.1%}"
            )
    output_dir.joinpath("early_accumulation_validation_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_validation(config: EarlyAccumulationValidationConfig) -> dict[str, Any]:
    paths = get_domain_paths(project_root=config.project_root, data_domain=config.data_domain)
    output_dir = Path(config.output_dir) if config.output_dir else paths.reports_dir / "early_accumulation_validation"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
    try:
        snapshots = _snapshot_dates(
            con,
            exchange=config.exchange,
            start_date=config.start_date,
            end_date=config.end_date,
            cadence=config.cadence,
            max_snapshots=config.max_snapshots,
        )
        rows = []
        scan_config = EarlyAccumulationConfig(top_n=1_000_000, min_score=0.0, require_liquidity=False, exclude_illiquid=False)
        for snapshot_date in snapshots:
            factor_frame = _snapshot_factor_frame(
                con,
                snapshot_date=snapshot_date,
                exchange=config.exchange,
                lookback_days=config.lookback_days,
                min_history_bars=config.min_history_bars,
            )
            scan, _scan_summary = build_early_accumulation_scan(
                ranked_universe=factor_frame,
                pattern_df=pd.DataFrame(),
                breakout_df=pd.DataFrame(),
                as_of_date=snapshot_date.date().isoformat(),
                config=scan_config,
            )
            if scan.empty:
                continue
            future = _future_returns(
                con,
                exchange=config.exchange,
                snapshot_date=snapshot_date,
                symbols=scan["symbol_id"].astype(str).tolist(),
            )
            merged = scan.merge(future, on="symbol_id", how="left")
            merged.loc[:, "snapshot_date"] = snapshot_date.date().isoformat()
            merged.loc[:, "regime_bucket"] = REGIME_BY_YEAR.get(snapshot_date.year, "unknown")
            rows.append(merged)
    finally:
        con.close()
    examples = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    deciles = _decile_returns(examples) if not examples.empty else _decile_returns(pd.DataFrame())
    summary = _summary(examples, config)
    examples.to_csv(output_dir / "early_accumulation_examples.csv", index=False)
    deciles.to_csv(output_dir / "early_accumulation_decile_returns.csv", index=False)
    (output_dir / "early_accumulation_validation_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    _write_markdown(summary, output_dir)
    summary["artifact_dir"] = str(output_dir)
    return summary


def parse_args(argv: list[str] | None = None) -> EarlyAccumulationValidationConfig:
    parser = argparse.ArgumentParser(description="Validate early accumulation scores on historical snapshots.")
    parser.add_argument("--data-domain", default="research")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--cadence", choices=["weekly", "monthly"], default="weekly")
    parser.add_argument("--lookback-days", type=int, default=260)
    parser.add_argument("--min-history-bars", type=int, default=80)
    parser.add_argument("--max-snapshots", type=int, default=None)
    args = parser.parse_args(argv)
    return EarlyAccumulationValidationConfig(
        data_domain=args.data_domain,
        exchange=args.exchange,
        project_root=args.project_root,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        start_date=args.start_date,
        end_date=args.end_date,
        cadence=args.cadence,
        lookback_days=args.lookback_days,
        min_history_bars=args.min_history_bars,
        max_snapshots=args.max_snapshots,
    )


def main(argv: list[str] | None = None) -> None:
    summary = run_validation(parse_args(argv))
    print(f"early accumulation validation written to {summary['artifact_dir']}")


if __name__ == "__main__":
    main()
