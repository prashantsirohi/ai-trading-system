"""Phase 8 research gate: cross-sectional alpha by (regime, velocity_bucket, decile).

Answers the question that gates live activation of the breadth-impulse risk
matrix: when we stratify forward stock returns by ``(confirmed_regime,
breadth_velocity_bucket, rank_decile)``, is there at least one cell where the
top-rank decile consistently outperforms the bottom-rank decile, with
statistical confidence?

Exit contract:
    Returns 0 (GO) when ≥1 cell satisfies *all* of:
      - n_stocks × n_days >= 5000     (enough data to trust the estimate)
      - 20d spread >= all-cell mean spread + 0.5 percentage points
      - bootstrap 95% CI lower bound for the spread > 0
      - Spearman IC > 0
      - hit rate > 50%
    Returns 1 otherwise. Live sizing in execute.py stays in dry-run until
    this exits 0.

This script is read-only on data and writes a JSON report into
``reports/phase8_cross_sectional_alpha/`` (configurable via --out).
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import random
import sys
from dataclasses import asdict, dataclass
from datetime import date as date_type
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.analytics.regime.breadth import (
    _load_recent_raw_snapshots,
    confirmed_regime,
    load_regime_rules,
)
from ai_trading_system.analytics.regime.profiles import VELOCITY_BUCKETS, REGIMES

LOG = logging.getLogger(__name__)

DEFAULT_HORIZON_DAYS: int = 20
DEFAULT_DECILES: int = 10
DEFAULT_BOOTSTRAP_ITERS: int = 1000
DEFAULT_BOOTSTRAP_SEED: int = 20260520
# Minimum n_stocks × n_days for a cell to be eligible for the gate.
MIN_CELL_SAMPLES: int = 5000
# A cell's 20d spread must beat the all-cell mean by ≥ this margin (in pp).
SPREAD_EDGE_PP: float = 0.5


@dataclass(frozen=True)
class CellStats:
    regime: str
    velocity_bucket: str
    n_days: int
    n_stocks_total: int
    top_decile_mean_return_pct: float
    bottom_decile_mean_return_pct: float
    top_minus_bottom_spread_pct: float
    hit_rate: float
    spearman_ic: float
    max_drawdown_pct: float
    bootstrap_ci_low_pct: float
    bootstrap_ci_high_pct: float


def _load_per_day_snapshots(
    *,
    project_root: Path,
    db_path: Path,
    from_date: str,
    to_date: str,
    benchmark: str,
    confirmation_days: int,
) -> pd.DataFrame:
    """Return a DataFrame indexed by date with regime + velocity bucket columns.

    Re-uses ``_load_recent_raw_snapshots`` so the velocity buckets are
    computed point-in-time (the no-lookahead loop inside breadth.py).
    """
    rules_payload = load_regime_rules(project_root)
    raw_rules: dict[str, Any] = dict(rules_payload.get("rules") or {})
    priority = rules_payload.get("priority")
    if isinstance(priority, (list, tuple)):
        raw_rules["__priority__"] = list(priority)

    snapshots = _load_recent_raw_snapshots(
        db_path,
        as_of=to_date,
        exchange="NSE",
        index_code=benchmark,
        limit=20000,  # plenty of headroom
        rules=raw_rules,
        previous_regime=None,
    )
    if not snapshots:
        raise RuntimeError(
            f"No regime snapshots for {benchmark} up to {to_date}."
        )

    from_d = pd.to_datetime(from_date).date()
    to_d = pd.to_datetime(to_date).date()
    raw_seq: list[str] = []
    rows: list[dict[str, Any]] = []
    for snap in snapshots:
        raw_seq.append(snap.regime)
        d = pd.to_datetime(snap.date).date()
        if d < from_d or d > to_d:
            continue
        # Re-derive confirmed regime from the trailing N raw days, matching
        # regime_report.py's pattern.
        window = raw_seq[-max(confirmation_days, 1):]
        confirmed = confirmed_regime(window, confirmation_days=confirmation_days)
        rows.append(
            {
                "date": d,
                "confirmed_regime": confirmed,
                "velocity_bucket": snap.breadth_velocity_bucket or "neutral",
                "bucket_confidence": snap.bucket_confidence,
            }
        )
    if not rows:
        raise RuntimeError(
            f"No in-window snapshots between {from_date} and {to_date}."
        )
    return pd.DataFrame(rows)


def _load_forward_returns_per_stock(
    db_path: Path,
    *,
    from_date: str,
    to_date: str,
    horizon_days: int,
    exchange: str = "NSE",
) -> pd.DataFrame:
    """For every (symbol, date), compute the H-day forward simple return.

    Returns a frame with columns ``symbol_id, date, fwd_return_pct``.
    """
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            f"""
            WITH px AS (
                SELECT symbol_id, CAST(timestamp AS DATE) AS d, close
                FROM _catalog
                WHERE exchange = ?
                  AND CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE
                  AND close IS NOT NULL AND close > 0
            ),
            fwd AS (
                SELECT
                    symbol_id,
                    d,
                    close,
                    LEAD(close, ?) OVER (PARTITION BY symbol_id ORDER BY d) AS fwd_close
                FROM px
            )
            SELECT symbol_id, d, ((fwd_close / close) - 1.0) * 100.0 AS fwd_return_pct
            FROM fwd
            WHERE fwd_close IS NOT NULL
            """,
            [exchange, from_date, to_date, int(horizon_days)],
        ).fetchall()
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=["symbol_id", "date", "fwd_return_pct"])


def _load_daily_ranks(
    project_root: Path, *, from_date: str, to_date: str
) -> pd.DataFrame:
    """Cross-sectional daily ranks (1 = best) from the research loader.

    Returns columns ``symbol_id, date, eligible_rank``.
    """
    from ai_trading_system.research.backtesting.research_loader import (
        load_research_ranked_by_date,
    )

    frames = load_research_ranked_by_date(
        project_root,
        from_date=pd.to_datetime(from_date).date(),
        to_date=pd.to_datetime(to_date).date(),
    )
    rows: list[dict[str, Any]] = []
    for d, df in frames.items():
        if df is None or df.empty or "eligible_rank" not in df.columns:
            continue
        for r in df.to_dict(orient="records"):
            rows.append(
                {
                    "symbol_id": r.get("symbol_id"),
                    "date": d,
                    "eligible_rank": r.get("eligible_rank"),
                }
            )
    return pd.DataFrame(rows)


def _spearman_ic(rank_pct: pd.Series, fwd_return: pd.Series) -> float:
    """Spearman correlation of (rank, fwd_return). Rank lower = better stock."""
    if len(rank_pct) < 2:
        return 0.0
    # Spearman = Pearson on ranks. Lower rank should mean higher return, so
    # we negate one side so a positive IC means "rank predicts return".
    a = (-rank_pct).rank()
    b = fwd_return.rank()
    a_mean = a.mean()
    b_mean = b.mean()
    num = ((a - a_mean) * (b - b_mean)).sum()
    den = math.sqrt(((a - a_mean) ** 2).sum() * ((b - b_mean) ** 2).sum())
    return float(num / den) if den > 0 else 0.0


def _bootstrap_ci(
    per_day_spreads: list[float],
    *,
    iterations: int,
    seed: int,
) -> tuple[float, float]:
    """Return 95% CI for the mean of per-day spreads via day-resampling."""
    if not per_day_spreads:
        return (0.0, 0.0)
    rng = random.Random(seed)
    n = len(per_day_spreads)
    means: list[float] = []
    for _ in range(iterations):
        sample = [per_day_spreads[rng.randrange(n)] for _ in range(n)]
        means.append(sum(sample) / n)
    means.sort()
    lo = means[int(0.025 * iterations)]
    hi = means[int(0.975 * iterations) - 1]
    return (float(lo), float(hi))


def _max_drawdown_pct(per_day_spreads: list[float]) -> float:
    """Max drawdown of the cumulative spread curve (in pp).

    Treats spread as a daily PnL series for diagnostic purposes only.
    """
    if not per_day_spreads:
        return 0.0
    cumulative = 0.0
    peak = 0.0
    mdd = 0.0
    for s in per_day_spreads:
        cumulative += s
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > mdd:
            mdd = drawdown
    return float(mdd)


def _compute_cell_stats(
    cell_df: pd.DataFrame,
    *,
    regime: str,
    bucket: str,
    n_deciles: int,
    bootstrap_iters: int,
    bootstrap_seed: int,
) -> CellStats:
    # cell_df columns: date, symbol_id, eligible_rank, fwd_return_pct
    n_days = cell_df["date"].nunique()
    n_stocks_total = len(cell_df)
    if n_stocks_total == 0 or n_days == 0:
        return CellStats(
            regime=regime, velocity_bucket=bucket,
            n_days=0, n_stocks_total=0,
            top_decile_mean_return_pct=0.0,
            bottom_decile_mean_return_pct=0.0,
            top_minus_bottom_spread_pct=0.0,
            hit_rate=0.0, spearman_ic=0.0, max_drawdown_pct=0.0,
            bootstrap_ci_low_pct=0.0, bootstrap_ci_high_pct=0.0,
        )

    # Per-day decile assignment (1 = top stocks, n_deciles = bottom).
    per_day_spreads: list[float] = []
    top_returns: list[float] = []
    bot_returns: list[float] = []
    hits = 0
    days_with_both = 0
    for _, group in cell_df.groupby("date"):
        if len(group) < n_deciles:
            continue
        ranks = group["eligible_rank"].rank(method="first")
        bins = pd.qcut(ranks, n_deciles, labels=False, duplicates="drop")
        if bins is None or bins.isna().all():
            continue
        group = group.assign(_decile=bins)
        top = group[group["_decile"] == 0]["fwd_return_pct"]
        bot = group[group["_decile"] == bins.max()]["fwd_return_pct"]
        if top.empty or bot.empty:
            continue
        top_mean = float(top.mean())
        bot_mean = float(bot.mean())
        spread = top_mean - bot_mean
        per_day_spreads.append(spread)
        top_returns.append(top_mean)
        bot_returns.append(bot_mean)
        if spread > 0:
            hits += 1
        days_with_both += 1

    if days_with_both == 0:
        return CellStats(
            regime=regime, velocity_bucket=bucket,
            n_days=n_days, n_stocks_total=n_stocks_total,
            top_decile_mean_return_pct=0.0,
            bottom_decile_mean_return_pct=0.0,
            top_minus_bottom_spread_pct=0.0,
            hit_rate=0.0, spearman_ic=0.0, max_drawdown_pct=0.0,
            bootstrap_ci_low_pct=0.0, bootstrap_ci_high_pct=0.0,
        )

    spread_mean = sum(per_day_spreads) / days_with_both
    top_mean = sum(top_returns) / days_with_both
    bot_mean = sum(bot_returns) / days_with_both
    hit_rate = hits / days_with_both
    ic = _spearman_ic(cell_df["eligible_rank"], cell_df["fwd_return_pct"])
    mdd = _max_drawdown_pct(per_day_spreads)
    lo, hi = _bootstrap_ci(per_day_spreads, iterations=bootstrap_iters, seed=bootstrap_seed)
    return CellStats(
        regime=regime,
        velocity_bucket=bucket,
        n_days=days_with_both,
        n_stocks_total=n_stocks_total,
        top_decile_mean_return_pct=round(top_mean, 4),
        bottom_decile_mean_return_pct=round(bot_mean, 4),
        top_minus_bottom_spread_pct=round(spread_mean, 4),
        hit_rate=round(hit_rate, 4),
        spearman_ic=round(ic, 4),
        max_drawdown_pct=round(mdd, 4),
        bootstrap_ci_low_pct=round(lo, 4),
        bootstrap_ci_high_pct=round(hi, 4),
    )


def _evaluate_gate(cells: list[CellStats]) -> tuple[bool, list[dict[str, Any]]]:
    """Return (passes, qualifying_cells). Gate passes when ≥1 cell qualifies."""
    if not cells:
        return False, []
    all_spreads = [c.top_minus_bottom_spread_pct for c in cells]
    avg_spread = sum(all_spreads) / len(all_spreads)
    threshold = avg_spread + SPREAD_EDGE_PP
    qualifying: list[dict[str, Any]] = []
    for c in cells:
        ok_n = c.n_stocks_total >= MIN_CELL_SAMPLES
        ok_spread = c.top_minus_bottom_spread_pct >= threshold
        ok_ci = c.bootstrap_ci_low_pct > 0
        ok_ic = c.spearman_ic > 0
        ok_hit = c.hit_rate > 0.50
        if ok_n and ok_spread and ok_ci and ok_ic and ok_hit:
            qualifying.append(asdict(c))
    return bool(qualifying), qualifying


def build_report(
    *,
    project_root: Path,
    db_path: Path,
    from_date: str,
    to_date: str,
    benchmark: str = "UNIV_TOP1000",
    confirmation_days: int = 3,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    n_deciles: int = DEFAULT_DECILES,
    bootstrap_iters: int = DEFAULT_BOOTSTRAP_ITERS,
    bootstrap_seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> dict[str, Any]:
    LOG.info("Loading per-day regime snapshots …")
    snaps_df = _load_per_day_snapshots(
        project_root=project_root,
        db_path=db_path,
        from_date=from_date,
        to_date=to_date,
        benchmark=benchmark,
        confirmation_days=confirmation_days,
    )
    LOG.info("Loading per-stock forward returns (horizon=%dd) …", horizon_days)
    fwd_df = _load_forward_returns_per_stock(
        db_path,
        from_date=from_date,
        to_date=to_date,
        horizon_days=horizon_days,
    )
    LOG.info("Loading daily ranks …")
    ranks_df = _load_daily_ranks(project_root, from_date=from_date, to_date=to_date)
    if ranks_df.empty:
        raise RuntimeError("No daily ranks returned by research_loader.")

    ranks_df["date"] = pd.to_datetime(ranks_df["date"]).dt.date
    fwd_df["date"] = pd.to_datetime(fwd_df["date"]).dt.date

    joined = ranks_df.merge(fwd_df, on=["symbol_id", "date"], how="inner")
    joined = joined.merge(snaps_df, on="date", how="inner")
    if joined.empty:
        raise RuntimeError(
            "Empty join across ranks × forward returns × regime snapshots."
        )

    cells: list[CellStats] = []
    for regime in REGIMES:
        for bucket in VELOCITY_BUCKETS:
            sub = joined[
                (joined["confirmed_regime"] == regime)
                & (joined["velocity_bucket"] == bucket)
            ]
            cells.append(
                _compute_cell_stats(
                    sub,
                    regime=regime,
                    bucket=bucket,
                    n_deciles=n_deciles,
                    bootstrap_iters=bootstrap_iters,
                    bootstrap_seed=bootstrap_seed,
                )
            )

    passes, qualifying = _evaluate_gate(cells)
    return {
        "from_date": from_date,
        "to_date": to_date,
        "benchmark": benchmark,
        "horizon_days": horizon_days,
        "n_deciles": n_deciles,
        "bootstrap_iters": bootstrap_iters,
        "all_cells": [asdict(c) for c in cells],
        "gate": {
            "min_cell_samples": MIN_CELL_SAMPLES,
            "spread_edge_pp": SPREAD_EDGE_PP,
            "passes": passes,
            "qualifying_cells": qualifying,
        },
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--from", dest="from_date", default="2005-01-01")
    p.add_argument("--to", dest="to_date", default=str(date_type.today()))
    p.add_argument("--db", type=Path, default=None)
    p.add_argument("--project-root", type=Path, default=Path("."))
    p.add_argument("--benchmark", default="UNIV_TOP1000")
    p.add_argument("--horizon", type=int, default=DEFAULT_HORIZON_DAYS)
    p.add_argument("--deciles", type=int, default=DEFAULT_DECILES)
    p.add_argument("--bootstrap", type=int, default=DEFAULT_BOOTSTRAP_ITERS)
    p.add_argument("--seed", type=int, default=DEFAULT_BOOTSTRAP_SEED)
    p.add_argument(
        "--out",
        type=Path,
        default=Path("reports/phase8_cross_sectional_alpha"),
        help="Output directory for the JSON report.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)

    if args.db:
        db_path = args.db
    else:
        from ai_trading_system.platform.db.paths import get_domain_paths

        db_path = get_domain_paths(project_root=args.project_root, data_domain="research").ohlcv_db_path
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1

    report = build_report(
        project_root=args.project_root,
        db_path=db_path,
        from_date=args.from_date,
        to_date=args.to_date,
        benchmark=args.benchmark,
        horizon_days=args.horizon,
        n_deciles=args.deciles,
        bootstrap_iters=args.bootstrap,
        bootstrap_seed=args.seed,
    )

    args.out.mkdir(parents=True, exist_ok=True)
    report_path = args.out / "cross_sectional_alpha.json"
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"wrote {report_path}")

    passes = report["gate"]["passes"]
    if passes:
        n = len(report["gate"]["qualifying_cells"])
        print(f"GO: {n} cell(s) clear the alpha gate.")
        return 0
    print(
        "NO-GO: no (regime, velocity_bucket) cell satisfies all gate criteria. "
        "Breadth-impulse live sizing stays disabled.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
