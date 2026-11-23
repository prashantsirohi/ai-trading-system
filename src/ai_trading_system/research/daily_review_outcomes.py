"""Outcome-by-queue scoreboard for the shadow daily operator review.

Read-only research tool. For each verified daily-review pack it takes the first
session a listing appeared in a queue and measures its forward return from that
session's close, net of the median return of the 1,000 most liquid NSE stocks
over the same window. Nothing here changes ranking, queues or execution.

Measurement rules (see the 2026-11 independent review):
- only fully matured horizons count (a future bar must exist);
- one observation per (exchange, symbol, queue): repeat days are not new evidence;
- baseline is the same-date liquid-universe median, not NIFTY_50;
- windows holding an unexplained one-day move on adjusted prices are excluded
  and counted (usually an unrecorded split/bonus, not a real return);
- confidence intervals resample whole sessions because cohorts overlap.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

HORIZONS = (5, 10, 20)
LIQUID_UNIVERSE_SIZE = 1000
JUMP_THRESHOLD = 0.40
MIN_NAMES_FOR_EVIDENCE = 30
BOOTSTRAP_DRAWS = 2000


def load_listings(pipeline_runs_dir: Path) -> pd.DataFrame:
    """Return queue rows from the latest attempt of each calendar-PASS NSE pack."""
    frames = []
    for run_dir in sorted(Path(pipeline_runs_dir).glob("*/daily_review")):
        attempts = sorted(run_dir.glob("attempt_*"), key=lambda p: int(p.name.split("_")[1]))
        for attempt in reversed(attempts):
            list_path = attempt / "daily_review_list.csv"
            summary_path = attempt / "daily_review_summary.json"
            if not (list_path.exists() and summary_path.exists()):
                continue
            summary = json.loads(summary_path.read_text())
            if summary.get("calendar", {}).get("status") != "PASS":
                break
            frame = pd.read_csv(list_path, usecols=["exchange", "symbol", "session", "queue"])
            frame["run_id"] = run_dir.parent.name
            frames.append(frame)
            break
    if not frames:
        return pd.DataFrame(columns=["exchange", "symbol", "session", "queue", "run_id"])
    out = pd.concat(frames, ignore_index=True)
    out = out[out["exchange"] == "NSE"].copy()
    out["session"] = pd.to_datetime(out["session"])
    return out


def compute_outcomes(bars: pd.DataFrame, horizons: tuple[int, ...] = HORIZONS) -> pd.DataFrame:
    """Forward returns, jump flags and liquid-universe median per (symbol, date, horizon).

    ``bars`` needs columns symbol_id, d, adjusted_close, close, volume for one exchange.
    """
    b = bars.sort_values(["symbol_id", "d"]).copy()
    g = b.groupby("symbol_id", sort=False)
    b["turnover20"] = g.apply(
        lambda x: (x["close"] * x["volume"]).rolling(20, min_periods=20).mean(),
        include_groups=False,
    ).reset_index(level=0, drop=True)
    b["jump"] = (g["adjusted_close"].pct_change().abs() > JUMP_THRESHOLD).astype(int)
    b["jump_cum"] = b.groupby("symbol_id", sort=False)["jump"].cumsum()
    b["liquid"] = (
        b.groupby("d")["turnover20"].rank(ascending=False, method="first") <= LIQUID_UNIVERSE_SIZE
    )
    pieces = []
    for h in horizons:
        gh = b.groupby("symbol_id", sort=False)
        fwd = gh["adjusted_close"].shift(-h)
        piece = b[["symbol_id", "d", "liquid"]].copy()
        piece["h"] = h
        piece["ret"] = (fwd / b["adjusted_close"] - 1) * 100
        piece["unverified_jump"] = (gh["jump_cum"].shift(-h) - b["jump_cum"]) > 0
        pieces.append(piece[piece["ret"].notna()])
    out = pd.concat(pieces, ignore_index=True)
    clean = out[out["liquid"] & ~out["unverified_jump"]]
    baseline = clean.groupby(["d", "h"])["ret"].median().rename("universe_median")
    return out.join(baseline, on=["d", "h"]).drop(columns="liquid")


def _clustered_median_ci(rows: pd.DataFrame, rng: np.random.Generator) -> tuple[float, float]:
    sessions = [g["excess"].to_numpy() for _, g in rows.groupby("session")]
    if len(sessions) < 3:
        return float("nan"), float("nan")
    draws = []
    for _ in range(BOOTSTRAP_DRAWS):
        pick = rng.integers(0, len(sessions), len(sessions))
        draws.append(np.median(np.concatenate([sessions[i] for i in pick])))
    return float(np.percentile(draws, 5)), float(np.percentile(draws, 95))


def score_queues(listings: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    """Aggregate matured, first-episode queue outcomes into one row per queue and horizon."""
    first = (
        listings.sort_values("session")
        .groupby(["exchange", "symbol", "queue"], as_index=False)
        .head(1)
    )
    merged = first.merge(
        outcomes, left_on=["symbol", "session"], right_on=["symbol_id", "d"], how="inner"
    )
    excluded = merged[merged["unverified_jump"]].groupby(["queue", "h"]).size().rename("jump_excluded")
    usable = merged[~merged["unverified_jump"] & merged["universe_median"].notna()].copy()
    usable["excess"] = usable["ret"] - usable["universe_median"]
    all_rows = usable.assign(queue="ALL_LISTED")
    rng = np.random.default_rng(0)
    records = []
    for (queue, h), rows in pd.concat([usable, all_rows]).groupby(["queue", "h"]):
        lo, hi = _clustered_median_ci(rows, rng)
        records.append(
            {
                "queue": queue,
                "horizon": int(h),
                "names": len(rows),
                "sessions": rows["session"].nunique(),
                "median_return": rows["ret"].median(),
                "universe_median": rows["universe_median"].median(),
                "median_excess": rows["excess"].median(),
                "ci5": lo,
                "ci95": hi,
                "beat_universe_pct": (rows["excess"] > 0).mean() * 100,
                "jump_excluded": int(excluded.get((queue, h), 0)),
                "evidence": "OK" if len(rows) >= MIN_NAMES_FOR_EVIDENCE else "THIN",
            }
        )
    cols = [
        "queue", "horizon", "names", "sessions", "median_return", "universe_median",
        "median_excess", "ci5", "ci95", "beat_universe_pct", "jump_excluded", "evidence",
    ]
    return pd.DataFrame(records, columns=cols).sort_values(["horizon", "queue"], ignore_index=True)


def load_bars(ohlcv_db: Path, start: pd.Timestamp) -> pd.DataFrame:
    import duckdb

    con = duckdb.connect(str(ohlcv_db), read_only=True)
    try:
        return con.execute(
            """
            select symbol_id, cast(timestamp as date) as d, adjusted_close, close, volume
            from _catalog
            where exchange = 'NSE' and timestamp >= ? and coalesce(is_benchmark, false) = false
              and adjusted_close > 0
            """,
            [start.to_pydatetime()],
        ).df().assign(d=lambda f: pd.to_datetime(f["d"]))
    finally:
        con.close()


def build_scoreboard(pipeline_runs_dir: Path, ohlcv_db: Path) -> pd.DataFrame:
    listings = load_listings(pipeline_runs_dir)
    if listings.empty:
        raise SystemExit("No calendar-PASS daily_review packs found.")
    # 20-session turnover warm-up plus calendar slack before the first session.
    bars = load_bars(ohlcv_db, listings["session"].min() - pd.Timedelta(days=45))
    return score_queues(listings, compute_outcomes(bars))


def main(argv: list[str] | None = None) -> int:
    from ai_trading_system.platform.db.paths import get_domain_paths

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-domain", default="operational")
    parser.add_argument("--output-csv", type=Path, default=None)
    args = parser.parse_args(argv)
    paths = get_domain_paths(data_domain=args.data_domain)
    board = build_scoreboard(paths.pipeline_runs_dir, paths.ohlcv_db_path)
    print(board.round(2).to_string(index=False))
    # Default location is the one the 00_Decision publisher reads.
    out = args.output_csv or paths.reports_dir / "daily_review_outcomes" / "queue_scoreboard.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    board.to_csv(out, index=False)
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
