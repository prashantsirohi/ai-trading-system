"""Weekly performance digest for the rank cohort tracker.

Produces a markdown report covering:

  1. Cohort forward returns       — top-10 / top-50 / top-200 / rest
  2. Bucket attribution           — TRIGGERED_TODAY / CORE_MOMENTUM / EARLY_STAGE2 / AVOID
  3. Factor information coefficient — Spearman correlation of factor scores
                                    with fwd-20d return, rolling 30 / 90 days
  4. Drift watch                  — flag factors whose IC dropped > 30% vs
                                    6-month baseline

The report is written to ``data/research/perf_digests/digest_<YYYY-WW>.md``
and also returned as a string so callers can pipe to Telegram/Slack.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.research.perf_tracker.schema import open_research_db

logger = logging.getLogger(__name__)

# Cohort definitions used across cohort + bucket sections.
COHORT_BANDS: tuple[tuple[str, int, int], ...] = (
    ("top-10",   1,    10),
    ("top-50",   1,    50),
    ("top-200",  1,    200),
    ("51-200",   51,   200),
    ("201+",     201,  10_000_000),
)

FACTOR_COLUMNS: tuple[str, ...] = (
    "factor_rs",
    "factor_vol",
    "factor_trend",
    "factor_prox",
    "factor_deliv",
    "factor_sector",
    "factor_momentum_accel",
)

DRIFT_THRESHOLD_PCT = 30.0  # flag if rolling-90d IC drops > 30% vs 180d baseline


@dataclass
class DigestResult:
    """Bundle of artifacts produced by ``build_digest``."""

    markdown: str
    output_path: Path
    section_data: dict[str, pd.DataFrame]


def _cohort_returns(con) -> pd.DataFrame:
    rows = []
    for label, lo, hi in COHORT_BANDS:
        # n_5d / n_20d are the matured-row counts — hit rates must divide by
        # those, not by total row count, otherwise pending rows deflate the
        # ratio. ``COUNT(col)`` skips NULLs in DuckDB; ``AVG`` does too.
        result = con.execute(
            f"""
            SELECT
                COUNT(*)                                                     AS n_total,
                COUNT(fwd_5d_return)                                         AS n_5d,
                COUNT(fwd_20d_return)                                        AS n_20d,
                AVG(fwd_5d_return)                                           AS avg_5d,
                AVG(fwd_10d_return)                                          AS avg_10d,
                AVG(fwd_20d_return)                                          AS avg_20d,
                AVG(fwd_60d_return)                                          AS avg_60d,
                100.0 * SUM(CASE WHEN fwd_5d_return  > 0 THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(fwd_5d_return),  0)                     AS hitrate_5d,
                100.0 * SUM(CASE WHEN fwd_20d_return > 0 THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(fwd_20d_return), 0)                     AS hitrate_20d
            FROM rank_cohort_performance
            WHERE rank_position BETWEEN {lo} AND {hi}
            """
        ).fetchone()
        rows.append({
            "cohort":      label,
            "n_total":     int(result[0] or 0),
            "n_20d":       int(result[2] or 0),
            "avg_5d":      _round(result[3]),
            "avg_10d":     _round(result[4]),
            "avg_20d":     _round(result[5]),
            "avg_60d":     _round(result[6]),
            "hitrate_5d":  _round(result[7]),
            "hitrate_20d": _round(result[8]),
        })
    return pd.DataFrame(rows)


def _bucket_attribution(con) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT
            COALESCE(watchlist_bucket, 'unassigned') AS bucket,
            COUNT(*) AS n,
            ROUND(AVG(fwd_5d_return),  2) AS avg_5d,
            ROUND(AVG(fwd_10d_return), 2) AS avg_10d,
            ROUND(AVG(fwd_20d_return), 2) AS avg_20d,
            ROUND(100.0 * SUM(CASE WHEN fwd_5d_return  > 0 THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(fwd_5d_return),  0), 1) AS hitrate_5d,
            ROUND(100.0 * SUM(CASE WHEN fwd_20d_return > 0 THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(fwd_20d_return), 0), 1) AS hitrate_20d
        FROM rank_cohort_performance
        GROUP BY bucket
        ORDER BY
            CASE bucket
                WHEN 'TRIGGERED_TODAY' THEN 1
                WHEN 'CORE_MOMENTUM'   THEN 2
                WHEN 'EARLY_STAGE2'    THEN 3
                WHEN 'AVOID_WEAK_CONFIRMATION' THEN 4
                ELSE 5
            END
        """
    ).fetchdf()
    return df


def _factor_ic(con, *, window_days: int) -> pd.DataFrame:
    """Spearman rank correlation between each factor score and fwd-20d return.

    Computed over the last ``window_days`` trading days. Spearman rather than
    Pearson because we care about ordinal predictive power (does higher factor
    score map to higher forward return), not linear fit.
    """
    available = [
        col for col in FACTOR_COLUMNS
        if con.execute(
            f"SELECT COUNT(*) FROM rank_cohort_performance WHERE {col} IS NOT NULL"
        ).fetchone()[0] > 0
    ]
    if not available:
        return pd.DataFrame(columns=["factor", "n", f"ic_20d_{window_days}d"])

    cols = ", ".join(available)
    df = con.execute(
        f"""
        SELECT {cols}, fwd_20d_return
        FROM rank_cohort_performance
        WHERE fwd_20d_return IS NOT NULL
          AND run_date >= (
              SELECT MAX(run_date) - INTERVAL '{window_days} days'
              FROM rank_cohort_performance
          )
        """
    ).fetchdf()

    if df.empty:
        return pd.DataFrame(columns=["factor", "n", f"ic_20d_{window_days}d"])

    rows = []
    fwd = df["fwd_20d_return"]
    for factor in available:
        valid = df[[factor, "fwd_20d_return"]].dropna()
        if len(valid) < 30:
            ic = None
        else:
            # Spearman = Pearson on the ranks. Done by hand to avoid the
            # scipy dependency that pandas.corr(method='spearman') would pull in.
            x_rank = valid[factor].rank()
            y_rank = valid["fwd_20d_return"].rank()
            ic = float(x_rank.corr(y_rank))
        rows.append({
            "factor": factor.replace("factor_", ""),
            "n": int(len(valid)),
            f"ic_20d_{window_days}d": _round(ic),
        })
    return pd.DataFrame(rows)


def _drift_watch(ic_30d: pd.DataFrame, ic_180d: pd.DataFrame) -> pd.DataFrame:
    """Flag factors whose recent IC dropped > DRIFT_THRESHOLD_PCT vs baseline."""
    if ic_30d.empty or ic_180d.empty:
        return pd.DataFrame(columns=["factor", "ic_recent", "ic_baseline", "delta_pct", "alert"])
    merged = ic_30d.merge(ic_180d, on="factor", how="outer")
    recent_col = next(c for c in ic_30d.columns if c.startswith("ic_"))
    base_col = next(c for c in ic_180d.columns if c.startswith("ic_"))
    rows = []
    for _, row in merged.iterrows():
        recent = row[recent_col]
        base = row[base_col]
        if pd.isna(recent) or pd.isna(base) or abs(base) < 1e-6:
            delta_pct = None
            alert = ""
        else:
            delta_pct = (recent - base) / abs(base) * 100.0
            alert = "⚠ DRIFT" if delta_pct < -DRIFT_THRESHOLD_PCT else ""
        rows.append({
            "factor":      row["factor"],
            "ic_recent":   _round(recent),
            "ic_baseline": _round(base),
            "delta_pct":   _round(delta_pct),
            "alert":       alert,
        })
    return pd.DataFrame(rows)


def _round(value, ndigits: int = 2):
    if value is None or pd.isna(value):
        return None
    return round(float(value), ndigits)


def _df_to_md(df: pd.DataFrame) -> str:
    """Tiny markdown table renderer (avoids tabulate dependency)."""
    if df.empty:
        return "_(no rows)_"
    headers = list(df.columns)
    sep = "| " + " | ".join(headers) + " |\n"
    sep += "|" + "|".join(["---"] * len(headers)) + "|\n"
    for _, row in df.iterrows():
        cells = ["" if pd.isna(v) else str(v) for v in row]
        sep += "| " + " | ".join(cells) + " |\n"
    return sep


def build_digest(
    *,
    project_root: str | Path | None = None,
    as_of: date | None = None,
) -> DigestResult:
    """Generate the weekly digest. Writes to disk + returns the markdown."""
    as_of = as_of or date.today()
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    digest_dir = paths.root_dir / "research" / "perf_digests"
    digest_dir.mkdir(parents=True, exist_ok=True)

    iso_year, iso_week, _ = as_of.isocalendar()
    output_path = digest_dir / f"digest_{iso_year}-W{iso_week:02d}.md"

    sections: dict[str, pd.DataFrame] = {}
    with open_research_db(project_root=project_root, read_only=True) as con:
        meta = con.execute(
            "SELECT MIN(run_date), MAX(run_date), COUNT(DISTINCT run_date), COUNT(*) "
            "FROM rank_cohort_performance"
        ).fetchone()
        first_date, last_date, n_dates, n_rows = meta
        sections["cohorts"] = _cohort_returns(con)
        sections["buckets"] = _bucket_attribution(con)
        sections["ic_30d"]  = _factor_ic(con, window_days=30)
        sections["ic_90d"]  = _factor_ic(con, window_days=90)
        sections["ic_180d"] = _factor_ic(con, window_days=180)
        sections["drift"]   = _drift_watch(sections["ic_30d"], sections["ic_180d"])

    md = _render_markdown(
        as_of=as_of,
        first_date=first_date,
        last_date=last_date,
        n_dates=n_dates,
        n_rows=n_rows,
        sections=sections,
    )
    output_path.write_text(md, encoding="utf-8")
    logger.info("perf_tracker digest written to %s", output_path)
    return DigestResult(markdown=md, output_path=output_path, section_data=sections)


def _render_markdown(
    *,
    as_of: date,
    first_date,
    last_date,
    n_dates: int,
    n_rows: int,
    sections: dict[str, pd.DataFrame],
) -> str:
    parts: list[str] = []
    parts.append(f"# Performance Tracker Digest — {as_of.isoformat()}\n")
    parts.append(
        f"_Coverage: {first_date} → {last_date} · {n_dates} ranking dates · "
        f"{n_rows:,} (date, symbol) rows_\n"
    )

    parts.append("\n## 1. Cohort forward returns")
    parts.append("Top-N picks should outperform the rest. If `top-10 avg_20d` is "
                 "indistinguishable from `201+ avg_20d`, the ranking isn't "
                 "discriminating.\n")
    parts.append(_df_to_md(sections["cohorts"]))

    parts.append("\n## 2. Bucket attribution (Phase 5 watchlist taxonomy)")
    parts.append("`TRIGGERED_TODAY` 5d return should lead `CORE_MOMENTUM` 5d, "
                 "and `AVOID_WEAK_CONFIRMATION` 5d should be ≤ 0 if the bucket "
                 "label is honest. Empty until Phase 5-aware runs accumulate.\n")
    parts.append(_df_to_md(sections["buckets"]))

    parts.append("\n## 3. Factor information coefficient (Spearman vs fwd_20d)")
    parts.append("Higher IC = factor is doing real predictive work. Compare 30d "
                 "vs 90d vs 180d to spot drift.\n")
    combined = sections["ic_30d"].merge(
        sections["ic_90d"], on="factor", how="outer"
    ).merge(sections["ic_180d"], on="factor", how="outer")
    parts.append(_df_to_md(combined))

    parts.append("\n## 4. Drift watch")
    parts.append(f"Flags any factor whose 30-day IC dropped > {DRIFT_THRESHOLD_PCT:.0f}% "
                 "vs the 180-day baseline.\n")
    parts.append(_df_to_md(sections["drift"]))

    return "\n".join(parts) + "\n"


def main() -> None:  # pragma: no cover - CLI entry
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = build_digest()
    print(f"\nDigest written to: {result.output_path}\n")
    print(result.markdown)


if __name__ == "__main__":  # pragma: no cover
    main()
