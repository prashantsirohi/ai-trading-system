"""Generate a read-only Phase 1 audit of the performance-tracker database."""

from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


SYNTHETIC_SYMBOL_RE = r"^(BASE|DRIFT|REC|OLD|R|T|SYM)[0-9]+$|^(AAA|BBB|CCC)$"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _query(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    rendered = frame.map(lambda value: "" if pd.isna(value) else str(value))
    headers = "| " + " | ".join(rendered.columns) + " |"
    divider = "| " + " | ".join("---" for _ in rendered.columns) + " |"
    rows = [
        "| " + " | ".join(row) + " |"
        for row in rendered.astype(str).itertuples(index=False, name=None)
    ]
    return "\n".join([headers, divider, *rows])


def main() -> int:
    args = _parse_args()
    db_path = args.db.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0)
    db_sha256 = _sha256(db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        coverage = _query(
            con,
            """
            SELECT MIN(run_date) AS first_date,
                   MAX(run_date) AS last_date,
                   COUNT(DISTINCT run_date) AS dates,
                   COUNT(*) AS rows
            FROM rank_cohort_performance
            """,
        )
        contamination = _query(
            con,
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE sector_name = 'Test') AS test_sector_rows,
                COUNT(*) FILTER (
                    WHERE regexp_matches(symbol_id, '{SYNTHETIC_SYMBOL_RE}')
                ) AS fixture_symbol_rows,
                COUNT(*) FILTER (
                    WHERE sector_name = 'Test'
                       OR regexp_matches(symbol_id, '{SYNTHETIC_SYMBOL_RE}')
                ) AS quarantinable_rows,
                COUNT(DISTINCT symbol_id) FILTER (
                    WHERE sector_name = 'Test'
                       OR regexp_matches(symbol_id, '{SYNTHETIC_SYMBOL_RE}')
                ) AS quarantinable_symbols
            FROM rank_cohort_performance
            """,
        )
        synthetic_prefixes = _query(
            con,
            f"""
            SELECT regexp_extract(symbol_id, '^[A-Za-z_]+') AS prefix,
                   COUNT(*) AS rows,
                   COUNT(DISTINCT symbol_id) AS symbols,
                   MIN(run_date) AS first_date,
                   MAX(run_date) AS last_date,
                   ROUND(AVG(fwd_20d_return), 2) AS avg_20d
            FROM rank_cohort_performance
            WHERE sector_name = 'Test'
               OR regexp_matches(symbol_id, '{SYNTHETIC_SYMBOL_RE}')
            GROUP BY prefix
            ORDER BY rows DESC, prefix
            """,
        )
        weekend_dates = _query(
            con,
            """
            SELECT run_date,
                   DAYNAME(run_date) AS weekday,
                   COUNT(*) AS rows,
                   COUNT(*) FILTER (WHERE sector_name = 'Test') AS test_rows
            FROM rank_cohort_performance
            WHERE DAYOFWEEK(run_date) IN (0, 6)
            GROUP BY run_date
            ORDER BY run_date
            """,
        )
        anomaly_summary = _query(
            con,
            """
            SELECT
                COUNT(*) FILTER (WHERE COALESCE(fwd_return_anomaly, FALSE)) AS persisted_any_horizon,
                COUNT(*) FILTER (WHERE ABS(fwd_5d_return) > 50) AS abs_5d_gt_50,
                COUNT(*) FILTER (WHERE ABS(fwd_10d_return) > 75) AS abs_10d_gt_75,
                COUNT(*) FILTER (WHERE ABS(fwd_20d_return) > 100) AS abs_20d_gt_100,
                COUNT(*) FILTER (WHERE ABS(fwd_60d_return) > 200) AS abs_60d_gt_200
            FROM rank_cohort_performance
            """,
        )
        factor_coverage = _query(
            con,
            """
            SELECT factor,
                   non_null_rows,
                   ROUND(100.0 * non_null_rows / NULLIF(total_rows, 0), 1) AS coverage_pct
            FROM (
                SELECT COUNT(*) AS total_rows,
                       COUNT(factor_rs) AS factor_rs,
                       COUNT(factor_vol) AS factor_vol,
                       COUNT(factor_trend) AS factor_trend,
                       COUNT(factor_prox) AS factor_prox,
                       COUNT(factor_deliv) AS factor_deliv,
                       COUNT(factor_sector) AS factor_sector,
                       COUNT(factor_momentum_accel) AS factor_momentum_accel,
                       COUNT(factor_above_200dma) AS factor_above_200dma,
                       COUNT(factor_liquidity) AS factor_liquidity,
                       COUNT(factor_delivery_trend) AS factor_delivery_trend
                FROM rank_cohort_performance
            )
            UNPIVOT (non_null_rows FOR factor IN (
                factor_rs, factor_vol, factor_trend, factor_prox, factor_deliv,
                factor_sector, factor_momentum_accel, factor_above_200dma,
                factor_liquidity, factor_delivery_trend
            ))
            ORDER BY factor
            """,
        )
        daily_counts = _query(
            con,
            """
            SELECT run_date,
                   COUNT(*) AS rows,
                   COUNT(*) FILTER (WHERE sector_name = 'Test') AS test_rows,
                   COUNT(*) FILTER (WHERE sector_name IS DISTINCT FROM 'Test') AS non_test_rows,
                   COUNT(fwd_5d_return) AS matured_5d,
                   COUNT(fwd_20d_return) AS matured_20d
            FROM rank_cohort_performance
            GROUP BY run_date
            ORDER BY run_date
            """,
        )
        bucket_coverage = _query(
            con,
            """
            SELECT COALESCE(watchlist_bucket, 'unassigned') AS bucket,
                   COUNT(*) AS rows,
                   COUNT(DISTINCT run_date) AS dates,
                   COUNT(DISTINCT symbol_id) AS symbols,
                   COUNT(fwd_5d_return) AS matured_5d,
                   COUNT(fwd_20d_return) AS matured_20d
            FROM rank_cohort_performance
            GROUP BY bucket
            ORDER BY rows DESC
            """,
        )
        suspect_rows = _query(
            con,
            f"""
            SELECT *
            FROM rank_cohort_performance
            WHERE sector_name = 'Test'
               OR regexp_matches(symbol_id, '{SYNTHETIC_SYMBOL_RE}')
            ORDER BY run_date, symbol_id, exchange
            """,
        )
        outlier_rows = _query(
            con,
            """
            SELECT run_date, symbol_id, exchange, rank_position, sector_name,
                   watchlist_bucket, fwd_5d_return, fwd_20d_return
            FROM rank_cohort_performance
            WHERE ABS(fwd_5d_return) > 50 OR ABS(fwd_20d_return) > 50
            ORDER BY run_date, symbol_id, exchange
            """,
        )
    finally:
        con.close()

    suspect_rows.to_csv(output_dir / "suspect_rows.csv", index=False)
    outlier_rows.to_csv(output_dir / "return_outliers.csv", index=False)
    daily_counts.to_csv(output_dir / "daily_counts.csv", index=False)
    weekend_dates.to_csv(output_dir / "weekend_dates.csv", index=False)

    report = f"""# Performance Tracker Phase 1 Audit

- Generated at: `{generated_at.isoformat()}`
- Source DB: `{db_path}`
- Source SHA-256: `{db_sha256}`
- Mode: read-only audit; no database rows changed

## Coverage

{_markdown_table(coverage)}

## Contamination Summary

{_markdown_table(contamination)}

## Fixture-Like Prefixes

{_markdown_table(synthetic_prefixes)}

## Weekend Dates

{_markdown_table(weekend_dates)}

## Return Anomalies

{_markdown_table(anomaly_summary)}

## Factor Coverage

{_markdown_table(factor_coverage)}

## Bucket Coverage

{_markdown_table(bucket_coverage)}

## Exported Evidence

- `suspect_rows.csv`: rows classified conservatively as fixture-like
- `return_outliers.csv`: rows with `abs(fwd_5d_return) > 50` or `abs(fwd_20d_return) > 50`
- `daily_counts.csv`: date-level volume and maturation counts
- `weekend_dates.csv`: weekend rows requiring review
"""
    (output_dir / "audit_report.md").write_text(report, encoding="utf-8")
    print(output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
