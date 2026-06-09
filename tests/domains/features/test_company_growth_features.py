from __future__ import annotations

from pathlib import Path

import duckdb

from ai_trading_system.domains.features.company_growth_features import refresh_company_growth_features
from ai_trading_system.domains.fundamentals.analytical_store import ensure_fundamentals_analytical_schema


def _create_fundamentals_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        ensure_fundamentals_analytical_schema(conn)
        rows = []
        reports = [
            "2024-03-31",
            "2024-06-30",
            "2024-09-30",
            "2024-12-31",
            "2025-03-31",
            "2025-06-30",
            "2025-09-30",
            "2025-12-31",
            "2026-03-31",
        ]
        sales = [100, 105, 110, 115, 130, 140, 150, 165, 180]
        profit = [10, 11, 12, 13, 18, 21, 24, 28, 33]
        op_profit = [20, 21, 23, 24, 31, 35, 39, 43, 50]
        for report, s, p, op in zip(reports, sales, profit, op_profit, strict=True):
            for metric, value in {"sales": s, "net_profit": p, "operating_profit": op, "expenses": s - op}.items():
                rows.append(("AAA", "quarterly", report, "standalone", metric, value, report, "fixture", "b1", "2026-01-01"))
        rows.extend(
            [
                ("NEG", "quarterly", "2025-03-31", "standalone", "sales", 100, "2025-04-30", "fixture", "b1", "2026-01-01"),
                ("NEG", "quarterly", "2025-03-31", "standalone", "net_profit", -5, "2025-04-30", "fixture", "b1", "2026-01-01"),
                ("NEG", "quarterly", "2025-03-31", "standalone", "operating_profit", 5, "2025-04-30", "fixture", "b1", "2026-01-01"),
                ("NEG", "quarterly", "2025-06-30", "standalone", "sales", 110, "2025-07-30", "fixture", "b1", "2026-01-01"),
                ("NEG", "quarterly", "2025-06-30", "standalone", "net_profit", 4, "2025-07-30", "fixture", "b1", "2026-01-01"),
                ("NEG", "quarterly", "2025-06-30", "standalone", "operating_profit", 9, "2025-07-30", "fixture", "b1", "2026-01-01"),
                ("AAA", "quarterly", "2026-03-31", "consolidated", "sales", 999, "2026-03-31", "fixture", "b1", "2026-01-01"),
                ("AAA", "quarterly", "2026-03-31", "consolidated", "net_profit", 999, "2026-03-31", "fixture", "b1", "2026-01-01"),
                ("AAA", "quarterly", "2026-03-31", "consolidated", "operating_profit", 999, "2026-03-31", "fixture", "b1", "2026-01-01"),
            ]
        )
        conn.executemany("INSERT INTO screener_financials VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


def test_company_growth_features_compute_qoq_yoy_cagr_and_counts(tmp_path: Path) -> None:
    db = tmp_path / "fundamentals.duckdb"
    _create_fundamentals_db(db)

    result = refresh_company_growth_features(fundamentals_db_path=db)

    assert result.rows == 11
    conn = duckdb.connect(str(db), read_only=True)
    try:
        latest = conn.execute("SELECT * FROM company_growth_features WHERE symbol='AAA' AND report_date='2026-03-31'").df().iloc[0]
        neg = conn.execute("SELECT profit_qoq_growth FROM company_growth_features WHERE symbol='NEG' AND report_date='2025-06-30'").fetchone()[0]
    finally:
        conn.close()
    assert round(float(latest["sales_yoy_growth"]), 4) == round(180 / 130 - 1, 4)
    assert latest["statement_basis"] == "standalone"
    assert round(float(latest["profit_qoq_growth"]), 4) == round(33 / 28 - 1, 4)
    assert round(float(latest["opm_yoy_change"]), 4) == round((50 / 180 * 100) - (31 / 130 * 100), 4)
    assert latest["positive_profit_quarters_4q"] == 4
    assert latest["sales_growth_positive_quarters_4q"] == 4
    assert latest["profit_growth_positive_quarters_4q"] == 4
    assert neg is None
