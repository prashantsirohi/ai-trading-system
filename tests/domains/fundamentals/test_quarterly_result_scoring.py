from __future__ import annotations

from pathlib import Path

import duckdb

from ai_trading_system.domains.fundamentals.quarterly_result_scoring import build_quarterly_result_scores


def _create_db(path: Path, rows: list[tuple]) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE company_growth_features (
                symbol VARCHAR,
                report_date DATE,
                statement_basis VARCHAR,
                available_at DATE,
                sales_qoq_growth DOUBLE,
                sales_yoy_growth DOUBLE,
                profit_qoq_growth DOUBLE,
                profit_yoy_growth DOUBLE,
                operating_profit_qoq_growth DOUBLE,
                operating_profit_yoy_growth DOUBLE,
                opm_pct DOUBLE,
                opm_qoq_change DOUBLE,
                opm_yoy_change DOUBLE,
                positive_profit_quarters_4q INTEGER,
                sales_growth_positive_quarters_4q INTEGER,
                profit_growth_positive_quarters_4q INTEGER,
                margin_expansion_quarters_4q INTEGER
            )
            """
        )
        conn.executemany(
            "INSERT INTO company_growth_features VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    finally:
        conn.close()


def _row(
    symbol: str,
    *,
    available_at: str = "2026-05-01",
    sales_yoy: float,
    sales_qoq: float,
    op_yoy: float,
    op_qoq: float,
    profit_yoy: float,
    profit_qoq: float,
    opm_yoy: float,
    opm_qoq: float,
) -> tuple:
    return (
        symbol,
        "2026-03-31",
        "standalone",
        available_at,
        sales_qoq,
        sales_yoy,
        profit_qoq,
        profit_yoy,
        op_qoq,
        op_yoy,
        25.0,
        opm_qoq,
        opm_yoy,
        4,
        4,
        4,
        4,
    )


def test_strong_sales_profit_opm_expansion_is_blowout(tmp_path: Path) -> None:
    db = tmp_path / "fundamentals.duckdb"
    _create_db(
        db,
        [_row("AAA", sales_yoy=0.30, sales_qoq=0.10, op_yoy=0.50, op_qoq=0.20, profit_yoy=0.45, profit_qoq=0.20, opm_yoy=4.0, opm_qoq=1.0)],
    )

    result = build_quarterly_result_scores(fundamentals_db_path=db, asof_date="2026-06-01")

    assert result.iloc[0]["quarterly_result_bucket"] == "BLOWOUT_RESULT"


def test_good_but_not_blowout_is_great(tmp_path: Path) -> None:
    db = tmp_path / "fundamentals.duckdb"
    _create_db(
        db,
        [_row("AAA", sales_yoy=0.20, sales_qoq=0.08, op_yoy=0.32, op_qoq=0.15, profit_yoy=0.35, profit_qoq=0.15, opm_yoy=1.5, opm_qoq=0.8)],
    )

    result = build_quarterly_result_scores(fundamentals_db_path=db, asof_date="2026-06-01")

    assert result.iloc[0]["quarterly_result_bucket"] == "GREAT_RESULT"


def test_qoq_acceleration_bucket(tmp_path: Path) -> None:
    db = tmp_path / "fundamentals.duckdb"
    _create_db(
        db,
        [_row("AAA", sales_yoy=0.20, sales_qoq=0.08, op_yoy=0.25, op_qoq=0.15, profit_yoy=0.30, profit_qoq=0.15, opm_yoy=0.8, opm_qoq=0.9)],
    )

    result = build_quarterly_result_scores(fundamentals_db_path=db, asof_date="2026-06-01")

    assert result.iloc[0]["quarterly_result_bucket"] == "RESULT_ACCELERATION"


def test_margin_contraction_is_deteriorating(tmp_path: Path) -> None:
    db = tmp_path / "fundamentals.duckdb"
    _create_db(
        db,
        [_row("AAA", sales_yoy=0.10, sales_qoq=0.02, op_yoy=0.05, op_qoq=0.01, profit_yoy=0.10, profit_qoq=0.01, opm_yoy=-3.0, opm_qoq=-1.0)],
    )

    result = build_quarterly_result_scores(fundamentals_db_path=db, asof_date="2026-06-01")

    assert result.iloc[0]["quarterly_result_bucket"] == "DETERIORATING"


def test_available_after_asof_date_is_excluded(tmp_path: Path) -> None:
    db = tmp_path / "fundamentals.duckdb"
    _create_db(
        db,
        [
            _row("AAA", available_at="2026-05-01", sales_yoy=0.20, sales_qoq=0.08, op_yoy=0.32, op_qoq=0.15, profit_yoy=0.35, profit_qoq=0.15, opm_yoy=1.5, opm_qoq=0.8),
            _row("FUT", available_at="2026-07-01", sales_yoy=0.50, sales_qoq=0.20, op_yoy=0.80, op_qoq=0.30, profit_yoy=0.80, profit_qoq=0.30, opm_yoy=5.0, opm_qoq=2.0),
        ],
    )

    result = build_quarterly_result_scores(fundamentals_db_path=db, asof_date="2026-06-01")

    assert set(result["symbol"]) == {"AAA"}


def test_quarterly_scoring_filters_to_standalone_basis(tmp_path: Path) -> None:
    db = tmp_path / "fundamentals.duckdb"
    standalone = _row("AAA", sales_yoy=0.20, sales_qoq=0.08, op_yoy=0.32, op_qoq=0.15, profit_yoy=0.35, profit_qoq=0.15, opm_yoy=1.5, opm_qoq=0.8)
    consolidated = list(_row("AAA", sales_yoy=0.90, sales_qoq=0.90, op_yoy=0.90, op_qoq=0.90, profit_yoy=0.90, profit_qoq=0.90, opm_yoy=9.0, opm_qoq=9.0))
    consolidated[2] = "consolidated"
    _create_db(db, [standalone, tuple(consolidated)])

    result = build_quarterly_result_scores(fundamentals_db_path=db, asof_date="2026-06-01")

    assert len(result) == 1
    assert result.iloc[0]["statement_basis"] == "standalone"
    assert result.iloc[0]["quarterly_result_bucket"] == "GREAT_RESULT"
