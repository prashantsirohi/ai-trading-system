"""Shared fixtures for the Performance Tracker diagnostics tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ai_trading_system.research.perf_tracker.schema import open_research_db
from ai_trading_system.ui.execution_api.app import create_app


API_HEADERS = {"x-api-key": "test-api-key"}


def insert_perf_rows(project_root: Path, rows: list[dict]) -> None:
    """Bulk-insert into rank_cohort_performance using the canonical 24-col tuple."""
    with open_research_db(project_root=project_root, read_only=False) as con:
        con.executemany(
            """
            INSERT INTO rank_cohort_performance (
                run_date, symbol_id, exchange, rank_position,
                composite_score, composite_score_adjusted, rank_mode,
                watchlist_bucket, config_id,
                fwd_5d_return, fwd_10d_return, fwd_20d_return, fwd_60d_return,
                fwd_5d_matured_at, fwd_10d_matured_at, fwd_20d_matured_at,
                fwd_60d_matured_at,
                factor_rs, factor_vol, factor_trend, factor_prox, factor_deliv,
                factor_sector, factor_momentum_accel, factor_above_200dma,
                factor_liquidity, factor_delivery_trend, sector_name
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                (
                    r["run_date"], r["symbol_id"], r.get("exchange", "NSE"), r["rank_position"],
                    r.get("composite_score", 0.0), r.get("composite_score_adjusted", 0.0),
                    r.get("rank_mode", "state_only"), r.get("watchlist_bucket"), r.get("config_id"),
                    r.get("fwd_5d_return"), r.get("fwd_10d_return"), r.get("fwd_20d_return"),
                    r.get("fwd_60d_return"), r.get("fwd_5d_matured_at", r["run_date"]),
                    r.get("fwd_10d_matured_at", r["run_date"]),
                    r.get("fwd_20d_matured_at", r["run_date"]), r.get("fwd_60d_matured_at"),
                    r.get("factor_rs"), r.get("factor_vol"), r.get("factor_trend"),
                    r.get("factor_prox"), r.get("factor_deliv"), r.get("factor_sector"),
                    r.get("factor_momentum_accel"), r.get("factor_above_200dma"),
                    r.get("factor_liquidity"), r.get("factor_delivery_trend"),
                    r.get("sector_name", "Test"),
                )
                for r in rows
            ],
        )


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    return tmp_path


@pytest.fixture
def api_client(project: Path) -> TestClient:
    return TestClient(create_app())
