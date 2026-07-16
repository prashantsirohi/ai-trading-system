from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.execution.models import FillRecord
from ai_trading_system.domains.execution.store import ExecutionStore
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.pipeline.stages.scan_router import ScanRouterStage
from ai_trading_system.pipeline.stages.weekly_stage import WeeklyStageCoverageStage


def test_weekly_coverage_routes_active_position_and_persists_history(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "runtime"
    data_root.mkdir()
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    ohlcv = data_root / "ohlcv.duckdb"
    dates = pd.bdate_range("2025-09-01", periods=210)
    rows = []
    for symbol_index, symbol in enumerate(("AAA", "BBB", "CCC", "DDD", "EEE")):
        for index, session in enumerate(dates):
            close = 100.0 + symbol_index * 5 + index * 0.2
            rows.append((symbol, "NSE", session.to_pydatetime(), close - 0.5, close + 1, close - 1, close, 1_000_000 + symbol_index * 10_000))
    conn = duckdb.connect(str(ohlcv))
    conn.execute("CREATE TABLE _catalog(symbol_id VARCHAR, exchange VARCHAR, timestamp TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE)")
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()

    master = sqlite3.connect(str(data_root / "masterdata.db"))
    master.execute("CREATE TABLE symbols(symbol_id TEXT, sector TEXT, industry TEXT)")
    master.executemany("INSERT INTO symbols VALUES (?, 'Tech', 'Software')", [(symbol,) for symbol in ("AAA", "BBB", "CCC", "DDD", "EEE")])
    master.commit()
    master.close()

    registry = RegistryStore(tmp_path, db_path=data_root / "control_plane.duckdb")
    run_date = dates[-1].date().isoformat()
    params = {
        "data_domain": "operational",
        "opportunity_scan_routing_mode": "compare",
        "minimum_sector_constituents": 1,
    }
    project_root = Path.cwd()
    weekly_context = StageContext(project_root, ohlcv, "phase3b-test", run_date, "weekly_stage", 1, registry=registry, params=params)
    weekly = WeeklyStageCoverageStage().run(weekly_context)
    assert weekly.metadata["eligible_full_universe"] >= 4
    assert weekly.metadata["sector_mapping_missing"] == 0
    assert weekly.metadata["sector_mapping_coverage_ratio"] == 1.0

    rank_path = tmp_path / "ranked_signals.csv"
    pd.DataFrame({"symbol_id": ["AAA", "BBB"], "rank_position": [1, 2]}).to_csv(rank_path, index=False)
    rank_artifact = StageArtifact.from_file("ranked_signals", rank_path, row_count=2, attempt_number=1)

    execution = ExecutionStore(project_root, db_path=data_root / "execution.duckdb")
    execution.append_fills([FillRecord(
        fill_id="active-fill",
        order_id="active-order",
        broker="paper",
        symbol_id="OUTSIDE",
        quantity=10,
        price=100.0,
        filled_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        side="BUY",
        exchange="NSE",
    )])
    weekly_artifacts = {artifact.artifact_type: artifact for artifact in weekly.artifacts}
    router_context = StageContext(
        project_root,
        ohlcv,
        "phase3b-test",
        run_date,
        "scan_router",
        1,
        registry=registry,
        params=params,
        artifacts={"rank": {"ranked_signals": rank_artifact}, "weekly_stage": weekly_artifacts},
    )
    routed = ScanRouterStage().run(router_context)
    assert routed.metadata["active_positions_total"] == 1
    assert routed.metadata["active_positions_with_position_monitor"] == 1
    assert routed.metadata["active_positions_fully_monitored"] == 0
    assert routed.metadata["active_positions_missing_coverage"] == 1
    assert routed.metadata["active_positions_missing_market_data"] == 1
    with registry._reader() as reader:  # noqa: SLF001
        assert reader.execute("SELECT COUNT(*) FROM weekly_stock_stage_history").fetchone()[0] >= 4
        assert reader.execute("SELECT COUNT(*) FROM opportunity_scan_routing_history").fetchone()[0] >= 1
        assert reader.execute(
            "SELECT COUNT(*) FROM pipeline_alert WHERE alert_type = 'active_position_missing_market_data'"
        ).fetchone()[0] == 1
