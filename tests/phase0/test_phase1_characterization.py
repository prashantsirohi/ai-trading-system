"""Executable characterizations for correctness work scheduled in Phase 1."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.execution import (
    AutoTrader,
    ExecutionService,
    ExecutionStore,
    PaperExecutionAdapter,
    PortfolioManager,
)
from ai_trading_system.domains.execution.models import OrderIntent
from ai_trading_system.domains.ranking.input_loader import RankerInputLoader
from ai_trading_system.domains.ranking.ranker import StockRanker
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.pipeline.registry import RegistryStore


def _seed_rank_catalog(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog VALUES
                ('ACME', 'NSE', TIMESTAMP '2026-01-01 15:30:00',
                 89, 92, 88, 90, 800),
                ('ACME', 'NSE', TIMESTAMP '2026-01-02 15:30:00',
                 99, 102, 98, 100, 1000)
            """
        )
    finally:
        conn.close()


def test_aud_001_future_rows_do_not_change_historical_rank_inputs_or_result(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _seed_rank_catalog(db_path)
    loader = RankerInputLoader(
        ohlcv_db_path=str(db_path),
        feature_store_dir=str(tmp_path / "features"),
        master_db_path=str(tmp_path / "masterdata.db"),
    )

    historical_base = loader.load_latest_market_data(
        as_of="2026-01-02",
        exchanges=["NSE"],
    )
    historical_returns = loader.load_return_frame_multi(
        as_of="2026-01-02",
        periods=[1],
        exchanges=["NSE"],
    )
    historical_volume = loader.load_volume_frame(
        as_of="2026-01-02",
        exchanges=["NSE"],
    )
    ranker = StockRanker(
        ohlcv_db_path=str(db_path),
        feature_store_dir=str(tmp_path / "features"),
    )
    historical_rank = ranker.rank_all(
        date="2026-01-02",
        exchanges=["NSE"],
        min_score=0,
    )
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO _catalog VALUES
                ('ACME', 'NSE', TIMESTAMP '2026-02-02 15:30:00',
                 149, 152, 148, 150, 9000)
            """
        )
    finally:
        conn.close()
    historical_base_after_future_ingest = loader.load_latest_market_data(
        as_of="2026-01-02",
        exchanges=["NSE"],
    )
    historical_returns_after_future_ingest = loader.load_return_frame_multi(
        as_of="2026-01-02",
        periods=[1],
        exchanges=["NSE"],
    )
    historical_volume_after_future_ingest = loader.load_volume_frame(
        as_of="2026-01-02",
        exchanges=["NSE"],
    )
    historical_rank_after_future_ingest = ranker.rank_all(
        date="2026-01-02",
        exchanges=["NSE"],
        min_score=0,
    )

    pd.testing.assert_frame_equal(
        historical_base,
        historical_base_after_future_ingest,
        check_like=True,
    )
    pd.testing.assert_frame_equal(
        historical_returns,
        historical_returns_after_future_ingest,
        check_like=True,
    )
    pd.testing.assert_frame_equal(
        historical_volume,
        historical_volume_after_future_ingest,
        check_like=True,
    )
    pd.testing.assert_frame_equal(
        historical_rank,
        historical_rank_after_future_ingest,
        check_like=True,
    )


def test_aud_002_failed_attempt_artifact_is_not_authoritative(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    run_id = "phase0-artifact-promotion"
    registry.create_run(run_id, "phase0", "2026-07-13")
    stage_run_id = registry.start_stage(run_id, "rank", 1)
    artifact_path = tmp_path / "ranked_signals.csv"
    artifact_path.write_text("symbol_id,composite_score\nACME,90\n", encoding="utf-8")
    registry.record_artifact(
        run_id,
        "rank",
        1,
        StageArtifact.from_file(
            "ranked_signals",
            artifact_path,
            row_count=1,
            attempt_number=1,
        ),
    )
    registry.finish_stage(
        stage_run_id,
        "failed",
        error_class="DataQualityError",
        error_message="rank hard floor failed",
    )

    artifact_map = registry.get_artifact_map(run_id)
    failed_attempt_artifacts = registry.get_attempt_artifacts(run_id, "rank", 1)

    assert "ranked_signals" not in artifact_map.get("rank", {})
    assert failed_attempt_artifacts["ranked_signals"].uri == str(artifact_path)
    assert registry.get_latest_artifact(
        stage_name="rank",
        artifact_type="ranked_signals",
        run_status=None,
    ) == []
    conn = duckdb.connect(str(tmp_path / "control_plane.duckdb"), read_only=True)
    try:
        failed_lifecycle = conn.execute(
            """
            SELECT lifecycle_status FROM pipeline_artifact
            WHERE run_id = ? AND stage_name = ? AND attempt_number = ?
            """,
            [run_id, "rank", 1],
        ).fetchone()[0]
    finally:
        conn.close()
    assert failed_lifecycle == "written"

    successful_stage_run_id = registry.start_stage(run_id, "rank", 2)
    successful_artifact_path = tmp_path / "ranked_signals_attempt_2.csv"
    successful_artifact_path.write_text(
        "symbol_id,composite_score\nACME,95\n",
        encoding="utf-8",
    )
    registry.record_artifact(
        run_id,
        "rank",
        2,
        StageArtifact.from_file(
            "ranked_signals",
            successful_artifact_path,
            row_count=1,
            attempt_number=2,
        ),
    )
    assert registry.mark_attempt_artifacts_dq_passed(run_id, "rank", 2) == 1
    conn = duckdb.connect(str(tmp_path / "control_plane.duckdb"), read_only=True)
    try:
        dq_lifecycle = conn.execute(
            """
            SELECT lifecycle_status FROM pipeline_artifact
            WHERE run_id = ? AND stage_name = ? AND attempt_number = ?
            """,
            [run_id, "rank", 2],
        ).fetchone()[0]
    finally:
        conn.close()
    assert dq_lifecycle == "dq_passed"
    registry.finish_stage(successful_stage_run_id, "completed")

    promoted_artifact = registry.get_artifact_map(run_id)["rank"]["ranked_signals"]
    failed_attempt_artifacts = registry.get_attempt_artifacts(run_id, "rank", 1)
    latest_promoted_artifact = registry.get_latest_artifact(
        stage_name="rank",
        artifact_type="ranked_signals",
        run_status=None,
    )[0]

    assert promoted_artifact.uri == str(successful_artifact_path)
    assert promoted_artifact.attempt_number == 2
    assert latest_promoted_artifact.uri == str(successful_artifact_path)
    assert failed_attempt_artifacts["ranked_signals"].uri == str(artifact_path)
    conn = duckdb.connect(str(tmp_path / "control_plane.duckdb"), read_only=True)
    try:
        promoted_lifecycle = conn.execute(
            """
            SELECT lifecycle_status FROM pipeline_artifact
            WHERE run_id = ? AND stage_name = ? AND attempt_number = ?
            """,
            [run_id, "rank", 2],
        ).fetchone()[0]
    finally:
        conn.close()
    assert promoted_lifecycle == "promoted"


class _FixedRiskManager:
    def compute_position_size(self, symbol_id: str, **_: object) -> dict:
        return {
            "symbol_id": symbol_id,
            "shares": 10,
            "position_value": 1000.0,
            "risk_amount": 100.0,
            "stop_loss": 90.0,
            "atr": 5.0,
            "close": 100.0,
        }


def test_aud_004_batch_orders_cannot_exceed_portfolio_heat(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(
        store,
        PaperExecutionAdapter(slippage_bps=0),
        risk_manager=_FixedRiskManager(),
    )
    portfolio = PortfolioManager(store)
    ranked = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "close": 100.0,
                "composite_score": 92.0,
                "atr_14": 5.0,
            },
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "close": 100.0,
                "composite_score": 91.0,
                "atr_14": 5.0,
            },
        ]
    )

    result = AutoTrader(service, portfolio).run(
        ranked_df=ranked,
        strategy_mode="technical",
        target_position_count=2,
        buy_quantity=10,
        capital=1_000.0,
        heat_gate_threshold=0.15,
    )
    accepted_buys = [
        item
        for item in result["executions"]
        if item["action"]["action"] == "BUY"
        and item["result"].get("status") not in {"REJECTED", "ERROR"}
    ]
    rejected_buy = next(
        item
        for item in result["executions"]
        if item["action"]["action"] == "BUY"
        and item["result"].get("status") == "REJECTED"
    )
    heat_ok, _ = portfolio.check_heat_gate(
        portfolio.open_positions(),
        capital=1_000.0,
        threshold=0.15,
    )

    assert len(accepted_buys) == 1
    assert rejected_buy["result"]["reason"] == "heat_gate_exceeded"
    assert rejected_buy["result"]["open_risk"] == 0.1
    assert rejected_buy["result"]["candidate_risk"] == 0.1
    assert rejected_buy["result"]["projected_risk"] == 0.2
    assert len(store.list_orders()) == 1
    assert len(store.list_fills()) == 1
    assert heat_ok is True


def test_aud_005_repeated_submission_creates_one_order_and_fill(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    intent = OrderIntent(
        symbol_id="ACME",
        exchange="NSE",
        quantity=10,
        side="BUY",
        correlation_id="phase0-idempotency-key",
    )

    first = service.submit_order(intent, market_price=100.0)
    replay = service.submit_order(intent, market_price=100.0)

    assert len(store.list_orders()) == 1
    assert len(store.list_fills()) == 1
    assert replay["idempotent_replay"] is True
    assert replay["order"]["order_id"] == first["order"]["order_id"]
    assert replay["fills"][0]["fill_id"] == first["fills"][0]["fill_id"]
