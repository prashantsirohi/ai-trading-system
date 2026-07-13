from __future__ import annotations

from pathlib import Path

import duckdb

from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.ui.execution_api.services.readmodels.decision_reads import (
    PatternHistoryReadRepository,
    RankHistoryReadRepository,
    Stage1LifecycleReadRepository,
)


def _db(tmp_path: Path) -> Path:
    path = tmp_path / "control_plane.duckdb"
    RegistryStore(tmp_path, db_path=path)
    return path


def test_rank_read_selects_approved_version_and_computes_movers(tmp_path: Path) -> None:
    db = _db(tmp_path)
    with duckdb.connect(str(db)) as conn:
        conn.execute("""INSERT INTO decision_model_deployment
            (decision_domain, model_version, config_hash, environment, effective_from, status)
            VALUES ('rank','rank-v1','cfg','production','2026-01-01','approved')""")
        for day, rank in (("2026-07-01", 9), ("2026-07-02", 5), ("2026-07-03", 2)):
            conn.execute("""INSERT INTO rank_history
                (symbol_id,exchange,trade_date,universe_id,rank_position,composite_score,
                 rank_model_version,rank_formula_name,rank_config_hash,pipeline_run_id,source_attempt)
                VALUES ('ABC','NSE',CAST(? AS DATE),'NSE_OPERATIONAL',?,80,'rank-v1','weighted','cfg','run',1)""", [day, rank])
    payload = RankHistoryReadRepository(tmp_path, db_path=db).get_current_rankings(trade_date="2026-07-03")
    assert payload["metadata"]["data_source"] == "DUCKDB"
    assert payload["rows"][0]["rank_position"] == 2
    assert payload["rows"][0]["previous_rank"] == 5
    assert payload["rows"][0]["best_rank"] == 2


def test_pattern_history_preserves_multiple_families(tmp_path: Path) -> None:
    db = _db(tmp_path)
    with duckdb.connect(str(db)) as conn:
        conn.execute("""INSERT INTO decision_model_deployment
            (decision_domain, model_version, config_hash, environment, effective_from, status)
            VALUES ('pattern','p1','cfg','production','2026-01-01','approved')""")
        conn.execute("""INSERT INTO pattern_history
            (symbol_id,exchange,trade_date,pattern_family,pattern_model_version,pattern_config_hash,pipeline_run_id,source_attempt)
            VALUES ('ABC','NSE','2026-07-03','VCP','p1','cfg','run',1),
                   ('ABC','NSE','2026-07-03','CUP','p1','cfg','run',1)""")
    payload = PatternHistoryReadRepository(tmp_path, db_path=db).history("ABC")
    assert {row["pattern_family"] for row in payload["rows"]} == {"VCP", "CUP"}


def test_lifecycle_reconciliation_reports_missing_current(tmp_path: Path) -> None:
    db = _db(tmp_path)
    with duckdb.connect(str(db)) as conn:
        conn.execute("""INSERT INTO investigator_stage1_state
            (symbol_id,exchange,trade_date,stage1_lifecycle_state,stage1_lifecycle_model_version,
             stage1_lifecycle_config_hash,pipeline_run_id,run_id,attempt_number)
            VALUES ('MISS','NSE','2026-07-03','BASE_BUILDING','v1','cfg','run','run',1)""")
    result = Stage1LifecycleReadRepository(tmp_path, db_path=db).reconciliation()
    assert result["missing_current_rows"] == 1
