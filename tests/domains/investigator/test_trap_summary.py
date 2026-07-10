from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.trap_summary import build_trap_summary_metrics


def test_trap_summary_separates_symbols_from_evidence_events() -> None:
    current = pd.DataFrame(
        [
            {"symbol_id": " aaa ", "trade_date": "2026-07-10", "drop_reason": "ONE_CANDLE_DRAMA"},
            {"symbol_id": "AAA", "trade_date": "2026-07-10", "low_delivery_flag": True},
            {"symbol_id": "bbb", "trade_date": "2026-07-10", "price_progression_pct": -2},
        ]
    )
    archive = pd.DataFrame(
        [
            {"symbol_id": "AAA", "trade_date": "2026-07-10", "drop_reason": "ONE_CANDLE_DRAMA"},
            {"symbol_id": "AAA", "trade_date": "2026-07-09", "low_delivery_flag": True},
            {"symbol_id": "OLD", "trade_date": "2026-07-01", "drop_reason": "ONE_CANDLE_DRAMA"},
        ]
    )

    metrics = build_trap_summary_metrics(
        current_traps=current,
        archive=archive,
        run_date="2026-07-10",
        candidate_union_rows=10,
    )

    assert metrics["unique_trap_symbols"] == 2
    assert metrics["fresh_trap_symbols_today"] == 2
    assert metrics["trap_evidence_events"] > metrics["unique_trap_symbols"]
    assert metrics["trap_candidate_rate"] == 0.2
    assert metrics["trap_count"] == metrics["unique_trap_symbols"]
    assert metrics["fresh_trap_today"] == metrics["fresh_trap_symbols_today"]
    assert metrics["trap_evidence_count"] == metrics["trap_evidence_events"]
    assert metrics["trap_rate"] == metrics["trap_candidate_rate"]
    assert metrics["trap_summary_valid"] is True


def test_trap_summary_zero_denominator_and_invalid_subset_are_non_blocking() -> None:
    metrics = build_trap_summary_metrics(
        current_traps=pd.DataFrame([{"symbol_id": "AAA", "trade_date": "2026-07-10"}]),
        archive=pd.DataFrame(),
        run_date="2026-07-10",
        candidate_union_rows=0,
    )

    assert metrics["trap_candidate_rate"] == 0.0
    assert metrics["trap_summary_valid"] is False
    assert "unique_trap_symbols exceeds candidate_union_rows" in metrics["trap_summary_validation_errors"]
    assert 0 <= metrics["trap_rate"] <= 1
