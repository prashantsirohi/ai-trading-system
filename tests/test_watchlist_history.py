from __future__ import annotations

import duckdb

from ai_trading_system.pipeline.registry import RegistryStore


def test_replace_watchlist_candidates_computes_history_metrics(tmp_path) -> None:
    registry = RegistryStore(tmp_path)

    first = registry.replace_watchlist_candidates(
        "2026-05-05",
        "run-1",
        1,
        [
            {"symbol_id": "AAA", "rank": 3, "sector": "Tech", "watchlist_score": 80.0},
            {"symbol_id": "BBB", "rank": 1, "sector": "Pharma", "watchlist_score": 90.0},
        ],
        "rank/attempt_1/watchlist_candidates.csv",
    )
    assert {row["symbol_id"]: row["is_new_entry"] for row in first} == {"AAA": True, "BBB": True}
    assert {row["symbol_id"]: row["days_on_watchlist"] for row in first} == {"AAA": 1, "BBB": 1}

    second = registry.replace_watchlist_candidates(
        "2026-05-06",
        "run-2",
        1,
        [
            {"symbol_id": "AAA", "rank": 1, "sector": "Tech", "watchlist_score": 95.0},
            {"symbol_id": "CCC", "rank": 2, "sector": "Industrial", "watchlist_score": 88.0},
        ],
        "rank/attempt_1/watchlist_candidates.csv",
    )
    lookup = {row["symbol_id"]: row for row in second}
    assert lookup["AAA"]["previous_rank"] == 3
    assert lookup["AAA"]["rank_change"] == 2
    assert lookup["AAA"]["days_on_watchlist"] == 2
    assert lookup["AAA"]["is_new_entry"] is False
    assert lookup["CCC"]["previous_rank"] is None
    assert lookup["CCC"]["rank_change"] is None
    assert lookup["CCC"]["days_on_watchlist"] == 1
    assert lookup["CCC"]["is_new_entry"] is True

    replacement = registry.replace_watchlist_candidates(
        "2026-05-06",
        "run-2",
        2,
        [{"symbol_id": "AAA", "rank": 2, "sector": "Tech", "watchlist_score": 91.0}],
        "rank/attempt_2/watchlist_candidates.csv",
    )
    assert len(replacement) == 1

    with duckdb.connect(str(registry.db_path)) as conn:
        rows = conn.execute(
            """
            SELECT symbol_id, rank, attempt_number
            FROM watchlist_candidate_history
            WHERE watchlist_date = '2026-05-06'
              AND run_id = 'run-2'
            """
        ).fetchall()
    assert rows == [("AAA", 2, 2)]
