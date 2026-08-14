from ai_trading_system.domains.research_screener.repair_analysis import (
    combined_price_factor,
    corporate_action_repair_lanes,
    fundamental_repair_lane,
    parse_split_bonus_terms,
)


def _member(*, scope="standalone", disclosed="2026-03-31", parsed="2026-03-31", missing=None):
    return {
        "symbol": "TEST", "isin": "INETEST00001",
        "identity": {"listings": [{"listing_date": "2024-01-01"}]},
        "inputs": {"fundamentals": {
            "scope": scope,
            "latest_disclosed_periods": {"annual": disclosed, "quarterly": "2026-06-30"},
            "latest_parsed_periods": {"annual": parsed, "quarterly": "2026-06-30"},
            "missing_target_periods": missing or {"annual": [], "quarterly": []},
        }},
    }


def test_fundamental_repair_lanes_are_mutually_exclusive():
    assert fundamental_repair_lane(_member(scope="SCOPE_UNRESOLVED")) == "STATEMENT_SCOPE_UNRESOLVED"
    assert fundamental_repair_lane(_member(parsed="2025-03-31")) == "LATEST_DISCLOSED_DOCUMENT_NOT_VALIDATED"
    assert fundamental_repair_lane(_member(missing={"annual": ["2023-03-31"], "quarterly": []})) == "GENUINE_POST_LISTING_HISTORY_GAP"
    assert fundamental_repair_lane(_member(missing={"annual": ["2025-03-31"], "quarterly": []})) == "MISSING_HISTORICAL_FILING_PERIODS"
    assert fundamental_repair_lane(_member()) == "FILED_METRIC_COMPLETENESS_GAP"


def test_corporate_action_lanes_keep_complex_actions_blocked():
    member = {
        "symbol": "TEST", "isin": "INETEST00001",
        "inputs": {"corporate_action_validation": {"unmatched_events": [
            {"action_type": "split", "ex_date": "2026-01-01", "source_row_hash": "a"},
            {"action_type": "rights", "ex_date": "2026-02-01", "source_row_hash": "b"},
            {"action_type": "demerger", "ex_date": "2026-03-01", "source_row_hash": "c"},
        ]}},
    }
    assert [row["repair_lane"] for row in corporate_action_repair_lanes(member)] == [
        "SPLIT_BONUS_OPERATIONAL_BACKFILL_CANDIDATE",
        "RIGHTS_TERMS_AND_PRICE_BASIS_REQUIRED",
        "SCHEME_AND_SUCCESSOR_PRICE_BASIS_REQUIRED",
    ]


def test_split_bonus_terms_and_same_date_combination_are_deterministic():
    split = {"action_type": "split", "raw_subject": "Stock Split From Rs.10/- to Rs.5/-"}
    bonus = {"action_type": "bonus", "raw_subject": "Bonus issue 3:2"}

    assert parse_split_bonus_terms(split) == (0.5, 2.0)
    assert parse_split_bonus_terms(bonus) == (0.4, 2.5)
    assert combined_price_factor([split, bonus]) == 0.2
    assert parse_split_bonus_terms({"action_type": "split", "raw_subject": "Stock Split"}) == (None, None)
