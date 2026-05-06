from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.ranking.watchlist import (
    build_final_watchlist,
    build_watchlist_prefilter,
    compute_watchlist_score,
)
from ai_trading_system.domains.ranking.watchlist_catalyst import enrich_with_catalyst


class _FakeCatalystClient:
    def __init__(self) -> None:
        self.payloads = []

    def complete_json(self, payload):
        self.payloads.append(payload)
        return {
            "catalyst_tags": ["ORDER_WIN"],
            "catalyst_confidence": "HIGH",
            "bull_case": "Fresh contract disclosure supports the technical setup.",
            "risk_flags": ["Execution risk"],
            "watchlist_reason": "Technical breakout plus fresh contract disclosure.",
        }


class _FailingCatalystClient:
    def complete_json(self, payload):
        raise RuntimeError("llm unavailable")


def _ranked() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol_id": "AAA", "sector": "Capital Goods", "rank": 1, "stage2_label": "stage2", "return_1": 4, "return_5": 6, "near_52w_high_pct": 3, "volume_ratio": 2.0, "delivery_pct": 55, "close": 120, "sma_50": 110, "composite_score": 91},
            {"symbol_id": "BBB", "sector": "IT", "rank": 2, "stage2_label": "non_stage2", "return_1": 4, "return_5": 6, "near_52w_high_pct": 3, "volume_ratio": 2.0, "delivery_pct": 50, "close": 120, "sma_50": 110, "composite_score": 90},
            {"symbol_id": "CCC", "sector": "Metals", "rank": 3, "stage2_label": "stage1_to_stage2", "return_1": 4, "return_5": 6, "near_52w_high_pct": 3, "volume_ratio": 2.0, "delivery_pct": 50, "close": 120, "sma_50": 110, "composite_score": 89},
            {"symbol_id": "DDD", "sector": "FMCG", "rank": 4, "stage2_label": "stage2", "return_1": 4, "return_5": 6, "near_52w_high_pct": 3, "volume_ratio": 2.0, "delivery_pct": 50, "close": 150, "sma_50": 100, "composite_score": 88},
            {"symbol_id": "EEE", "sector": "Energy", "rank": 5, "stage2_label": "stage2", "return_1": 4, "return_5": 6, "near_52w_high_pct": 3, "volume_ratio": 2.0, "delivery_pct": 50, "close": 120, "sma_50": 110, "composite_score": 87},
            {"symbol_id": "FFF", "sector": "Energy", "rank": 6, "stage2_label": "stage2", "return_1": 4, "return_5": 6, "near_52w_high_pct": 3, "volume_ratio": 2.0, "delivery_pct": 50, "close": 120, "sma_50": 110, "composite_score": 86},
            {"symbol_id": "GGG", "sector": "Energy", "rank": 7, "stage2_label": "stage2", "return_1": 4, "return_5": 6, "near_52w_high_pct": 3, "volume_ratio": 2.0, "delivery_pct": 50, "close": 120, "sma_50": 110, "composite_score": 85},
        ]
    )


def _breakout() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol_id": symbol, "candidate_tier": "A", "qualified": True, "setup_quality": "flat-base breakout", "breakout_score": score}
            for symbol, score in [
                ("AAA", 85),
                ("BBB", 85),
                ("CCC", 85),
                ("DDD", 85),
                ("EEE", 92),
                ("FFF", 91),
                ("GGG", 90),
            ]
        ]
    )


def _pattern() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol_id": "AAA", "pattern_score": 70, "pattern_lifecycle_state": "watchlist", "pattern_operational_tier": "tier_1", "pattern_name": "flat_base"}
        ]
    )


def _sectors() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Sector": "Capital Goods", "Quadrant": "Leading"},
            {"Sector": "IT", "Quadrant": "Leading"},
            {"Sector": "Metals", "Quadrant": "Improving"},
            {"Sector": "FMCG", "Quadrant": "Leading"},
            {"Sector": "Energy", "Quadrant": "Weakening"},
        ]
    )


def test_sector_and_stage_gates_filter_candidates() -> None:
    prefilter = build_watchlist_prefilter(_ranked(), _breakout(), _pattern(), _sectors(), top_n=30)
    symbols = set(prefilter["symbol_id"])
    assert "AAA" in symbols
    assert "CCC" in symbols
    assert "BBB" not in symbols


def test_escape_hatch_admits_at_most_two_non_leading_breakouts() -> None:
    prefilter = build_watchlist_prefilter(_ranked(), _breakout(), _pattern(), _sectors(), top_n=30)
    escapes = prefilter.loc[prefilter["sector_escape_hatch"]]
    assert set(escapes["symbol_id"]) == {"EEE", "FFF"}
    assert "GGG" not in set(prefilter["symbol_id"])


def test_extension_cap_drops_symbols_above_25_pct_sma50() -> None:
    prefilter = build_watchlist_prefilter(_ranked(), _breakout(), _pattern(), _sectors(), top_n=30)
    assert "DDD" not in set(prefilter["symbol_id"])


def test_watchlist_score_is_bounded_and_deterministic() -> None:
    prefilter = build_watchlist_prefilter(_ranked(), _breakout(), _pattern(), _sectors(), top_n=30)
    score = compute_watchlist_score(prefilter.iloc[0])
    assert 0 <= score <= 100
    assert score == compute_watchlist_score(prefilter.iloc[0])


def test_llm_failure_path_final_has_empty_llm_fields() -> None:
    prefilter = build_watchlist_prefilter(_ranked(), _breakout(), _pattern(), _sectors(), top_n=30)
    final = build_final_watchlist(prefilter, catalyst_enrichment=None, top_n=15, data_trust_status="trusted")
    assert not final.empty
    assert final["catalyst_tags"].fillna("").eq("").all()
    assert final["catalyst_confidence"].fillna("").eq("").all()
    assert final["watchlist_reason"].str.len().gt(0).all()


def test_catalyst_enrichment_uses_market_intel_and_final_merges(tmp_path) -> None:
    prefilter = build_watchlist_prefilter(_ranked(), _breakout(), _pattern(), _sectors(), top_n=30)
    client = _FakeCatalystClient()
    enrichment = enrich_with_catalyst(
        prefilter.head(1),
        market_intel={
            "top_events": [
                {
                    "symbol": "AAA",
                    "event_date": "2026-05-05",
                    "category": "order_win",
                    "materiality_label": "important",
                    "severity": "high",
                    "title": "AAA wins a large contract",
                }
            ]
        },
        llm_client=client,
        run_date="2026-05-06",
        cache_dir=tmp_path,
    )
    assert "AAA" in enrichment
    assert client.payloads[0]["market_intel"][0]["category"] == "order_win"
    final = build_final_watchlist(prefilter.head(1), catalyst_enrichment=enrichment, top_n=1, data_trust_status="trusted")
    assert final.iloc[0]["catalyst_tags"] == "ORDER_WIN"
    assert final.iloc[0]["catalyst_confidence"] == "HIGH"
    assert "contract" in final.iloc[0]["watchlist_reason"]


def test_catalyst_enrichment_falls_back_per_symbol(tmp_path) -> None:
    prefilter = build_watchlist_prefilter(_ranked(), _breakout(), _pattern(), _sectors(), top_n=30)
    enrichment = enrich_with_catalyst(
        prefilter.head(1),
        market_intel={},
        llm_client=_FailingCatalystClient(),
        run_date="2026-05-06",
        cache_dir=tmp_path,
    )
    record = enrichment["AAA"]
    assert record["catalyst_tags"] == []
    assert record["catalyst_confidence"] == ""
    assert record["watchlist_reason"]
    assert record["status"].startswith("fallback_after_error")


def test_prefilter_empty_candidate_set_does_not_crash() -> None:
    ranked = pd.DataFrame(
        [
            {
                "symbol_id": "ZZZ",
                "sector": "Energy",
                "rank": 99,
                "stage2_label": "non_stage2",
                "return_1": 0,
                "return_5": 0,
                "near_52w_high_pct": 30,
                "volume_ratio": 0.8,
                "delivery_pct": 10,
                "close": 150,
                "sma_50": 100,
                "composite_score": 20,
            }
        ]
    )
    breakout = pd.DataFrame(columns=["symbol_id", "candidate_tier", "qualified", "breakout_score"])
    pattern = pd.DataFrame(columns=["symbol_id", "pattern_score", "pattern_lifecycle_state", "pattern_operational_tier"])
    sectors = pd.DataFrame([{"Sector": "Energy", "Quadrant": "Lagging"}])
    prefilter = build_watchlist_prefilter(ranked, breakout, pattern, sectors, top_n=30)
    assert prefilter.empty


def test_prefilter_accepts_sector_name_alias() -> None:
    ranked = _ranked().drop(columns=["sector"]).rename(columns={"sector": "sector_name"})
    ranked.loc[:, "sector_name"] = _ranked()["sector"]
    prefilter = build_watchlist_prefilter(ranked, _breakout(), _pattern(), _sectors(), top_n=30)
    assert "AAA" in set(prefilter["symbol_id"])
    assert prefilter.loc[prefilter["symbol_id"].eq("AAA"), "sector_status"].iloc[0] == "LEADING"
