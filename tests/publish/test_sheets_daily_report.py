from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.publish.sheets_daily_report import (
    MarketContext,
    blocked_by_flags,
    build_confirmed_breakouts,
    build_daily_report_sections,
    build_top_ranked,
    classify_stage,
    compute_new_entry,
    compute_watchlist_score,
    determine_status,
    generate_reason,
    generate_risk_note,
    symbol_links,
)


def _context(**overrides) -> MarketContext:
    data = {
        "run_date": "2026-06-23",
        "trust_status": "trusted",
        "market_state": "neutral",
        "allowed_exposure": 0.5,
        "regime_phase": "Neutral",
        "breadth_velocity": "neutral",
    }
    data.update(overrides)
    return MarketContext(**data)


def _row(**overrides) -> dict:
    data = {
        "symbol_id": "AAA",
        "sector": "Capital Goods",
        "sector_status": "Leading",
        "pattern_family": "VCP",
        "pattern_score": 82,
        "breakout_score": 76,
        "breakout_level": 105.0,
        "close": 103.0,
        "volume_ratio_20": 1.6,
        "delivery_pct": 62,
        "rel_strength_20d": 78,
        "rel_strength_60d": 72,
        "composite_score": 88,
        "sma_50": 96,
        "sma_200": 82,
        "sma50_slope_20d_pct": 4.0,
        "near_52w_high_pct": 8.0,
        "days_on_watchlist": 1,
    }
    data.update(overrides)
    return data


def test_watchlist_score_is_non_zero_when_inputs_are_available() -> None:
    score = compute_watchlist_score(_row(), _context())

    assert score > 0
    assert score <= 100


def test_compute_new_entry_prefers_previous_symbols_and_falls_back_to_days() -> None:
    assert compute_new_entry("AAA", {"BBB"}, 1) == "NEW"
    assert compute_new_entry("AAA", {"AAA"}, 1) == ""
    assert compute_new_entry("AAA", None, 1) == "NEW"
    assert compute_new_entry("AAA", None, 3) == ""


def test_empty_qualified_breakouts_produces_visible_fallback_row() -> None:
    frame = build_confirmed_breakouts(
        [_row(qualified=False, breakout_state="watchlist")],
        _context(regime_phase="Bear / Stage 4", breadth_velocity="very_negative", trust_status="degraded"),
    )

    assert frame.iloc[0]["Status"] == "No confirmed / trade-qualified breakouts today."
    assert frame.iloc[0]["Blocked By"] == "REGIME, BREADTH, TRUST, NO_CONFIRMATION"


def test_classify_stage_core_scenarios() -> None:
    assert classify_stage(_row(close=120, sma_50=110, sma_200=90, sma50_slope_20d_pct=3, near_52w_high_pct=10, stage2_label="")) == "Stage 2 / Uptrend"
    assert classify_stage(_row(close=80, sma_50=95, sma_200=100, stage2_label="")) == "Weak / Below 200DMA"
    assert classify_stage({"symbol_id": "MISS"}) == "Unknown"


def test_reason_and_risk_note_tolerate_missing_optional_columns() -> None:
    sparse = {"symbol_id": "AAA", "pattern_family": "darvas"}

    assert generate_reason(sparse, _context())
    assert generate_risk_note(sparse, _context())


def test_status_downgrades_non_qualified_rows_in_weak_regime() -> None:
    context = _context(regime_phase="Bear / Stage 4", allowed_exposure=0.15)

    assert determine_status(_row(qualified=False, breakout_state="watchlist"), context) == "BLOCKED_BY_REGIME"


def test_status_taxonomy_and_blocked_by_flags() -> None:
    assert determine_status(_row(qualified=True, breakout_state="qualified"), _context()) == "TRADE_READY"
    assert determine_status(_row(qualified=True, breakout_state="qualified"), _context(trust_status="blocked")) == "BLOCKED_BY_TRUST"
    assert determine_status(_row(qualified=False, breakout_state="watchlist", volume_ratio_20=0.2), _context()) == "AVOID_RISK"
    assert determine_status(_row(qualified=False, breakout_state="watchlist", sector_status="Leading"), _context()) == "WATCH"

    context = _context(regime_phase="Bear / Stage 4", trust_status="degraded", breadth_velocity="very_negative")
    flags = blocked_by_flags(
        _row(sector_status="Lagging", volume_ratio_20=None, risk_flags="trap flag; days stale > threshold"),
        context,
        risk_note="Bear regime; Trust degraded; Too far from breakout level; Illiquid / avoid; Volume confirmation missing; Lagging sector; Days stale > threshold; Trap flag",
    )
    assert flags == "REGIME, TRUST, DISTANCE, LIQUIDITY, VOLUME, SECTOR, STALE, TRAP"


def test_symbol_links_uses_safe_hyperlink_formula() -> None:
    links = symbol_links("RELIANCE")

    assert links.startswith("=HYPERLINK(")
    assert "NSE:RELIANCE" in links
    assert "screener.in/company/RELIANCE" in links


def test_daily_report_builds_fallback_and_new_watchlist_row() -> None:
    result = build_daily_report_sections(
        payload={
            "summary": {
                "run_date": "2026-06-23",
                "data_trust_status": "degraded",
                "allowed_exposure": 0.15,
            },
            "market_regime_phase": {
                "phase_label": "Bear / Stage 4",
                "driven_by": {"breadth_velocity_bucket": "very_negative"},
            },
        },
        run_date="2026-06-23",
        ranked_df=pd.DataFrame([_row(symbol_id="AAA")]),
        breakout_df=pd.DataFrame([{"symbol_id": "AAA", "qualified": False, "breakout_state": "watchlist", "breakout_score": 76}]),
        pattern_df=pd.DataFrame([{"symbol_id": "AAA", "pattern_family": "VCP", "pattern_score": 82, "breakout_level": 105.0}]),
        sector_df=pd.DataFrame([{"Sector": "Capital Goods", "Quadrant": "Leading"}]),
        watchlist_df=pd.DataFrame([{"symbol_id": "AAA", "days_on_watchlist": 1}]),
        prior_watchlist_df=pd.DataFrame([{"symbol_id": "BBB"}]),
        rank_artifact_uri="/tmp/ranked_signals.csv",
        run_id="pipeline-2026-06-23-test",
    )
    sections = dict(result.sections)

    assert "No trade-qualified breakouts today." in result.metadata["operator_message"]
    assert sections["CONFIRMED BREAKOUTS"].iloc[0]["Status"] == "No confirmed / trade-qualified breakouts today."
    assert sections["STUDY WATCHLIST TOP 10"].iloc[0]["New Entry"] == "NEW"
    assert sections["STUDY WATCHLIST TOP 10"].iloc[0]["Status"] == "BLOCKED_BY_REGIME"
    assert sections["STUDY WATCHLIST TOP 10"].iloc[0]["Watchlist Score"] > 0
    assert sections["STUDY WATCHLIST TOP 10"].iloc[0]["Reason"]
    assert sections["STUDY WATCHLIST TOP 10"].iloc[0]["Risk Note"]
    assert sections["TOP RANKED"].iloc[0]["Symbol"] == "AAA"
    assert "DIAGNOSTICS" not in sections
    assert "ranked_signals" in str(result.metadata["missing_optional_columns"])
    main_text = "\n".join(
        str(cell)
        for _title, frame in result.sections
        for row in frame.fillna("").values.tolist()
        for cell in row
        if cell != ""
    )
    assert "ranked_signals:" not in main_text
    assert "Missing optional columns" not in main_text
    assert "factor rs increase_candidate" not in main_text


def test_top_ranked_section_keeps_ranked_names_without_pattern_setup() -> None:
    frame = build_top_ranked(
        [
            {
                "symbol_id": "RANKONLY",
                "rank": 1,
                "sector": "Banks",
                "sector_status": "Leading",
                "composite_score": 91,
                "close": 100,
                "sma_50": 95,
                "sma_200": 80,
                "stage2_label": "",
            }
        ],
        _context(),
    )

    assert frame.iloc[0]["Symbol"] == "RANKONLY"
    assert frame.iloc[0]["Composite Score"] == 91
