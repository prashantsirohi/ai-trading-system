import json

import pandas as pd

from ai_trading_system.domains.publish.channels import decision


def _row(priority, symbol, queue, sources, reasons=(), **extra):
    base = {
        "priority": priority, "exchange": "NSE", "symbol": symbol, "queue": queue,
        "sources_json": json.dumps(sources), "attention_reasons_json": json.dumps(list(reasons)),
        "readiness": "", "trigger": "", "invalidation": "", "extension_pct": "", "rank_score": 50,
        "investigator_score": "", "pattern_state": "", "stage": "S2", "sma20_break_state": "NOT_MET",
        "tracker_status": "", "position_status": "NONE", "blockers_json": "[]",
    }
    return {**base, **extra}


def _frames():
    rows = pd.DataFrame([
        _row(2, "WATCH1", "WATCH", ["FUNDAMENTAL"]),
        _row(1, "POS1", "POSITION_ATTENTION", ["RANK", "POSITION"], ["OPEN_POSITION_REVIEW", "SMA20_BREAK"],
             position_status="CURRENT", sma20_break_state="MET"),
        _row(3, "POS2", "POSITION_ATTENTION", ["POSITION"], ["OPEN_POSITION_REVIEW"], position_status="CURRENT"),
        _row(5, "ENT2", "ENTRY_REVIEW", ["FUNDAMENTAL", "PATTERN", "INVESTIGATOR"], readiness="SETUP_REVIEW",
             trigger=10.0, invalidation=9.0, extension_pct=1.234),
        _row(4, "ENT1", "ENTRY_REVIEW", ["PATTERN"], readiness="SETUP_REVIEW"),
        *[_row(10 + i, f"INV{i}", "INVESTIGATE", ["RANK"]) for i in range(14)],
    ])
    changes = pd.DataFrame([
        {"exchange": "NSE", "symbol": "ENT1", "session": "d", "change": "NEW", "previous_queue": "", "queue": "ENTRY_REVIEW", "reason": "X"},
        {"exchange": "NSE", "symbol": "OLD", "session": "d", "change": "REMOVED", "previous_queue": "ENTRY_REVIEW", "queue": "WATCH", "reason": "Y"},
        {"exchange": "NSE", "symbol": "W", "session": "d", "change": "NEW", "previous_queue": "", "queue": "WATCH", "reason": "Z"},
    ])
    return rows, changes


def _flat(grid):
    return [str(c) for r in grid for c in r]


def test_verdict_rules():
    ok = {"summary": {"new_buys_allowed": True, "data_trust_status": "trusted"}}
    assert decision.verdict(ok, {"status": "complete"})[0] == "GO"
    assert decision.verdict(ok, {"status": "degraded"})[0] == "CAUTION"
    degraded = {"summary": {"new_buys_allowed": True, "data_trust_status": "degraded"}}
    assert decision.verdict(degraded, {"status": "complete"})[0] == "CAUTION"
    blocked = {"summary": {"new_buys_allowed": False, "data_trust_status": "trusted"}}
    assert decision.verdict(blocked, {"status": "complete"})[0] == "NO NEW BUYS"
    assert decision.verdict({}, {"status": "complete"})[0] == "CAUTION"  # missing market data never reads GO


def test_grid_sections_ordering_caps_and_flags():
    rows, changes = _frames()
    summary = {"status": "degraded", "final_review_status": "degraded", "previous_session": "2026-11-12",
               "calendar": {"status": "PASS"}}
    payload = {"summary": {"new_buys_allowed": True, "data_trust_status": "degraded", "allowed_exposure": 0.425,
                           "pct_above_200dma": 0.47, "top_sector": "Realty", "regime_phase_label": "Stage 4"}}
    grid, marks, state = decision.build_decision_grid(rows, changes, summary, payload, None, run_id="r1", run_date="2026-11-16")
    flat = _flat(grid)
    assert state == "CAUTION"
    # positions in priority order with risk flag only where the policy reason exists
    pos = [r for r in grid if r and r[0] in {"POS1", "POS2"}]
    assert [r[0] for r in pos] == ["POS1", "POS2"]
    assert pos[0][1] == "RISK REVIEW" and pos[1][1] == "NO FLAGS"
    # entry rows ordered by priority; lane chips are a count, not a score
    entry = [r for r in grid if r and r[0] in {"ENT1", "ENT2"} and len(r) > 2 and r[2] == "SETUP_REVIEW"]
    assert [r[0] for r in entry] == ["ENT1", "ENT2"]
    assert entry[1][1] == "IFP (3)" and entry[0][1] == "P (1)"
    # investigate capped at 10 with an overflow pointer
    assert sum(1 for r in grid if r and str(r[0]).startswith("INV")) == decision.INVESTIGATE_CAP
    assert any("+4 more in 00_Daily_Review_Pilot" in c for c in flat)
    # moves include entry in/out but not the unrelated watch listing
    assert any(r[:1] == ["OLD"] for r in grid) and not any(r[:1] == ["W"] for r in grid)
    assert any("Outcome evidence not computed" in c for c in flat)
    assert grid[0][0].endswith("no trade permission")
    assert len(marks["sections"]) == 6


def test_empty_entry_is_explicit_and_evidence_is_rendered():
    rows, changes = _frames()
    rows = rows[rows["queue"] != "ENTRY_REVIEW"]
    evidence = pd.DataFrame([{"queue": "ENTRY_REVIEW", "horizon": 5, "names": 17, "sessions": 7,
                              "median_excess": -1.98, "ci5": -3.65, "ci95": 1.51, "evidence": "THIN"}])
    grid, _, _ = decision.build_decision_grid(
        rows, changes, {"status": "complete", "calendar": {}}, {}, evidence, run_id="r", run_date="d"
    )
    flat = _flat(grid)
    assert any("Nothing in Entry review today" in c for c in flat)
    assert any(r[:2] == ["ENTRY_REVIEW", "5d"] and r[-1] == "THIN" for r in grid)


def test_format_requests_color_banner_and_freeze():
    _, marks, _ = decision.build_decision_grid(*_frames()[:1], _frames()[1], {"status": "complete", "calendar": {}}, {}, None,
                                               run_id="r", run_date="d")
    reqs = decision.format_requests(7, marks, "NO NEW BUYS", 40)
    banner = reqs[3]["repeatCell"]["cell"]["userEnteredFormat"]["backgroundColor"]
    assert banner["red"] > banner["green"]
    assert reqs[1]["updateSheetProperties"]["properties"]["index"] == 0
