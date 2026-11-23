"""``00_Decision``: one read-only page over the verified daily-review pack.

Presentation only. It reuses rows already materialized by ``daily_review`` and
the rank dashboard summary; it never re-ranks, scores or admits anything. Lane
agreement is a count of independent memberships, never a blended score. Row
order inside each section is the daily-review ``priority`` order.
"""

from __future__ import annotations

from urllib.parse import quote

import pandas as pd

from ai_trading_system.domains.opportunities.daily_review import POLICY, strings
from ai_trading_system.domains.publish.channels.google_sheets_manager import (
    GoogleSheetsManager,
)

SHEET = "00_Decision"
COLUMNS = 9
ENTRY_CAP = 10
INVESTIGATE_CAP = 10
MOVES_CAP = 12
PILOT_SHEET = "00_Daily_Review_Pilot"
# Reasons the daily-review policy itself treats as risk (see assign_queue).
RISK_REASONS = {*POLICY["risk_tracker_states"], "INVESTIGATOR_TRAP", "SMA20_BREAK", "FINAL_REVIEW_DEFER"}
LANES = (
    ("I", ("INVESTIGATOR", "INVESTIGATOR_ACTIVE")),
    ("F", ("FUNDAMENTAL",)),
    ("P", ("PATTERN",)),
)
FILL = {
    "GO": (0.80, 0.93, 0.80),
    "CAUTION": (1.0, 0.92, 0.65),
    "NO NEW BUYS": (0.96, 0.75, 0.75),
}


def _lanes(row) -> tuple[str, int]:
    sources = set(strings(row["sources_json"]))
    hit = [code for code, names in LANES if sources & set(names)]
    return "".join(hit) or "-", len(hit)


def _num(value, digits=1):
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return ""


def _pct(value):
    return _num(value * 100) if isinstance(value, (int, float)) else ""


def _link(exchange, symbol):
    tv = quote(f"{exchange}:{symbol}", safe=":.-_")
    return f'=HYPERLINK("https://www.tradingview.com/chart/?symbol={tv}","Chart")'


def verdict(payload: dict, summary: dict) -> tuple[str, list[str]]:
    """GO / CAUTION / NO NEW BUYS from fields the pipeline already publishes."""
    market = (payload or {}).get("summary", {}) or {}
    if market.get("new_buys_allowed") is False:
        return "NO NEW BUYS", ["new_buys_allowed is false"]
    reasons = []
    trust = market.get("data_trust_status")
    if trust != "trusted":
        reasons.append(f"data trust: {trust or 'unknown'}")
    if summary.get("status") != "complete":
        reasons.append(f"daily review: {summary.get('status', 'unknown')}")
    if not market:
        reasons.append("market summary unavailable")
    return ("CAUTION", reasons) if reasons else ("GO", ["market and data trust clear"])


def _section(grid, marks, title, headers, rows, empty, footer=None):
    grid.append([])
    marks["sections"].append(len(grid))
    grid.append([title])
    if not rows:
        grid.append([empty])
        return
    marks["headers"].append(len(grid))
    grid.append(headers)
    grid.extend(rows)
    if footer:
        grid.append([footer])


def build_decision_grid(
    rows: pd.DataFrame,
    changes: pd.DataFrame,
    summary: dict,
    payload: dict | None,
    evidence: pd.DataFrame | None,
    *,
    run_id: str,
    run_date: str,
):
    """Return (grid, marks, verdict). ``marks`` holds row indexes for formatting."""
    state, why = verdict(payload or {}, summary)
    market = (payload or {}).get("summary", {}) or {}
    marks = {"sections": [], "headers": [], "banner": 1}
    grid = [
        [f"00 Decision — {run_date} — SHADOW: presentation only, no trade permission"],
        [f"VERDICT: {state}", "; ".join(why)],
        [
            "Market", market.get("regime_phase_label") or market.get("market_regime") or "unknown",
            "Allowed exposure", market.get("allowed_exposure", ""),
            "Above 200DMA %", _pct(market.get("pct_above_200dma")),
            "Top sector", market.get("top_sector", ""),
        ],
        [
            "Trust", market.get("data_trust_status", "unknown"),
            "Daily review", summary.get("status", "unknown"),
            "Calendar", summary.get("calendar", {}).get("status", "UNKNOWN"),
            "Run", run_id,
        ],
    ]
    rows = rows.copy()
    rows["_p"] = pd.to_numeric(rows["priority"], errors="coerce")
    rows = rows.sort_values("_p", kind="stable")

    pos = []
    for r in rows[rows["queue"] == "POSITION_ATTENTION"].to_dict("records"):
        flags = [x for x in strings(r["attention_reasons_json"]) if x in RISK_REASONS]
        pos.append([
            r["symbol"], "RISK REVIEW" if flags else "NO FLAGS", ", ".join(flags),
            r["sma20_break_state"], r["tracker_status"], r["position_status"],
            "; ".join(strings(r["blockers_json"])), _link(r["exchange"], r["symbol"]),
        ])
    _section(
        grid, marks, "1. OPEN POSITIONS (paper ledger) — risk flags from the review, not exit signals",
        ["Symbol", "Status", "Risk flags", "SMA20 break", "Tracker", "Position", "Blockers"],
        pos, "No open paper positions.",
    )

    entry_all = rows[rows["queue"] == "ENTRY_REVIEW"]
    entry = []
    for r in entry_all.head(ENTRY_CAP).to_dict("records"):
        chips, count = _lanes(r)
        entry.append([
            r["symbol"], f"{chips} ({count})", r["readiness"], r["trigger"], r["invalidation"],
            _num(r["extension_pct"]), "; ".join(strings(r["blockers_json"])),
            _link(r["exchange"], r["symbol"]),
        ])
    n_entry = len(entry_all)
    _section(
        grid, marks,
        f"2. ENTRY REVIEW — {n_entry} listed (Final Review: {summary.get('final_review_status', 'unknown')})",
        ["Symbol", "Lanes (count)", "Readiness", "Trigger", "Invalidation", "Ext %", "Blockers / next check", "Chart"],
        entry, "Nothing in Entry review today — do not backfill from rank.",
        f"+{n_entry - ENTRY_CAP} more in {PILOT_SHEET}" if n_entry > ENTRY_CAP else None,
    )

    inv_all = rows[rows["queue"] == "INVESTIGATE"]
    inv = []
    for r in inv_all.head(INVESTIGATE_CAP).to_dict("records"):
        chips, count = _lanes(r)
        inv.append([
            r["symbol"], f"{chips} ({count})", _num(r["rank_score"]), _num(r["investigator_score"], 0),
            r["pattern_state"], r["stage"], "; ".join(strings(r["attention_reasons_json"])),
        ])
    _section(
        grid, marks,
        f"3. INVESTIGATE — first {min(INVESTIGATE_CAP, len(inv_all))} of {len(inv_all)} by review priority",
        ["Symbol", "Lanes (count)", "Rank score", "Investigator", "Pattern", "Stage", "Why listed"],
        inv, "Nothing to investigate.",
        f"+{len(inv_all) - INVESTIGATE_CAP} more in {PILOT_SHEET}" if len(inv_all) > INVESTIGATE_CAP else None,
    )

    moves = changes[
        (changes["previous_queue"] != changes["queue"])
        & (changes["queue"].isin(["ENTRY_REVIEW", "POSITION_ATTENTION"])
           | changes["previous_queue"].isin(["ENTRY_REVIEW", "POSITION_ATTENTION"]))
    ]
    move_rows = [
        [m["symbol"], m["change"], m["previous_queue"] or "-", m["queue"] or "-", m["reason"]]
        for m in moves.head(MOVES_CAP).to_dict("records")
    ]
    new_counts = changes[changes["change"] == "NEW"]["queue"].value_counts().to_dict()
    _section(
        grid, marks,
        f"4. WHAT CHANGED vs {summary.get('previous_session') or 'no prior session'} — "
        f"new listings: {', '.join(f'{k} {v}' for k, v in sorted(new_counts.items())) or 'none'}",
        ["Symbol", "Change", "From", "To", "Reason"],
        move_rows, "No names moved into or out of Entry review or Position attention.",
        f"+{len(moves) - MOVES_CAP} more in the daily-review changes helper" if len(moves) > MOVES_CAP else None,
    )

    lane_rows = []
    for code, names in LANES:
        counts = [
            int(rows[(rows["queue"] == q) & rows["sources_json"].map(lambda s: bool(set(strings(s)) & set(names)))].shape[0])
            for q in ("POSITION_ATTENTION", "ENTRY_REVIEW", "INVESTIGATE", "WATCH")
        ]
        lane_rows.append([{"I": "Investigator", "F": "Fundamental", "P": "Pattern"}[code], *counts])
    _section(
        grid, marks, "5. LANE COUNTS by queue (membership, not performance)",
        ["Lane", "Position", "Entry review", "Investigate", "Watch"], lane_rows, "",
    )

    ev_rows = []
    if evidence is not None and not evidence.empty:
        for e in evidence.sort_values(["queue", "horizon"]).to_dict("records"):
            ev_rows.append([
                e["queue"], f"{int(e['horizon'])}d", int(e["names"]), int(e["sessions"]),
                _num(e["median_excess"], 2), f"{_num(e['ci5'], 2)} to {_num(e['ci95'], 2)}", e["evidence"],
            ])
    _section(
        grid, marks,
        "6. OUTCOME EVIDENCE — median return minus liquid-universe median (pp), first-episode names",
        ["Queue", "Horizon", "Names", "Sessions", "Excess (pp)", "90% interval", "Evidence"],
        ev_rows,
        "Outcome evidence not computed yet (run ai_trading_system.research.daily_review_outcomes). "
        "Treat every queue as unvalidated.",
        "THIN = fewer than 30 names; intervals resample sessions. No queue is validated until its interval clears zero.",
    )
    return grid, marks, state


def _rgb(c):
    return {"red": c[0], "green": c[1], "blue": c[2]}


def format_requests(sheet_id: int, marks: dict, state: str, n_rows: int) -> list[dict]:
    def rng(r0, r1, c1=COLUMNS):
        return {"sheetId": sheet_id, "startRowIndex": r0, "endRowIndex": r1, "startColumnIndex": 0, "endColumnIndex": c1}

    def fmt(r0, r1, fill, bold=True, white=False, size=None):
        text = {"bold": bold, "foregroundColor": _rgb((1, 1, 1) if white else (0, 0, 0))}
        if size:
            text["fontSize"] = size
        return {"repeatCell": {
            "range": rng(r0, r1),
            "cell": {"userEnteredFormat": {"backgroundColor": _rgb(fill), "textFormat": text}},
            "fields": "userEnteredFormat(backgroundColor,textFormat)"}}

    requests = [
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": COLUMNS},
                                       "properties": {"pixelSize": 150}, "fields": "pixelSize"}},
        {"updateSheetProperties": {"properties": {"sheetId": sheet_id, "index": 0, "gridProperties": {"frozenRowCount": 2}},
                                   "fields": "index,gridProperties.frozenRowCount"}},
        fmt(0, 1, (0.10, 0.20, 0.30), white=True, size=12),
        fmt(1, 2, FILL[state], size=12),
    ]
    requests += [fmt(r, r + 1, (0.10, 0.20, 0.30), white=True) for r in marks["sections"]]
    requests += [fmt(r, r + 1, (0.88, 0.90, 0.93)) for r in marks["headers"]]
    return requests


def publish_decision(rows, changes, summary, payload, evidence, *, run_id: str, run_date: str):
    grid, marks, state = build_decision_grid(rows, changes, summary, payload, evidence, run_id=run_id, run_date=run_date)
    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError("Could not authenticate decision workbook")
    sheet = manager.get_or_create_sheet(SHEET, rows=max(80, len(grid) + 5), cols=COLUMNS)
    if sheet is None:
        raise RuntimeError("Could not open decision tab")
    if sheet.row_count < len(grid) + 5 or sheet.col_count < COLUMNS:
        manager._execute_with_backoff(
            lambda: sheet.resize(rows=max(sheet.row_count, len(grid) + 5), cols=max(sheet.col_count, COLUMNS)),
            is_write=True,
        )
    # The tab is fully generated and holds no operator input, so a clean rewrite is safe.
    manager._execute_with_backoff(lambda: sheet.clear(), is_write=True)
    padded = [list(r) + [""] * (COLUMNS - len(r)) for r in grid]
    manager.update_worksheet_values(sheet, padded, range_name=f"A1:I{len(padded)}")
    manager.batch_update({"requests": format_requests(int(sheet.id), marks, state, len(padded))})
    return {"report_id": "decision", "verdict": state, "rows_written": len(padded), **manager.quota_metadata()}
