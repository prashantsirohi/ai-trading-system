"""Sheets pilot over materialized daily decisions; no selection or sorting."""

from __future__ import annotations

import json
from urllib.parse import quote

import pandas as pd

from ai_trading_system.domains.opportunities.daily_review import strings
from ai_trading_system.domains.publish.channels.google_sheets import (
    FINAL_REVIEW_PILOT_HEADERS,
    FINAL_REVIEW_PILOT_SHEET,
    FINAL_REVIEW_STATE_SHEET,
    _FINAL_REVIEW_MANUAL_DEFAULTS,
    _FINAL_REVIEW_MANUAL_HEADERS,
)
from ai_trading_system.domains.publish.channels.google_sheets_manager import (
    GoogleSheetsManager,
)

SHEET = "00_Daily_Review_Pilot"
STATE_SHEET = "_DATA_DAILY_REVIEW_OPERATOR_STATE"
HEADERS = (
    "Priority",
    "Symbol",
    "Exchange",
    "Queue",
    "Change",
    "Sources",
    "Attention Reason",
    "Evidence Status",
    "Stage",
    "Rank Score",
    "Investigator",
    "Investigator Score",
    "Fundamental",
    "Thesis",
    "Pattern",
    "Final Review Selected",
    "Final Review Priority",
    "Readiness",
    "Trigger",
    "Invalidation",
    "Extension %",
    "Near High",
    "Above SMA20",
    "SMA20 Break",
    "Tracker",
    "Position Mode",
    "Position State",
    "Blockers / Next Check",
    "Session",
    "Run ID",
    "Screener",
    "TradingView",
    *_FINAL_REVIEW_MANUAL_HEADERS,
)
STATE_HEADERS = ("Exchange", "Symbol", *_FINAL_REVIEW_MANUAL_HEADERS)


def column_name(index: int) -> str:
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def literal(value):
    if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


def read_notes(values, headers, *, header_index=4):
    if not values:
        return {}
    if len(values) <= header_index or tuple(
        values[header_index][: len(headers)]
    ) != tuple(headers):
        raise ValueError(
            "Unexpected daily-review/operator-state headers; refusing to overwrite notes"
        )
    columns = {
        name: headers.index(name)
        for name in ("Exchange", "Symbol", *_FINAL_REVIEW_MANUAL_HEADERS)
    }
    notes = {}
    for values_row in values[header_index + 1 :]:
        padded = list(values_row) + [""] * len(headers)
        key = (
            padded[columns["Exchange"]].strip().upper(),
            padded[columns["Symbol"]].strip().upper(),
        )
        if all(key):
            if key in notes:
                raise ValueError(
                    "Duplicate operator-note identity; refusing ambiguous overwrite"
                )
            notes[key] = tuple(padded[columns[n]] for n in _FINAL_REVIEW_MANUAL_HEADERS)
    return notes


def helper_frames(rows, changes, summary):
    """Keep complete evidence while respecting Sheets' 50,000-character cells."""
    compact, parts = rows.copy(), []
    for index, row in rows.iterrows():
        for field in rows.columns:
            value = row[field]
            if not isinstance(value, str) or len(value) <= 20000:
                continue
            chunks = [value[i : i + 20000] for i in range(0, len(value), 20000)]
            compact.loc[index, field] = (
                f"See _DATA_DAILY_REVIEW_DETAILS: {field}, {len(chunks)} ordered parts"
            )
            for part, chunk in enumerate(chunks, 1):
                parts.append(
                    [
                        row["exchange"],
                        row["symbol"],
                        row["session"],
                        field,
                        part,
                        len(chunks),
                        chunk,
                    ]
                )
    return {
        "_DATA_DAILY_REVIEW": compact,
        "_DATA_DAILY_REVIEW_DETAILS": pd.DataFrame(
            parts,
            columns=[
                "Exchange",
                "Symbol",
                "Session",
                "Field",
                "Part",
                "Parts",
                "Value",
            ],
        ),
        "_DATA_DAILY_REVIEW_CHANGES": changes,
        "_DATA_DAILY_REVIEW_SUMMARY": pd.DataFrame(
            [
                {"Key": k, "Value": json.dumps(v, default=str)}
                for k, v in summary.items()
            ]
        ),
    }


def write_helper(manager, name, frame):
    """Bound request payloads, never the population or evidence content."""
    sheet = manager.get_or_create_sheet(
        name, rows=max(6, len(frame) + 5), cols=max(1, len(frame.columns))
    )
    if sheet is None:
        raise RuntimeError(f"Could not open {name}")
    if sheet.row_count < len(frame) + 1 or sheet.col_count < len(frame.columns):
        manager._execute_with_backoff(
            lambda: sheet.resize(
                rows=max(sheet.row_count, len(frame) + 5),
                cols=max(sheet.col_count, len(frame.columns)),
            ),
            is_write=True,
        )
    manager._execute_with_backoff(lambda: sheet.clear(), is_write=True)
    data = [list(frame.columns), *frame.fillna("").values.tolist()]
    offset, batch, size = 1, [], 0
    for row in data:
        safe = row  # Raw helper values preserve JSON fragments and literal notes.
        row_size = len(
            json.dumps(safe, default=str, ensure_ascii=False).encode("utf-8")
        )
        if batch and size + row_size > 750000:
            manager.update_worksheet_values(
                sheet, batch, range_name=f"A{offset}", raw=True
            )
            offset += len(batch)
            batch, size = [], 0
        batch.append(safe)
        size += row_size
    if batch:
        manager.update_worksheet_values(sheet, batch, range_name=f"A{offset}", raw=True)
    if not manager.hide_worksheet(name):
        raise RuntimeError(f"Could not hide {name}")


def publish_daily_review(
    rows: pd.DataFrame,
    changes: pd.DataFrame,
    summary: dict,
    *,
    run_id: str,
    run_date: str,
):
    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError("Could not authenticate daily-review workbook")
    names = manager.list_worksheets()
    sheet = manager.get_worksheet(SHEET) if SHEET in names else None
    existing = (
        manager._execute_with_backoff(lambda: sheet.get_all_values())
        if sheet is not None
        else []
    )
    notes = {}
    if STATE_SHEET in names:
        state = manager.get_worksheet(STATE_SHEET)
        notes.update(
            read_notes(
                manager._execute_with_backoff(lambda: state.get_all_values()),
                STATE_HEADERS,
                header_index=0,
            )
        )
    # Seed the first consolidated pilot with existing Final Review annotations.
    # Thereafter the daily pilot owns its notes; specialist-tab notes stay separate.
    if sheet is None and FINAL_REVIEW_PILOT_SHEET in names:
        if FINAL_REVIEW_STATE_SHEET in names:
            old_state = manager.get_worksheet(FINAL_REVIEW_STATE_SHEET)
            notes.update(
                read_notes(
                    manager._execute_with_backoff(lambda: old_state.get_all_values()),
                    STATE_HEADERS,
                    header_index=0,
                )
            )
        old = manager.get_worksheet(FINAL_REVIEW_PILOT_SHEET)
        notes.update(
            read_notes(
                manager._execute_with_backoff(lambda: old.get_all_values()),
                FINAL_REVIEW_PILOT_HEADERS,
            )
        )
    notes.update(read_notes(existing, HEADERS))
    active = []
    seen = set()
    for row in rows.to_dict("records"):
        key = (str(row["exchange"]).upper(), str(row["symbol"]).upper())
        if key in seen or key[0] not in {"NSE", "BSE"} or not key[1]:
            raise ValueError("Daily-review listing identity is invalid or duplicated")
        seen.add(key)
        saved = notes.get(key, _FINAL_REVIEW_MANUAL_DEFAULTS)
        notes[key] = saved
        values = [
            row["priority"],
            key[1],
            key[0],
            row["queue"],
            row["change"],
            ", ".join(strings(row["sources_json"])),
            "; ".join(strings(row["attention_reasons_json"])),
            row["evidence_status"],
            row["stage"],
            row["rank_score"],
            row["investigator_state"],
            row["investigator_score"],
            row["fundamental_state"],
            row["fundamental_thesis"],
            row["pattern_state"],
            row["final_review_selected"],
            row["final_review_priority"],
            row["readiness"],
            row["trigger"],
            row["invalidation"],
            row["extension_pct"],
            row["near_high_state"],
            row["above_sma20_state"],
            row["sma20_break_state"],
            row["tracker_status"],
            row["position_mode"],
            row["position_status"],
            "; ".join(strings(row["blockers_json"])),
            row["session"],
            run_id,
        ]
        values = [literal(v) for v in values]
        symbol = quote(key[1], safe=".-_")
        values += [
            f'=HYPERLINK("https://www.screener.in/company/{symbol}/consolidated/","Open")',
            f'=HYPERLINK("https://www.tradingview.com/chart/?symbol={quote(key[0] + ":" + key[1], safe=":.-_")}","Open")',
        ]
        active.append([*values, *(literal(v) for v in saved)])
    archive = pd.DataFrame(
        [[e, s, *v] for (e, s), v in sorted(notes.items())], columns=STATE_HEADERS
    )
    # Save all annotations before replacing generated rows, including inactive names.
    write_helper(manager, STATE_SHEET, archive)
    if sheet is None:
        sheet = manager.get_or_create_sheet(
            SHEET, rows=max(60, len(active) + 5), cols=len(HEADERS)
        )
    if sheet is None:
        raise RuntimeError("Could not open daily-review pilot")
    if sheet.row_count < len(active) + 5 or sheet.col_count < len(HEADERS):
        manager._execute_with_backoff(
            lambda: sheet.resize(
                rows=max(sheet.row_count, len(active) + 5),
                cols=max(sheet.col_count, len(HEADERS)),
            ),
            is_write=True,
        )
    grid = [
        [f"Daily Review Pilot — {run_date} — SHADOW"],
        [
            f"Run: {run_id} | Status: {summary['status'].upper()} | Listings: {len(active)} | Policy: {summary['policy_version']}"
        ],
        [
            f"Last successful refresh session: {run_date}. Positions: PAPER pipeline ledger only. Missing sources: {', '.join(summary.get('source_issues', {})) or 'none'}. Unverified prior positions: {summary.get('unverified_prior_positions', 0)}. Calendar: {summary.get('calendar', {}).get('status', 'UNKNOWN')}"
        ],
        [
            f"Comparison: {summary.get('comparison_status')} | No trade permission. Final Review keeps its original policy. Operator notes persist by exchange/symbol."
        ],
        list(HEADERS),
        *active,
    ]
    end = column_name(len(HEADERS))
    manager.update_worksheet_values(sheet, grid, range_name=f"A1:{end}{len(grid)}")
    if len(existing) > len(grid):
        manager._execute_with_backoff(
            lambda: sheet.batch_clear([f"A{len(grid) + 1}:{end}{len(existing)}"]),
            is_write=True,
        )
    requests = [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": int(sheet.id),
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": len(HEADERS),
                },
                "properties": {"pixelSize": 145},
                "fields": "pixelSize",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": int(sheet.id),
                    "dimension": "COLUMNS",
                    "startIndex": 5,
                    "endIndex": 7,
                },
                "properties": {"pixelSize": 280},
                "fields": "pixelSize",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": int(sheet.id),
                    "index": 0,
                    "gridProperties": {"frozenRowCount": 5, "frozenColumnCount": 3},
                },
                "fields": "index,gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }
        },
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": int(sheet.id),
                        "startRowIndex": 4,
                        "endRowIndex": max(6, len(grid)),
                        "endColumnIndex": len(HEADERS),
                    }
                }
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": int(sheet.id),
                    "startRowIndex": 4,
                    "endRowIndex": 5,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.10, "green": 0.20, "blue": 0.30},
                        "textFormat": {
                            "bold": True,
                            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                        },
                    }
                },
                "fields": "userEnteredFormat",
            }
        },
    ]
    manager.batch_update({"requests": requests})
    for name, frame in helper_frames(rows, changes, summary).items():
        write_helper(manager, name, frame)
    return {
        "report_id": "daily_review_pilot",
        "rows_written": len(active),
        "operator_state_rows": len(archive),
        **manager.quota_metadata(),
    }
