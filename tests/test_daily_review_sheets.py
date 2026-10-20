from copy import deepcopy

import pandas as pd
import pytest

from ai_trading_system.domains.opportunities.daily_review import FIELDS, POLICY_VERSION
from ai_trading_system.domains.publish.channels import daily_review as channel


class Sheet:
    def __init__(self, values=None):
        self.values = values or []
        self.id, self.row_count, self.col_count = 11, 60, 50

    def get_all_values(self):
        return deepcopy(self.values)

    def resize(self, rows, cols):
        self.row_count, self.col_count = rows, cols

    def batch_clear(self, _ranges):
        import re

        for cell_range in _ranges:
            start, end = map(int, re.findall(r"\d+", cell_range))
            if end >= len(self.values):
                self.values = self.values[: start - 1]
            else:
                self.values[start - 1 : end] = [[] for _ in range(end - start + 1)]

    def clear(self):
        self.values = []


class Manager:
    def __init__(self):
        self.sheets = {}
        self.helpers = {}

    def open_spreadsheet(self):
        return True

    def list_worksheets(self):
        return list(self.sheets)

    def get_worksheet(self, name):
        return self.sheets.get(name)

    def get_or_create_sheet(self, name, **_kwargs):
        return self.sheets.setdefault(name, Sheet())

    def _execute_with_backoff(self, action, **_kwargs):
        return action()

    def write_hidden_data_sheet(self, name, frame, **_kwargs):
        self.helpers[name] = frame.copy()
        self.sheets[name] = Sheet(
            [list(frame.columns), *frame.fillna("").astype(str).values.tolist()]
        )
        return True

    def update_worksheet_values(self, sheet, values, range_name="A1", **_kwargs):
        import re

        start = int(re.search(r"\d+", range_name).group()) - 1
        while len(sheet.values) < start + len(values):
            sheet.values.append([])
        sheet.values[start : start + len(values)] = deepcopy(values)

    def hide_worksheet(self, name):
        return True

    def batch_update(self, body):
        self.formatting = body

    def quota_metadata(self):
        return {}


def frame(symbols):
    rows = []
    for i, symbol in enumerate(symbols, 1):
        row = dict.fromkeys(FIELDS, "")
        row.update(
            priority=i,
            symbol=symbol,
            exchange="NSE",
            sources_json='["RANK"]',
            attention_reasons_json='["DISCOVERY_REVIEW"]',
            blockers_json="[]",
            queue="INVESTIGATE",
            session="2026-10-19",
        )
        rows.append(row)
    return pd.DataFrame(rows, columns=FIELDS)


def publish(rows):
    return channel.publish_daily_review(
        rows,
        pd.DataFrame(columns=["change"]),
        {
            "status": "complete",
            "policy_version": POLICY_VERSION,
            "calendar": {"status": "PASS"},
        },
        run_id="test-run",
        run_date="2026-10-19",
    )


def test_full_list_no_cap_and_notes_follow_identity_through_absence(monkeypatch):
    manager = Manager()
    monkeypatch.setattr(channel, "GoogleSheetsManager", lambda: manager)
    publish(frame([f"S{i}" for i in range(80)]))
    sheet = manager.sheets[channel.SHEET]
    assert len(sheet.values) == 85
    assert len(manager.sheets["_DATA_DAILY_REVIEW"].values) == 81
    notes_column = channel.HEADERS.index("Operator Notes")
    sheet.values[5][notes_column] = "Keep my research"
    publish(frame(["S1"]))
    publish(frame(["S1", "S0"]))
    assert manager.sheets[channel.SHEET].values[6][notes_column] == "Keep my research"
    assert [r[1] for r in manager.sheets[channel.SHEET].values[5:]] == ["S1", "S0"]


def test_first_pilot_seeds_existing_final_review_annotations(monkeypatch):
    manager = Manager()
    old_row = [""] * len(channel.FINAL_REVIEW_PILOT_HEADERS)
    old_row[1:3] = ["AAA", "NSE"]
    old_row[channel.FINAL_REVIEW_PILOT_HEADERS.index("Operator Notes")] = (
        "Existing pilot research"
    )
    manager.sheets[channel.FINAL_REVIEW_PILOT_SHEET] = Sheet(
        [[], [], [], [], list(channel.FINAL_REVIEW_PILOT_HEADERS), old_row]
    )
    monkeypatch.setattr(channel, "GoogleSheetsManager", lambda: manager)
    publish(frame(["AAA"]))
    assert (
        manager.sheets[channel.SHEET].values[5][channel.HEADERS.index("Operator Notes")]
        == "Existing pilot research"
    )


def test_unknown_layout_fails_before_overwrite(monkeypatch):
    manager = Manager()
    original = [[], [], [], [], ["unexpected header"], ["do not overwrite"]]
    manager.sheets[channel.SHEET] = Sheet(original)
    monkeypatch.setattr(channel, "GoogleSheetsManager", lambda: manager)
    with pytest.raises(ValueError, match="headers"):
        publish(frame(["AAA"]))
    assert manager.sheets[channel.SHEET].values == original
    assert not manager.helpers


def test_oversized_evidence_is_losslessly_split_and_requests_are_bounded(monkeypatch):
    manager = Manager()
    monkeypatch.setattr(channel, "GoogleSheetsManager", lambda: manager)
    rows = frame([f"S{i}" for i in range(30)])
    evidence = "x" * 56000
    rows["details_json"] = evidence
    publish(rows)
    parts = manager.sheets["_DATA_DAILY_REVIEW_DETAILS"].values[1:]
    assert "".join(row[-1] for row in parts if row[1] == "S0") == evidence
    assert max(len(row[-1]) for row in parts) <= 20000
