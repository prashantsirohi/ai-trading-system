from datetime import date, datetime
import sqlite3
from zoneinfo import ZoneInfo

import pytest

from ai_trading_system.interfaces.cli.resolve_shadow_session import (
    resolve_session,
    validate_run_session,
)


@pytest.fixture
def master(tmp_path):
    path = tmp_path / "masterdata.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE nse_holidays (date TEXT)")
        conn.executemany(
            "INSERT INTO nse_holidays VALUES (?)",
            [("2026-10-02",), ("2026-01-26",), ("2025-01-26",)],
        )
    return path


@pytest.mark.parametrize(
    "clock,expected",
    [
        ("2026-10-02T11:09:00", "2026-10-01"),
        ("2026-10-04T20:00:00", "2026-10-01"),
        ("2026-10-05T10:00:00", "2026-10-01"),
        ("2026-10-05T15:29:59", "2026-10-01"),
        ("2026-10-05T15:30:00", "2026-10-05"),
    ],
)
def test_expected_completed_session_ignores_price_availability(master, clock, expected):
    before = master.read_bytes()
    now = datetime.fromisoformat(clock).replace(tzinfo=ZoneInfo("Asia/Kolkata"))
    assert resolve_session(master, now=now) == date.fromisoformat(expected)
    assert master.read_bytes() == before


@pytest.mark.parametrize(
    "requested", ["2026-10-02", "2026-10-03", "2026-10-05", "2026-10-06"]
)
def test_explicit_invalid_or_incomplete_date_is_rejected(master, requested):
    now = datetime(2026, 10, 5, 10, tzinfo=ZoneInfo("Asia/Kolkata"))
    with pytest.raises(ValueError):
        resolve_session(master, requested=date.fromisoformat(requested), now=now)


def test_missing_year_or_database_never_assumes_weekdays(master, tmp_path):
    with pytest.raises(ValueError, match="no coverage for 2027"):
        resolve_session(master, now=datetime(2027, 6, 1, 20, tzinfo=ZoneInfo("UTC")))
    absent = tmp_path / "absent.db"
    with pytest.raises(sqlite3.OperationalError):
        resolve_session(absent)
    assert not absent.exists()


def test_clock_is_converted_to_ist(master):
    assert resolve_session(
        master, now=datetime(2026, 10, 5, 10, tzinfo=ZoneInfo("UTC"))
    ) == date(2026, 10, 5)


def test_explicit_historical_session_is_preserved(master):
    assert resolve_session(
        master,
        requested=date(2026, 9, 29),
        now=datetime(2026, 10, 2, 11, tzinfo=ZoneInfo("Asia/Kolkata")),
    ) == date(2026, 9, 29)


def test_pinned_run_cannot_be_resumed_under_holiday_fallback_date(tmp_path):
    import duckdb

    path = tmp_path / "control_plane.duckdb"
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE pipeline_run (run_id VARCHAR, run_date DATE)")
        conn.execute(
            "INSERT INTO pipeline_run VALUES (?, ?)", ["holiday-run", "2026-10-02"]
        )
    before = path.read_bytes()
    with pytest.raises(ValueError, match="belongs to"):
        validate_run_session(path, "holiday-run", date(2026, 10, 1))
    validate_run_session(path, "new-run", date(2026, 10, 1))
    validate_run_session(path, "holiday-run", date(2026, 10, 2))
    assert path.read_bytes() == before
