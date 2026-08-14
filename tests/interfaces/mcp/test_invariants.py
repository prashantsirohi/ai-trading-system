"""Tests for the MCP safety invariants themselves (I1, I2, I3).

If these pass but a tool test fails, the tool is wrong. If these fail, the
guarantees the whole interface rests on are gone.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pytest

from ai_trading_system.interfaces.mcp.context import (
    McpConfigurationError,
    McpContext,
    McpProfile,
    StoreBusyError,
    StoreUnavailableError,
)
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_UNSUPPORTED,
    FutureDataError,
    assert_not_future,
    clamp_limit,
    envelope,
    json_safe,
)

from .conftest import ConnectionGuard, ReadOnlyViolation


# ---------------------------------------------------------------------------
# I1 — no writable handle
# ---------------------------------------------------------------------------


def test_guard_rejects_writable_duckdb(data_root: Path) -> None:
    """A DuckDB open without read_only=True fails at the constructor."""

    with pytest.raises(ReadOnlyViolation, match="read_only=True"):
        duckdb.connect(str(data_root / "ohlcv.duckdb"))


def test_guard_rejects_writable_sqlite(data_root: Path) -> None:
    """A plain SQLite open fails; only a mode=ro URI is allowed."""

    with pytest.raises(ReadOnlyViolation, match="read-only URI"):
        sqlite3.connect(str(data_root / "masterdata.db"))


def test_guard_allows_memory_duckdb() -> None:
    """In-memory DuckDB stays available for Parquet scans."""

    conn = duckdb.connect(":memory:")
    try:
        assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        conn.close()


def test_context_connections_pass_the_guard(ctx: McpContext) -> None:
    """Every McpContext accessor opens a handle the guard accepts."""

    with ctx.ohlcv() as conn:
        assert conn.execute("SELECT COUNT(*) FROM _catalog").fetchone()[0] > 0
    with ctx.control_plane() as conn:
        assert conn.execute("SELECT COUNT(*) FROM rank_history").fetchone()[0] > 0
    with ctx.fundamentals() as conn:
        assert conn.execute("SELECT COUNT(*) FROM fundamental_scores").fetchone()[0] > 0
    with ctx.sqlite(ctx.master_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM symbols").fetchone()[0] > 0
    with ctx.sqlite(ctx.screener_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM screener_financials").fetchone()[0] > 0
    with ctx.parquet() as conn:
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_sqlite_handle_actually_rejects_writes(ctx: McpContext) -> None:
    """The mode=ro URI is enforced by SQLite, not just by our guard."""

    with ctx.sqlite(ctx.master_db) as conn:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("DELETE FROM symbols")


def test_adjusted_view_available_on_read_only_connection(ctx: McpContext) -> None:
    """The TEMP VIEW that supplies adjusted prices works read-only."""

    with ctx.ohlcv(adjusted_view=True) as conn:
        raw, adjusted = conn.execute(
            """
            SELECT
              (SELECT close FROM _catalog
                WHERE symbol_id = ? AND exchange = ?
                  AND CAST(timestamp AS DATE) = CAST(? AS DATE)),
              (SELECT close FROM _catalog_feature_source
                WHERE symbol_id = ? AND exchange = ?
                  AND CAST(timestamp AS DATE) = CAST(? AS DATE))
            """,
            ["AAA", "NSE", "2026-01-05", "AAA", "NSE", "2026-01-05"],
        ).fetchone()
    assert raw == 200.0
    assert adjusted == 100.0


def test_missing_store_raises_typed_error(ctx: McpContext, tmp_path: Path) -> None:
    with pytest.raises(StoreUnavailableError):
        with ctx.duckdb(tmp_path / "absent.duckdb"):
            pass


def test_locked_store_raises_a_typed_actionable_error(
    ctx: McpContext, connection_guard: ConnectionGuard
) -> None:
    """A running pipeline holds a writer lock; DuckDB then refuses read-only.

    Observed against the live store while a shadow pipeline was running. The
    tools must say so rather than crash with a raw IOException — and must not
    return an empty result, which would read as "no such data".
    """

    def raise_lock_error(*args: object, **kwargs: object):
        raise duckdb.IOException(
            'IO Error: Could not set lock on file "ohlcv.duckdb": '
            "Conflicting lock is held"
        )

    with connection_guard.paused():
        original = duckdb.connect
        duckdb.connect = raise_lock_error  # type: ignore[assignment]
        try:
            with pytest.raises(StoreBusyError, match="locked by another process"):
                with ctx.ohlcv():
                    pass
        finally:
            duckdb.connect = original  # type: ignore[assignment]


def test_non_lock_io_errors_are_not_masked(
    ctx: McpContext, connection_guard: ConnectionGuard
) -> None:
    """Only lock conflicts are translated; real IO faults still surface."""

    def raise_other_error(*args: object, **kwargs: object):
        raise duckdb.IOException("IO Error: disk is on fire")

    with connection_guard.paused():
        original = duckdb.connect
        duckdb.connect = raise_other_error  # type: ignore[assignment]
        try:
            with pytest.raises(duckdb.IOException, match="disk is on fire"):
                with ctx.ohlcv():
                    pass
        finally:
            duckdb.connect = original  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# I2 — as_of never returns future data
# ---------------------------------------------------------------------------


def test_assert_not_future_raises_on_leaked_row() -> None:
    rows = [{"trade_date": "2026-01-05"}, {"trade_date": "2026-08-11"}]
    with pytest.raises(FutureDataError, match="after as_of"):
        assert_not_future(rows, "2026-01-31", ["trade_date"])


def test_assert_not_future_allows_rows_at_the_cutoff() -> None:
    rows = [{"trade_date": "2026-01-31"}]
    assert_not_future(rows, "2026-01-31", ["trade_date"])


def test_assert_not_future_is_a_noop_without_as_of() -> None:
    rows = [{"trade_date": "2026-08-11"}]
    assert_not_future(rows, None, ["trade_date"])


def test_assert_not_future_ignores_missing_and_null_fields() -> None:
    rows = [{"other": 1}, {"trade_date": None}]
    assert_not_future(rows, "2026-01-31", ["trade_date"])


def test_envelope_enforces_the_cutoff() -> None:
    """A tool that forgets its WHERE clause cannot return a response."""

    with pytest.raises(FutureDataError):
        envelope(
            [{"trade_date": "2026-08-11"}],
            source="control_plane.duckdb:rank_history",
            as_of_status=AS_OF_EXACT,
            as_of_requested="2026-01-31",
            as_of_effective="2026-01-31",
            date_fields=["trade_date"],
        )


def test_envelope_rejects_an_unknown_status() -> None:
    with pytest.raises(ValueError, match="Unknown as_of_status"):
        envelope([], source="x:y", as_of_status="MAYBE")


def test_unsupported_surface_returns_no_rows() -> None:
    """AS_OF_UNSUPPORTED is an empty answer, never the present."""

    response = envelope(
        [],
        source="ohlcv.duckdb:sector_dashboard",
        as_of_status=AS_OF_UNSUPPORTED,
        as_of_requested="2026-01-31",
        notes=["sector overview is latest-only"],
    )
    assert response["data"] == []
    assert response["meta"]["as_of_status"] == AS_OF_UNSUPPORTED
    assert response["meta"]["as_of_effective"] is None
    assert response["meta"]["notes"]


# ---------------------------------------------------------------------------
# I3 — DATA_ROOT must be explicit for the operator profile
# ---------------------------------------------------------------------------


def test_operator_profile_requires_data_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATA_ROOT", raising=False)
    with pytest.raises(McpConfigurationError, match="DATA_ROOT is not set"):
        McpContext.from_env(McpProfile.OPERATOR)


def test_operator_profile_rejects_a_missing_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "not-mounted"))
    with pytest.raises(McpConfigurationError, match="does not exist"):
        McpContext.from_env(McpProfile.OPERATOR)


def test_operator_profile_rejects_a_repo_local_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The exact silent fallback require_data_root_available() misses.

    Built against a synthetic checkout rather than the real one, so the test
    never creates a second repo-local data tree (AGENTS.md).
    """

    repo = tmp_path / "checkout"
    (repo / "src" / "ai_trading_system").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("[project]\nname = 'fake'\n")
    repo_local = repo / "data"
    repo_local.mkdir()

    monkeypatch.setenv("DATA_ROOT", str(repo_local))
    with pytest.raises(McpConfigurationError, match="inside the repository"):
        McpContext.from_env(McpProfile.OPERATOR, project_root=repo)


def test_operator_profile_accepts_an_external_root(
    monkeypatch: pytest.MonkeyPatch, data_root: Path
) -> None:
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    context = McpContext.from_env(McpProfile.OPERATOR)
    assert context.profile is McpProfile.OPERATOR
    assert context.ohlcv_db == data_root / "ohlcv.duckdb"
    assert context.control_plane_db == data_root / "control_plane.duckdb"
    assert context.screener_db == data_root / "fundamentals" / "screener_financials.db"


def test_fixture_profile_allows_a_temporary_root(ctx: McpContext) -> None:
    assert ctx.profile is McpProfile.FIXTURE


# ---------------------------------------------------------------------------
# Envelope helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, 250), (0, 1), (-5, 1), (10, 10), (99999, 2000), ("abc", 250)],
)
def test_clamp_limit(value: object, expected: int) -> None:
    assert clamp_limit(value) == expected  # type: ignore[arg-type]


def test_json_safe_handles_store_scalars() -> None:
    import numpy as np
    import pandas as pd

    assert json_safe(np.int64(3)) == 3
    assert json_safe(np.float64(1.5)) == 1.5
    assert json_safe(float("nan")) is None
    assert json_safe(pd.NaT) is None
    assert json_safe(pd.Timestamp("2026-01-05")) == "2026-01-05T00:00:00"
    assert json_safe(np.bool_(True)) is True
    assert json_safe({"a": np.int64(1)}) == {"a": 1}
    assert json_safe([np.float64(2.0)]) == [2.0]


def test_exchange_normalization() -> None:
    assert McpContext.resolve_exchange(None) == "NSE"
    assert McpContext.resolve_exchange(" bse ") == "BSE"
    with pytest.raises(ValueError, match="Unsupported exchange"):
        McpContext.resolve_exchange("NYSE")


def test_store_label_names_the_file(ctx: McpContext) -> None:
    """Two stores share table names, so the file is part of the identifier."""

    assert ctx.store_label(ctx.fundamentals_db, "sector_earnings_leadership") == (
        "fundamentals.duckdb:sector_earnings_leadership"
    )
    assert ctx.store_label(ctx.ohlcv_db, "sector_earnings_leadership") == (
        "ohlcv.duckdb:sector_earnings_leadership"
    )


def test_guard_records_every_connection(
    ctx: McpContext, connection_guard: ConnectionGuard
) -> None:
    connection_guard.calls.clear()
    with ctx.ohlcv():
        pass
    with ctx.sqlite(ctx.master_db):
        pass
    drivers = {driver for driver, _, _ in connection_guard.calls}
    assert drivers == {"duckdb", "sqlite3"}
