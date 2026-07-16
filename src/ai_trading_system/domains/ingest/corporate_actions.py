"""NSE split/bonus corporate-action sync and adjusted OHLC normalization."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable

import duckdb
import pandas as pd
import requests

from ai_trading_system.domains.ingest.symbol_master import SymbolMaster
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.platform.logging.logger import logger

MODULE_NAME = "corporate_action_normalizer"
SOURCE_NAME = "nse_corporate_actions"
NSE_CORP_ACTIONS_URL = "https://www.nseindia.com/api/corporates-corporateActions"
NSE_WARMUP_URL = "https://www.nseindia.com/companies-listing/corporate-filings-corporate-actions"
ProgressCallback = Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class ParsedCorporateAction:
    symbol: str
    isin: str | None
    ex_date: date
    action_type: str
    parsed_ratio: str
    price_factor: float
    share_factor: float
    source: str
    raw_subject: str
    raw_payload_hash: str
    raw_payload_json: str


def ensure_corporate_action_schema(db_path: str | Path) -> None:
    """Ensure adjusted-price columns and module tables exist."""
    from ai_trading_system.domains.ingest.repository import initialize_ingest_duckdb

    if not Path(str(db_path)).exists():
        initialize_ingest_duckdb(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        table_exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '_catalog'
            """
        ).fetchone()[0]
        if not table_exists:
            conn.close()
            initialize_ingest_duckdb(db_path)
            conn = duckdb.connect(str(db_path))
        _ensure_catalog_adjustment_columns(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _corporate_actions (
                symbol VARCHAR NOT NULL,
                isin VARCHAR,
                ex_date DATE NOT NULL,
                action_type VARCHAR NOT NULL,
                parsed_ratio VARCHAR NOT NULL,
                price_factor DOUBLE NOT NULL,
                share_factor DOUBLE NOT NULL,
                source VARCHAR NOT NULL,
                raw_subject VARCHAR,
                raw_payload_hash VARCHAR NOT NULL,
                raw_payload_json VARCHAR,
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        _ensure_corporate_action_columns(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _corporate_action_sync_state (
                module_name VARCHAR PRIMARY KEY,
                source VARCHAR NOT NULL,
                last_successful_fetch_from_date DATE,
                last_successful_fetch_to_date DATE,
                last_successful_normalized_at TIMESTAMP,
                last_action_set_hash VARCHAR,
                normalizer_version BIGINT NOT NULL DEFAULT 1,
                overlap_days INTEGER NOT NULL DEFAULT 45,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata_json VARCHAR
            )
            """
        )
        conn.execute("DROP INDEX IF EXISTS idx_corporate_actions_identity")
        _backfill_corporate_action_keys(conn)
        try:
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_corporate_actions_action_key
                ON _corporate_actions (action_key)
                """
            )
        except duckdb.Error as exc:
            logger.warning("Could not create corporate-action stable-key index: %s", exc)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _module_execution_log (
                execution_id VARCHAR PRIMARY KEY,
                module_name VARCHAR NOT NULL,
                run_id VARCHAR,
                execution_mode VARCHAR,
                status VARCHAR NOT NULL,
                started_at TIMESTAMP NOT NULL,
                ended_at TIMESTAMP,
                last_success_at TIMESTAMP,
                actions_fetched BIGINT DEFAULT 0,
                actions_inserted BIGINT DEFAULT 0,
                rows_adjusted BIGINT DEFAULT 0,
                symbols_adjusted BIGINT DEFAULT 0,
                error_message VARCHAR,
                metadata_json VARCHAR
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _ensure_corporate_action_columns(conn: duckdb.DuckDBPyConnection) -> None:
    existing = {
        row[0]
        for row in conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '_corporate_actions'
            """
        ).fetchall()
    }
    additions = {
        "action_key": "VARCHAR",
        "status": "VARCHAR DEFAULT 'active'",
        "changed_at": "TIMESTAMP",
        "deactivated_at": "TIMESTAMP",
        "normalizer_version": "BIGINT DEFAULT 1",
    }
    for column, dtype in additions.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE _corporate_actions ADD COLUMN {column} {dtype}")


def _backfill_corporate_action_keys(conn: duckdb.DuckDBPyConnection) -> None:
    rows = conn.execute(
        """
        SELECT rowid, symbol, isin, ex_date, action_type, source, last_seen_at, first_seen_at,
               action_key, status
        FROM _corporate_actions
        ORDER BY COALESCE(last_seen_at, first_seen_at) DESC NULLS LAST, rowid DESC
        """
    ).fetchall()
    claimed: set[str] = set()
    for rowid, symbol, isin, ex_date, action_type, source, _last_seen, _first_seen, current_key, status in rows:
        action_key = make_corporate_action_key(
            {
                "symbol": symbol,
                "isin": isin,
                "ex_date": ex_date,
                "action_type": action_type,
                "source": source,
            }
        )
        if action_key in claimed:
            if current_key is not None or status != "inactive":
                conn.execute(
                    """
                    UPDATE _corporate_actions
                    SET action_key = NULL,
                        status = 'inactive',
                        deactivated_at = COALESCE(deactivated_at, CURRENT_TIMESTAMP)
                    WHERE rowid = ?
                    """,
                    [rowid],
                )
            continue
        claimed.add(action_key)
        if current_key != action_key or status is None:
            conn.execute(
                """
                UPDATE _corporate_actions
                SET action_key = ?,
                    status = COALESCE(status, 'active'),
                    normalizer_version = COALESCE(normalizer_version, 1)
                WHERE rowid = ?
                """,
                [action_key, rowid],
            )


def _ensure_catalog_adjustment_columns(conn: duckdb.DuckDBPyConnection) -> None:
    existing = {
        row[0]
        for row in conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '_catalog'
            """
        ).fetchall()
    }
    additions = {
        "adjusted_open": "DOUBLE",
        "adjusted_high": "DOUBLE",
        "adjusted_low": "DOUBLE",
        "adjusted_close": "DOUBLE",
        "adjustment_factor": "DOUBLE",
        "adjustment_source": "VARCHAR",
        "adjusted_at": "TIMESTAMP",
        "adjustment_version": "BIGINT",
    }
    for column, dtype in additions.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE _catalog ADD COLUMN {column} {dtype}")


def parse_corporate_action(raw: dict[str, Any], *, symbol_master: SymbolMaster | None = None) -> ParsedCorporateAction | None:
    """Parse one NSE corporate-action payload into split/bonus factors."""
    subject = str(raw.get("subject") or "").strip()
    subject_lower = subject.lower()
    symbol = str(raw.get("symbol") or "").strip().upper()
    isin = _normalize_isin(raw.get("isin"))
    if symbol_master is not None and symbol:
        symbol = symbol_master.canonicalize(symbol)
        if not isin:
            isin = _normalize_isin(symbol_master.isin_for(symbol))
    if not symbol:
        return None

    ex_date = _parse_nse_date(raw.get("exDate") or raw.get("ex_date"))
    if ex_date is None:
        return None

    action_type: str
    parsed_ratio: str
    price_factor: float
    share_factor: float
    has_bonus = "bonus" in subject_lower
    has_split = _is_split_subject(subject_lower)
    if has_bonus:
        ratio = _parse_ratio(subject)
        if ratio is None:
            return None
        x, y = ratio
        if y <= 0 or x < 0:
            return None
        action_type = "bonus"
        parsed_ratio = f"{x:g}:{y:g}"
        price_factor = y / (x + y)
        share_factor = (x + y) / y
        if has_split:
            face_values = _parse_split_face_values(subject)
            if face_values is None:
                return None
            old_fv, new_fv = face_values
            if old_fv <= 0 or new_fv <= 0:
                return None
            action_type = "bonus_split"
            parsed_ratio = f"{parsed_ratio};{old_fv:g}->{new_fv:g}"
            price_factor *= new_fv / old_fv
            share_factor *= old_fv / new_fv
    elif has_split:
        face_values = _parse_split_face_values(subject)
        if face_values is None:
            return None
        old_fv, new_fv = face_values
        if old_fv <= 0 or new_fv <= 0:
            return None
        action_type = "split"
        parsed_ratio = f"{old_fv:g}->{new_fv:g}"
        price_factor = new_fv / old_fv
        share_factor = old_fv / new_fv
    else:
        return None

    raw_payload_json = json.dumps(raw, sort_keys=True, default=str)
    raw_payload_hash = hashlib.sha256(raw_payload_json.encode("utf-8")).hexdigest()
    return ParsedCorporateAction(
        symbol=symbol,
        isin=isin or None,
        ex_date=ex_date,
        action_type=action_type,
        parsed_ratio=parsed_ratio,
        price_factor=float(price_factor),
        share_factor=float(share_factor),
        source=SOURCE_NAME,
        raw_subject=subject,
        raw_payload_hash=raw_payload_hash,
        raw_payload_json=raw_payload_json,
    )


def _is_split_subject(subject_lower: str) -> bool:
    return any(term in subject_lower for term in ("split", "sub-division", "sub division", "subdivision"))


def _parse_ratio(text: str) -> tuple[float, float] | None:
    match = re.search(r"(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)", text)
    if not match:
        return None
    return float(match.group(1)), float(match.group(2))


def _parse_split_face_values(text: str) -> tuple[float, float] | None:
    normalized = text.lower().replace("₹", "rs")
    match = re.search(
        r"from\s+(?:rs\.?|re\.?)?\s*(\d+(?:\.\d+)?)\s*(?:/-)?[^0-9]*?\bto\s+(?:rs\.?|re\.?)?\s*(\d+(?:\.\d+)?)",
        normalized,
    )
    if match:
        return float(match.group(1)), float(match.group(2))
    return None


def _parse_nse_date(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_isin(value: object) -> str:
    return str(value or "").strip().upper()


def make_corporate_action_key(action: ParsedCorporateAction | dict[str, Any]) -> str:
    """Build the stable identity used for corrected NSE action payloads."""
    if isinstance(action, ParsedCorporateAction):
        values = action.__dict__
    else:
        values = action
    identity = _normalize_isin(values.get("isin")) or str(values.get("symbol") or "").strip().upper()
    ex_date = values.get("ex_date")
    if isinstance(ex_date, (date, datetime)):
        ex_date = ex_date.date().isoformat() if isinstance(ex_date, datetime) else ex_date.isoformat()
    raw = "|".join(
        [
            str(values.get("source") or SOURCE_NAME),
            identity,
            str(ex_date or ""),
            str(values.get("action_type") or ""),
        ]
    ).upper()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def fetch_nse_corporate_actions(
    *,
    start_date: date,
    end_date: date,
    timeout_sec: float = 20.0,
    progress: ProgressCallback | None = None,
) -> list[dict[str, Any]]:
    """Fetch NSE corporate actions in yearly API windows."""
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "Mozilla/5.0",
            "Referer": NSE_WARMUP_URL,
            "Origin": "https://www.nseindia.com",
        }
    )
    session.get("https://www.nseindia.com/", timeout=timeout_sec)
    session.get(NSE_WARMUP_URL, timeout=timeout_sec)

    rows: list[dict[str, Any]] = []
    years = list(range(start_date.year, end_date.year + 1))
    _report(
        progress,
        "years_start",
        total=len(years),
        description=f"Fetching NSE actions {start_date.year}-{end_date.year}",
    )
    for year in years:
        window_start = max(start_date, date(year, 1, 1))
        window_end = min(end_date, date(year, 12, 31))
        _report(
            progress,
            "year_start",
            year=year,
            from_date=window_start.isoformat(),
            to_date=window_end.isoformat(),
        )
        params = {
            "index": "equities",
            "from_date": window_start.strftime("%d-%m-%Y"),
            "to_date": window_end.strftime("%d-%m-%Y"),
        }
        response = session.get(NSE_CORP_ACTIONS_URL, params=params, timeout=timeout_sec)
        response.raise_for_status()
        payload = response.json()
        fetched = len(payload) if isinstance(payload, list) else 0
        if isinstance(payload, list):
            rows.extend(item for item in payload if isinstance(item, dict))
        _report(progress, "year_done", year=year, fetched=fetched, total_fetched=len(rows))
    _report(progress, "years_done", total_fetched=len(rows))
    return rows


def run_corporate_action_normalization(
    *,
    ohlcv_db_path: str | Path,
    masterdb_path: str | Path | None,
    run_id: str | None = None,
    force: bool = False,
    max_age_days: int = 30,
    overlap_days: int = 45,
    normalizer_version: int = 1,
    recompute_symbols: list[str] | None = None,
    today: date | None = None,
    fetcher: Callable[..., list[dict[str, Any]]] | None = None,
    show_progress: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Sync NSE actions and recompute adjusted OHLC from raw prices."""
    started_at = _utc_now()
    today = today or date.today()
    progress = _ProgressMux(show_progress=show_progress, callback=progress_callback)
    result: dict[str, Any] = {
        "module_name": MODULE_NAME,
        "status": "started",
        "execution_mode": None,
        "actions_fetched": 0,
        "actions_parsed": 0,
        "actions_seen": 0,
        "actions_inserted": 0,
        "actions_changed": 0,
        "actions_unchanged": 0,
        "actions_deactivated": 0,
        "affected_symbols": [],
        "affected_symbols_count": 0,
        "rows_adjusted": 0,
        "symbols_adjusted": 0,
        "raw_ohlc_unchanged": 1,
        "recompute_scope": "skipped",
        "skipped": False,
        "skip_reason": None,
        "overlap_days": int(overlap_days),
        "normalizer_version": int(normalizer_version),
        "error_message": None,
        "ingest_refreshed_symbols_count": len(
            {str(symbol).strip() for symbol in recompute_symbols or [] if str(symbol).strip()}
        ),
        "ingest_recompute_symbols_count": 0,
    }

    try:
        _report(progress, "step_start", step="Preparing schema", total=9)
        ensure_corporate_action_schema(ohlcv_db_path)
        _report(progress, "step_done", step="Preparing schema")
        state = load_corporate_action_sync_state(ohlcv_db_path)
        execution_mode = "full" if force or state is None else "incremental"
        result["execution_mode"] = execution_mode
        previous_hash = state.get("last_action_set_hash") if state else None
        previous_version = int(state.get("normalizer_version") or 1) if state else None
        result["previous_action_set_hash"] = previous_hash
        fetch_to = today
        fetch_from = (
            date(2000, 1, 1)
            if execution_mode == "full"
            else pd.Timestamp(state["last_successful_fetch_to_date"]).date() - timedelta(days=int(overlap_days))
        )
        result["fetch_from_date"] = fetch_from.isoformat()
        result["fetch_to_date"] = fetch_to.isoformat()
        fetcher = fetcher or fetch_nse_corporate_actions
        _report(progress, "step_start", step=f"Fetching NSE actions {fetch_from.year}-{fetch_to.year}")
        raw_actions = fetcher(start_date=fetch_from, end_date=fetch_to, progress=progress)
        result["actions_fetched"] = len(raw_actions)
        _report(progress, "step_done", step=f"Fetching NSE actions {fetch_from.year}-{fetch_to.year}")

        _report(progress, "step_start", step="Parsing split/bonus actions")
        symbol_master = SymbolMaster.from_masterdb(masterdb_path)
        parsed = [
            action
            for action in (parse_corporate_action(raw, symbol_master=symbol_master) for raw in raw_actions)
            if action is not None
        ]
        result["actions_parsed"] = len(parsed)
        _report(progress, "step_done", step="Parsing split/bonus actions", parsed_actions=len(parsed))

        _report(progress, "step_start", step="Saving corporate actions")
        conn = duckdb.connect(str(ohlcv_db_path))
        try:
            conn.execute("BEGIN TRANSACTION")
            reconcile_result = reconcile_corporate_actions(
                ohlcv_db_path,
                parsed,
                fetch_from=fetch_from,
                fetch_to=fetch_to,
                full_reconcile=execution_mode == "full",
                normalizer_version=normalizer_version,
                _conn=conn,
            )
            result.update(reconcile_result)
            result["affected_symbols_count"] = len(result["affected_symbols"])
            _report(progress, "step_done", step="Saving corporate actions", **reconcile_result)

            ingest_refreshed_symbols = sorted(
                {str(symbol).strip() for symbol in recompute_symbols or [] if str(symbol).strip()}
            )
            ingest_recompute_symbols = _symbols_with_active_actions(
                conn,
                ingest_refreshed_symbols,
            )
            result["ingest_recompute_symbols_count"] = len(ingest_recompute_symbols)
            skip_recompute = not ingest_recompute_symbols and should_skip_adjustment_recompute(
                force=force,
                reconcile_result=reconcile_result,
                previous_action_set_hash=previous_hash,
                normalizer_version=normalizer_version,
                previous_normalizer_version=previous_version,
            )
            if skip_recompute:
                result.update(
                    {
                        "rows_adjusted": 0,
                        "symbols_adjusted": 0,
                        "raw_ohlc_unchanged": 1,
                        "recompute_scope": "skipped",
                        "skipped": True,
                        "skip_reason": "no_action_state_change",
                    }
                )
                _report(progress, "message", message="Skipped adjusted-price rewrite: no action state change")
            else:
                full_recompute = execution_mode == "full" or previous_version != int(normalizer_version)
                scoped_symbols = sorted(
                    set(reconcile_result["affected_symbols"]) | set(ingest_recompute_symbols)
                )
                result.update(
                    recompute_adjusted_prices(
                        ohlcv_db_path,
                        symbols=None if full_recompute else scoped_symbols,
                        force=full_recompute,
                        progress=progress,
                        _conn=conn,
                    )
                )
                result["affected_symbols_count"] = len(result.get("affected_symbols") or [])
            normalized_at = _utc_now()
            update_corporate_action_sync_state(
                ohlcv_db_path,
                fetch_from=fetch_from,
                fetch_to=fetch_to,
                action_set_hash=reconcile_result["action_set_hash"],
                normalizer_version=normalizer_version,
                overlap_days=overlap_days,
                normalized_at=normalized_at,
                metadata=result,
                _conn=conn,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        result["status"] = "success"
        result["last_success_at"] = _utc_now().isoformat(sep=" ")
        _report(progress, "step_start", step="Recording execution log")
        _record_execution(ohlcv_db_path, run_id=run_id, started_at=started_at, result=result)
        _report(progress, "step_done", step="Recording execution log")
        return result
    except Exception as exc:
        result["status"] = "failed"
        result["error_message"] = str(exc)
        logger.warning("corporate action normalization failed: %s", exc)
        _report(progress, "message", message=f"Failed: {exc}")
        _record_execution(ohlcv_db_path, run_id=run_id, started_at=started_at, result=result)
        return result
    finally:
        progress.close()


def _symbols_with_active_actions(
    conn: duckdb.DuckDBPyConnection,
    symbols: list[str],
) -> list[str]:
    """Return refreshed catalog symbols whose adjusted history can differ from raw."""
    scoped_symbols = sorted({str(symbol).strip() for symbol in symbols if str(symbol).strip()})
    if not scoped_symbols:
        return []
    rows = conn.execute(
        """
        SELECT DISTINCT c.symbol_id
        FROM _catalog c
        WHERE c.symbol_id IN (SELECT UNNEST(?))
          AND EXISTS (
              SELECT 1
              FROM _corporate_actions a
              WHERE COALESCE(a.status, 'active') = 'active'
                AND a.price_factor > 0
                AND (
                     UPPER(a.symbol) = UPPER(c.symbol_id)
                  OR (
                       COALESCE(TRIM(a.isin), '') <> ''
                   AND UPPER(a.isin) = UPPER(COALESCE(c.isin, ''))
                  )
                )
          )
        ORDER BY c.symbol_id
        """,
        [scoped_symbols],
    ).fetchall()
    return [str(row[0]) for row in rows]


def _last_successful_execution_at(db_path: str | Path, *, execution_mode: str) -> datetime | None:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT MAX(COALESCE(last_success_at, ended_at))
            FROM _module_execution_log
            WHERE module_name = ?
              AND execution_mode = ?
              AND status = 'success'
            """,
            [MODULE_NAME, execution_mode],
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def load_corporate_action_sync_state(db_path: str | Path) -> dict[str, Any] | None:
    ensure_corporate_action_schema(db_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT module_name, source, last_successful_fetch_from_date,
                   last_successful_fetch_to_date, last_successful_normalized_at,
                   last_action_set_hash, normalizer_version, overlap_days,
                   updated_at, metadata_json
            FROM _corporate_action_sync_state
            WHERE module_name = ?
            """,
            [MODULE_NAME],
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    keys = [
        "module_name",
        "source",
        "last_successful_fetch_from_date",
        "last_successful_fetch_to_date",
        "last_successful_normalized_at",
        "last_action_set_hash",
        "normalizer_version",
        "overlap_days",
        "updated_at",
        "metadata_json",
    ]
    return dict(zip(keys, row))


def update_corporate_action_sync_state(
    db_path: str | Path,
    *,
    fetch_from: date,
    fetch_to: date,
    action_set_hash: str,
    normalizer_version: int,
    overlap_days: int,
    normalized_at: datetime,
    metadata: dict[str, Any] | None = None,
    _conn: duckdb.DuckDBPyConnection | None = None,
) -> None:
    conn = _conn or duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO _corporate_action_sync_state
            (module_name, source, last_successful_fetch_from_date, last_successful_fetch_to_date,
             last_successful_normalized_at, last_action_set_hash, normalizer_version,
             overlap_days, updated_at, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, now(), ?)
            ON CONFLICT (module_name) DO UPDATE SET
                source = EXCLUDED.source,
                last_successful_fetch_from_date = EXCLUDED.last_successful_fetch_from_date,
                last_successful_fetch_to_date = EXCLUDED.last_successful_fetch_to_date,
                last_successful_normalized_at = EXCLUDED.last_successful_normalized_at,
                last_action_set_hash = EXCLUDED.last_action_set_hash,
                normalizer_version = EXCLUDED.normalizer_version,
                overlap_days = EXCLUDED.overlap_days,
                updated_at = now(),
                metadata_json = EXCLUDED.metadata_json
            """,
            [
                MODULE_NAME,
                SOURCE_NAME,
                fetch_from,
                fetch_to,
                normalized_at,
                action_set_hash,
                int(normalizer_version),
                int(overlap_days),
                json.dumps(metadata or {}, sort_keys=True, default=str),
            ],
        )
        if _conn is None:
            conn.commit()
    finally:
        if _conn is None:
            conn.close()


def compute_active_action_set_hash(
    db_path: str | Path,
    *,
    _conn: duckdb.DuckDBPyConnection | None = None,
) -> str:
    conn = _conn or duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT symbol, isin, ex_date, action_type, parsed_ratio, price_factor,
                   share_factor, raw_payload_hash, normalizer_version
            FROM _corporate_actions
            WHERE COALESCE(status, 'active') = 'active'
            ORDER BY symbol, isin, ex_date, action_type, parsed_ratio, price_factor,
                     share_factor, raw_payload_hash, normalizer_version
            """
        ).fetchall()
    finally:
        if _conn is None:
            conn.close()
    encoded = json.dumps(rows, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def reconcile_corporate_actions(
    db_path: str | Path,
    actions: Iterable[ParsedCorporateAction],
    *,
    fetch_from: date,
    fetch_to: date,
    full_reconcile: bool = False,
    normalizer_version: int = 1,
    _deactivate_missing: bool = True,
    _conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, Any]:
    """Upsert mutable action state and deactivate records absent from the fetched window."""
    if _conn is None:
        ensure_corporate_action_schema(db_path)
    conn = _conn or duckdb.connect(str(db_path))
    counters = {"actions_inserted": 0, "actions_changed": 0, "actions_unchanged": 0}
    affected_symbols: set[str] = set()
    seen_keys: set[str] = set()
    deduped = {make_corporate_action_key(action): action for action in actions}
    try:
        for action_key, action in deduped.items():
            seen_keys.add(action_key)
            existing = conn.execute(
                """
                SELECT parsed_ratio, price_factor, share_factor, raw_payload_hash,
                       raw_subject, raw_payload_json, normalizer_version, status, symbol
                FROM _corporate_actions
                WHERE action_key = ?
                """,
                [action_key],
            ).fetchone()
            values = [
                action.symbol,
                action.isin,
                action.ex_date,
                action.action_type,
                action.parsed_ratio,
                action.price_factor,
                action.share_factor,
                action.source,
                action.raw_subject,
                action.raw_payload_hash,
                action.raw_payload_json,
                action_key,
                int(normalizer_version),
            ]
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO _corporate_actions
                    (symbol, isin, ex_date, action_type, parsed_ratio, price_factor, share_factor,
                     source, raw_subject, raw_payload_hash, raw_payload_json, action_key,
                     status, changed_at, normalizer_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', CURRENT_TIMESTAMP, ?)
                    """,
                    values,
                )
                counters["actions_inserted"] += 1
                affected_symbols.add(action.symbol)
                continue
            comparable = (
                action.parsed_ratio,
                action.price_factor,
                action.share_factor,
                action.raw_payload_hash,
                action.raw_subject,
                action.raw_payload_json,
                int(normalizer_version),
                "active",
            )
            if tuple(existing[:8]) != comparable:
                conn.execute(
                    """
                    UPDATE _corporate_actions
                    SET symbol = ?, isin = ?, ex_date = ?, action_type = ?, parsed_ratio = ?,
                        price_factor = ?, share_factor = ?, source = ?, raw_subject = ?,
                        raw_payload_hash = ?, raw_payload_json = ?,
                        status = 'active', changed_at = CURRENT_TIMESTAMP, deactivated_at = NULL,
                        normalizer_version = ?, last_seen_at = CURRENT_TIMESTAMP
                    WHERE action_key = ?
                    """,
                    [*values[:11], int(normalizer_version), action_key],
                )
                counters["actions_changed"] += 1
                affected_symbols.update([str(existing[8]), action.symbol])
            else:
                conn.execute(
                    "UPDATE _corporate_actions SET last_seen_at = CURRENT_TIMESTAMP WHERE action_key = ?",
                    [action_key],
                )
                counters["actions_unchanged"] += 1

        if _deactivate_missing:
            if full_reconcile:
                candidates = conn.execute(
                    """
                    SELECT action_key, symbol
                    FROM _corporate_actions
                    WHERE COALESCE(status, 'active') = 'active'
                    """
                ).fetchall()
            else:
                candidates = conn.execute(
                    """
                    SELECT action_key, symbol
                    FROM _corporate_actions
                    WHERE COALESCE(status, 'active') = 'active'
                      AND ex_date BETWEEN ? AND ?
                    """,
                    [fetch_from, fetch_to],
                ).fetchall()
            missing_keys = [key for key, _symbol in candidates if key and key not in seen_keys]
            for action_key in missing_keys:
                conn.execute(
                    """
                    UPDATE _corporate_actions
                    SET status = 'inactive', deactivated_at = CURRENT_TIMESTAMP,
                        changed_at = CURRENT_TIMESTAMP
                    WHERE action_key = ?
                    """,
                    [action_key],
                )
            affected_symbols.update(str(symbol) for key, symbol in candidates if key in missing_keys)
        else:
            missing_keys = []
        result = {
            "actions_seen": len(deduped),
            **counters,
            "actions_deactivated": len(missing_keys),
            "affected_symbols": sorted(symbol for symbol in affected_symbols if symbol),
            "action_set_hash": compute_active_action_set_hash(db_path, _conn=conn),
        }
        if _conn is None:
            conn.commit()
        return result
    except Exception:
        if _conn is None:
            conn.rollback()
        raise
    finally:
        if _conn is None:
            conn.close()


def should_skip_adjustment_recompute(
    *,
    force: bool,
    reconcile_result: dict[str, Any],
    previous_action_set_hash: str | None,
    normalizer_version: int,
    previous_normalizer_version: int | None,
) -> bool:
    return (
        not force
        and int(reconcile_result.get("actions_inserted") or 0) == 0
        and int(reconcile_result.get("actions_changed") or 0) == 0
        and int(reconcile_result.get("actions_deactivated") or 0) == 0
        and reconcile_result.get("action_set_hash") == previous_action_set_hash
        and previous_normalizer_version == int(normalizer_version)
    )


def upsert_corporate_actions(db_path: str | Path, actions: Iterable[ParsedCorporateAction]) -> int:
    """Compatibility wrapper that upserts without deactivating unseen rows."""
    action_rows = list(actions)
    ensure_corporate_action_schema(db_path)
    if not action_rows:
        return 0
    dates = [action.ex_date for action in action_rows]
    conn = duckdb.connect(str(db_path))
    try:
        for action in action_rows:
            conn.execute(
                """
                DELETE FROM _corporate_actions
                WHERE source = ?
                  AND symbol = ?
                  AND COALESCE(isin, '') = COALESCE(?, '')
                  AND ex_date = ?
                  AND raw_payload_hash = ?
                  AND COALESCE(action_key, '') <> ?
                """,
                [
                    action.source,
                    action.symbol,
                    action.isin,
                    action.ex_date,
                    action.raw_payload_hash,
                    make_corporate_action_key(action),
                ],
            )
        conn.commit()
    finally:
        conn.close()
    result = reconcile_corporate_actions(
        db_path,
        action_rows,
        fetch_from=min(dates),
        fetch_to=max(dates),
        _deactivate_missing=False,
    )
    return int(result["actions_inserted"])


def recompute_adjusted_prices(
    db_path: str | Path,
    *,
    symbols: list[str] | None = None,
    force: bool = False,
    progress: ProgressCallback | None = None,
    _conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, Any]:
    """Recompute adjusted OHLC values from raw prices and active stored actions."""
    _report(progress, "step_start", step="Loading catalog rows")
    if _conn is None:
        ensure_corporate_action_schema(db_path)
    conn = _conn or duckdb.connect(str(db_path))
    scoped_symbols = sorted({str(symbol).strip() for symbol in symbols or [] if str(symbol).strip()})
    full_recompute = force or symbols is None
    scope = "full" if full_recompute else "symbols"
    try:
        raw_before = _raw_ohlc_checksum(conn)
        if full_recompute:
            conn.execute(
                """
                UPDATE _catalog
                SET adjusted_open = open, adjusted_high = high, adjusted_low = low,
                    adjusted_close = close, adjustment_factor = 1.0, adjustment_source = NULL,
                    adjusted_at = CURRENT_TIMESTAMP,
                    adjustment_version = COALESCE(adjustment_version, 0) + 1
                WHERE COALESCE(is_benchmark, FALSE)
                   OR COALESCE(instrument_type, 'equity') <> 'equity'
                   OR exchange <> 'NSE'
                """
            )
        elif scoped_symbols:
            conn.execute(
                """
                UPDATE _catalog
                SET adjusted_open = open, adjusted_high = high, adjusted_low = low,
                    adjusted_close = close, adjustment_factor = 1.0, adjustment_source = NULL,
                    adjusted_at = CURRENT_TIMESTAMP,
                    adjustment_version = COALESCE(adjustment_version, 0) + 1
                WHERE symbol_id IN (SELECT UNNEST(?))
                  AND (
                       COALESCE(is_benchmark, FALSE)
                    OR COALESCE(instrument_type, 'equity') <> 'equity'
                    OR exchange <> 'NSE'
                  )
                """,
                [scoped_symbols],
            )

        catalog_query = (
            """
            SELECT symbol_id, isin, exchange, timestamp, open, high, low, close,
                   adjusted_open, adjusted_high, adjusted_low, adjusted_close,
                   adjustment_factor AS prior_adjustment_factor,
                   adjustment_source AS prior_adjustment_source
            FROM _catalog
            WHERE exchange = 'NSE'
              AND NOT COALESCE(is_benchmark, FALSE)
              AND COALESCE(instrument_type, 'equity') = 'equity'
            """
        )
        if not full_recompute:
            catalog_query += " AND symbol_id IN (SELECT UNNEST(?))"
        catalog = conn.execute(catalog_query, [] if full_recompute else [scoped_symbols]).fetchdf()
        if catalog.empty:
            _report(progress, "step_done", step="Loading catalog rows", catalog_rows=0)
            return {
                "rows_adjusted": 0,
                "symbols_adjusted": 0,
                "raw_ohlc_unchanged": 1,
                "recompute_scope": scope,
                "affected_symbols": scoped_symbols,
            }

        action_query = (
            """
            SELECT symbol, isin, ex_date, price_factor
            FROM _corporate_actions
            WHERE COALESCE(status, 'active') = 'active'
              AND price_factor > 0
            """
        )
        if not full_recompute:
            scoped_isins = sorted(
                {
                    _normalize_isin(value)
                    for value in catalog["isin"].tolist()
                    if _normalize_isin(value)
                }
            )
            action_query += " AND (symbol IN (SELECT UNNEST(?)) OR isin IN (SELECT UNNEST(?)))"
        action_query += (
            """
            ORDER BY ex_date
            """
        )
        actions = conn.execute(action_query, [] if full_recompute else [scoped_symbols, scoped_isins]).fetchdf()
        _report(progress, "step_done", step="Loading catalog rows", catalog_rows=len(catalog), action_count=len(actions))
        _report(progress, "step_start", step="Applying action factors")
        catalog = catalog.copy(deep=True)
        catalog.loc[:, "trade_date"] = pd.to_datetime(catalog["timestamp"]).dt.normalize()
        catalog.loc[:, "adjustment_factor"] = 1.0
        if not actions.empty:
            actions.loc[:, "ex_date"] = pd.to_datetime(actions["ex_date"]).dt.normalize()
            _report(progress, "actions_start", total=len(actions), description="Applying action factors")
            for row in actions.itertuples(index=False):
                symbol = str(row.symbol or "").strip().upper()
                isin = _normalize_isin(row.isin)
                factor = float(row.price_factor)
                if factor <= 0:
                    _report(progress, "action_done", symbol=symbol, skipped=True)
                    continue
                mask = catalog["trade_date"] < row.ex_date
                symbol_match = catalog["symbol_id"].astype(str).str.upper().eq(symbol)
                if isin:
                    isin_match = catalog["isin"].fillna("").astype(str).str.upper().eq(isin)
                    mask &= isin_match | symbol_match
                else:
                    mask &= symbol_match
                catalog.loc[mask, "adjustment_factor"] = catalog.loc[mask, "adjustment_factor"] * factor
                _report(progress, "action_done", symbol=symbol, rows=int(mask.sum()))
            _report(progress, "actions_done", total=len(actions))
        _report(progress, "step_done", step="Applying action factors")

        _report(progress, "step_start", step="Writing adjusted prices")
        for source, target in (
            ("open", "adjusted_open"),
            ("high", "adjusted_high"),
            ("low", "adjusted_low"),
            ("close", "adjusted_close"),
        ):
            catalog.loc[:, target] = pd.to_numeric(catalog[source], errors="coerce") * catalog["adjustment_factor"]
        catalog.loc[:, "adjustment_source"] = None
        catalog.loc[catalog["adjustment_factor"].ne(1.0), "adjustment_source"] = SOURCE_NAME
        updates = catalog[
            [
                "symbol_id",
                "exchange",
                "timestamp",
                "adjusted_open",
                "adjusted_high",
                "adjusted_low",
                "adjusted_close",
                "adjustment_factor",
                "adjustment_source",
            ]
        ].copy()
        conn.register("_corporate_action_adjustments", updates)
        conn.execute(
            """
            UPDATE _catalog
            SET adjusted_open = u.adjusted_open,
                adjusted_high = u.adjusted_high,
                adjusted_low = u.adjusted_low,
                adjusted_close = u.adjusted_close,
                adjustment_factor = u.adjustment_factor,
                adjustment_source = u.adjustment_source,
                adjusted_at = CURRENT_TIMESTAMP,
                adjustment_version = COALESCE(_catalog.adjustment_version, 0) + 1
            FROM _corporate_action_adjustments u
            WHERE _catalog.symbol_id = u.symbol_id
              AND _catalog.exchange = u.exchange
              AND _catalog.timestamp = u.timestamp
            """
        )
        _report(progress, "step_done", step="Writing adjusted prices", rows=len(updates))
        _report(progress, "step_start", step="Verifying raw OHLC unchanged")
        raw_after = _raw_ohlc_checksum(conn)
        if raw_before != raw_after:
            raise RuntimeError("raw OHLC checksum changed during corporate-action normalization")
        if _conn is None:
            conn.commit()
        _report(progress, "step_done", step="Verifying raw OHLC unchanged")
        resulting_adjusted = pd.to_numeric(catalog["adjustment_factor"], errors="coerce").fillna(1.0).ne(1.0)
        changed_back_to_raw = pd.to_numeric(catalog["prior_adjustment_factor"], errors="coerce").fillna(1.0).ne(
            pd.to_numeric(catalog["adjustment_factor"], errors="coerce").fillna(1.0)
        )
        changed_rows = catalog[resulting_adjusted | changed_back_to_raw]
        return {
            "rows_adjusted": int(len(changed_rows)),
            "symbols_adjusted": int(changed_rows["symbol_id"].nunique()) if not changed_rows.empty else 0,
            "raw_ohlc_unchanged": 1,
            "recompute_scope": scope,
            "affected_symbols": scoped_symbols if not full_recompute else sorted(catalog["symbol_id"].astype(str).unique()),
        }
    except Exception:
        if _conn is None:
            conn.rollback()
        raise
    finally:
        if _conn is None:
            conn.close()


def _raw_ohlc_checksum(conn: duckdb.DuckDBPyConnection) -> str:
    row = conn.execute(
        """
        SELECT sha256(string_agg(
            CONCAT_WS('|', symbol_id, exchange, CAST(timestamp AS VARCHAR),
                      CAST(open AS VARCHAR), CAST(high AS VARCHAR), CAST(low AS VARCHAR), CAST(close AS VARCHAR)),
            '\n' ORDER BY symbol_id, exchange, timestamp
        ))
        FROM _catalog
        """
    ).fetchone()
    return str(row[0] or "")


def _record_execution(
    db_path: str | Path,
    *,
    run_id: str | None,
    started_at: datetime,
    result: dict[str, Any],
) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        last_success_at = result.get("last_success_at")
        if not last_success_at and result.get("status") != "success":
            row = conn.execute(
                """
                SELECT MAX(ended_at)
                FROM _module_execution_log
                WHERE module_name = ? AND status = 'success'
                """,
                [MODULE_NAME],
            ).fetchone()
            last_success_at = row[0] if row else None
        execution_id = f"{MODULE_NAME}-{int(time.time() * 1000)}"
        conn.execute(
            """
            INSERT INTO _module_execution_log
            (execution_id, module_name, run_id, execution_mode, status, started_at, ended_at,
             last_success_at, actions_fetched, actions_inserted, rows_adjusted, symbols_adjusted,
             error_message, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                execution_id,
                MODULE_NAME,
                run_id,
                result.get("execution_mode"),
                result.get("status"),
                started_at,
                last_success_at,
                int(result.get("actions_fetched") or 0),
                int(result.get("actions_inserted") or 0),
                int(result.get("rows_adjusted") or 0),
                int(result.get("symbols_adjusted") or 0),
                result.get("error_message"),
                json.dumps(result, sort_keys=True, default=str),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _report(progress: ProgressCallback | None, event: str, **payload: Any) -> None:
    if progress is not None:
        progress({"event": event, **payload})


class _ProgressMux:
    def __init__(self, *, show_progress: bool, callback: ProgressCallback | None = None):
        self.callback = callback
        self.renderer = _TqdmProgressRenderer() if show_progress else None

    def __call__(self, event: dict[str, Any]) -> None:
        if self.callback is not None:
            self.callback(event)
        if self.renderer is not None:
            self.renderer(event)

    def close(self) -> None:
        if self.renderer is not None:
            self.renderer.close()


class _TqdmProgressRenderer:
    def __init__(self) -> None:
        from tqdm.auto import tqdm

        self._tqdm = tqdm
        self.step_bar = None
        self.year_bar = None
        self.action_bar = None

    def __call__(self, event: dict[str, Any]) -> None:
        kind = str(event.get("event") or "")
        if kind == "step_start":
            self._ensure_step_bar(int(event.get("total") or 8))
            self.step_bar.set_description_str(str(event.get("step") or "Working"))
            self._tqdm.write(f"Now: {event.get('step')}", file=sys.stderr)
        elif kind == "step_done":
            self._ensure_step_bar(int(event.get("total") or 8))
            self.step_bar.update(1)
            self.step_bar.set_postfix_str(str(event.get("step") or "done"))
        elif kind == "years_start":
            self._close_bar("year_bar")
            self.year_bar = self._tqdm(
                total=int(event.get("total") or 0),
                desc=str(event.get("description") or "Fetching NSE actions"),
                unit="year",
                leave=False,
                dynamic_ncols=True,
                file=sys.stderr,
            )
        elif kind == "year_start" and self.year_bar is not None:
            self.year_bar.set_postfix_str(f"year={event.get('year')}")
        elif kind == "year_done" and self.year_bar is not None:
            self.year_bar.update(1)
            self.year_bar.set_postfix_str(f"year={event.get('year')} rows={event.get('fetched')}")
        elif kind == "years_done":
            self._close_bar("year_bar")
            self._tqdm.write(f"Fetched NSE rows: {event.get('total_fetched', 0)}", file=sys.stderr)
        elif kind == "actions_start":
            self._close_bar("action_bar")
            self.action_bar = self._tqdm(
                total=int(event.get("total") or 0),
                desc=str(event.get("description") or "Applying action factors"),
                unit="action",
                leave=False,
                dynamic_ncols=True,
                file=sys.stderr,
            )
        elif kind == "action_done" and self.action_bar is not None:
            self.action_bar.update(1)
            symbol = event.get("symbol")
            rows = event.get("rows")
            if symbol:
                self.action_bar.set_postfix_str(f"{symbol} rows={rows}")
        elif kind == "actions_done":
            self._close_bar("action_bar")
        elif kind == "message":
            self._tqdm.write(str(event.get("message") or ""), file=sys.stderr)

    def _ensure_step_bar(self, total: int):
        if self.step_bar is None:
            self.step_bar = self._tqdm(
                total=total,
                desc="Corporate actions",
                unit="step",
                dynamic_ncols=True,
                file=sys.stderr,
            )

    def _close_bar(self, attr: str) -> None:
        bar = getattr(self, attr)
        if bar is not None:
            bar.close()
            setattr(self, attr, None)

    def close(self) -> None:
        self._close_bar("action_bar")
        self._close_bar("year_bar")
        self._close_bar("step_bar")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    paths = get_domain_paths(data_domain=args.data_domain)
    show_progress = bool(sys.stdout.isatty()) if args.progress is None else bool(args.progress)
    result = run_corporate_action_normalization(
        ohlcv_db_path=paths.ohlcv_db_path,
        masterdb_path=paths.master_db_path,
        force=args.force,
        overlap_days=args.overlap_days,
        normalizer_version=args.normalizer_version,
        show_progress=show_progress,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync NSE split/bonus actions and normalize adjusted OHLC.")
    parser.add_argument("--force", action="store_true", help="Force full NSE corporate-action backfill")
    parser.add_argument("--overlap-days", type=int, default=45, help="Incremental fetch overlap window")
    parser.add_argument("--normalizer-version", type=int, default=1, help="Adjusted-price algorithm version")
    parser.add_argument("--data-domain", default="operational")
    progress_group = parser.add_mutually_exclusive_group()
    progress_group.add_argument("--progress", dest="progress", action="store_true", help="Show terminal progress bars")
    progress_group.add_argument("--no-progress", dest="progress", action="store_false", help="Suppress terminal progress bars")
    parser.set_defaults(progress=None)
    return parser


if __name__ == "__main__":
    main()
