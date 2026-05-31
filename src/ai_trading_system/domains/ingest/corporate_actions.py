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
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_corporate_actions_identity
            ON _corporate_actions (
                symbol, isin, ex_date, action_type, parsed_ratio, source, raw_payload_hash
            )
            """
        )
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
    today: date | None = None,
    fetcher: Callable[..., list[dict[str, Any]]] | None = None,
    show_progress: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Sync NSE actions and recompute adjusted OHLC from raw prices."""
    started_at = _utc_now()
    today = today or date.today()
    execution_mode = "full" if _should_run_full(ohlcv_db_path, force=force, max_age_days=max_age_days, today=today) else "recent"
    progress = _ProgressMux(show_progress=show_progress, callback=progress_callback)
    result: dict[str, Any] = {
        "module_name": MODULE_NAME,
        "status": "started",
        "execution_mode": execution_mode,
        "actions_fetched": 0,
        "actions_inserted": 0,
        "rows_adjusted": 0,
        "symbols_adjusted": 0,
        "error_message": None,
    }

    try:
        _report(progress, "step_start", step="Preparing schema", total=9)
        ensure_corporate_action_schema(ohlcv_db_path)
        _report(progress, "step_done", step="Preparing schema")
        if execution_mode == "recent" and not force:
            last_recent_success = _last_successful_execution_at(
                ohlcv_db_path,
                execution_mode="recent",
            )
            if last_recent_success is not None and pd.Timestamp(last_recent_success).date() == today:
                result["status"] = "success"
                result["skipped"] = True
                result["skip_reason"] = "recent_success_today"
                result["last_success_at"] = last_recent_success
                logger.info(
                    "Skipping corporate action normalization; recent success already recorded at %s",
                    last_recent_success,
                )
                _report(progress, "message", message="Skipped: recent success already recorded today")
                return result

        fetcher = fetcher or fetch_nse_corporate_actions
        start_date = date(2000, 1, 1) if execution_mode == "full" else today - timedelta(days=45)
        _report(progress, "step_start", step=f"Fetching NSE actions {start_date.year}-{today.year}")
        raw_actions = fetcher(start_date=start_date, end_date=today, progress=progress)
        result["actions_fetched"] = len(raw_actions)
        _report(progress, "step_done", step=f"Fetching NSE actions {start_date.year}-{today.year}")

        _report(progress, "step_start", step="Parsing split/bonus actions")
        symbol_master = SymbolMaster.from_masterdb(masterdb_path)
        parsed = [
            action
            for action in (parse_corporate_action(raw, symbol_master=symbol_master) for raw in raw_actions)
            if action is not None
        ]
        _report(progress, "step_done", step="Parsing split/bonus actions", parsed_actions=len(parsed))

        _report(progress, "step_start", step="Saving corporate actions")
        inserted = upsert_corporate_actions(ohlcv_db_path, parsed)
        result["actions_inserted"] = inserted
        _report(progress, "step_done", step="Saving corporate actions", actions_inserted=inserted)

        normalize_result = recompute_adjusted_prices(ohlcv_db_path, progress=progress)
        result.update(normalize_result)
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


def _should_run_full(db_path: str | Path, *, force: bool, max_age_days: int, today: date) -> bool:
    if force:
        return True
    ensure_corporate_action_schema(db_path)
    last_success = _last_successful_execution_at(db_path, execution_mode="full")
    if last_success is None:
        return True
    last_date = pd.Timestamp(last_success).date()
    return (today - last_date).days > int(max_age_days)


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


def upsert_corporate_actions(db_path: str | Path, actions: Iterable[ParsedCorporateAction]) -> int:
    rows = [
        {
            "symbol": action.symbol,
            "isin": action.isin,
            "ex_date": action.ex_date,
            "action_type": action.action_type,
            "parsed_ratio": action.parsed_ratio,
            "price_factor": action.price_factor,
            "share_factor": action.share_factor,
            "source": action.source,
            "raw_subject": action.raw_subject,
            "raw_payload_hash": action.raw_payload_hash,
            "raw_payload_json": action.raw_payload_json,
        }
        for action in actions
    ]
    if not rows:
        return 0
    frame = pd.DataFrame(rows).drop_duplicates(
        subset=["symbol", "isin", "ex_date", "action_type", "parsed_ratio", "source", "raw_payload_hash"]
    )
    conn = duckdb.connect(str(db_path))
    inserted = 0
    try:
        for row in frame.to_dict("records"):
            conn.execute(
                """
                DELETE FROM _corporate_actions
                WHERE symbol = ?
                  AND COALESCE(isin, '') = COALESCE(?, '')
                  AND ex_date = ?
                  AND source = ?
                  AND raw_payload_hash = ?
                  AND (
                        action_type <> ?
                     OR parsed_ratio <> ?
                  )
                """,
                [
                    row["symbol"],
                    row["isin"],
                    row["ex_date"],
                    row["source"],
                    row["raw_payload_hash"],
                    row["action_type"],
                    row["parsed_ratio"],
                ],
            )
            exists = conn.execute(
                """
                SELECT 1
                FROM _corporate_actions
                WHERE symbol = ?
                  AND COALESCE(isin, '') = COALESCE(?, '')
                  AND ex_date = ?
                  AND action_type = ?
                  AND parsed_ratio = ?
                  AND source = ?
                  AND raw_payload_hash = ?
                LIMIT 1
                """,
                [
                    row["symbol"],
                    row["isin"],
                    row["ex_date"],
                    row["action_type"],
                    row["parsed_ratio"],
                    row["source"],
                    row["raw_payload_hash"],
                ],
            ).fetchone()
            if exists:
                conn.execute(
                    """
                    UPDATE _corporate_actions
                    SET last_seen_at = CURRENT_TIMESTAMP
                    WHERE symbol = ?
                      AND COALESCE(isin, '') = COALESCE(?, '')
                      AND ex_date = ?
                      AND action_type = ?
                      AND parsed_ratio = ?
                      AND source = ?
                      AND raw_payload_hash = ?
                    """,
                    [
                        row["symbol"],
                        row["isin"],
                        row["ex_date"],
                        row["action_type"],
                        row["parsed_ratio"],
                        row["source"],
                        row["raw_payload_hash"],
                    ],
                )
                continue
            conn.execute(
                """
                INSERT INTO _corporate_actions
                (symbol, isin, ex_date, action_type, parsed_ratio, price_factor, share_factor,
                 source, raw_subject, raw_payload_hash, raw_payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["symbol"],
                    row["isin"],
                    row["ex_date"],
                    row["action_type"],
                    row["parsed_ratio"],
                    row["price_factor"],
                    row["share_factor"],
                    row["source"],
                    row["raw_subject"],
                    row["raw_payload_hash"],
                    row["raw_payload_json"],
                ],
            )
            inserted += 1
        conn.commit()
        return inserted
    finally:
        conn.close()


def recompute_adjusted_prices(db_path: str | Path, *, progress: ProgressCallback | None = None) -> dict[str, int]:
    """Recompute all adjusted OHLC values from raw prices and stored actions."""
    _report(progress, "step_start", step="Loading catalog rows")
    ensure_corporate_action_schema(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        raw_before = _raw_ohlc_checksum(conn)
        conn.execute(
            """
            UPDATE _catalog
            SET adjusted_open = open,
                adjusted_high = high,
                adjusted_low = low,
                adjusted_close = close,
                adjustment_factor = 1.0,
                adjustment_source = NULL,
                adjusted_at = CURRENT_TIMESTAMP,
                adjustment_version = COALESCE(adjustment_version, 0) + 1
            WHERE COALESCE(is_benchmark, FALSE)
               OR COALESCE(instrument_type, 'equity') <> 'equity'
               OR exchange <> 'NSE'
            """
        )

        catalog = conn.execute(
            """
            SELECT symbol_id, isin, exchange, timestamp, open, high, low, close
            FROM _catalog
            WHERE exchange = 'NSE'
              AND NOT COALESCE(is_benchmark, FALSE)
              AND COALESCE(instrument_type, 'equity') = 'equity'
            """
        ).fetchdf()
        if catalog.empty:
            _report(progress, "step_done", step="Loading catalog rows", catalog_rows=0)
            return {"rows_adjusted": 0, "symbols_adjusted": 0, "raw_ohlc_unchanged": 1}

        actions = conn.execute(
            """
            SELECT symbol, isin, ex_date, price_factor
            FROM _corporate_actions
            WHERE price_factor > 0
            ORDER BY ex_date
            """
        ).fetchdf()
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
        conn.commit()
        _report(progress, "step_done", step="Verifying raw OHLC unchanged")
        adjusted = updates[pd.to_numeric(updates["adjustment_factor"], errors="coerce").fillna(1.0).ne(1.0)]
        return {
            "rows_adjusted": int(len(adjusted)),
            "symbols_adjusted": int(adjusted["symbol_id"].nunique()) if not adjusted.empty else 0,
            "raw_ohlc_unchanged": 1,
        }
    finally:
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
        show_progress=show_progress,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync NSE split/bonus actions and normalize adjusted OHLC.")
    parser.add_argument("--force", action="store_true", help="Force full NSE corporate-action backfill")
    parser.add_argument("--data-domain", default="operational")
    progress_group = parser.add_mutually_exclusive_group()
    progress_group.add_argument("--progress", dest="progress", action="store_true", help="Show terminal progress bars")
    progress_group.add_argument("--no-progress", dest="progress", action="store_false", help="Suppress terminal progress bars")
    parser.set_defaults(progress=None)
    return parser


if __name__ == "__main__":
    main()
