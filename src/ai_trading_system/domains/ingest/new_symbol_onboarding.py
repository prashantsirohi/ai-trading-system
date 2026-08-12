"""One-command discovery and onboarding for new BSE-only equities.

Preview is the default and only inspects local coverage. An explicit
``--discover-missing --promote-discovered --apply`` sequence can promote a
clean official discovery scope after checkpointing the master. ``--apply`` creates a
checkpoint, resolves official BSE classification, backfills official bhavcopy
history, recomputes targeted technical features, refreshes BSE Phase-1
features, optionally syncs Screener fundamentals, and writes a verification
report.

The workflow is intentionally honest about unsupported coverage: BSE delivery
history and BSE corporate-action adjustment are reported as gaps until their
exchange-aware stores are implemented.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import duckdb
import requests

from ai_trading_system.domains.ingest.bse_bhavcopy_backfill import (
    FEATURE_TYPES,
    backfill_bse_bhavcopy,
)
from ai_trading_system.platform.db.paths import (
    DataDomainPaths,
    get_domain_paths,
    require_data_root_available,
)
from ai_trading_system.platform.utils.env import load_project_env


BSE_PROFILE_URL = "https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w"
BSE_ACTIVE_EQUITIES_URL = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
UNKNOWN_CLASSIFICATIONS = {"", "UNKNOWN", "OTHER", "NA", "N/A", "NONE", "NULL"}


@dataclass(frozen=True)
class SymbolTarget:
    symbol_id: str
    security_id: str
    exchange: str
    isin: str
    symbol_name: str
    sector: str
    industry: str


@dataclass(frozen=True)
class BSEClassification:
    symbol_id: str
    exchange: str
    security_id: str
    isin: str
    sector: str
    industry: str
    industry_new: str
    industry_group: str
    industry_subgroup: str
    source: str
    source_url: str
    raw_payload_hash: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalized_symbols(values: Iterable[str] | None) -> list[str]:
    return sorted({str(value).strip().upper() for value in values or [] if str(value).strip()})


def load_symbol_targets(master_db_path: str | Path, symbols: Iterable[str]) -> list[SymbolTarget]:
    """Resolve an explicit BSE-only onboarding scope from the canonical master."""
    requested = _normalized_symbols(symbols)
    if not requested:
        raise ValueError("At least one explicit --symbol or --symbols-file entry is required.")
    placeholders = ", ".join("?" for _ in requested)
    conn = sqlite3.connect(f"file:{Path(master_db_path)}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            f"""
            SELECT symbol_id, security_id, exchange, COALESCE(isin, ''),
                   COALESCE(symbol_name, ''), COALESCE(sector, ''), COALESCE(industry, '')
            FROM symbols
            WHERE UPPER(symbol_id) IN ({placeholders})
            ORDER BY symbol_id
            """,
            requested,
        ).fetchall()
    finally:
        conn.close()
    found = {str(row[0]).upper() for row in rows}
    missing = sorted(set(requested) - found)
    if missing:
        raise ValueError(f"Symbols are not present in masterdata.db: {missing}")
    unsupported = sorted(str(row[0]) for row in rows if str(row[2]).upper() != "BSE")
    if unsupported:
        raise ValueError(
            "The unified onboarding command currently supports BSE-only master rows; "
            f"unsupported symbols: {unsupported}"
        )
    targets = [
        SymbolTarget(
            symbol_id=str(row[0]).strip().upper(),
            security_id=str(row[1] or "").strip(),
            exchange=str(row[2]).strip().upper(),
            isin=str(row[3] or "").strip().upper(),
            symbol_name=str(row[4] or "").strip(),
            sector=str(row[5] or "").strip(),
            industry=str(row[6] or "").strip(),
        )
        for row in rows
    ]
    invalid = sorted(target.symbol_id for target in targets if not target.security_id.isdigit())
    if invalid:
        raise ValueError(f"BSE targets are missing numeric security_id values: {invalid}")
    return targets


class BSEProfileClient:
    """Small, identity-checked client for BSE's company-header endpoint."""

    def __init__(self, *, timeout_sec: float = 30.0, session: requests.Session | None = None):
        self.timeout_sec = float(timeout_sec)
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json,text/plain,*/*",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 Chrome/124 Safari/537.36",
                "Referer": "https://www.bseindia.com/",
            }
        )

    def fetch(self, target: SymbolTarget) -> BSEClassification:
        response = self.session.get(
            BSE_PROFILE_URL,
            params={"quotetype": "EQ", "scripcode": target.security_id, "seriesid": ""},
            timeout=self.timeout_sec,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"BSE profile returned a non-object for {target.symbol_id}")
        observed_code = str(payload.get("SecurityCode") or "").strip()
        observed_isin = str(payload.get("ISIN") or "").strip().upper()
        if observed_code != target.security_id:
            raise RuntimeError(
                f"BSE profile identity mismatch for {target.symbol_id}: "
                f"expected code {target.security_id}, observed {observed_code or '<empty>'}"
            )
        if target.isin and observed_isin and observed_isin != target.isin:
            raise RuntimeError(
                f"BSE profile ISIN mismatch for {target.symbol_id}: "
                f"expected {target.isin}, observed {observed_isin}"
            )
        sector = str(payload.get("Sector") or "").strip()
        industry_new = str(payload.get("IndustryNew") or "").strip()
        industry_group = str(payload.get("IGroup") or "").strip()
        industry_subgroup = str(payload.get("ISubGroup") or payload.get("Industry") or "").strip()
        industry = industry_subgroup or industry_group or industry_new
        if sector.upper() in UNKNOWN_CLASSIFICATIONS or industry.upper() in UNKNOWN_CLASSIFICATIONS:
            raise RuntimeError(f"BSE profile lacks usable classification for {target.symbol_id}")
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return BSEClassification(
            symbol_id=target.symbol_id,
            exchange="BSE",
            security_id=target.security_id,
            isin=observed_isin or target.isin,
            sector=sector,
            industry=industry,
            industry_new=industry_new,
            industry_group=industry_group,
            industry_subgroup=industry_subgroup,
            source="BSE_COMHEADERNEW",
            source_url=str(response.url),
            raw_payload_hash=hashlib.sha256(raw).hexdigest(),
        )


class BSEActiveEquityClient:
    """Read the official active BSE equity master for pre-master discovery."""

    def __init__(self, *, timeout_sec: float = 60.0, session: requests.Session | None = None):
        self.timeout_sec = float(timeout_sec)
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json,text/plain,*/*",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 Chrome/124 Safari/537.36",
                "Referer": "https://www.bseindia.com/corporates/List_Scrips.html",
            }
        )

    def fetch(self) -> tuple[list[dict[str, Any]], str]:
        response = self.session.get(
            BSE_ACTIVE_EQUITIES_URL,
            params={
                "Group": "",
                "Scripcode": "",
                "industry": "",
                "segment": "Equity",
                "status": "Active",
            },
            timeout=self.timeout_sec,
        )
        response.raise_for_status()
        payload = response.json()
        required = {"SCRIP_CD", "Scrip_Name", "Status", "ISIN_NUMBER", "scrip_id", "Mktcap"}
        if not isinstance(payload, list) or not payload:
            raise RuntimeError("BSE active-equity master returned no rows")
        missing = sorted(required - set(payload[0]))
        if missing:
            raise RuntimeError(f"BSE active-equity master schema changed; missing {missing}")
        rows = [dict(row) for row in payload if isinstance(row, dict)]
        return rows, str(response.url)


def _parse_market_cap(value: object) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text or text.upper() in {"NA", "N/A", "NULL", "-"}:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _load_master_identity_rows(master_db_path: str | Path) -> list[dict[str, str]]:
    conn = sqlite3.connect(f"file:{Path(master_db_path)}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT symbol_id, COALESCE(security_id, ''), COALESCE(exchange, ''),
                   COALESCE(isin, ''), COALESCE(bse_symbol, '')
            FROM symbols
            """
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "symbol_id": str(row[0] or "").strip().upper(),
            "security_id": str(row[1] or "").strip().removesuffix(".0"),
            "exchange": str(row[2] or "").strip().upper(),
            "isin": str(row[3] or "").strip().upper(),
            "bse_symbol": str(row[4] or "").strip().upper(),
        }
        for row in rows
    ]


def discover_bse_missing_symbols(
    *,
    master_db_path: str | Path,
    symbols: Iterable[str],
    active_client: BSEActiveEquityClient | None = None,
    profile_client: BSEProfileClient | None = None,
    resolve_classification: bool = True,
) -> dict[str, Any]:
    """Resolve missing symbols without writing master or operational stores."""
    requested = _normalized_symbols(symbols)
    if not requested:
        raise ValueError("At least one explicit --symbol or --symbols-file entry is required.")
    client = active_client or BSEActiveEquityClient()
    active_rows, source_url = client.fetch()
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in active_rows:
        source_symbol = str(row.get("scrip_id") or "").strip().upper()
        if source_symbol:
            by_symbol.setdefault(source_symbol, []).append(row)

    master_rows = _load_master_identity_rows(master_db_path)
    master_by_symbol = {
        value: row
        for row in master_rows
        for value in (row["symbol_id"], row["bse_symbol"])
        if value
    }
    master_by_isin = {row["isin"]: row for row in master_rows if row["isin"]}
    master_by_bse_code = {
        row["security_id"]: row
        for row in master_rows
        if row["exchange"] == "BSE" and row["security_id"]
    }

    candidates: list[dict[str, Any]] = []
    already_mastered: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    not_found: list[str] = []
    invalid: dict[str, str] = {}
    targets_for_classification: list[SymbolTarget] = []
    for symbol in requested:
        matches = by_symbol.get(symbol, [])
        if not matches:
            not_found.append(symbol)
            continue
        if len(matches) != 1:
            invalid[symbol] = f"official_active_master_duplicate_count={len(matches)}"
            continue
        row = matches[0]
        security_id = str(row.get("SCRIP_CD") or "").strip().removesuffix(".0")
        isin = str(row.get("ISIN_NUMBER") or "").strip().upper()
        company_name = str(row.get("Scrip_Name") or row.get("Issuer_Name") or "").strip()
        if not security_id.isdigit():
            invalid[symbol] = "official_active_master_missing_numeric_security_code"
            continue
        if not isin.startswith("INE"):
            invalid[symbol] = f"not_a_company_equity_isin={isin or '<empty>'}"
            continue
        identity = {
            "symbol_id": symbol,
            "security_id": security_id,
            "symbol_name": company_name,
            "exchange": "BSE",
            "instrument_type": "EQUITY",
            "isin": isin,
            "bse_symbol": symbol,
            "market_cap_cr": _parse_market_cap(row.get("Mktcap")),
            "bse_group": str(row.get("GROUP") or "").strip().upper(),
            "face_value": _parse_market_cap(row.get("FACE_VALUE")),
            "status": str(row.get("Status") or "").strip(),
            "source_url": str(row.get("NSURL") or source_url),
        }
        exact = master_by_symbol.get(symbol)
        if exact is not None:
            already_mastered.append({**identity, "local_record": exact})
            continue
        isin_collision = master_by_isin.get(isin)
        code_collision = master_by_bse_code.get(security_id)
        if isin_collision is not None or code_collision is not None:
            conflicts.append(
                {
                    **identity,
                    "reason": "isin_collision" if isin_collision is not None else "bse_code_collision",
                    "local_record": isin_collision or code_collision,
                }
            )
            continue
        candidates.append(identity)
        targets_for_classification.append(
            SymbolTarget(
                symbol_id=symbol,
                security_id=security_id,
                exchange="BSE",
                isin=isin,
                symbol_name=company_name,
                sector="",
                industry="",
            )
        )

    classification_failures: dict[str, str] = {}
    if resolve_classification and targets_for_classification:
        shared_profile_client = profile_client or BSEProfileClient(session=client.session)
        classifications, classification_failures = fetch_bse_classifications(
            targets_for_classification,
            client=shared_profile_client,
        )
        classification_by_symbol = {item.symbol_id: asdict(item) for item in classifications}
        for candidate in candidates:
            candidate["classification"] = classification_by_symbol.get(str(candidate["symbol_id"]))
    else:
        for candidate in candidates:
            candidate["classification"] = None

    return {
        "source": "BSE_ACTIVE_EQUITY_MASTER+BSE_COMHEADERNEW",
        "source_url": source_url,
        "active_master_rows": len(active_rows),
        "requested_count": len(requested),
        "candidate_count": len(candidates),
        "already_mastered_count": len(already_mastered),
        "conflict_count": len(conflicts),
        "not_found_count": len(not_found),
        "invalid_count": len(invalid),
        "classification_failure_count": len(classification_failures),
        "candidates": candidates,
        "already_mastered": already_mastered,
        "conflicts": conflicts,
        "not_found": not_found,
        "invalid": invalid,
        "classification_failures": classification_failures,
    }


def _prepare_discovered_candidates(
    discovery: dict[str, Any],
    requested_symbols: Iterable[str],
) -> tuple[list[dict[str, Any]], list[SymbolTarget], list[BSEClassification]]:
    """Validate that discovery is complete enough for an atomic master promotion."""
    requested = _normalized_symbols(requested_symbols)
    gap_fields = (
        "already_mastered_count",
        "conflict_count",
        "not_found_count",
        "invalid_count",
        "classification_failure_count",
    )
    gaps = {field: int(discovery.get(field, 0) or 0) for field in gap_fields}
    if any(gaps.values()):
        raise ValueError(f"Discovery contains promotion-blocking gaps: {gaps}")

    candidates = list(discovery.get("candidates") or [])
    candidate_symbols = _normalized_symbols(str(item.get("symbol_id") or "") for item in candidates)
    if candidate_symbols != requested or len(candidates) != len(requested):
        raise ValueError(
            "Discovery candidate scope does not exactly match the requested symbols: "
            f"requested={requested}, candidates={candidate_symbols}"
        )

    targets: list[SymbolTarget] = []
    classifications: list[BSEClassification] = []
    seen_codes: set[str] = set()
    seen_isins: set[str] = set()
    for candidate in candidates:
        symbol = str(candidate.get("symbol_id") or "").strip().upper()
        security_id = str(candidate.get("security_id") or "").strip().removesuffix(".0")
        isin = str(candidate.get("isin") or "").strip().upper()
        classification_payload = candidate.get("classification")
        if not security_id.isdigit() or not isin.startswith("INE"):
            raise ValueError(f"Discovered identity is not promotable for {symbol}")
        if security_id in seen_codes or isin in seen_isins:
            raise ValueError(f"Duplicate BSE security code or ISIN in promotion scope: {symbol}")
        if not isinstance(classification_payload, dict):
            raise ValueError(f"Missing official BSE classification for {symbol}")
        classification = BSEClassification(
            symbol_id=str(classification_payload.get("symbol_id") or "").strip().upper(),
            exchange=str(classification_payload.get("exchange") or "").strip().upper(),
            security_id=str(classification_payload.get("security_id") or "").strip(),
            isin=str(classification_payload.get("isin") or "").strip().upper(),
            sector=str(classification_payload.get("sector") or "").strip(),
            industry=str(classification_payload.get("industry") or "").strip(),
            industry_new=str(classification_payload.get("industry_new") or "").strip(),
            industry_group=str(classification_payload.get("industry_group") or "").strip(),
            industry_subgroup=str(classification_payload.get("industry_subgroup") or "").strip(),
            source=str(classification_payload.get("source") or "").strip(),
            source_url=str(classification_payload.get("source_url") or "").strip(),
            raw_payload_hash=str(classification_payload.get("raw_payload_hash") or "").strip(),
        )
        if (
            classification.symbol_id != symbol
            or classification.exchange != "BSE"
            or classification.security_id != security_id
            or classification.isin != isin
            or classification.sector.upper() in UNKNOWN_CLASSIFICATIONS
            or classification.industry.upper() in UNKNOWN_CLASSIFICATIONS
            or classification.source != "BSE_COMHEADERNEW"
            or len(classification.raw_payload_hash) != 64
        ):
            raise ValueError(f"Official BSE classification identity is incomplete for {symbol}")
        seen_codes.add(security_id)
        seen_isins.add(isin)
        targets.append(
            SymbolTarget(
                symbol_id=symbol,
                security_id=security_id,
                exchange="BSE",
                isin=isin,
                symbol_name=str(candidate.get("symbol_name") or "").strip(),
                sector=classification.sector,
                industry=classification.industry,
            )
        )
        classifications.append(classification)
    return candidates, targets, classifications


def apply_discovered_master_candidates(
    master_db_path: str | Path,
    candidates: Iterable[dict[str, Any]],
    *,
    observed_at: datetime | None = None,
) -> int:
    """Insert an already validated, explicit BSE-only discovery scope."""
    values = list(candidates)
    if not values:
        raise ValueError("No discovered candidates were supplied for promotion")
    timestamp = (observed_at or _utc_now()).isoformat()
    conn = sqlite3.connect(master_db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        available_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(symbols)").fetchall()
        }
        required_columns = {
            "symbol_id", "security_id", "symbol_name", "exchange", "instrument_type",
            "isin", "sector", "industry", "bse_symbol", "last_updated",
        }
        missing_columns = sorted(required_columns - available_columns)
        if missing_columns:
            raise RuntimeError(f"masterdata symbols schema is missing columns: {missing_columns}")

        ordered_columns = [
            column
            for column in (
                "symbol_id", "security_id", "symbol_name", "exchange", "instrument_type",
                "isin", "lot_size", "tick_size", "freeze_quantity", "sector", "industry",
                "nse_symbol", "bse_symbol", "mcap", "last_updated",
            )
            if column in available_columns
        ]
        for candidate in values:
            symbol = str(candidate.get("symbol_id") or "").strip().upper()
            security_id = str(candidate.get("security_id") or "").strip().removesuffix(".0")
            isin = str(candidate.get("isin") or "").strip().upper()
            collision = conn.execute(
                """
                SELECT symbol_id, security_id, exchange, isin, bse_symbol
                FROM symbols
                WHERE UPPER(symbol_id) = ?
                   OR UPPER(COALESCE(bse_symbol, '')) = ?
                   OR UPPER(COALESCE(isin, '')) = ?
                   OR (UPPER(COALESCE(exchange, '')) = 'BSE' AND security_id = ?)
                LIMIT 1
                """,
                [symbol, symbol, isin, security_id],
            ).fetchone()
            if collision is not None:
                raise RuntimeError(f"Master identity collision while promoting {symbol}: {collision}")
            classification = dict(candidate.get("classification") or {})
            row_values: dict[str, Any] = {
                "symbol_id": symbol,
                "security_id": security_id,
                "symbol_name": str(candidate.get("symbol_name") or "").strip(),
                "exchange": "BSE",
                "instrument_type": "EQUITY",
                "isin": isin,
                "lot_size": 1,
                "tick_size": None,
                "freeze_quantity": None,
                "sector": str(classification.get("sector") or "").strip(),
                "industry": str(classification.get("industry") or "").strip(),
                "nse_symbol": None,
                "bse_symbol": symbol,
                "mcap": _parse_market_cap(candidate.get("market_cap_cr")),
                "last_updated": timestamp,
            }
            placeholders = ", ".join("?" for _ in ordered_columns)
            conn.execute(
                f"INSERT INTO symbols ({', '.join(ordered_columns)}) VALUES ({placeholders})",
                [row_values[column] for column in ordered_columns],
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(values)


def fetch_bse_classifications(
    targets: Iterable[SymbolTarget],
    *,
    client: BSEProfileClient | None = None,
) -> tuple[list[BSEClassification], dict[str, str]]:
    resolved: list[BSEClassification] = []
    failed: dict[str, str] = {}
    profile_client = client or BSEProfileClient()
    for target in targets:
        try:
            resolved.append(profile_client.fetch(target))
        except Exception as exc:  # noqa: BLE001 - each symbol is independently auditable
            failed[target.symbol_id] = f"{type(exc).__name__}: {exc}"
    return resolved, failed


def _ensure_classification_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_classification (
            symbol_id TEXT NOT NULL,
            exchange TEXT NOT NULL,
            security_id TEXT NOT NULL,
            isin TEXT,
            sector TEXT NOT NULL,
            industry TEXT NOT NULL,
            industry_new TEXT,
            industry_group TEXT,
            industry_subgroup TEXT,
            source TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_as_of TEXT NOT NULL,
            raw_payload_hash TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (symbol_id, exchange, source)
        )
        """
    )


def apply_bse_classifications(
    master_db_path: str | Path,
    classifications: Iterable[BSEClassification],
    *,
    observed_at: datetime | None = None,
) -> int:
    """Persist official classification lineage and update current master fields."""
    values = list(classifications)
    if not values:
        return 0
    timestamp = (observed_at or _utc_now()).isoformat()
    conn = sqlite3.connect(master_db_path)
    try:
        _ensure_classification_schema(conn)
        for item in values:
            cursor = conn.execute(
                """
                UPDATE symbols
                SET sector = ?, industry = ?, last_updated = ?
                WHERE symbol_id = ? AND exchange = ? AND security_id = ?
                """,
                [item.sector, item.industry, timestamp, item.symbol_id, item.exchange, item.security_id],
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Classification target changed during onboarding: {item.symbol_id}")
            conn.execute(
                """
                INSERT INTO symbol_classification (
                    symbol_id, exchange, security_id, isin, sector, industry,
                    industry_new, industry_group, industry_subgroup, source,
                    source_url, source_as_of, raw_payload_hash, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol_id, exchange, source) DO UPDATE SET
                    security_id = excluded.security_id,
                    isin = excluded.isin,
                    sector = excluded.sector,
                    industry = excluded.industry,
                    industry_new = excluded.industry_new,
                    industry_group = excluded.industry_group,
                    industry_subgroup = excluded.industry_subgroup,
                    source_url = excluded.source_url,
                    source_as_of = excluded.source_as_of,
                    raw_payload_hash = excluded.raw_payload_hash,
                    updated_at = excluded.updated_at
                """,
                [
                    item.symbol_id, item.exchange, item.security_id, item.isin,
                    item.sector, item.industry, item.industry_new, item.industry_group,
                    item.industry_subgroup, item.source, item.source_url, timestamp[:10],
                    item.raw_payload_hash, timestamp,
                ],
            )
            mapping = conn.execute(
                """
                UPDATE sector_mapping
                SET system_sector = ?, last_updated = ?
                WHERE industry = ?
                """,
                [item.sector, timestamp, item.sector],
            )
            if mapping.rowcount == 0:
                conn.execute(
                    """
                    INSERT INTO sector_mapping (industry, system_sector, last_updated)
                    VALUES (?, ?, ?)
                    """,
                    [item.sector, item.sector, timestamp],
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(values)


def _sqlite_backup(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_conn = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    destination_conn = sqlite3.connect(destination)
    try:
        source_conn.backup(destination_conn)
    finally:
        destination_conn.close()
        source_conn.close()


def create_onboarding_checkpoint(
    paths: DataDomainPaths,
    *,
    targets: Iterable[SymbolTarget],
    run_id: str,
) -> dict[str, Any]:
    """Back up every existing store this workflow can mutate directly."""
    backup_dir = paths.root_dir / "backups" / run_id
    backup_dir.mkdir(parents=True, exist_ok=False)
    files: list[str] = []
    master_backup = backup_dir / paths.master_db_path.name
    _sqlite_backup(paths.master_db_path, master_backup)
    files.append(str(master_backup))

    screener_db = paths.fundamentals_dir / "screener_financials.db"
    if screener_db.exists():
        screener_backup = backup_dir / "fundamentals" / screener_db.name
        _sqlite_backup(screener_db, screener_backup)
        files.append(str(screener_backup))

    feature_backup_root = backup_dir / "feature_store"
    for target in targets:
        for feature_type in FEATURE_TYPES:
            source = paths.feature_store_dir / feature_type / target.exchange / f"{target.symbol_id}.parquet"
            if not source.exists():
                continue
            destination = feature_backup_root / feature_type / target.exchange / source.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            files.append(str(destination))
    return {"backup_dir": str(backup_dir), "files": files}


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0]
    )


def inspect_onboarding_coverage(
    paths: DataDomainPaths,
    targets: Iterable[SymbolTarget],
    *,
    as_of: str,
) -> dict[str, Any]:
    """Return per-symbol master, OHLCV, feature, Phase-1, and fundamental coverage."""
    target_list = list(targets)
    coverage: dict[str, Any] = {}
    ohlcv = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
    master = sqlite3.connect(f"file:{paths.master_db_path}?mode=ro", uri=True)
    screener_path = paths.fundamentals_dir / "screener_financials.db"
    screener = sqlite3.connect(f"file:{screener_path}?mode=ro", uri=True) if screener_path.exists() else None
    try:
        has_phase1 = _table_exists(ohlcv, "feat_phase1_symbol_features")
        has_financials = False
        if screener is not None:
            has_financials = bool(
                screener.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
                    ["screener_financials"],
                ).fetchone()[0]
            )
        for target in target_list:
            master_row = master.execute(
                """
                SELECT COALESCE(sector, ''), COALESCE(industry, '')
                FROM symbols
                WHERE symbol_id = ? AND exchange = ?
                """,
                [target.symbol_id, target.exchange],
            ).fetchone()
            price_row = ohlcv.execute(
                """
                SELECT COUNT(*), MIN(CAST(timestamp AS DATE)), MAX(CAST(timestamp AS DATE))
                FROM _catalog
                WHERE symbol_id = ? AND exchange = ? AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
                """,
                [target.symbol_id, target.exchange, as_of],
            ).fetchone()
            phase1_rows = 0
            if has_phase1:
                phase1_rows = int(
                    ohlcv.execute(
                        """
                        SELECT COUNT(*) FROM feat_phase1_symbol_features
                        WHERE symbol_id = ? AND exchange = ? AND date <= CAST(? AS DATE)
                        """,
                        [target.symbol_id, target.exchange, as_of],
                    ).fetchone()[0]
                    or 0
                )
            fundamental_rows = 0
            if has_financials and screener is not None:
                fundamental_rows = int(
                    screener.execute(
                        "SELECT COUNT(*) FROM screener_financials WHERE UPPER(symbol) = ?",
                        [target.symbol_id],
                    ).fetchone()[0]
                    or 0
                )
            feature_files = {
                feature_type: str(
                    paths.feature_store_dir / feature_type / target.exchange / f"{target.symbol_id}.parquet"
                )
                for feature_type in FEATURE_TYPES
                if (paths.feature_store_dir / feature_type / target.exchange / f"{target.symbol_id}.parquet").exists()
            }
            sector = str(master_row[0] if master_row else "").strip()
            industry = str(master_row[1] if master_row else "").strip()
            coverage[target.symbol_id] = {
                "exchange": target.exchange,
                "sector": sector,
                "industry": industry,
                "classification_complete": (
                    sector.upper() not in UNKNOWN_CLASSIFICATIONS
                    and industry.upper() not in UNKNOWN_CLASSIFICATIONS
                ),
                "ohlcv_rows": int(price_row[0] or 0),
                "first_ohlcv_date": str(price_row[1]) if price_row[1] is not None else None,
                "last_ohlcv_date": str(price_row[2]) if price_row[2] is not None else None,
                "technical_feature_files": sorted(feature_files),
                "technical_feature_file_count": len(feature_files),
                "phase1_rows": phase1_rows,
                "fundamental_rows": fundamental_rows,
            }
    finally:
        if screener is not None:
            screener.close()
        ohlcv.close()
        master.close()
    return coverage


def _run_fundamentals(
    *,
    paths: DataDomainPaths,
    symbols: list[str],
    statement_basis: str,
    allow_download: bool,
    backup_dir: str,
) -> dict[str, Any]:
    from ai_trading_system.domains.fundamentals.screener_sync import run_sync

    return dict(
        run_sync(
            statement_basis=statement_basis,
            force=False,
            db_path=paths.fundamentals_dir / "screener_financials.db",
            master_db_path=paths.master_db_path,
            exports_dir=paths.fundamentals_dir / "exports",
            allow_download=allow_download,
            refresh_readmodels=True,
            symbols=symbols,
            valuation_migration_backup_dir=Path(backup_dir) / "screener_basis_migration",
        )
    )


def run_new_symbol_onboarding(
    *,
    project_root: str | Path,
    symbols: Iterable[str],
    from_date: str,
    to_date: str,
    apply: bool = False,
    discover_missing: bool = False,
    promote_discovered: bool = False,
    resolve_discovery_classification: bool = True,
    include_fundamentals: bool = True,
    allow_fundamentals_download: bool = False,
    statement_basis: str = "consolidated",
    max_workers: int = 4,
    profile_client: BSEProfileClient | None = None,
    active_equity_client: BSEActiveEquityClient | None = None,
    discovery_runner: Callable[..., dict[str, Any]] = discover_bse_missing_symbols,
    classification_fetcher: Callable[..., tuple[list[BSEClassification], dict[str, str]]] = fetch_bse_classifications,
    history_runner: Callable[..., dict[str, Any]] = backfill_bse_bhavcopy,
    technical_runner: Callable[..., dict[str, Any]] | None = None,
    phase1_runner: Callable[..., Any] | None = None,
    fundamentals_runner: Callable[..., dict[str, Any]] = _run_fundamentals,
) -> dict[str, Any]:
    """Run or preview the complete currently supported BSE onboarding workflow."""
    root = Path(project_root).resolve()
    paths = get_domain_paths(project_root=root, data_domain="operational")
    require_data_root_available(paths)
    if date.fromisoformat(from_date) > date.fromisoformat(to_date):
        raise ValueError("from_date must be on or before to_date")
    run_id = f"symbol-onboarding-{_utc_now().strftime('%Y%m%dT%H%M%SZ')}"
    requested_symbols = _normalized_symbols(symbols)
    discovery: dict[str, Any] | None = None
    discovered_candidates: list[dict[str, Any]] = []
    discovered_classifications: list[BSEClassification] = []
    if discover_missing:
        if apply and not promote_discovered:
            raise ValueError(
                "--discover-missing remains read-only unless --promote-discovered is also supplied"
            )
        if promote_discovered and not apply:
            raise ValueError("--promote-discovered requires --apply")
        discovery = discovery_runner(
            master_db_path=paths.master_db_path,
            symbols=requested_symbols,
            active_client=active_equity_client,
            profile_client=profile_client,
            resolve_classification=resolve_discovery_classification,
        )
        has_gaps = any(
            int(discovery.get(field, 0) or 0) > 0
            for field in (
                "conflict_count",
                "not_found_count",
                "invalid_count",
                "classification_failure_count",
            )
        )
        if not promote_discovered:
            return {
                "run_id": run_id,
                "status": "discovery_preview_with_gaps" if has_gaps else "discovery_preview",
                "apply": False,
                "discover_missing": True,
                "writes_performed": False,
                "from_date": from_date,
                "to_date": to_date,
                "symbols": requested_symbols,
                "discovery": discovery,
                "planned_next_steps": [
                    "review_candidate_identity_market_cap_group_and_classification",
                    "run_with_discover_missing_promote_discovered_and_apply",
                ],
                "known_gaps": {
                    "bse_delivery_history": "unsupported: current delivery store and collector are NSE-only",
                    "bse_corporate_action_adjustment": (
                        "unsupported: current corporate-action normalizer and adjusted-price recompute are NSE-only"
                    ),
                },
            }
        if has_gaps:
            raise ValueError("Discovery contains gaps; refusing to promote any requested symbol")
        discovered_candidates, targets, discovered_classifications = _prepare_discovered_candidates(
            discovery,
            requested_symbols,
        )
    else:
        if promote_discovered:
            raise ValueError("--promote-discovered requires --discover-missing")
        targets = load_symbol_targets(paths.master_db_path, requested_symbols)
    before_error: str | None = None
    try:
        before = inspect_onboarding_coverage(paths, targets, as_of=to_date)
    except Exception as exc:  # noqa: BLE001 - preview must remain useful during a writer lock
        before = {}
        before_error = f"{type(exc).__name__}: {exc}"
        if apply:
            raise RuntimeError(
                "Cannot verify pre-write onboarding coverage; operational stores may be locked or unavailable: "
                f"{before_error}"
            ) from exc
    report: dict[str, Any] = {
        "run_id": run_id,
        "apply": bool(apply),
        "from_date": from_date,
        "to_date": to_date,
        "symbols": [target.symbol_id for target in targets],
        "targets": [asdict(target) for target in targets],
        "before": before,
        "before_coverage_error": before_error,
        "steps": {},
        "known_gaps": {
            "bse_delivery_history": "unsupported: current delivery store and collector are NSE-only",
            "bse_corporate_action_adjustment": (
                "unsupported: current corporate-action normalizer and adjusted-price recompute are NSE-only"
            ),
        },
    }
    if discovery is not None:
        report["discover_missing"] = True
        report["discovery"] = discovery
    if not apply:
        report["status"] = "preview"
        report["planned_steps"] = [
            "checkpoint",
            "bse_official_classification",
            "bse_official_bhavcopy_history",
            "targeted_technical_features",
            "bse_phase1_features",
            "screener_fundamentals" if include_fundamentals else "screener_fundamentals:disabled",
            "verification",
        ]
        return report

    checkpoint = create_onboarding_checkpoint(paths, targets=targets, run_id=run_id)
    report["steps"]["checkpoint"] = {"status": "completed", **checkpoint}

    if discovered_candidates:
        promoted = apply_discovered_master_candidates(paths.master_db_path, discovered_candidates)
        report["steps"]["master_promotion"] = {
            "status": "completed",
            "inserted": promoted,
            "symbols": [target.symbol_id for target in targets],
        }

    if discovered_classifications:
        classifications, classification_failures = discovered_classifications, {}
    else:
        classifications, classification_failures = classification_fetcher(targets, client=profile_client)
    classified = apply_bse_classifications(paths.master_db_path, classifications)
    report["steps"]["classification"] = {
        "status": "completed" if not classification_failures else "completed_with_gaps",
        "updated": classified,
        "resolved": [asdict(item) for item in classifications],
        "failed": classification_failures,
    }

    history_ok = False
    try:
        history = history_runner(
            project_root=root,
            from_date=from_date,
            to_date=to_date,
            symbols=[target.symbol_id for target in targets],
            apply=True,
            recompute_features=False,
            max_workers=max(1, min(int(max_workers), 8)),
        )
        history_ok = int(history.get("candidate_rows", 0) or 0) > 0
        report["steps"]["history"] = {
            "status": "completed" if history_ok else "failed",
            **history,
        }
    except Exception as exc:  # noqa: BLE001 - preserve checkpoint/report for resumability
        report["steps"]["history"] = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}

    if technical_runner is None:
        from ai_trading_system.domains.features.compute_features_batch import run_batch_feature_computation

        technical_runner = run_batch_feature_computation
    if phase1_runner is None:
        from ai_trading_system.domains.features.phase1 import refresh_phase1_features

        phase1_runner = refresh_phase1_features

    if history_ok:
        try:
            technical = technical_runner(
                project_root=root,
                data_domain="operational",
                symbols=[target.symbol_id for target in targets],
                exchanges=["BSE"],
                feature_types=FEATURE_TYPES,
                full_rebuild=True,
                incremental=False,
            )
            report["steps"]["technical_features"] = {"status": "completed", **dict(technical)}
        except Exception as exc:  # noqa: BLE001
            report["steps"]["technical_features"] = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }
        try:
            phase1 = phase1_runner(ohlcv_db_path=paths.ohlcv_db_path, as_of=to_date, exchange="BSE")
            phase1_payload = phase1.to_dict() if hasattr(phase1, "to_dict") else dict(phase1)
            report["steps"]["phase1_features"] = {"status": "completed", **phase1_payload}
        except Exception as exc:  # noqa: BLE001
            report["steps"]["phase1_features"] = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }
    else:
        blocked = {"status": "blocked", "reason": "official_history_failed"}
        report["steps"]["technical_features"] = dict(blocked)
        report["steps"]["phase1_features"] = dict(blocked)

    if include_fundamentals:
        try:
            fundamentals = fundamentals_runner(
                paths=paths,
                symbols=[target.symbol_id for target in targets],
                statement_basis=statement_basis,
                allow_download=allow_fundamentals_download,
                backup_dir=checkpoint["backup_dir"],
            )
            failures = int(fundamentals.get("failed", 0) or 0)
            report["steps"]["fundamentals"] = {
                "status": "completed" if failures == 0 else "completed_with_gaps",
                **fundamentals,
            }
        except Exception as exc:  # noqa: BLE001
            report["steps"]["fundamentals"] = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }
    else:
        report["steps"]["fundamentals"] = {"status": "disabled"}

    after_error: str | None = None
    try:
        report["after"] = inspect_onboarding_coverage(paths, targets, as_of=to_date)
    except Exception as exc:  # noqa: BLE001 - persist a failed verification report
        report["after"] = {}
        after_error = f"{type(exc).__name__}: {exc}"
    report["after_coverage_error"] = after_error
    critical_failures = [
        name
        for name in ("history", "technical_features", "phase1_features")
        if report["steps"].get(name, {}).get("status") in {"failed", "blocked"}
    ]
    if after_error:
        critical_failures.append("verification")
    incomplete_symbols = [
        symbol
        for symbol, item in report["after"].items()
        if int(item.get("ohlcv_rows", 0) or 0) == 0
        or not bool(item.get("classification_complete"))
    ]
    noncritical_gaps = [
        name
        for name in ("classification", "fundamentals")
        if report["steps"].get(name, {}).get("status") in {"failed", "completed_with_gaps"}
    ]
    report["verification"] = {
        "critical_failures": critical_failures,
        "incomplete_symbols": incomplete_symbols,
        "noncritical_gaps": noncritical_gaps,
    }
    report["status"] = (
        "failed"
        if critical_failures or any(int(item.get("ohlcv_rows", 0) or 0) == 0 for item in report["after"].values())
        else "completed_with_gaps"
        if incomplete_symbols or noncritical_gaps or report["known_gaps"]
        else "completed"
    )
    report_dir = paths.reports_dir / "symbol_onboarding"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{run_id}.json"
    report["report_path"] = str(report_path)
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return report


def _symbols_from_args(symbols: list[str] | None, symbols_file: str | None) -> list[str]:
    values = list(symbols or [])
    if symbols_file:
        values.extend(Path(symbols_file).read_text(encoding="utf-8").splitlines())
    return _normalized_symbols(value for value in values if not str(value).strip().startswith("#"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover, preview, or apply supported onboarding for new BSE-only symbols.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbol", action="append", dest="symbols", help="BSE symbol_id; repeatable")
    parser.add_argument("--symbols-file", help="Text file containing one BSE symbol_id per line")
    parser.add_argument("--from-date", required=True, help="Historical backfill start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", default=date.today().isoformat(), help="Historical backfill end date")
    parser.add_argument("--apply", action="store_true", help="Create backups and mutate operational stores")
    parser.add_argument(
        "--discover-missing",
        action="store_true",
        help="Resolve missing symbols from official BSE sources; read-only unless promotion is explicit",
    )
    parser.add_argument(
        "--promote-discovered",
        action="store_true",
        help="With --discover-missing --apply, checkpoint and insert the exact validated discovery scope",
    )
    parser.add_argument(
        "--skip-discovery-classification",
        action="store_true",
        help="In discovery mode, skip per-company BSE profile classification calls",
    )
    parser.add_argument("--skip-fundamentals", action="store_true")
    parser.add_argument(
        "--allow-fundamentals-download",
        action="store_true",
        help="Allow authenticated Screener downloads; otherwise only cached exports are parsed",
    )
    parser.add_argument("--statement-basis", choices=["standalone", "consolidated"], default="consolidated")
    parser.add_argument("--max-workers", type=int, default=4, help="BSE bhavcopy download workers (1-8)")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[4]
    load_project_env(project_root)
    report = run_new_symbol_onboarding(
        project_root=project_root,
        symbols=_symbols_from_args(args.symbols, args.symbols_file),
        from_date=args.from_date,
        to_date=args.to_date,
        apply=bool(args.apply),
        discover_missing=bool(args.discover_missing),
        promote_discovered=bool(args.promote_discovered),
        resolve_discovery_classification=not bool(args.skip_discovery_classification),
        include_fundamentals=not bool(args.skip_fundamentals),
        allow_fundamentals_download=bool(args.allow_fundamentals_download),
        statement_basis=args.statement_basis,
        max_workers=args.max_workers,
    )
    print(json.dumps(report, indent=2, default=str))
    if report.get("status") == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
