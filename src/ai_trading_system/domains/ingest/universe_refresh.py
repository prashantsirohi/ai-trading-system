"""Additive, resumable monthly Screener universe onboarding before daily ingest."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
import io
import json
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
import time
from uuid import uuid4

import duckdb
import pandas as pd
import requests

from ai_trading_system.domains.ingest.bse_bhavcopy_backfill import _sha256
from ai_trading_system.platform.db.paths import (
    get_domain_paths,
    require_data_root_available,
)
from ai_trading_system.domains.ingest.new_symbol_onboarding import (
    BSEActiveEquityClient,
    BSEProfileClient,
    SymbolTarget,
    _load_master_identity_rows,
    create_onboarding_checkpoint,
    run_new_symbol_onboarding,
)

SCREEN_ID = 3553765
SCREEN_URL = "https://www.screener.in/screens/3553765/all-stock-non-sme/"
NSE_MASTER_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
DHAN_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def clean(value):
    return "" if pd.isna(value) else str(value).strip().upper().removesuffix(".0")


def read_export(path: Path) -> list[dict]:
    """Reject unidentifiable rows and missing caps; never accept a partial page."""
    raw = path.read_bytes()
    frame = (
        pd.read_excel(io.BytesIO(raw))
        if raw.startswith(b"PK")
        else pd.read_csv(io.BytesIO(raw))
    )
    frame = frame.rename(
        columns={c: re.sub(r"[^a-z0-9]", "", str(c).lower()) for c in frame.columns}
    )
    cap_col = next(
        (
            c
            for c in (
                "marketcapitalization",
                "marketcapitalizationcr",
                "marketcap",
                "mcap",
                "marcaprscr",
            )
            if c in frame
        ),
        None,
    )
    if (
        frame.empty
        or cap_col is None
        or not {"nsecode", "bsecode", "isin", "isincode"}.intersection(frame.columns)
    ):
        raise ValueError(
            "Export requires company rows, NSE Code, BSE Code or ISIN, and Market Capitalization in crore"
        )
    rows = []
    seen = set()
    for row_number, row in enumerate(frame.to_dict("records"), start=2):
        nse, bse = clean(row.get("nsecode")), clean(row.get("bsecode"))
        isin = clean(row.get("isin", row.get("isincode")))
        if not nse and not bse and not re.fullmatch(r"INE[A-Z0-9]{9}", isin):
            raise ValueError(
                f"Export row {row_number} has no listing code or valid company ISIN"
            )
        cap = float(str(row[cap_col]).replace(",", ""))
        if not pd.notna(cap) or not 0 < cap < float("inf"):
            raise ValueError("Export contains an invalid market cap")
        if cap <= 500:
            continue
        key = nse or bse or f"ISIN:{isin}"
        if key in seen:
            raise ValueError(f"Duplicate export identity: {key}")
        seen.add(key)
        rows.append(
            {
                "nse_symbol": nse,
                "bse_code": bse,
                "isin": isin,
                "mcap": cap,
            }
        )
    if not rows:
        raise ValueError("No companies above INR 500 crore in export")
    return rows


def due(last_success: str | None, today: date, cadence: str) -> bool:
    if cadence not in {"monthly", "28-days"}:
        raise ValueError("Unsupported cadence")
    if not last_success:
        return True
    previous = date.fromisoformat(last_success)
    if today < previous:
        raise ValueError("Refresh date precedes last successful refresh")
    return (
        (today.year, today.month) != (previous.year, previous.month)
        if cadence == "monthly"
        else today >= previous + timedelta(days=28)
    )


def load_sources() -> dict:
    def csv(url):
        response = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        frame = pd.read_csv(io.BytesIO(response.content), dtype=str).fillna("")
        frame.columns = frame.columns.str.strip()
        return frame.to_dict("records")

    bse, _ = BSEActiveEquityClient().fetch()
    sources = {"nse": csv(NSE_MASTER_URL), "bse": bse, "dhan": csv(DHAN_MASTER_URL)}
    required = {
        "nse": {"SYMBOL", "ISIN NUMBER", "SERIES"},
        "bse": {"SCRIP_CD", "ISIN_NUMBER", "scrip_id"},
        "dhan": {"SEM_EXM_EXCH_ID", "SEM_TRADING_SYMBOL", "SEM_SMST_SECURITY_ID"},
    }
    for name, columns in required.items():
        if not sources[name] or not columns.issubset(sources[name][0]):
            raise ValueError(
                f"{name} identity source is empty or has an unexpected schema"
            )
    return sources


def plan_additions(rows: list[dict], master: list[dict], sources: dict) -> dict:
    additions, existing, blocked, excluded = [], [], [], []
    resolved_isins = set()
    for row in rows:
        try:
            nse, bse = row["nse_symbol"], row["bse_code"]
            if not nse and not bse:
                nse_matches = [
                    r
                    for r in sources["nse"]
                    if clean(r.get("ISIN NUMBER")) == row["isin"]
                ]
                bse_matches = [
                    r
                    for r in sources["bse"]
                    if clean(r.get("ISIN_NUMBER")) == row["isin"]
                ]
                if len(nse_matches) > 1 or len(bse_matches) > 1:
                    raise ValueError("ambiguous official listing for export ISIN")
                if not nse_matches and not bse_matches:
                    excluded.append(
                        {
                            "identity": row["isin"],
                            "reason": "no_active_exchange_listing",
                            "mcap": row["mcap"],
                        }
                    )
                    continue
                nse = clean(nse_matches[0].get("SYMBOL")) if nse_matches else ""
                bse = clean(bse_matches[0].get("SCRIP_CD")) if bse_matches else ""
            # A BSE-only export identifier can still refer to an NSE listing.
            if not nse and bse:
                bse_identity = [
                    r for r in sources["bse"] if clean(r.get("SCRIP_CD")) == bse
                ]
                if len(bse_identity) == 1:
                    official_nse = [
                        r
                        for r in sources["nse"]
                        if clean(r.get("ISIN NUMBER"))
                        == clean(bse_identity[0].get("ISIN_NUMBER"))
                    ]
                    if len(official_nse) > 1:
                        raise ValueError("ambiguous official NSE listing for ISIN")
                    if official_nse:
                        nse = clean(official_nse[0].get("SYMBOL"))
            matches = (
                [r for r in sources["nse"] if clean(r.get("SYMBOL")) == nse]
                if nse
                else [r for r in sources["bse"] if clean(r.get("SCRIP_CD")) == bse]
            )
            if not matches and nse and bse and row["isin"]:
                bse_fallback = [
                    r
                    for r in sources["bse"]
                    if clean(r.get("SCRIP_CD")) == bse
                    and clean(r.get("ISIN_NUMBER")) == row["isin"]
                ]
                if len(bse_fallback) == 1:
                    nse, matches = "", bse_fallback
            if len(matches) != 1:
                raise ValueError("official listing missing or ambiguous")
            official = matches[0]
            isin = clean(
                official.get("ISIN NUMBER") if nse else official.get("ISIN_NUMBER")
            )
            if not re.fullmatch(r"INE[A-Z0-9]{9}", isin):
                raise ValueError("not an identified Indian company equity")
            if row["isin"] and row["isin"] != isin:
                raise ValueError("export/official ISIN conflict")
            if nse and clean(official.get("SERIES")) not in {"EQ", "BE", "BZ"}:
                raise ValueError("not a supported NSE main-board series")
            if not nse and (
                clean(official.get("Status")) != "ACTIVE"
                or clean(official.get("GROUP", official.get("Group")))
                in {"M", "MT", "MS", "TS", ""}
            ):
                raise ValueError("not a verified active BSE non-SME equity")
            symbol = nse or clean(official.get("scrip_id"))
            if not re.fullmatch(r"[A-Z0-9&_.-]+", symbol):
                raise ValueError("invalid canonical symbol")
            collisions = [
                m
                for m in master
                if m["isin"] == isin or symbol in {m["symbol_id"], m["bse_symbol"]}
            ]
            if collisions:
                if len(collisions) != 1 or collisions[0]["isin"] != isin:
                    raise ValueError(
                        "master identity collision; manual reconciliation required"
                    )
                existing.append(collisions[0]["symbol_id"])
                continue
            if isin in resolved_isins:
                raise ValueError("duplicate security across export listings")
            resolved_isins.add(isin)
            bse_matches = [
                r for r in sources["bse"] if clean(r.get("ISIN_NUMBER")) == isin
            ]
            if len(bse_matches) != 1:
                raise ValueError(
                    "unique BSE identity required for official sector classification"
                )
            bse_row = bse_matches[0]
            if bse and clean(bse_row.get("SCRIP_CD")) != bse:
                raise ValueError("BSE code/ISIN conflict")
            security_id = clean(bse_row.get("SCRIP_CD"))
            if nse:
                dhan = [
                    r
                    for r in sources["dhan"]
                    if clean(r.get("SEM_EXM_EXCH_ID")) == "NSE"
                    and clean(r.get("SEM_TRADING_SYMBOL")) == nse
                    and clean(r.get("SEM_SEGMENT")) == "E"
                    and clean(r.get("SEM_SERIES")) == clean(official.get("SERIES"))
                    and clean(r.get("SEM_EXCH_INSTRUMENT_TYPE")) == "ES"
                ]
                if len(dhan) != 1:
                    raise ValueError("unique Dhan NSE security mapping unavailable")
                security_id = clean(dhan[0].get("SEM_SMST_SECURITY_ID"))
            if not security_id.isdigit():
                raise ValueError("missing numeric security ID")
            if any(
                m["exchange"] == ("NSE" if nse else "BSE")
                and m["security_id"] == security_id
                for m in master
            ):
                raise ValueError("master security ID collision")
            additions.append(
                {
                    **row,
                    "symbol_id": symbol,
                    "nse_symbol": nse,
                    "isin": isin,
                    "exchange": "NSE" if nse else "BSE",
                    "security_id": security_id,
                    "bse_code": clean(bse_row["SCRIP_CD"]),
                    "bse_symbol": clean(bse_row["scrip_id"]),
                    "symbol_name": clean(bse_row.get("Scrip_Name")),
                    "listing_date": (
                        clean(official.get("DATE OF LISTING")) if nse else ""
                    ),
                }
            )
        except ValueError as exc:
            blocked.append(
                {
                    "identity": row["nse_symbol"] or row["bse_code"] or row["isin"],
                    "reason": str(exc),
                    "source_row": dict(row),
                }
            )
    return {
        "additions": additions,
        "existing": existing,
        "blocked": blocked,
        "excluded": excluded,
    }


def promote_nse(path: Path, item: dict):
    with sqlite3.connect(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        collision = conn.execute(
            "SELECT 1 FROM symbols WHERE UPPER(symbol_id)=? OR UPPER(isin)=? OR (exchange='NSE' AND security_id=?)",
            [item["symbol_id"], item["isin"], item["security_id"]],
        ).fetchone()
        if collision:
            raise ValueError("Master changed since discovery; retry refresh")
        conn.execute(
            "INSERT INTO symbols (symbol_id,security_id,symbol_name,exchange,instrument_type,isin,lot_size,sector,industry,nse_symbol,bse_symbol,mcap,last_updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                item["symbol_id"],
                item["security_id"],
                item["symbol_name"],
                "NSE",
                "EQUITY",
                item["isin"],
                1,
                item["sector"],
                item["industry"],
                item["symbol_id"],
                item["bse_symbol"],
                item["mcap"],
                datetime.now(timezone.utc).isoformat(),
            ],
        )


class CompanyOnboardingError(RuntimeError):
    """A supported-data gap isolated to one company; keep it out of consumers."""


def transient_failure(exc: Exception) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError, TimeoutError)):
        return True
    status = getattr(exc, "status", None) or getattr(
        getattr(exc, "response", None), "status_code", None
    )
    return status == 429 or (isinstance(status, int) and 500 <= status < 600)


def retry_due(item: dict, as_of: date, force: bool) -> bool:
    retry_after = item.get("onboarding_failure", {}).get("retry_after")
    return force or not retry_after or as_of >= date.fromisoformat(retry_after)


def onboard_nse(
    root: Path,
    paths,
    item: dict,
    start: str,
    end: str,
    backup: str,
    *,
    progress_callback=None,
) -> dict:
    from ai_trading_system.domains.ingest.repair import repair_window
    from ai_trading_system.domains.ingest.corporate_actions import (
        fetch_nse_corporate_actions,
        parse_corporate_action,
        upsert_corporate_actions,
        recompute_adjusted_prices,
    )
    from ai_trading_system.domains.ingest.delivery import DeliveryCollector
    from ai_trading_system.domains.features.compute_features_batch import (
        run_batch_feature_computation,
    )
    from ai_trading_system.domains.features.phase1 import refresh_phase1_features
    from ai_trading_system.domains.ingest.bse_bhavcopy_backfill import FEATURE_TYPES
    from ai_trading_system.domains.ingest.new_symbol_onboarding import _run_fundamentals

    progress = progress_callback or (lambda detail: None)
    symbol = item["symbol_id"]
    if item.get("listing_date"):
        start = max(start, pd.to_datetime(item["listing_date"]).date().isoformat())
    if start > end:
        raise ValueError("Listing is after backfill cutoff")
    progress("Backfilling official NSE OHLCV")
    history = repair_window(
        project_root=root,
        from_date=start,
        to_date=end,
        symbols=[symbol],
        apply_changes=True,
        recompute_features=False,
    )
    if history["repair_status"] != "completed" or not any(
        r["api_rows"] for r in history["symbols"]
    ):
        raise CompanyOnboardingError("Official NSE history incomplete")
    progress("Backfilling corporate actions and adjusted prices")
    raw_actions = fetch_nse_corporate_actions(
        start_date=date.fromisoformat(start), end_date=date.fromisoformat(end)
    )
    actions = []
    for raw in raw_actions:
        if clean(raw.get("symbol")) != symbol:
            continue
        if clean(raw.get("isin")) and clean(raw.get("isin")) != item["isin"]:
            raise CompanyOnboardingError("Corporate action ISIN conflict")
        parsed = parse_corporate_action(raw)
        if parsed is not None:
            actions.append(parsed)
        elif any(
            term in str(raw.get("subject", "")).lower()
            for term in ("split", "bonus", "face value")
        ):
            raise CompanyOnboardingError("Unparseable split/bonus action")
    upsert_corporate_actions(paths.ohlcv_db_path, actions)
    recompute_adjusted_prices(paths.ohlcv_db_path, symbols=[symbol])
    progress("Backfilling delivery history")
    delivery = DeliveryCollector(
        ohlcv_db_path=str(paths.ohlcv_db_path),
        feature_store_dir=str(paths.feature_store_dir),
        raw_dir=str(paths.raw_dir / "NSE_MTO"),
        masterdb_path=str(paths.master_db_path),
        source="nse_securitywise",
    )
    delivery_rows = delivery.fetch_range(start, end, symbols=[symbol])
    if delivery_rows == 0:
        raise CompanyOnboardingError("Delivery backfill returned no rows")
    delivery.compute_delivery_features()
    progress("Rebuilding technical indicators")
    technical = run_batch_feature_computation(
        project_root=root,
        data_domain="operational",
        symbols=[symbol],
        exchanges=["NSE"],
        feature_types=FEATURE_TYPES,
        full_rebuild=True,
        incremental=False,
    )
    if technical["rows_written_total"] == 0:
        raise CompanyOnboardingError("Technical rebuild returned no rows")
    progress("Refreshing Phase 1 features")
    refresh_phase1_features(
        ohlcv_db_path=paths.ohlcv_db_path, as_of=end, exchange="NSE"
    )
    progress("Syncing fundamental history")
    fundamentals = []
    for basis in ("standalone", "consolidated"):
        result = _run_fundamentals(
            paths=paths,
            symbols=[symbol],
            statement_basis=basis,
            allow_download=True,
            backup_dir=backup,
        )
        if int(result.get("failed", 0)):
            raise CompanyOnboardingError("Fundamentals sync failed")
        fundamentals.append(result)
    from ai_trading_system.domains.ingest.new_symbol_onboarding import (
        inspect_onboarding_coverage,
    )

    progress("Verifying backfill coverage")
    coverage = inspect_onboarding_coverage(
        paths,
        [
            SymbolTarget(
                symbol,
                item["security_id"],
                "NSE",
                item["isin"],
                item["symbol_name"],
                item["sector"],
                item["industry"],
            )
        ],
        as_of=end,
    )[symbol]
    if (
        not coverage["fundamental_rows"]
        or not coverage["phase1_rows"]
        or not coverage["technical_feature_file_count"]
    ):
        raise CompanyOnboardingError(
            "Post-backfill feature/fundamental verification failed"
        )
    return {
        "status": "completed",
        "coverage": coverage,
        "history": history,
        "delivery_rows": delivery_rows,
        "technical": technical,
        "fundamentals": fundamentals,
    }


def write_json(path: Path, payload: dict):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2, default=str))
    temporary.replace(path)


@contextmanager
def refresh_lock(directory: Path):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "refresh.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another universe refresh is running") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def load_negative_list(project_root: Path) -> list[dict]:
    path = Path(project_root) / "configs" / "universe_refresh_negative_list.json"
    if not path.exists():
        return []
    payload = json.loads(path.read_text())
    if payload.get("version") != 1 or not isinstance(payload.get("entries"), list):
        raise ValueError("Invalid universe refresh negative list")
    entries = payload["entries"]
    seen = set()
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("reason"):
            raise ValueError("Negative-list entries require a reason")
        key = tuple(entry.get(k) for k in ("nse_symbol", "bse_code", "isin"))
        if (
            not all(isinstance(value, str) for value in key)
            or not any(key)
            or key in seen
        ):
            raise ValueError("Invalid or duplicate negative-list identity")
        seen.add(key)
    return entries


def negative_match(row: dict, entries: list[dict]) -> dict | None:
    # Exact identifiers, ignoring market cap. A changed identity requires fresh
    # validation rather than inheriting the exclusion of another security.
    return next(
        (
            entry
            for entry in entries
            if all(
                clean(row.get(key, "")) == clean(entry[key])
                for key in ("nse_symbol", "bse_code", "isin")
            )
        ),
        None,
    )


def pending_negative_match(item: dict, entries: list[dict]) -> bool:
    return any(
        (not entry["isin"] or entry["isin"] == item["isin"])
        and (
            (entry["nse_symbol"] and entry["nse_symbol"] == item["symbol_id"])
            or (entry["bse_code"] and entry["bse_code"] == item["bse_code"])
        )
        for entry in entries
    )


def run_refresh(
    *,
    project_root: Path,
    as_of: date,
    cadence="monthly",
    apply=False,
    export: Path | None = None,
    force=False,
    lookback_years=5,
    source_loader=load_sources,
    nse_runner=onboard_nse,
    bse_runner=run_new_symbol_onboarding,
    profile_client=None,
    screener_client=None,
    progress_callback=None,
) -> dict:
    completed, total = 0, 100

    def progress(detail, *, status="running"):
        if progress_callback is not None:
            progress_callback(
                {
                    "detail": detail,
                    "status": status,
                    "completed": completed,
                    "total": total,
                }
            )

    progress("Checking monthly universe refresh cadence")
    if apply and as_of != date.today():
        raise ValueError("Current universe exports can only be applied for today")
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    require_data_root_available(paths)
    if not 1 <= lookback_years <= 20:
        raise ValueError("lookback_years must be 1..20")
    directory = paths.stage_store_dir / "universe_refresh"
    # Preview takes no persistent lock and writes only within a temporary directory.
    with tempfile.TemporaryDirectory(prefix="universe-refresh-") as temp:
        with refresh_lock(directory if apply else Path(temp)):
            state_path = directory / "state.json"
            state = (
                json.loads(state_path.read_text())
                if state_path.exists()
                else {"pending": {}, "last_success": None}
            )
            negative_list = load_negative_list(project_root)
            retry_pending = any(
                not pending_negative_match(item, negative_list)
                and retry_due(item, as_of, force)
                for item in state["pending"].values()
            )
            retry_discovery = any(
                not negative_match(item.get("source_row", {}), negative_list)
                for item in state.get("discovery_quarantine", {}).values()
            )
            if (
                not force
                and not retry_pending
                and not retry_discovery
                and not due(state["last_success"], as_of, cadence)
            ):
                progress(
                    "Universe refresh not due; continuing daily ingest", status="ok"
                )
                return {
                    "status": "not_due",
                    "negative_list_count": len(negative_list),
                    "quarantined_symbols": sorted(state["pending"]),
                    "last_success": state["last_success"],
                    "updated_symbols": (
                        state.get("updated_symbols", [])
                        if state["last_success"] == as_of.isoformat()
                        else []
                    ),
                }
            progress(
                "Downloading complete Screener universe export"
                if export is None
                else "Reading supplied universe export"
            )
            if export is None:
                from ai_trading_system.domains.fundamentals.screener_client import (
                    ScreenerClient,
                )

                client = screener_client or ScreenerClient(
                    exports_dir=Path(temp), storage_state_path=Path(temp) / "auth.json"
                )
                result = client.download_screen_export(
                    SCREEN_ID,
                    destination=Path(temp) / "screen.xlsx",
                    screen_url=SCREEN_URL,
                    expected_query_terms=("Market Capitalization",),
                )
                export = result.path
            all_rows = read_export(export)
            negative_rows = [
                dict(entry, source_row=row)
                for row in all_rows
                if (entry := negative_match(row, negative_list))
            ]
            rows = [row for row in all_rows if not negative_match(row, negative_list)]
            completed = 5
            progress("Resolving official NSE/BSE identities")
            sources = source_loader()
            completed = 10
            progress("Comparing companies with master database")
            plan = plan_additions(
                rows, _load_master_identity_rows(paths.master_db_path), sources
            )
            report = {
                "status": "preview",
                "as_of": as_of.isoformat(),
                "history_cutoff": (as_of - timedelta(days=1)).isoformat(),
                "cadence": cadence,
                "screen_url": SCREEN_URL,
                "export_sha256": hashlib.sha256(export.read_bytes()).hexdigest(),
                **plan,
                "results": {},
                "negative_list": negative_rows,
                "negative_list_count": len(negative_rows),
            }
            progress(
                f"Comparison complete: {len(plan['additions'])} new, {len(plan['existing'])} existing, {len(plan['blocked'])} blocked, {len(negative_rows)} saved exclusions, {len(plan['excluded'])} without active listing"
            )
            if not apply:
                report["pending"] = state["pending"]
                return report
            run_id = (
                "universe-refresh-"
                + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
                + "-"
                + uuid4().hex[:8]
            )
            run_dir = directory / run_id
            run_dir.mkdir()
            shutil.copy2(export, run_dir / "screen.export")
            write_json(run_dir / "plan.json", report)
            write_json(run_dir / "report.json", report)
            write_json(run_dir / "identity_sources.json", sources)
            previous_quarantine = state.get("discovery_quarantine", {})
            timestamp = datetime.now(timezone.utc).isoformat()
            state["discovery_quarantine"] = {
                item["identity"]: {
                    **item,
                    "first_seen": previous_quarantine.get(item["identity"], {}).get(
                        "first_seen", timestamp
                    ),
                    "last_seen": timestamp,
                    "attempts": int(
                        previous_quarantine.get(item["identity"], {}).get("attempts", 0)
                    )
                    + 1,
                }
                for item in plan["blocked"]
            }
            report["discovery_quarantine"] = state["discovery_quarantine"]
            screened_identities = {
                row["nse_symbol"] or row["bse_code"] or row["isin"] for row in rows
            }
            report["quarantine_closed"] = [
                {
                    "identity": identity,
                    "reason": (
                        "saved_negative_list"
                        if negative_match(
                            previous_quarantine[identity].get("source_row", {}),
                            negative_list,
                        )
                        else (
                            "resolved_in_current_discovery"
                            if identity in screened_identities
                            else "no_longer_in_screen"
                        )
                    ),
                }
                for identity in previous_quarantine
                if identity not in state["discovery_quarantine"]
            ]
            report["report_path"] = str(run_dir / "report.json")
            if plan["blocked"]:
                progress(
                    f"Quarantined {len(plan['blocked'])} unresolved discovery identities; continuing validated additions"
                )
            # Save pending identities BEFORE inserting any master rows; retries survive a crash.
            for item in plan["additions"]:
                state["pending"].setdefault(item["symbol_id"], item)
            if state.get("updated_on") != as_of.isoformat():
                state["updated_on"] = as_of.isoformat()
                state["updated_symbols"] = []
            write_json(state_path, state)
            start = (as_of - timedelta(days=365 * lookback_years)).isoformat()
            # Do not retry a prior partially onboarded identity while discovery
            # now disputes it. Retain its checkpoint, but defer its mutation.
            blocked_rows = [item["source_row"] for item in plan["blocked"]] + [
                item["source_row"] for item in negative_rows
            ]
            deferred = {
                symbol
                for symbol, item in state["pending"].items()
                if not retry_due(item, as_of, force)
                or pending_negative_match(item, negative_list)
                or any(
                    (row["isin"] and row["isin"] == item["isin"])
                    or (row["nse_symbol"] and row["nse_symbol"] == symbol)
                    or (row["bse_code"] and row["bse_code"] == item["bse_code"])
                    for row in blocked_rows
                )
            }
            report["deferred_pending"] = sorted(deferred)
            pending_items = [
                item
                for symbol, item in state["pending"].items()
                if symbol not in deferred
            ]
            completed = 15
            progress(f"Backing up stores for {len(pending_items)} pending companies")
            checkpoint = None
            if pending_items:
                targets = [
                    SymbolTarget(
                        item["symbol_id"],
                        item["security_id"],
                        item["exchange"],
                        item["isin"],
                        item["symbol_name"],
                        "",
                        "",
                    )
                    for item in pending_items
                ]
                checkpoint = create_onboarding_checkpoint(
                    paths, targets=targets, run_id=run_id
                )
                backup_dir = Path(checkpoint["backup_dir"])
                for db in [
                    paths.ohlcv_db_path,
                    paths.fundamentals_dir / "fundamentals.duckdb",
                ]:
                    if db.exists():
                        with duckdb.connect(str(db)) as conn:
                            conn.execute("CHECKPOINT")
                        destination = backup_dir / db.name
                        shutil.copy2(db, destination)
                        if _sha256(db) != _sha256(destination):
                            raise RuntimeError("Backup checksum mismatch")
                delivery_dir = paths.feature_store_dir / "delivery" / "NSE"
                if delivery_dir.exists():
                    shutil.copytree(delivery_dir, backup_dir / "delivery_features")
                for filename in (
                    "fundamental_scores_latest.csv",
                    "fundamental_trends_latest.csv",
                ):
                    source = paths.fundamentals_dir / filename
                    if source.exists():
                        shutil.copy2(source, backup_dir / filename)
                report["checkpoint"] = checkpoint
            completed = 20
            for index, (symbol, item) in enumerate(
                [
                    (symbol, item)
                    for symbol, item in state["pending"].items()
                    if symbol not in deferred
                ],
                1,
            ):

                def symbol_progress(detail):
                    progress(
                        f"[{index}/{len(pending_items)}] {symbol} ({item['exchange']}) · {detail}"
                    )

                symbol_progress("Resolving classification")
                for attempt in range(1, 4):
                    try:
                        target = SymbolTarget(
                            symbol,
                            item["bse_code"],
                            "BSE",
                            item["isin"],
                            item["symbol_name"],
                            "",
                            "",
                        )
                        classification = (profile_client or BSEProfileClient()).fetch(
                            target
                        )
                        item.update(
                            sector=classification.sector,
                            industry=classification.industry,
                        )
                        report["results"][symbol] = {
                            "classification": asdict(classification)
                        }
                        current = [
                            m
                            for m in _load_master_identity_rows(paths.master_db_path)
                            if m["symbol_id"] == symbol
                        ]
                        if current and (
                            len(current) != 1
                            or current[0]["isin"] != item["isin"]
                            or current[0]["exchange"] != item["exchange"]
                            or current[0]["security_id"] != item["security_id"]
                        ):
                            raise ValueError(
                                "Pending identity conflicts with current master"
                            )
                        if item["exchange"] == "BSE":
                            result = bse_runner(
                                project_root=project_root,
                                symbols=[symbol],
                                from_date=start,
                                to_date=report["history_cutoff"],
                                apply=True,
                                discover_missing=not current,
                                promote_discovered=not current,
                                allow_fundamentals_download=True,
                                progress_callback=symbol_progress,
                            )
                            report["results"][symbol].update(result)
                            verification = result.get("verification", {})
                            if verification.get("critical_failures"):
                                raise RuntimeError(
                                    f"BSE backfill infrastructure/validation failure: {verification['critical_failures']}"
                                )
                            if (
                                not result.get("after", {})
                                .get(symbol, {})
                                .get("fundamental_rows")
                                or not result.get("after", {})
                                .get(symbol, {})
                                .get("technical_feature_file_count")
                                or not result.get("after", {})
                                .get(symbol, {})
                                .get("phase1_rows")
                                or result["status"] == "failed"
                                or verification.get("incomplete_symbols")
                                or verification.get("noncritical_gaps")
                                or verification.get("critical_failures")
                            ):
                                raise CompanyOnboardingError(
                                    "BSE onboarding has unresolved supported-data gaps"
                                )
                        else:
                            if not current:
                                promote_nse(paths.master_db_path, item)
                            result = nse_runner(
                                project_root,
                                paths,
                                item,
                                start,
                                report["history_cutoff"],
                                checkpoint["backup_dir"],
                                progress_callback=symbol_progress,
                            )
                            if result.get("status") != "completed":
                                raise CompanyOnboardingError(
                                    "NSE onboarding did not complete"
                                )
                        symbol_progress("Verification complete")
                        report["results"][symbol].update(result)
                        state["updated_symbols"] = sorted(
                            set(state.get("updated_symbols", [])) | {symbol}
                        )
                        del state["pending"][symbol]
                        break
                    except Exception as exc:
                        transient = transient_failure(exc)
                        if transient and attempt < 3:
                            symbol_progress(
                                f"Temporary failure; retry {attempt + 1}/3: {exc}"
                            )
                            time.sleep(2**attempt)
                            continue
                        isolated = transient or isinstance(exc, CompanyOnboardingError)
                        failure = {
                            "error": f"{type(exc).__name__}: {exc}",
                            "attempts": attempt,
                            "last_attempt": as_of.isoformat(),
                            "retry_after": (
                                as_of + timedelta(days=1 if transient else 28)
                            ).isoformat(),
                            "transient": transient,
                        }
                        if isolated:
                            state["pending"][symbol]["onboarding_failure"] = failure
                        report["results"].setdefault(symbol, {}).update(
                            status="quarantined" if isolated else "failed", **failure
                        )
                        break
                completed = 20 + 75 * index / len(pending_items)
                symbol_progress(report["results"][symbol]["status"])
                write_json(state_path, state)
                write_json(run_dir / "report.json", report)
                if report["results"][symbol]["status"] == "failed":
                    break
            report["quarantined_symbols"] = sorted(state["pending"])
            report["status"] = (
                "failed"
                if any(r.get("status") == "failed" for r in report["results"].values())
                else (
                    "completed_with_gaps"
                    if state["pending"]
                    or plan["blocked"]
                    or negative_rows
                    or any(r.get("known_gaps") for r in report["results"].values())
                    else "completed"
                )
            )
            progress("Verifying universe refresh results")
            report["updated_symbols"] = state.get("updated_symbols", [])
            if report["status"] != "failed":
                state["last_success"] = as_of.isoformat()
                state["updated_symbols"] = report["updated_symbols"]
            write_json(state_path, state)
            write_json(run_dir / "report.json", report)
            completed = total
            progress(
                f"Universe refresh {report['status']}: {len(report['updated_symbols'])} companies onboarded",
                status="failed" if report["status"] == "failed" else "ok",
            )
            return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    parser.add_argument("--cadence", choices=["monthly", "28-days"], default="monthly")
    parser.add_argument("--lookback-years", type=int, default=5)
    parser.add_argument("--screen-export", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[4]
    result = run_refresh(
        project_root=root,
        as_of=args.as_of,
        cadence=args.cadence,
        apply=args.apply,
        force=args.force,
        export=args.screen_export,
        lookback_years=args.lookback_years,
    )
    print(json.dumps(result, indent=2, default=str))
    if result["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
