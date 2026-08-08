"""Application service for imports, reconstruction, and reconciliation."""

from __future__ import annotations

import hashlib
import sqlite3
from collections import defaultdict, deque
from dataclasses import asdict
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from ai_trading_system.platform.db.paths import get_domain_paths

from .analysis_engine import JournalAnalysisEngine
from .config import JournalAnalyticsConfig
from .identity import canonical_json, is_valid_isin, stable_id
from .enrichment import JournalMarketDataReader
from .importers import DhanHoldingsParser, DhanTradebookParser
from .models import ImportResult, ParseResult, ParsedFill, ParsedHolding, SnapshotMode
from .store import TradeJournalStore, rows_as_dicts, utc_now

LOGIC_VERSION = "trade-journal-v2"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json(value: Any) -> str:
    return canonical_json(value)


def _money(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


class TradeJournalService:
    def __init__(self, store: TradeJournalStore):
        self.store = store

    def preview_tradebook(self, path: Path) -> dict[str, Any]:
        parsed = DhanTradebookParser().parse(path)
        return self._preview(path, parsed)

    def preview_holdings(self, path: Path) -> dict[str, Any]:
        parsed = DhanHoldingsParser().parse(path)
        return self._preview(path, parsed)

    @staticmethod
    def _preview(path: Path, parsed: ParseResult) -> dict[str, Any]:
        fills = [r for r in parsed.records if isinstance(r, ParsedFill)]
        orders: defaultdict[tuple[str, date, str], int] = defaultdict(int)
        for row in fills:
            orders[(row.exchange, row.trade_date, row.order_id)] += 1
        return {
            "file_sha256": file_sha256(path), "file_type": parsed.file_type,
            "format_version": parsed.format_version, "rows": len(parsed.records),
            "metadata": parsed.metadata, "issues": list(parsed.issues),
            "summary": {
                "orders": len(orders),
                "symbols": len({row.symbol for row in fills}),
                "buy_fills": sum(row.side == "buy" for row in fills),
                "sell_fills": sum(row.side == "sell" for row in fills),
                "multi_fill_orders": sum(count > 1 for count in orders.values()),
                "max_fills_per_order": max(orders.values(), default=0),
            },
        }

    def import_tradebook(
        self, *, path: Path, broker: str, account_ref: str,
        expected_sha256: str | None = None,
    ) -> ImportResult:
        parsed = DhanTradebookParser().parse(path)
        sha = file_sha256(path)
        self._check_expected(sha, expected_sha256)
        return self._commit_tradebook(parsed, sha, broker, account_ref)

    def import_holdings(
        self, *, path: Path, broker: str, account_ref: str, as_of: date,
        captured_at: datetime | None = None, market_state: str = "eod",
        mode: SnapshotMode = "reconciliation_only", expected_sha256: str | None = None,
    ) -> ImportResult:
        parsed = DhanHoldingsParser().parse(path)
        sha = file_sha256(path)
        self._check_expected(sha, expected_sha256)
        return self._commit_holdings(
            parsed, sha, broker, account_ref, as_of, captured_at, market_state, mode
        )

    @staticmethod
    def _check_expected(actual: str, expected: str | None) -> None:
        if expected is not None and actual != expected:
            raise ValueError("uploaded file does not match preview SHA-256")

    def _existing_import(self, broker: str, account: str, file_type: str, sha: str) -> dict[str, Any] | None:
        with self.store.reader() as conn:
            cursor = conn.execute(
                "SELECT * FROM journal_import_file WHERE broker=? AND account_ref=? AND file_type=? AND file_sha256=?",
                [broker, account, file_type, sha],
            )
            rows = rows_as_dicts(cursor)
            return rows[0] if rows else None

    def _master_identities(
        self, symbols: set[str]
    ) -> dict[str, tuple[str, str, str]]:
        """Resolve unambiguous symbols from the operational master, read-only."""
        if not symbols:
            return {}
        master_path = get_domain_paths(
            self.store.project_root, data_domain="operational"
        ).master_db_path
        if not master_path.is_file():
            return {}
        placeholders = ",".join("?" for _ in symbols)
        values = sorted(symbols)
        master = sqlite3.connect(f"file:{master_path}?mode=ro", uri=True)
        try:
            rows = master.execute(
                f"""SELECT symbol_id,nse_symbol,bse_symbol,isin,exchange FROM symbols
                    WHERE upper(symbol_id) IN ({placeholders})
                       OR upper(nse_symbol) IN ({placeholders})
                       OR upper(bse_symbol) IN ({placeholders})""",
                [*values, *values, *values],
            ).fetchall()
        finally:
            master.close()
        candidates: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
        for symbol_id, nse_symbol, bse_symbol, isin, exchange in rows:
            if not is_valid_isin(isin):
                continue
            for candidate in (symbol_id, nse_symbol, bse_symbol):
                normalized = str(candidate or "").strip().upper()
                if normalized in symbols:
                    candidates[normalized].add(
                        (str(isin).upper(), str(exchange or "NSE").upper(), str(symbol_id))
                    )
        resolved: dict[str, tuple[str, str, str]] = {}
        for symbol, matches in candidates.items():
            isins = {match[0] for match in matches}
            if len(isins) == 1:
                resolved[symbol] = sorted(matches)[0]
        return resolved

    def _record_provenance(
        self, *, parsed: ParseResult, sha: str, broker: str, account: str,
        mode: str, as_of: date | None = None, captured_at: datetime | None = None,
        market_state: str | None = None,
    ) -> tuple[str, str]:
        import_id = stable_id("imp", broker, account, parsed.file_type, sha)
        run_id = stable_id("irun", import_id, utc_now().isoformat())
        now = utc_now()
        with self.store.writer() as conn:
            conn.execute(
                """INSERT INTO journal_import_file(import_id,file_sha256,broker,account_ref,file_type,import_mode,
                   detected_format,detected_from,detected_to,as_of_date,captured_at,market_state,status,row_count,
                   normalized_count,canonical_snapshot,metadata_json,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(import_id) DO UPDATE SET status='PARSING',error_summary=NULL,completed_at=NULL""",
                [import_id, sha, broker.lower(), account, parsed.file_type, mode,
                 parsed.format_version, parsed.metadata.get("detected_from"),
                 parsed.metadata.get("detected_to"), as_of, captured_at, market_state,
                 "PARSING", len(parsed.raw_rows), 0, True, _json(parsed.metadata), now],
            )
            conn.execute(
                "INSERT INTO journal_import_run VALUES (?,?,?,?,?,?,?,?,?)",
                [run_id, import_id, "RUNNING", LOGIC_VERSION, None, None, now, None, None],
            )
            for row in parsed.raw_rows:
                conn.execute(
                    "INSERT INTO journal_raw_row VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    [stable_id("raw", import_id, row.sheet, row.row_number), import_id,
                     row.sheet, row.row_number, row.row_hash, _json(row.values), now],
                )
        return import_id, run_id

    def _fail_import(self, import_id: str, run_id: str, exc: Exception) -> None:
        summary = f"{exc.__class__.__name__}: {str(exc)[:500]}"
        with self.store.writer() as conn:
            conn.execute(
                "UPDATE journal_import_file SET status='FAILED',error_summary=?,completed_at=? WHERE import_id=?",
                [summary, utc_now(), import_id],
            )
            conn.execute(
                "UPDATE journal_import_run SET status='FAILED',error_summary=?,completed_at=? WHERE import_run_id=?",
                [summary, utc_now(), run_id],
            )

    def _insert_issue(
        self, conn: Any, *, account: str, issue_type: str, severity: str,
        evidence: dict[str, Any], import_id: str | None = None,
        analysis_run_id: str | None = None, entity_type: str | None = None,
        entity_id: str | None = None, row_number: int | None = None,
    ) -> None:
        issue_id = stable_id("dq", import_id, analysis_run_id, issue_type, entity_id, row_number, evidence)
        conn.execute(
            """INSERT INTO journal_dq_issue(issue_id,import_id,analysis_run_id,account_ref,severity,
               issue_type,entity_type,entity_id,source_row_number,evidence_json,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
            [issue_id, import_id, analysis_run_id, account, severity, issue_type,
             entity_type, entity_id, row_number, _json(evidence), utc_now()],
        )

    def _commit_tradebook(self, parsed: ParseResult, sha: str, broker: str, account: str) -> ImportResult:
        existing = self._existing_import(broker.lower(), account, "tradebook", sha)
        if existing:
            return ImportResult(existing["import_id"], "NO_OP", sha, existing["row_count"], existing["normalized_count"], {"duplicate": True})
        import_id, run_id = self._record_provenance(
            parsed=parsed, sha=sha, broker=broker, account=account, mode="fills"
        )
        fills = [record for record in parsed.records if isinstance(record, ParsedFill)]
        valid_by_symbol: dict[str, set[str]] = defaultdict(set)
        for fill in fills:
            if fill.isin:
                valid_by_symbol[fill.symbol].add(fill.isin)
        now = utc_now()
        normalized = 0
        try:
            with self.store.writer() as conn:
                for issue in parsed.issues:
                    self._insert_issue(conn, account=account, import_id=import_id,
                        issue_type=issue["issue_type"], severity=issue["severity"],
                        evidence=issue["evidence"], row_number=issue["row_number"])
                for fill in fills:
                    inferred = next(iter(valid_by_symbol[fill.symbol])) if len(valid_by_symbol[fill.symbol]) == 1 else None
                    resolved_isin = fill.isin or inferred
                    instrument_id = stable_id("ins", resolved_isin) if resolved_isin else None
                    if instrument_id:
                        conn.execute(
                            "INSERT INTO instrument_identity VALUES (?,?,?) ON CONFLICT DO NOTHING",
                            [instrument_id, resolved_isin, now],
                        )
                        alias_id = stable_id("alias", instrument_id, fill.symbol, fill.exchange, fill.series)
                        conn.execute(
                            """INSERT INTO instrument_alias VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
                            [alias_id, instrument_id, fill.symbol, resolved_isin, fill.exchange,
                             fill.segment, fill.series, fill.trade_date, None, import_id, now],
                        )
                    resolution_id = stable_id("res", import_id, fill.raw_row_number)
                    method = "isin" if fill.isin else ("same_file_symbol_isin" if inferred else "unresolved")
                    conn.execute(
                        "INSERT INTO identity_resolution VALUES (?,?,?,?,?,?,?,?,?)",
                        [resolution_id, import_id, fill.raw_row_number, instrument_id, method,
                         "HIGH" if instrument_id else "NONE", _json({"symbol": fill.symbol, "isin": fill.isin}),
                         "AUTO" if instrument_id else "REVIEW_REQUIRED", now],
                    )
                    fill_id = stable_id("fill", broker.lower(), account, fill.exchange, fill.trade_date, fill.trade_id)
                    economics = _json({"instrument_id": instrument_id, "symbol": fill.symbol, "side": fill.side,
                                      "quantity": fill.quantity, "price": fill.price, "order_id": fill.order_id,
                                      "executed_at": fill.executed_at, "series": fill.series})
                    economics_hash = hashlib.sha256(economics.encode()).hexdigest()
                    collision = conn.execute(
                        "SELECT economics_hash FROM journal_fill WHERE fill_id=?", [fill_id]
                    ).fetchone()
                    if collision:
                        if collision[0] != economics_hash:
                            raise ValueError(f"FILL_IDENTITY_CONFLICT at source row {fill.raw_row_number}")
                        continue
                    raw_id = stable_id("raw", import_id, "Equity", fill.raw_row_number)
                    conn.execute(
                        """INSERT INTO journal_fill VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        [fill_id, import_id, account, instrument_id, fill.symbol, fill.isin,
                         fill.exchange, fill.segment, fill.series, fill.trade_date, fill.executed_at,
                         fill.side, fill.auction, fill.quantity, fill.price, fill.trade_id, fill.order_id,
                         economics_hash, "TRUSTED" if instrument_id else "QUARANTINED", raw_id],
                    )
                    if instrument_id:
                        conn.execute(
                            "INSERT INTO portfolio_event VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            [stable_id("evt", fill_id), account, instrument_id, "FILL", fill.executed_at,
                             fill.quantity if fill.side == "buy" else -fill.quantity, fill.price,
                             fill.quantity * fill.price, fill_id, 20, "TRUSTED", "{}", now],
                        )
                    normalized += 1
                grouped: dict[tuple[str, date, str], list[ParsedFill]] = defaultdict(list)
                for fill in fills:
                    grouped[(fill.exchange, fill.trade_date, fill.order_id)].append(fill)
                for (exchange, trade_date, order_id), order_fills in grouped.items():
                    quantity = sum((fill.quantity for fill in order_fills), Decimal("0"))
                    notional = sum((fill.quantity * fill.price for fill in order_fills), Decimal("0"))
                    symbol = order_fills[0].symbol
                    inferred = next(iter(valid_by_symbol[symbol])) if len(valid_by_symbol[symbol]) == 1 else None
                    instrument_id = stable_id("ins", order_fills[0].isin or inferred) if order_fills[0].isin or inferred else None
                    order_key = stable_id("order", broker.lower(), account, exchange, trade_date, order_id)
                    conn.execute(
                        "INSERT INTO journal_order VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                        [order_key, account, instrument_id, symbol, exchange, trade_date,
                         order_id, order_fills[0].side, quantity, notional / quantity,
                         len(order_fills), min(f.executed_at for f in order_fills),
                         max(f.executed_at for f in order_fills), run_id, now],
                    )
                conn.execute(
                    "UPDATE journal_import_file SET status='IMPORTED',normalized_count=?,completed_at=? WHERE import_id=?",
                    [normalized, now, import_id],
                )
                conn.execute(
                    "UPDATE journal_import_run SET status='COMPLETED',completed_at=? WHERE import_run_id=?",
                    [now, run_id],
                )
            analysis = self.reconstruct(account)
        except Exception as exc:
            self._fail_import(import_id, run_id, exc)
            raise
        return ImportResult(import_id, "IMPORTED", sha, len(fills), normalized, analysis)

    def reconstruct(self, account: str) -> dict[str, Any]:
        with self.store.reader() as conn:
            fills = rows_as_dicts(conn.execute(
                "SELECT * FROM journal_fill WHERE account_ref=? AND instrument_id IS NOT NULL ORDER BY executed_at,fill_id",
                [account],
            ))
            openings = rows_as_dicts(conn.execute(
                """SELECT * FROM opening_position WHERE account_ref=? AND review_status='APPROVED'
                   ORDER BY effective_at,opening_position_id""",
                [account],
            ))
            actions = rows_as_dicts(conn.execute(
                """SELECT * FROM corporate_action_event WHERE review_status='APPROVED'
                   ORDER BY effective_date,action_id"""
            ))
        input_hash = hashlib.sha256(_json({
            "fills": [(f["fill_id"], f["economics_hash"]) for f in fills],
            "openings": [(o["opening_position_id"], o["quantity"], o["total_cost"]) for o in openings],
            "actions": [(a["action_id"], a["quantity_factor"], a["cost_factor"]) for a in actions],
        }).encode()).hexdigest()
        run_id = stable_id("arun", account, "reconstruction", input_hash)
        now = utc_now()
        by_instrument: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for fill in fills:
            by_instrument[fill["instrument_id"]].append(fill)
        openings_by_instrument: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for opening in openings:
            openings_by_instrument[opening["instrument_id"]].append(opening)
        actions_by_instrument: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for action in actions:
            actions_by_instrument[action["instrument_id"]].append(action)
        deficits = 0
        with self.store.writer() as conn:
            exists = conn.execute("SELECT status FROM journal_analysis_run WHERE analysis_run_id=?", [run_id]).fetchone()
            if exists and exists[0] == "COMPLETED":
                return {"analysis_run_id": run_id, "status": "NO_OP", "fills": len(fills)}
            conn.execute(
                "INSERT INTO journal_analysis_run VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                [run_id, account, "reconstruction", "RUNNING", LOGIC_VERSION, input_hash, None, now, None, None],
            )
            by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for source_fill in fills:
                by_symbol[source_fill["symbol"]].append(source_fill)
            for symbol, symbol_rows in by_symbol.items():
                opening_quantity = sum(
                    (Decimal(item["quantity"]) for item in openings_by_instrument.get(symbol_rows[0]["instrument_id"], [])),
                    Decimal("0"),
                )
                prefix = minimum = opening_quantity
                for source_fill in symbol_rows:
                    signed = Decimal(source_fill["quantity"])
                    prefix += signed if source_fill["side"] == "buy" else -signed
                    minimum = min(minimum, prefix)
                if minimum < 0:
                    deficits += 1
                    self._insert_issue(conn, account=account, analysis_run_id=run_id,
                        issue_type="POSITION_DEFICIT", severity="BLOCKING",
                        entity_type="symbol", entity_id=symbol,
                        evidence={"required_opening_quantity": _money(-minimum)})
            for instrument_id in sorted(set(by_instrument) | set(openings_by_instrument)):
                rows = by_instrument.get(instrument_id, [])
                lots: deque[dict[str, Any]] = deque()
                running = Decimal("0")
                weighted_cost = Decimal("0")
                episode_id: str | None = None
                episode_opened: datetime | None = None
                episode_pnl = Decimal("0")
                trusted = True
                for opening in openings_by_instrument.get(instrument_id, []):
                    opening_qty = Decimal(opening["quantity"])
                    unit_cost = Decimal(opening["total_cost"]) / opening_qty
                    lots.append({
                        "id": stable_id("lot", opening["opening_position_id"]),
                        "opened": opening["effective_at"],
                        "original": opening_qty,
                        "remaining": opening_qty,
                        "cost": unit_cost,
                        "fill": opening["opening_position_id"],
                    })
                    running += opening_qty
                    weighted_cost += Decimal(opening["total_cost"])
                    if episode_id is None:
                        episode_opened = opening["effective_at"]
                        episode_id = stable_id("episode", account, instrument_id, episode_opened)
                prefix = running
                minimum_prefix = running
                for source_fill in rows:
                    signed = Decimal(source_fill["quantity"])
                    prefix += signed if source_fill["side"] == "buy" else -signed
                    minimum_prefix = min(minimum_prefix, prefix)
                if minimum_prefix < 0:
                    trusted = False
                pending_actions = deque(actions_by_instrument.get(instrument_id, []))

                def apply_actions_through(cutoff: datetime) -> None:
                    nonlocal running, weighted_cost
                    while pending_actions and datetime.combine(
                        pending_actions[0]["effective_date"], time.min
                    ) <= cutoff:
                        action = pending_actions.popleft()
                        quantity_factor = Decimal(action["quantity_factor"] or 1)
                        cost_factor = Decimal(action["cost_factor"] or (Decimal("1") / quantity_factor))
                        if quantity_factor <= 0 or cost_factor <= 0:
                            raise ValueError(f"invalid approved corporate action {action['action_id']}")
                        for lot in lots:
                            lot["original"] *= quantity_factor
                            lot["remaining"] *= quantity_factor
                            lot["cost"] *= cost_factor
                        running *= quantity_factor
                        weighted_cost *= quantity_factor * cost_factor

                for fill in rows:
                    apply_actions_through(fill["executed_at"])
                    qty, price = Decimal(fill["quantity"]), Decimal(fill["price"])
                    if fill["side"] == "buy":
                        if running == 0:
                            episode_opened = fill["executed_at"]
                            episode_id = stable_id("episode", account, instrument_id, episode_opened)
                        link_type = "INITIAL_ENTRY" if running == 0 else "ADD"
                        lot_id = stable_id("lot", fill["fill_id"])
                        lots.append({"id": lot_id, "opened": fill["executed_at"], "original": qty, "remaining": qty, "cost": price, "fill": fill["fill_id"]})
                        weighted_cost += qty * price
                        running += qty
                        if episode_id:
                            conn.execute("INSERT INTO episode_fill_link VALUES (?,?,?,?,?)", [episode_id, run_id, fill["fill_id"], link_type, qty])
                    else:
                        if qty > running:
                            trusted = False
                            continue
                        remaining = qty
                        while remaining > 0:
                            lot = lots[0]
                            disposed = min(remaining, lot["remaining"])
                            proceeds, cost = disposed * price, disposed * lot["cost"]
                            pnl = proceeds - cost
                            episode_pnl += pnl
                            conn.execute(
                                "INSERT INTO lot_disposal VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                [stable_id("disp", run_id, lot["id"], fill["fill_id"]), run_id,
                                 lot["id"], fill["fill_id"], disposed, proceeds, cost, pnl,
                                 (fill["executed_at"].date() - lot["opened"].date()).days,
                                 fill["executed_at"], now],
                            )
                            lot["remaining"] -= disposed
                            remaining -= disposed
                            if lot["remaining"] == 0:
                                lots.popleft()
                        weighted_cost -= qty * (weighted_cost / running if running else Decimal("0"))
                        running -= qty
                        link_type = "FINAL_EXIT" if running == 0 else "TRIM"
                        if episode_id:
                            conn.execute("INSERT INTO episode_fill_link VALUES (?,?,?,?,?)", [episode_id, run_id, fill["fill_id"], link_type, qty])
                            if running == 0:
                                conn.execute(
                                    "INSERT INTO trade_episode VALUES (?,?,?,?,?,?,?,?,?,?)",
                                    [episode_id, run_id, account, instrument_id, episode_opened,
                                     fill["executed_at"], "CLOSED", episode_pnl,
                                     "TRUSTED" if trusted else "UNTRUSTED", now],
                                )
                                episode_id, episode_opened, episode_pnl = None, None, Decimal("0")
                apply_actions_through(datetime.max)
                for lot in lots:
                    conn.execute(
                        "INSERT INTO position_lot VALUES (?,?,?,?,?,?,?,?,?,?)",
                        [lot["id"], run_id, account, instrument_id, lot["opened"],
                         lot["original"], lot["remaining"], lot["cost"], lot["fill"], now],
                    )
                if episode_id:
                    conn.execute(
                        "INSERT INTO trade_episode VALUES (?,?,?,?,?,?,?,?,?,?)",
                        [episode_id, run_id, account, instrument_id, episode_opened, None,
                         "OPEN", episode_pnl, "TRUSTED" if trusted else "UNTRUSTED", now],
                    )
                timestamps = [row["executed_at"] for row in rows]
                timestamps.extend(row["effective_at"] for row in openings_by_instrument.get(instrument_id, []))
                timestamps.extend(
                    datetime.combine(row["effective_date"], time.min)
                    for row in actions_by_instrument.get(instrument_id, [])
                )
                as_of_at = max(timestamps)
                fifo_cost = sum((lot["remaining"] * lot["cost"] for lot in lots), Decimal("0"))
                avg = weighted_cost / running if running > 0 and trusted else None
                conn.execute(
                    "INSERT INTO portfolio_reconstruction VALUES (?,?,?,?,?,?,?,?,?,?)",
                    [run_id, account, instrument_id, as_of_at, running, fifo_cost if trusted else None,
                     avg, "TRUSTED" if trusted else "UNTRUSTED", _json({"input_hash": input_hash}), now],
                )
                conn.execute(
                    "INSERT INTO weighted_average_position VALUES (?,?,?,?,?,?,?,?,?)",
                    [run_id, account, instrument_id, as_of_at, running, avg,
                     weighted_cost if trusted else None, "TRUSTED" if trusted else "UNTRUSTED", now],
                )
            conn.execute(
                "UPDATE journal_analysis_run SET status='COMPLETED',completed_at=? WHERE analysis_run_id=?",
                [now, run_id],
            )
        return {"analysis_run_id": run_id, "status": "COMPLETED", "fills": len(fills), "deficits": deficits, "instruments": len(set(by_instrument) | set(openings_by_instrument))}

    def _commit_holdings(
        self, parsed: ParseResult, sha: str, broker: str, account: str, as_of: date,
        captured_at: datetime | None, market_state: str, mode: SnapshotMode,
    ) -> ImportResult:
        existing = self._existing_import(broker.lower(), account, "holdings", sha)
        if existing:
            return ImportResult(existing["import_id"], "NO_OP", sha, existing["row_count"], existing["normalized_count"], {"duplicate": True})
        if mode not in {"reconciliation_only", "opening_anchor"}:
            raise ValueError("unsupported snapshot mode")
        import_id, run_id = self._record_provenance(parsed=parsed, sha=sha, broker=broker,
            account=account, mode=mode, as_of=as_of, captured_at=captured_at, market_state=market_state)
        holdings = [record for record in parsed.records if isinstance(record, ParsedHolding)]
        snapshot_id = stable_id("snap", broker.lower(), account, as_of, captured_at, market_state, sha)
        now = utc_now()
        master_identities = self._master_identities({holding.instrument for holding in holdings})
        try:
            with self.store.writer() as conn:
                alias_candidates: dict[str, set[str]] = defaultdict(set)
                for symbol, instrument_id in conn.execute(
                    """SELECT symbol,instrument_id FROM instrument_alias
                       WHERE (valid_from IS NULL OR valid_from<=?)
                       AND (valid_to IS NULL OR valid_to>=?)""",
                    [as_of, as_of],
                ).fetchall():
                    alias_candidates[symbol].add(instrument_id)
                aliases = {
                    symbol: next(iter(instruments))
                    for symbol, instruments in alias_candidates.items()
                    if len(instruments) == 1
                }
                prior = conn.execute(
                    "SELECT snapshot_id FROM portfolio_snapshot WHERE account_ref=? AND as_of_date=? AND canonical",
                    [account, as_of],
                ).fetchone()
                canonical = prior is None
                trust = "UNRECONCILED" if market_state.lower() == "eod" else "PROVISIONAL"
                totals = parsed.metadata["totals"]
                conn.execute(
                    "INSERT INTO portfolio_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [snapshot_id, import_id, account, as_of, captured_at, market_state.lower(), mode,
                     trust, Decimal(totals["invested"]), Decimal(totals["current_value"]),
                     Decimal(totals["pnl"]), canonical, None, now],
                )
                for holding in holdings:
                    instrument_id = aliases.get(holding.instrument)
                    resolution_method = "journal_alias"
                    master_match = master_identities.get(holding.instrument)
                    if instrument_id is None and master_match:
                        isin, exchange, master_symbol_id = master_match
                        instrument_id = stable_id("ins", isin)
                        resolution_method = "operational_master"
                        conn.execute(
                            "INSERT INTO instrument_identity VALUES (?,?,?) ON CONFLICT DO NOTHING",
                            [instrument_id, isin, now],
                        )
                        conn.execute(
                            """INSERT INTO instrument_alias VALUES (?,?,?,?,?,?,?,?,?,?,?)
                               ON CONFLICT DO NOTHING""",
                            [stable_id("alias", instrument_id, holding.instrument, exchange, "master"),
                             instrument_id, holding.instrument, isin, exchange, "EQ", None,
                             as_of, None, import_id, now],
                        )
                        conn.execute(
                            """INSERT INTO identity_resolution VALUES (?,?,?,?,?,?,?,?,?)
                               ON CONFLICT DO NOTHING""",
                            [stable_id("res", import_id, holding.raw_row_number), import_id,
                             holding.raw_row_number, instrument_id, resolution_method, "HIGH",
                             _json({"symbol": holding.instrument, "master_symbol_id": master_symbol_id,
                                    "isin": isin, "exchange": exchange}), "AUTO", now],
                        )
                    elif instrument_id:
                        conn.execute(
                            """INSERT INTO identity_resolution VALUES (?,?,?,?,?,?,?,?,?)
                               ON CONFLICT DO NOTHING""",
                            [stable_id("res", import_id, holding.raw_row_number), import_id,
                             holding.raw_row_number, instrument_id, resolution_method, "HIGH",
                             _json({"symbol": holding.instrument, "as_of": as_of}), "AUTO", now],
                        )
                    conn.execute(
                        "INSERT INTO portfolio_snapshot_position VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        [snapshot_id, instrument_id, holding.instrument, holding.quantity,
                         holding.average_cost, holding.ltp, holding.invested,
                         holding.current_value, holding.pnl, holding.net_change_pct,
                         holding.day_change_pct, _json(asdict(holding))],
                    )
                    if instrument_id is None:
                        self._insert_issue(conn, account=account, import_id=import_id,
                            issue_type="UNRESOLVED_IDENTITY", severity="BLOCKING",
                            entity_type="snapshot_position", entity_id=holding.instrument,
                            evidence={"snapshot_id": snapshot_id})
                    if mode == "opening_anchor" and instrument_id:
                        earliest_row = conn.execute(
                            "SELECT MIN(trade_date) FROM journal_fill WHERE account_ref=? AND instrument_id=?",
                            [account, instrument_id],
                        ).fetchone()
                        earliest = earliest_row[0] if earliest_row else None
                        if earliest is not None and as_of > earliest:
                            raise ValueError("opening_anchor must not be later than existing fills")
                        opening_id = stable_id("opening", snapshot_id, instrument_id)
                        effective = datetime.combine(as_of, time(23, 59, 59))
                        conn.execute(
                            "INSERT INTO opening_position VALUES (?,?,?,?,?,?,?,?,?,?)",
                            [opening_id, account, instrument_id, effective, holding.quantity,
                             holding.invested, "snapshot_bootstrap", import_id, "APPROVED", now],
                        )
                        conn.execute(
                            "INSERT INTO opening_lot VALUES (?,?,?,?,?,?,?,?,?,?)",
                            [opening_id, account, instrument_id, effective, holding.quantity,
                             holding.invested, "snapshot_bootstrap", import_id, "APPROVED", now],
                        )
                        conn.execute(
                            "INSERT INTO portfolio_event VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            [stable_id("evt", opening_id), account, instrument_id, "OPENING",
                             effective, holding.quantity, holding.invested / holding.quantity,
                             holding.invested, opening_id, 10, "BROKER_REPORTED_ROUNDED", "{}", now],
                        )
                if prior:
                    self._insert_issue(conn, account=account, import_id=import_id,
                        issue_type="SNAPSHOT_POINT_CONFLICT", severity="BLOCKING",
                        entity_type="snapshot", entity_id=snapshot_id,
                        evidence={"existing_canonical_snapshot_id": prior[0]})
                for issue in parsed.issues:
                    self._insert_issue(conn, account=account, import_id=import_id,
                        issue_type=issue["issue_type"], severity=issue["severity"],
                        row_number=issue["row_number"], evidence=issue["evidence"])
                conn.execute("UPDATE journal_import_file SET status='IMPORTED',normalized_count=?,canonical_snapshot=?,completed_at=? WHERE import_id=?", [len(holdings), canonical, now, import_id])
                conn.execute("UPDATE journal_import_run SET status='COMPLETED',completed_at=? WHERE import_run_id=?", [now, run_id])
            reconciliation = self.reconcile(account=account, snapshot_id=snapshot_id)
        except Exception as exc:
            self._fail_import(import_id, run_id, exc)
            raise
        return ImportResult(import_id, "IMPORTED", sha, len(holdings), len(holdings), reconciliation)

    def reconcile(self, *, account: str, snapshot_id: str) -> dict[str, Any]:
        now = utc_now()
        config = JournalAnalyticsConfig()
        with self.store.reader() as conn:
            snap = rows_as_dicts(conn.execute("SELECT * FROM portfolio_snapshot WHERE snapshot_id=?", [snapshot_id]))[0]
            positions = rows_as_dicts(conn.execute("SELECT * FROM portfolio_snapshot_position WHERE snapshot_id=? ORDER BY instrument", [snapshot_id]))
            ledger_rows = conn.execute(
                """SELECT instrument_id,SUM(CASE WHEN side='buy' THEN quantity ELSE -quantity END) quantity
                   FROM journal_fill WHERE account_ref=? AND trade_date<=? AND instrument_id IS NOT NULL GROUP BY instrument_id""",
                [account, snap["as_of_date"]],
            ).fetchall()
            cost_rows = conn.execute(
                """SELECT instrument_id,as_of_at,fifo_cost,weighted_average_cost,trust_status
                   FROM journal_current_positions WHERE account_ref=?""",
                [account],
            ).fetchall()
        ledger = {row[0]: Decimal(row[1]) for row in ledger_rows}
        costs = {
            row[0]: row for row in cost_rows
            if row[1].date() <= snap["as_of_date"]
        }
        input_hash = hashlib.sha256(_json({
            "snapshot_id": snapshot_id,
            "ledger": sorted((key, value) for key, value in ledger.items()),
            "costs": sorted(
                (key, row[2], row[3], row[4]) for key, row in costs.items()
            ),
            "logic_version": LOGIC_VERSION,
        }).encode()).hexdigest()
        analysis_run_id = stable_id("arun", account, "reconciliation", input_hash)
        reconciliation_id = stable_id("recon", snapshot_id, input_hash)
        with self.store.reader() as conn:
            existing = conn.execute(
                """SELECT status,matched_count,issue_count,trust_status
                   FROM portfolio_reconciliation WHERE reconciliation_id=?""",
                [reconciliation_id],
            ).fetchone()
        if existing:
            return {
                "reconciliation_id": reconciliation_id, "status": "NO_OP",
                "result_status": existing[0], "matched": existing[1],
                "issues": existing[2], "trust_status": existing[3],
            }
        matched = issues = 0
        with self.store.writer() as conn:
            conn.execute("INSERT INTO journal_analysis_run VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING", [analysis_run_id, account, "reconciliation", "RUNNING", LOGIC_VERSION, snapshot_id, None, now, None, None])
            seen: set[str] = set()
            for row in positions:
                instrument_id = row["instrument_id"]
                broker_qty = Decimal(row["quantity"])
                ledger_qty = ledger.get(instrument_id, Decimal("0")) if instrument_id else None
                cost_row = costs.get(instrument_id) if instrument_id else None
                fifo_cost = Decimal(cost_row[2]) if cost_row and cost_row[2] is not None else None
                weighted_cost = Decimal(cost_row[3]) if cost_row and cost_row[3] is not None else None
                broker_cost = Decimal(row["invested"])
                fifo_difference = broker_cost - fifo_cost if fifo_cost is not None else None
                weighted_difference = broker_cost - weighted_cost if weighted_cost is not None else None

                def within_tolerance(difference: Decimal | None) -> bool | None:
                    if difference is None:
                        return None
                    tolerance = max(
                        config.cost_absolute_tolerance,
                        abs(broker_cost) * config.cost_relative_tolerance,
                    )
                    return abs(difference) <= tolerance

                if instrument_id is None:
                    classification = "UNRESOLVED_IDENTITY"
                elif ledger_qty != broker_qty:
                    classification = "MISSING_IN_LEDGER" if ledger_qty == 0 else "QUANTITY_MISMATCH"
                elif fifo_cost is None and weighted_cost is None:
                    classification = "COST_UNAVAILABLE"
                elif within_tolerance(fifo_difference) is False:
                    classification = "FIFO_COST_VARIANCE"
                elif within_tolerance(weighted_difference) is False:
                    classification = "WEIGHTED_COST_VARIANCE"
                else:
                    classification = "MATCHED"
                if classification == "MATCHED":
                    matched += 1
                else:
                    issues += 1
                if instrument_id:
                    seen.add(instrument_id)
                conn.execute(
                    "INSERT INTO portfolio_reconciliation_item VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    [reconciliation_id, instrument_id, row["instrument"], classification,
                     broker_qty, ledger_qty, broker_cost, fifo_cost, weighted_cost,
                     None if ledger_qty is None else broker_qty - ledger_qty, fifo_difference,
                     _json({
                         "snapshot_id": snapshot_id,
                         "fifo_cost_difference": fifo_difference,
                         "weighted_average_cost_difference": weighted_difference,
                         "absolute_tolerance": config.cost_absolute_tolerance,
                         "relative_tolerance": config.cost_relative_tolerance,
                     })],
                )
            for instrument_id, ledger_qty in ledger.items():
                if ledger_qty != 0 and instrument_id not in seen:
                    issues += 1
                    conn.execute(
                        "INSERT INTO portfolio_reconciliation_item VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        [reconciliation_id, instrument_id, instrument_id, "MISSING_IN_SNAPSHOT",
                         None, ledger_qty, None, None, None, None, None,
                         _json({"snapshot_id": snapshot_id})],
                    )
            status = "MATCHED" if issues == 0 else "ISSUES"
            trust = "TRUSTED" if status == "MATCHED" and snap["trust_status"] != "PROVISIONAL" else "UNTRUSTED"
            as_of_at = snap["captured_at"] or datetime.combine(snap["as_of_date"], time(23, 59, 59))
            conn.execute("INSERT INTO portfolio_reconciliation VALUES (?,?,?,?,?,?,?,?,?,?)", [reconciliation_id, analysis_run_id, snapshot_id, account, as_of_at, status, matched, issues, trust, now])
            conn.execute("UPDATE journal_analysis_run SET status='COMPLETED',completed_at=? WHERE analysis_run_id=?", [now, analysis_run_id])
            conn.execute("UPDATE portfolio_snapshot SET trust_status=? WHERE snapshot_id=?", [trust, snapshot_id])
        return {"reconciliation_id": reconciliation_id, "status": status, "matched": matched, "issues": issues, "trust_status": trust}

    def reconcile_latest(self, account: str) -> dict[str, Any]:
        with self.store.reader() as conn:
            row = conn.execute(
                """SELECT snapshot_id FROM portfolio_snapshot WHERE account_ref=?
                   ORDER BY as_of_date DESC,captured_at DESC NULLS LAST,created_at DESC LIMIT 1""",
                [account],
            ).fetchone()
        if row is None:
            raise ValueError("no holdings snapshot exists for this account")
        return self.reconcile(account=account, snapshot_id=row[0])

    def analyze(self, account: str, *, market_data: JournalMarketDataReader) -> dict[str, Any]:
        return JournalAnalysisEngine(self.store, market_data).run(account)

    def propose_adjustment(
        self, *, account: str, instrument_id: str, adjustment_type: str,
        effective_at: datetime, quantity: Decimal | None, amount: Decimal | None,
        reason: str,
    ) -> dict[str, Any]:
        if adjustment_type not in {"opening_lot", "manual_adjustment"}:
            raise ValueError("unsupported adjustment type")
        if adjustment_type == "opening_lot" and (quantity is None or quantity <= 0 or amount is None or amount < 0):
            raise ValueError("opening lot requires positive quantity and non-negative total cost")
        adjustment_id = stable_id(
            "adjustment", account, instrument_id, adjustment_type, effective_at, quantity, amount, reason
        )
        with self.store.writer() as conn:
            conn.execute(
                "INSERT INTO journal_adjustment_request VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                [adjustment_id, account, instrument_id, adjustment_type, effective_at, quantity,
                 amount, "PROPOSED", reason, "{}", utc_now(), None, None],
            )
        return {"adjustment_id": adjustment_id, "status": "PROPOSED"}

    def approve_adjustment(self, adjustment_id: str, *, reviewer: str) -> dict[str, Any]:
        now = utc_now()
        with self.store.writer() as conn:
            row = conn.execute(
                "SELECT * FROM journal_adjustment_request WHERE adjustment_id=?", [adjustment_id]
            ).fetchone()
            if row is None:
                raise ValueError("adjustment proposal not found")
            columns = [item[0] for item in (conn.description or [])]
            proposal = dict(zip(columns, row, strict=True))
            if proposal["status"] != "PROPOSED":
                raise ValueError("adjustment proposal is not pending")
            conn.execute(
                "UPDATE journal_adjustment_request SET status='APPROVED',reviewed_by=?,reviewed_at=? WHERE adjustment_id=?",
                [reviewer, now, adjustment_id],
            )
            if proposal["adjustment_type"] == "opening_lot":
                opening_id = stable_id("opening", adjustment_id)
                conn.execute(
                    "INSERT INTO opening_position VALUES (?,?,?,?,?,?,?,?,?,?)",
                    [opening_id, proposal["account_ref"], proposal["instrument_id"],
                     proposal["effective_at"], proposal["quantity"], proposal["amount"],
                     "reviewed_manual_proposal", adjustment_id, "APPROVED", now],
                )
                conn.execute(
                    "INSERT INTO opening_lot SELECT * FROM opening_position WHERE opening_position_id=?",
                    [opening_id],
                )
                event_type = "OPENING"
            else:
                opening_id = adjustment_id
                event_type = "ADJUSTMENT"
            conn.execute(
                "INSERT INTO portfolio_event VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [stable_id("evt", adjustment_id), proposal["account_ref"], proposal["instrument_id"],
                 event_type, proposal["effective_at"], proposal["quantity"], None,
                 proposal["amount"], opening_id, 10, "REVIEWED", _json({"reviewer": reviewer}), now],
            )
        reconstruction = self.reconstruct(proposal["account_ref"])
        return {"adjustment_id": adjustment_id, "status": "APPROVED", "reconstruction": reconstruction}

    def propose_corporate_action(
        self, *, instrument_id: str, action_type: str, effective_date: date,
        quantity_factor: Decimal, cost_factor: Decimal | None, source_ref: str,
    ) -> dict[str, Any]:
        if action_type not in {"split", "bonus"} or quantity_factor <= 0:
            raise ValueError("supported corporate actions are positive split or bonus factors")
        action_id = stable_id(
            "corp_action", instrument_id, action_type, effective_date, quantity_factor, cost_factor, source_ref
        )
        with self.store.writer() as conn:
            conn.execute(
                "INSERT INTO corporate_action_event VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                [action_id, instrument_id, action_type, effective_date, quantity_factor,
                 cost_factor, "journal_proposal", source_ref, "PROPOSED", None, None, "{}", utc_now()],
            )
        return {"action_id": action_id, "status": "PROPOSED"}

    def approve_corporate_action(self, action_id: str, *, reviewer: str) -> dict[str, Any]:
        now = utc_now()
        with self.store.writer() as conn:
            row = conn.execute(
                "SELECT review_status FROM corporate_action_event WHERE action_id=?", [action_id]
            ).fetchone()
            if row is None:
                raise ValueError("corporate-action proposal not found")
            if row[0] != "PROPOSED":
                raise ValueError("corporate-action proposal is not pending")
            conn.execute(
                "UPDATE corporate_action_event SET review_status='APPROVED',reviewed_by=?,reviewed_at=? WHERE action_id=?",
                [reviewer, now, action_id],
            )
            accounts = [row[0] for row in conn.execute(
                """SELECT DISTINCT account_ref FROM journal_fill WHERE instrument_id=(
                       SELECT instrument_id FROM corporate_action_event WHERE action_id=?)
                   UNION SELECT DISTINCT account_ref FROM opening_position WHERE instrument_id=(
                       SELECT instrument_id FROM corporate_action_event WHERE action_id=?)""",
                [action_id, action_id],
            ).fetchall()]
        reconstructions = [self.reconstruct(account) for account in accounts]
        return {"action_id": action_id, "status": "APPROVED", "reconstructions": reconstructions}

    def enqueue_task(
        self, *, action: str, account: str, snapshot_id: str | None = None
    ) -> str:
        if action not in {"reconstruct", "reconcile", "analyze"}:
            raise ValueError("unsupported journal task action")
        journal_run_id = stable_id("jrun", action, account, snapshot_id, utc_now().isoformat())
        with self.store.writer() as conn:
            conn.execute(
                "INSERT INTO journal_task_request VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [journal_run_id, action, account, snapshot_id, "QUEUED", None,
                 utc_now(), None, None, None, None],
            )
        return journal_run_id

    def attach_operator_task(self, journal_run_id: str, operator_task_id: str) -> None:
        with self.store.writer() as conn:
            conn.execute(
                "UPDATE journal_task_request SET operator_task_id=? WHERE journal_run_id=?",
                [operator_task_id, journal_run_id],
            )

    def run_task(self, journal_run_id: str) -> dict[str, Any]:
        with self.store.writer() as conn:
            cursor = conn.execute(
                "SELECT action,account_ref,snapshot_id,status FROM journal_task_request WHERE journal_run_id=?",
                [journal_run_id],
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError("journal run does not exist")
            if row[3] not in {"QUEUED", "FAILED"}:
                raise ValueError("journal run is not executable")
            conn.execute(
                "UPDATE journal_task_request SET status='RUNNING',started_at=?,error_summary=NULL WHERE journal_run_id=?",
                [utc_now(), journal_run_id],
            )
        action, account, snapshot_id = row[0], row[1], row[2]
        try:
            if action == "reconstruct":
                result = self.reconstruct(account)
            elif action == "reconcile":
                result = (
                    self.reconcile(account=account, snapshot_id=snapshot_id)
                    if snapshot_id else self.reconcile_latest(account)
                )
            else:
                paths = get_domain_paths(self.store.project_root, data_domain="operational")
                reader = JournalMarketDataReader(
                    paths.ohlcv_db_path,
                    control_plane_db_path=paths.root_dir / "control_plane.duckdb",
                    master_db_path=paths.master_db_path,
                )
                result = self.analyze(account, market_data=reader)
        except Exception as exc:
            with self.store.writer() as conn:
                conn.execute(
                    "UPDATE journal_task_request SET status='FAILED',completed_at=?,error_summary=? WHERE journal_run_id=?",
                    [utc_now(), f"{exc.__class__.__name__}: {str(exc)[:500]}", journal_run_id],
                )
            raise
        with self.store.writer() as conn:
            conn.execute(
                "UPDATE journal_task_request SET status='COMPLETED',completed_at=?,result_json=? WHERE journal_run_id=?",
                [utc_now(), _json(result), journal_run_id],
            )
        return {"journal_run_id": journal_run_id, "status": "COMPLETED"}
