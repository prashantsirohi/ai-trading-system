"""Manually mark a symbol's state in _symbol_state_overrides.

States in {delisted, suspended, permanently_unavailable} cause the symbol
to be excluded from the critical universe used by the quarantine DQ
contract — so a known-dead/halted symbol stops blocking the pipeline.

Other states (corporate_action, provider_blacklist, t2t_intraday_excluded)
are recorded for downstream consumers but do not affect DQ gating.

Usage:
    python -m scripts.data_repair.mark_symbol_state \
        --db data/ohlcv.duckdb \
        --symbol AEPL --state suspended \
        --reason "ASM Stage 4 — confirmed with NSE" \
        [--effective-from 2026-04-27] [--effective-to 2026-12-31]

    # List existing overrides
    python -m scripts.data_repair.mark_symbol_state \
        --db data/ohlcv.duckdb --list

    # Clear an override
    python -m scripts.data_repair.mark_symbol_state \
        --db data/ohlcv.duckdb --symbol AEPL --clear
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

from ai_trading_system.domains.ingest.trust import (
    SYMBOL_STATE_BLOCKING,
    clear_symbol_state,
    ensure_data_trust_schema,
    mark_symbol_state,
)
from ai_trading_system.platform.logging.logger import logger


VALID_STATES = (
    "delisted",
    "suspended",
    "permanently_unavailable",
    "corporate_action",
    "provider_blacklist",
    "t2t_intraday_excluded",
)


def _list_overrides(db_path: Path) -> int:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT symbol_id, exchange, state, effective_from, effective_to, reason, source, created_at
            FROM _symbol_state_overrides
            ORDER BY created_at DESC
            """
        ).fetchall()
    except duckdb.CatalogException:
        logger.error("_symbol_state_overrides table does not exist yet. Run ensure_data_trust_schema first.")
        return 1
    finally:
        conn.close()

    if not rows:
        print("(no overrides)")
        return 0
    print(f"{'symbol':12s} {'exchange':5s} {'state':25s} {'eff_from':12s} {'eff_to':12s} reason")
    for r in rows:
        sym, exch, state, eff_from, eff_to, reason, _src, _ts = r
        blocking = "*" if state in SYMBOL_STATE_BLOCKING else " "
        print(f"{sym:12s} {exch:5s} {state:25s}{blocking}{str(eff_from or ''):12s} {str(eff_to or ''):12s} {reason or ''}")
    print()
    print("(* = state excludes symbol from critical universe)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--list", action="store_true", help="List existing overrides and exit.")
    parser.add_argument("--symbol", help="Symbol id (e.g. AEPL)")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument(
        "--state",
        choices=VALID_STATES,
        help=f"Symbol state. Blocking states: {sorted(SYMBOL_STATE_BLOCKING)}",
    )
    parser.add_argument("--reason", default=None)
    parser.add_argument("--effective-from", default=None)
    parser.add_argument("--effective-to", default=None)
    parser.add_argument("--source", default="manual")
    parser.add_argument("--clear", action="store_true", help="Remove override(s) for the symbol.")
    args = parser.parse_args()

    if not args.db.exists():
        logger.error("Database not found: %s", args.db)
        return 1

    if args.list:
        return _list_overrides(args.db)

    if not args.symbol:
        parser.error("--symbol is required (unless --list)")

    if args.clear:
        deleted = clear_symbol_state(
            str(args.db), symbol_id=args.symbol, exchange=args.exchange, state=args.state,
        )
        logger.info("Cleared %s override row(s) for %s/%s state=%s", deleted, args.exchange, args.symbol, args.state or "*")
        return 0

    if not args.state:
        parser.error("--state is required when adding an override")

    ensure_data_trust_schema(str(args.db))
    mark_symbol_state(
        str(args.db),
        symbol_id=args.symbol,
        exchange=args.exchange,
        state=args.state,
        reason=args.reason,
        effective_from=args.effective_from,
        effective_to=args.effective_to,
        source=args.source,
    )
    blocking = " (BLOCKING — excluded from critical universe)" if args.state in SYMBOL_STATE_BLOCKING else ""
    logger.info("Marked %s/%s state=%s%s", args.exchange, args.symbol, args.state, blocking)
    return 0


if __name__ == "__main__":
    sys.exit(main())
