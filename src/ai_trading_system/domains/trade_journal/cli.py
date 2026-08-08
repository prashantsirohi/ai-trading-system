"""Command line interface for the local Actual Portfolio journal."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from ai_trading_system.platform.db.paths import get_domain_paths

from .enrichment import JournalMarketDataReader
from .service import TradeJournalService
from .store import TradeJournalStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Actual Trading Journal and Portfolio")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--db-path", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    migrate = commands.add_parser("migrate")
    migrate.add_argument("--apply", action="store_true")
    for name in ("import-tradebook", "import-holdings"):
        command = commands.add_parser(name)
        command.add_argument("--broker", default="dhan", choices=["dhan"])
        command.add_argument("--account", required=True)
        command.add_argument("--file", required=True, type=Path)
        command.add_argument("--expected-sha256")
        command.add_argument("--preview", "--dry-run", action="store_true")
        command.add_argument("--commit", action="store_true")
        if name == "import-holdings":
            command.add_argument("--as-of", required=True, type=date.fromisoformat)
            command.add_argument("--captured-at", type=datetime.fromisoformat)
            command.add_argument("--market-state", default="eod", choices=["eod", "intraday", "unknown"])
            command.add_argument("--mode", default="reconciliation_only", choices=["reconciliation_only", "opening_anchor"])
    reconstruct = commands.add_parser("reconstruct")
    reconstruct.add_argument("--account", required=True)
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--account", required=True)
    reconcile.add_argument("--snapshot-id")
    analyze = commands.add_parser("analyze")
    analyze.add_argument("--account", required=True)
    for name in ("propose-opening-lot", "propose-adjustment"):
        command = commands.add_parser(name)
        command.add_argument("--account", required=True)
        command.add_argument("--instrument-id", required=True)
        command.add_argument("--effective-at", required=True, type=datetime.fromisoformat)
        command.add_argument("--quantity", type=Decimal)
        command.add_argument("--amount", type=Decimal)
        command.add_argument("--reason", required=True)
        command.add_argument("--commit", action="store_true")
    for name in ("approve-opening-lot", "approve-adjustment"):
        command = commands.add_parser(name)
        command.add_argument("--adjustment-id", required=True)
        command.add_argument("--reviewer", required=True)
        command.add_argument("--commit", action="store_true")
    propose_action = commands.add_parser("propose-corporate-action")
    propose_action.add_argument("--instrument-id", required=True)
    propose_action.add_argument("--action-type", required=True, choices=["split", "bonus"])
    propose_action.add_argument("--effective-date", required=True, type=date.fromisoformat)
    propose_action.add_argument("--quantity-factor", required=True, type=Decimal)
    propose_action.add_argument("--cost-factor", type=Decimal)
    propose_action.add_argument("--source-ref", required=True)
    propose_action.add_argument("--commit", action="store_true")
    approve_action = commands.add_parser("approve-corporate-action")
    approve_action.add_argument("--action-id", required=True)
    approve_action.add_argument("--reviewer", required=True)
    approve_action.add_argument("--commit", action="store_true")
    worker = commands.add_parser("worker", help=argparse.SUPPRESS)
    worker.add_argument("--journal-run-id", required=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    store = TradeJournalStore(args.project_root, args.db_path)
    result: dict[str, object]
    if args.command == "migrate":
        result = store.migrate(apply=args.apply)
    else:
        service = TradeJournalService(store)
        if args.command == "reconstruct":
            result = service.reconstruct(args.account)
        elif args.command == "worker":
            result = service.run_task(args.journal_run_id)
        elif args.command == "reconcile":
            result = (
                service.reconcile(account=args.account, snapshot_id=args.snapshot_id)
                if args.snapshot_id else service.reconcile_latest(args.account)
            )
        elif args.command == "analyze":
            paths = get_domain_paths(args.project_root, data_domain="operational")
            market_data = JournalMarketDataReader(
                paths.ohlcv_db_path,
                control_plane_db_path=paths.root_dir / "control_plane.duckdb",
                master_db_path=paths.master_db_path,
            )
            result = service.analyze(args.account, market_data=market_data)
        elif args.command in {"propose-opening-lot", "propose-adjustment"}:
            if not args.commit:
                result = {"status": "PREVIEW", "mutation": args.command}
            else:
                result = service.propose_adjustment(
                    account=args.account, instrument_id=args.instrument_id,
                    adjustment_type="opening_lot" if args.command == "propose-opening-lot" else "manual_adjustment",
                    effective_at=args.effective_at, quantity=args.quantity,
                    amount=args.amount, reason=args.reason,
                )
        elif args.command in {"approve-opening-lot", "approve-adjustment"}:
            if not args.commit:
                result = {"status": "PREVIEW", "mutation": args.command, "adjustment_id": args.adjustment_id}
            else:
                result = service.approve_adjustment(args.adjustment_id, reviewer=args.reviewer)
        elif args.command == "propose-corporate-action":
            if not args.commit:
                result = {"status": "PREVIEW", "mutation": args.command}
            else:
                result = service.propose_corporate_action(
                    instrument_id=args.instrument_id, action_type=args.action_type,
                    effective_date=args.effective_date, quantity_factor=args.quantity_factor,
                    cost_factor=args.cost_factor, source_ref=args.source_ref,
                )
        elif args.command == "approve-corporate-action":
            if not args.commit:
                result = {"status": "PREVIEW", "mutation": args.command, "action_id": args.action_id}
            else:
                result = service.approve_corporate_action(args.action_id, reviewer=args.reviewer)
        elif args.command == "import-tradebook":
            if args.preview or not args.commit:
                result = service.preview_tradebook(args.file)
            else:
                imported = service.import_tradebook(path=args.file, broker=args.broker,
                    account_ref=args.account, expected_sha256=args.expected_sha256)
                result = asdict(imported)
        else:
            if args.preview or not args.commit:
                result = service.preview_holdings(args.file)
            else:
                imported = service.import_holdings(path=args.file, broker=args.broker,
                    account_ref=args.account, as_of=args.as_of, captured_at=args.captured_at,
                    market_state=args.market_state, mode=args.mode,
                    expected_sha256=args.expected_sha256)
                result = asdict(imported)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
