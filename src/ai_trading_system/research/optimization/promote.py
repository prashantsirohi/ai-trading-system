"""Manual lifecycle CLI: advance a rule pack through the promotion ladder.

    draft → backtested → walkforward_passed → shadow
          → paper_approved → production_candidate → active

Every transition is an explicit operator action. No auto-promotion.

Two invocation forms (both routed through the same ``main`` entrypoint, so the
``ai-trading-optimize-promote`` console alias and ``python -m`` both work):

    # Explicit promote by hash (original form, backwards compatible).
    ai-trading-optimize-promote \\
        --rule-pack-id <hash> --to shadow --project-root .

    # Promote the latest champion for a recipe (operator shortcut).
    ai-trading-optimize-promote promote-latest \\
        --recipe-name momentum_breakout_optuna_v1 --to shadow

The ``promote-latest`` form resolves the most-recent completed run for the
named recipe and promotes its champion. Fails fast if the recipe has no
completed run, or the latest run produced no champion.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.research.optimization.store import OptimizationStore


LIFECYCLE_ORDER = (
    "draft",
    "backtested",
    "walkforward_passed",
    "shadow",
    "paper_approved",
    "production_candidate",
    "active",
)


def _allowed_transition(current: str, target: str) -> bool:
    if current not in LIFECYCLE_ORDER or target not in LIFECYCLE_ORDER:
        return False
    return LIFECYCLE_ORDER.index(target) >= LIFECYCLE_ORDER.index(current)


def _current_status(project_root: Path, rule_pack_id: str) -> str | None:
    db_path = RegistryStore(project_root=project_root).db_path
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            "SELECT lifecycle_status FROM strategy_rule_pack WHERE rule_pack_id = ?",
            [rule_pack_id],
        ).fetchone()
    finally:
        con.close()
    return row[0] if row else None


def _promote_by_id(project_root: Path, rule_pack_id: str, target: str) -> int:
    current = _current_status(project_root, rule_pack_id)
    if current is None:
        print(f"error: unknown rule_pack_id={rule_pack_id}")
        return 2
    if not _allowed_transition(current, target):
        print(f"error: cannot move backwards in lifecycle ({current} -> {target})")
        return 2

    store = OptimizationStore(project_root=project_root)
    store.set_lifecycle_status(rule_pack_id, target)
    print(f"ok: {rule_pack_id} {current} -> {target}")
    return 0


def _build_legacy_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote a rule pack along the lifecycle (by --rule-pack-id).",
    )
    parser.add_argument("--rule-pack-id", required=True)
    parser.add_argument(
        "--to",
        required=True,
        choices=LIFECYCLE_ORDER,
        help="Target lifecycle status",
    )
    parser.add_argument("--project-root", default=".")
    return parser


def _build_latest_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ai-trading-optimize-promote promote-latest",
        description="Promote the champion of the latest completed run for a recipe.",
    )
    parser.add_argument(
        "--recipe-name",
        required=True,
        help="Recipe name (matches strategy_optimization_run.recipe_name).",
    )
    parser.add_argument(
        "--to",
        default="shadow",
        choices=LIFECYCLE_ORDER,
        help="Target lifecycle status (default: shadow).",
    )
    parser.add_argument("--project-root", default=".")
    return parser


def main(argv: list[str] | None = None) -> int:
    # When argv is None (the entry-point case), fall back to sys.argv so the
    # subcommand sniff below can see the first arg (e.g. "promote-latest").
    if argv is None:
        argv = sys.argv[1:]
    # Sniff for the new subcommand without breaking the legacy flat form.
    if argv and argv[0] == "promote-latest":
        args = _build_latest_parser().parse_args(argv[1:])
        project_root = Path(args.project_root)
        store = OptimizationStore(project_root=project_root)
        rule_pack_id = store.get_latest_champion_rule_pack(args.recipe_name)
        if rule_pack_id is None:
            print(
                f"error: no completed run with a champion found for recipe={args.recipe_name!r}"
            )
            return 2
        print(f"resolved: recipe={args.recipe_name} -> rule_pack_id={rule_pack_id}")
        return _promote_by_id(project_root, rule_pack_id, args.to)

    if argv and argv[0] == "promote":
        # Explicit legacy form: ai-trading-optimize-promote promote --rule-pack-id ...
        argv = argv[1:]

    args = _build_legacy_parser().parse_args(argv)
    return _promote_by_id(Path(args.project_root), args.rule_pack_id, args.to)


if __name__ == "__main__":
    raise SystemExit(main())
