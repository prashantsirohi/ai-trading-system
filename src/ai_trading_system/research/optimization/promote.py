"""Manual lifecycle CLI: advance a rule pack through the promotion ladder.

    draft → backtested → walkforward_passed → shadow
          → paper_approved → production_candidate → active

Every transition is an explicit operator action. No auto-promotion.

Usage:
    python -m ai_trading_system.research.optimization.promote \\
        --rule-pack-id <hash> --to shadow --project-root .
"""

from __future__ import annotations

import argparse
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Promote a rule pack along the lifecycle.")
    parser.add_argument("--rule-pack-id", required=True)
    parser.add_argument(
        "--to",
        required=True,
        choices=LIFECYCLE_ORDER,
        help="Target lifecycle status",
    )
    parser.add_argument("--project-root", default=".")
    args = parser.parse_args(argv)

    project_root = Path(args.project_root)
    current = _current_status(project_root, args.rule_pack_id)
    if current is None:
        print(f"error: unknown rule_pack_id={args.rule_pack_id}")
        return 2
    if not _allowed_transition(current, args.to):
        print(f"error: cannot move backwards in lifecycle ({current} -> {args.to})")
        return 2

    store = OptimizationStore(project_root=project_root)
    store.set_lifecycle_status(args.rule_pack_id, args.to)
    print(f"ok: {args.rule_pack_id} {current} -> {args.to}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
