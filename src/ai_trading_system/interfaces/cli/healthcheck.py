"""Healthcheck CLI for trading-system integrations.

Reports on the freshness and last-cycle state of dependencies. Today this
covers ``market_intel`` (the corporate-actions enrichment service); the same
module can grow other subcommands later.

Usage::

    python -m ai_trading_system.interfaces.cli.healthcheck market-intel
    python -m ai_trading_system.interfaces.cli.healthcheck market-intel --json
    python -m ai_trading_system.interfaces.cli.healthcheck market-intel \\
        --db /path/to/market_intel.duckdb --max-stale-min 15

Exit codes:
  0 — healthy
  1 — degraded (heartbeat present but stale, or recent error_count > 0)
  2 — down (no heartbeat row at all, or DB missing)
  3 — usage error / unknown subcommand
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


DEFAULT_MAX_STALE_MIN = 15


def _ensure_aware(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def check_market_intel(
    *,
    db_path: str | None = None,
    max_stale_min: int = DEFAULT_MAX_STALE_MIN,
) -> dict[str, Any]:
    """Return a structured healthcheck verdict.

    Keys: status (ok|degraded|down), exit_code, reason, last_heartbeat,
    last_cycle_at, error_count, db_path, age_minutes.
    """
    from ai_trading_system.integrations.market_intel_client import (
        get_event_query_service,
        resolve_db_path,
    )

    resolved_path = resolve_db_path(db_path)
    if not Path(resolved_path).exists():
        return {
            "status": "down",
            "exit_code": 2,
            "reason": f"DB file not found at {resolved_path}",
            "db_path": resolved_path,
            "last_heartbeat": None,
            "last_cycle_at": None,
            "error_count": None,
            "age_minutes": None,
        }

    try:
        svc = get_event_query_service(db_path=resolved_path, refresh=True)
        health = svc.scheduler_health()
    except Exception as exc:  # pragma: no cover — defensive
        return {
            "status": "down",
            "exit_code": 2,
            "reason": f"Could not query scheduler_health: {exc}",
            "db_path": resolved_path,
            "last_heartbeat": None,
            "last_cycle_at": None,
            "error_count": None,
            "age_minutes": None,
        }

    last_hb = _ensure_aware(health.get("last_heartbeat"))
    last_cycle = _ensure_aware(health.get("last_cycle_at"))
    error_count = int(health.get("error_count") or 0)

    if last_hb is None:
        return {
            "status": "down",
            "exit_code": 2,
            "reason": "No heartbeat row in scheduler_state — collector has never run",
            "db_path": resolved_path,
            "last_heartbeat": None,
            "last_cycle_at": last_cycle.isoformat() if last_cycle else None,
            "error_count": error_count,
            "age_minutes": None,
        }

    now = datetime.now(timezone.utc)
    age_min = (now - last_hb).total_seconds() / 60.0
    is_stale = age_min > max_stale_min

    if is_stale:
        return {
            "status": "degraded",
            "exit_code": 1,
            "reason": f"Heartbeat is {age_min:.1f}min old (threshold {max_stale_min}min)",
            "db_path": resolved_path,
            "last_heartbeat": last_hb.isoformat(),
            "last_cycle_at": last_cycle.isoformat() if last_cycle else None,
            "error_count": error_count,
            "age_minutes": round(age_min, 2),
        }

    if error_count > 0:
        return {
            "status": "degraded",
            "exit_code": 1,
            "reason": f"Collector reports {error_count} recent error(s)",
            "db_path": resolved_path,
            "last_heartbeat": last_hb.isoformat(),
            "last_cycle_at": last_cycle.isoformat() if last_cycle else None,
            "error_count": error_count,
            "age_minutes": round(age_min, 2),
        }

    return {
        "status": "ok",
        "exit_code": 0,
        "reason": f"Heartbeat {age_min:.1f}min old",
        "db_path": resolved_path,
        "last_heartbeat": last_hb.isoformat(),
        "last_cycle_at": last_cycle.isoformat() if last_cycle else None,
        "error_count": error_count,
        "age_minutes": round(age_min, 2),
    }


def _format_human(verdict: dict[str, Any]) -> str:
    icon = {"ok": "✓", "degraded": "⚠", "down": "✗"}.get(verdict["status"], "?")
    lines = [
        f"{icon} market_intel: {verdict['status'].upper()}",
        f"  reason       : {verdict['reason']}",
        f"  db_path      : {verdict['db_path']}",
    ]
    if verdict.get("last_heartbeat"):
        lines.append(f"  last_heartbeat: {verdict['last_heartbeat']}")
    if verdict.get("last_cycle_at"):
        lines.append(f"  last_cycle_at : {verdict['last_cycle_at']}")
    if verdict.get("age_minutes") is not None:
        lines.append(f"  heartbeat_age : {verdict['age_minutes']:.1f} min")
    if verdict.get("error_count") is not None:
        lines.append(f"  error_count   : {verdict['error_count']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="healthcheck",
        description="Healthcheck for trading-system integrations.",
    )
    sub = parser.add_subparsers(dest="target", required=False)

    mi = sub.add_parser("market-intel", help="Check the market_intel collector.")
    mi.add_argument(
        "--db", default=None,
        help="Path to market_intel.duckdb (default: env or data/market_intel.duckdb)",
    )
    mi.add_argument(
        "--max-stale-min", type=int, default=DEFAULT_MAX_STALE_MIN,
        help=f"Heartbeat staleness threshold in minutes (default: {DEFAULT_MAX_STALE_MIN})",
    )
    mi.add_argument("--json", action="store_true", help="Emit JSON instead of text.")

    args = parser.parse_args(argv)
    if args.target is None:
        parser.print_help()
        return 3

    if args.target == "market-intel":
        verdict = check_market_intel(
            db_path=args.db,
            max_stale_min=args.max_stale_min,
        )
        if args.json:
            print(json.dumps(verdict, indent=2, default=str))
        else:
            print(_format_human(verdict))
        return int(verdict["exit_code"])

    parser.print_help()
    return 3


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
