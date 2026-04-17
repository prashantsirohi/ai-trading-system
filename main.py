"""Legacy entrypoint shim for AI Trading System.

This module is intentionally retained for compatibility with users who still run
`python main.py`, but the script-era pipeline implementation has been removed.
"""

from __future__ import annotations

import sys


def main() -> int:
    message = (
        "main.py is deprecated and no longer a runnable pipeline entrypoint.\n"
        "Use `python -m run.orchestrator` for canonical stage runs.\n"
        "Optional wrapper: `python -m run.daily_pipeline`.\n"
    )
    sys.stderr.write(message)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
