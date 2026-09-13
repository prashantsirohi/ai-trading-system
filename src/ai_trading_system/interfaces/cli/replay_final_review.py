"""Replay only final-review artifacts against explicitly copied runtime data."""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
import json
from pathlib import Path

from dotenv import load_dotenv

from ai_trading_system.domains.opportunities.review_projection import (
    build_review_projection,
    REVIEW_FIELDS,
)
from ai_trading_system.domains.opportunities.review_sources import (
    load_review_sources,
    utc,
)
from ai_trading_system.platform.db.paths import get_domain_paths


def replay(
    *,
    copied_data_root: Path,
    run_id: str,
    session: date,
    decision_at: datetime,
    output_dir: Path,
) -> dict:
    root = copied_data_root.resolve()
    live = get_domain_paths(data_domain="operational").root_dir.resolve()
    if root == live or root.is_relative_to(live) or live.is_relative_to(root):
        raise ValueError(
            "Replay requires an isolated copy outside the configured operational root"
        )
    if copied_data_root.is_symlink():
        raise ValueError("Copied root cannot be a symlink")
    for name in ("ohlcv.duckdb", "control_plane.duckdb", "masterdata.db"):
        path = root / name
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"Missing regular copied store: {name}")
    if output_dir.resolve().is_relative_to(live):
        raise ValueError("Replay output cannot be inside the operational root")
    output_dir.mkdir(parents=True, exist_ok=False)
    bundle = load_review_sources(
        data_root=root, run_id=run_id, session=session, decision_at=decision_at
    )
    projection = build_review_projection(bundle)
    fields = REVIEW_FIELDS
    for kind, rows in (
        ("final_review_universe", projection.universe),
        ("final_review_list", projection.ordered),
    ):
        with (output_dir / f"{kind}.csv").open(
            "x", newline="", encoding="utf-8"
        ) as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    summary = {
        **projection.summary,
        "replay_kind": "retrospective_copied_source_review",
        "source_run_id": run_id,
        "live_writes": False,
        "copied_database_writes": False,
    }
    (output_dir / "final_review_summary.json").write_text(
        json.dumps(summary, default=str, indent=2, sort_keys=True)
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--copied-data-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--decision-at",
        type=utc,
        required=True,
        help="Explicit UTC source availability cutoff; never implies a historical data vintage",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    load_dotenv(".env")
    if args.decision_at > datetime.now(timezone.utc):
        parser.error("decision cutoff cannot be in the future")
    result = replay(
        copied_data_root=args.copied_data_root,
        run_id=args.run_id,
        session=args.session,
        decision_at=args.decision_at,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                k: result[k]
                for k in (
                    "status",
                    "universe_rows",
                    "ordered_rows",
                    "decision_content_hash",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
