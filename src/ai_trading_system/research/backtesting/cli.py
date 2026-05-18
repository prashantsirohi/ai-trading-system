"""``python -m ai_trading_system.research.backtesting`` — engine-driven backtest CLI.

Usage::

    python -m ai_trading_system.research.backtesting \\
        --risk-profile balanced_swing \\
        --pipeline-runs-dir data/pipeline_runs \\
        --from 2026-01-01 --to 2026-04-30 \\
        --equity 1000000 \\
        --out data/research/engine_backtests
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

from ai_trading_system.domains.risk import load_profile
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.research.backtesting import (
    EngineBacktestRunner,
    load_ranked_by_date,
    load_research_ranked_by_date,
)
from ai_trading_system.research.sync_operational_data import sync_operational_to_research


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Engine-driven historical backtest")
    p.add_argument("--risk-profile", default="balanced_swing", help="Profile name in config/risk_profiles/")
    p.add_argument(
        "--data-source",
        choices=["pipeline_replay", "research_dynamic"],
        default="pipeline_replay",
    )
    p.add_argument(
        "--pipeline-runs-dir",
        default=str(get_domain_paths().pipeline_runs_dir),
        help="Directory holding pipeline-YYYY-MM-DD-* run folders",
    )
    p.add_argument("--from", dest="from_date", help="ISO date inclusive (e.g. 2026-01-01)")
    p.add_argument("--to", dest="to_date", help="ISO date inclusive (e.g. 2026-04-30)")
    p.add_argument("--equity", type=float, default=1_000_000.0, help="Starting equity")
    p.add_argument(
        "--out",
        default=str(get_domain_paths().root_dir / "research" / "engine_backtests"),
        help="Output directory for trades.csv + summary.json",
    )
    p.add_argument(
        "--strict-profile",
        action="store_true",
        help="Error out if --risk-profile is unknown (default: fall back to balanced_swing)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    profile = load_profile(args.risk_profile, strict=args.strict_profile)
    print(f"[engine-backtest] loaded profile={profile.name}", file=sys.stderr)

    from_date = _parse_date(args.from_date)
    to_date = _parse_date(args.to_date)
    if args.data_source == "research_dynamic":
        sync_result = sync_operational_to_research(project_root=Path.cwd(), apply=True)
        print(f"[engine-backtest] synced research DB: {sync_result}", file=sys.stderr)
        ranked_by_date = load_research_ranked_by_date(
            Path.cwd(),
            from_date=from_date,
            to_date=to_date,
        )
    else:
        ranked_by_date = load_ranked_by_date(
            args.pipeline_runs_dir,
            from_date=from_date,
            to_date=to_date,
        )
    if not ranked_by_date:
        print(
            f"[engine-backtest] no data found for source={args.data_source}",
            file=sys.stderr,
        )
        return 2
    print(f"[engine-backtest] loaded {len(ranked_by_date)} trading days", file=sys.stderr)

    runner = EngineBacktestRunner(risk_config=profile, starting_equity=args.equity)
    result = runner.run(ranked_by_date)

    out_dir = Path(args.out) / profile.name / datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    out_dir.mkdir(parents=True, exist_ok=True)

    trades_df = result.to_trades_df()
    trades_path = out_dir / "trades.csv"
    trades_df.to_csv(trades_path, index=False)

    equity_df = result.to_equity_df()
    equity_path = out_dir / "equity_curve.csv"
    equity_df.to_csv(equity_path, index=False)

    summary = {
        "profile": profile.name,
        "data_source": args.data_source,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "starting_equity": args.equity,
        "ending_equity": float(equity_df["equity"].iloc[-1]) if not equity_df.empty else args.equity,
        "trade_count": int(len(result.trades)),
        "trading_days": int(len(equity_df)),
        "exit_reason_counts": (
            trades_df["exit_reason"].value_counts(dropna=False).to_dict()
            if not trades_df.empty
            else {}
        ),
        "trades_path": str(trades_path),
        "equity_path": str(equity_path),
    }
    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, default=str))

    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
